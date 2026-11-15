// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.publisher;

import com.openexchange.core.application.engine.CoreEngine;
import com.openexchange.core.application.engine.feed.V3Rig;
import com.openexchange.core.domain.GateReason;
import com.openexchange.core.domain.GateState;
import com.openexchange.core.infrastructure.generated.BoolFlag;
import com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder;
import com.openexchange.core.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.core.infrastructure.generated.OrderEventDecoder;
import com.openexchange.core.infrastructure.generated.OrderEventsEndDecoder;
import com.openexchange.core.infrastructure.generated.OrderEventsPageDecoder;
import com.openexchange.core.infrastructure.generated.UserBalancesDecoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The wire form of the gateway query replies as the production publisher writes them: OrderEventsPage 115 (at most 44
 * entries per frame, each entry the broadcast OrderEvent body byte for byte), OrderEventsEnd 116, UserBalances 117 and
 * CoreConfigReport 118. Every frame is request-scoped (ORIGIN), never a broadcast.
 */
public class GatewayQueryWireTest {

    private record Frame(int templateId, byte[] bytes, int channel, CoreEventPublisher.Routing routing) { }

    private static final class Capture implements CoreEventPublisher.Egress {
        final List<Frame> frames = new ArrayList<>();

        @Override
        public void emit(final org.agrona.MutableDirectBuffer buffer, final int offset, final int length, final int channel,
                         final CoreEventPublisher.Routing routing) {
            final byte[] b = new byte[length];
            buffer.getBytes(offset, b);
            frames.add(new Frame(new MessageHeaderDecoder().wrap(buffer, offset).templateId(), b, channel, routing));
        }

        List<Frame> of(final int templateId) {
            return frames.stream().filter(f -> f.templateId == templateId).toList();
        }
    }

    /** A behaviour 4 core that publishes through the production publisher, with {@code orders} accepted creates. */
    private static V3Rig publishing(final Capture capture, final int orders) {
        final V3Rig r = V3Rig.openAt(4);
        r.engine.setEventSink(new CoreEventPublisher(capture, () -> false));
        r.deposit(1, V3Rig.USD, 10_000_000 * V3Rig.ONE);
        for (int i = 0; i < orders; i++) {
            r.engine.orders().applyCreate(r.limit(1, 0, V3Rig.ONE, V3Rig.ONE), r.now);
            r.now += 25;
        }
        assertEquals("precondition: one event per create", orders, r.engine.getEventSeq());
        return r;
    }

    @Test
    public void aPageIsFramedAt44AndEachEntryIsTheBroadcastBody() {
        final Capture capture = new Capture();
        final V3Rig r = publishing(capture, 100);
        final List<Frame> live = capture.of(OrderEventDecoder.TEMPLATE_ID);
        assertEquals(100, live.size());
        capture.frames.clear();
        r.engine.orders().applyRequestOrderEvents(9_001L, 0, 1_000);

        final List<Frame> pages = capture.of(OrderEventsPageDecoder.TEMPLATE_ID);
        assertEquals("100 events in frames of at most 44", 3, pages.size());
        assertEquals("the reply is the pages then the terminator", pages.size() + 1, capture.frames.size());
        final List<byte[]> replayed = new ArrayList<>();
        final int[] sizes = new int[pages.size()];
        for (int p = 0; p < pages.size(); p++) {
            final Frame f = pages.get(p);
            assertEquals(CoreEventPublisher.Routing.ORIGIN, f.routing);
            final UnsafeBuffer b = new UnsafeBuffer(f.bytes);
            final OrderEventsPageDecoder d = new OrderEventsPageDecoder().wrapAndApplyHeader(b, 0, new MessageHeaderDecoder());
            assertEquals(9_001L, d.correlationId());
            final OrderEventsPageDecoder.EventsDecoder g = d.events();
            assertEquals("an entry is the OrderEvent block", OrderEventDecoder.BLOCK_LENGTH, OrderEventsPageDecoder.EventsDecoder.sbeBlockLength());
            sizes[p] = g.count();
            int at = MessageHeaderDecoder.ENCODED_LENGTH + OrderEventsPageDecoder.BLOCK_LENGTH + 4;
            long expectedSeq = replayed.size() + 1;
            while (g.hasNext()) {
                g.next();
                assertEquals("ascending and dense", expectedSeq++, g.eventSeq());
                final byte[] entry = new byte[OrderEventDecoder.BLOCK_LENGTH];
                b.getBytes(at, entry);
                replayed.add(entry);
                at += OrderEventDecoder.BLOCK_LENGTH;
            }
            assertEquals("the frame is exactly its entries", at, f.bytes.length);
            assertTrue("one frame stays under an 8 KiB MTU", f.bytes.length <= 8_192 - 64);
        }
        assertArrayEquals(new int[] {44, 44, 12}, sizes);
        for (int i = 0; i < 100; i++) {
            final byte[] body = new byte[OrderEventDecoder.BLOCK_LENGTH];
            System.arraycopy(live.get(i).bytes, MessageHeaderDecoder.ENCODED_LENGTH, body, 0, body.length);
            assertArrayEquals("event " + (i + 1) + ": the replayed entry is the broadcast body", body, replayed.get(i));
        }

        final Frame endFrame = capture.frames.get(capture.frames.size() - 1);
        assertEquals(OrderEventsEndDecoder.TEMPLATE_ID, endFrame.templateId);
        assertEquals(CoreEventPublisher.Routing.ORIGIN, endFrame.routing);
        final OrderEventsEndDecoder end = new OrderEventsEndDecoder().wrapAndApplyHeader(new UnsafeBuffer(endFrame.bytes), 0,
                new MessageHeaderDecoder());
        assertEquals(9_001L, end.correlationId());
        assertEquals(100, end.count());
        assertEquals(1L, end.firstAvailableSeq());
        assertEquals(100L, end.lastSeq());
        assertEquals(100L, end.nextFromSeq());
    }

    @Test
    public void theLargestReplyStaysFarBelowASessionsEgressCeiling() {
        final Capture capture = new Capture();
        final V3Rig r = publishing(capture, 0);
        // the cap times the largest frame share, without building 4096 events: 94 frames of 7 368 bytes at most
        final int frames = (com.openexchange.core.application.engine.order.OrderEngine.ORDER_EVENTS_CAP
                + CoreEventPublisher.ORDER_EVENTS_FRAME_CAP - 1) / CoreEventPublisher.ORDER_EVENTS_FRAME_CAP;
        final long bytes = (long) com.openexchange.core.application.engine.order.OrderEngine.ORDER_EVENTS_CAP * OrderEventDecoder.BLOCK_LENGTH
                + (long) frames * (MessageHeaderDecoder.ENCODED_LENGTH + OrderEventsPageDecoder.BLOCK_LENGTH + 4);
        assertEquals(94, frames);
        assertTrue("one reply is " + bytes + " bytes", bytes < 1024 * 1024);
        assertEquals(0, r.engine.getEventSeq());
    }

    @Test
    public void userBalancesCarriesKnownAndTheNonZeroLines() {
        final Capture capture = new Capture();
        final V3Rig r = publishing(capture, 2);
        r.deposit(1, V3Rig.BTC, 5 * V3Rig.ONE);
        capture.frames.clear();
        r.engine.setLogPosition(4_096L);
        r.engine.requestUserBalances(9_002L, 1L);
        r.engine.requestUserBalances(9_003L, 404L);
        assertEquals(2, capture.frames.size());
        final Frame known = capture.frames.get(0);
        assertEquals(UserBalancesDecoder.TEMPLATE_ID, known.templateId);
        assertEquals(CoreEventPublisher.Routing.ORIGIN, known.routing);
        final UserBalancesDecoder d = new UserBalancesDecoder().wrapAndApplyHeader(new UnsafeBuffer(known.bytes), 0, new MessageHeaderDecoder());
        assertEquals(9_002L, d.correlationId());
        assertEquals(1L, d.userId());
        assertEquals(BoolFlag.TRUE, d.known());
        assertEquals(4_096L, d.asOfLogPosition());
        final UserBalancesDecoder.BalancesDecoder g = d.balances();
        assertEquals(2, g.count());
        g.next();
        assertEquals(V3Rig.USD, g.assetId());
        assertEquals(r.engine.account(1).available(V3Rig.USD), g.available());
        assertEquals(2 * V3Rig.ONE, g.locked());
        g.next();
        assertEquals(V3Rig.BTC, g.assetId());
        assertEquals(5 * V3Rig.ONE, g.available());
        assertEquals(0L, g.locked());
        final UserBalancesDecoder none = new UserBalancesDecoder().wrapAndApplyHeader(new UnsafeBuffer(capture.frames.get(1).bytes), 0,
                new MessageHeaderDecoder());
        assertEquals(404L, none.userId());
        assertEquals(BoolFlag.FALSE, none.known());
        assertEquals(0, none.balances().count());
    }

    @Test
    public void theConfigReportCarriesTheRecordTheLimitsAndEveryMarketsGate() {
        final Capture capture = new Capture();
        final V3Rig r = publishing(capture, 0);
        r.engine.applyCoreLimitsSet(1L, 7L, 30_000L, 12, 512, r.now);
        r.adminGate(2, GateState.SUSPENDED, GateReason.ADMIN);
        r.engine.orders().applyCircuitBreaker(2L, 5, true, r.now);
        capture.frames.clear();
        r.engine.setLogPosition(8_192L);
        r.engine.requestCoreConfig(9_004L);
        assertEquals(1, capture.frames.size());
        final Frame f = capture.frames.get(0);
        assertEquals(CoreConfigReportDecoder.TEMPLATE_ID, f.templateId);
        assertEquals(CoreEventPublisher.Routing.ORIGIN, f.routing);
        final CoreConfigReportDecoder d = new CoreConfigReportDecoder().wrapAndApplyHeader(new UnsafeBuffer(f.bytes), 0, new MessageHeaderDecoder());
        assertEquals(9_004L, d.correlationId());
        assertEquals(4L, d.configVersion());
        assertEquals(4L, d.behaviorVersion());
        assertEquals(7L, d.limitsVersion());
        assertEquals(30_000L, d.requestTtlMs());
        assertEquals(12, d.requestPerUserCap());
        assertEquals(512, d.orderEventRingCapacity());
        assertEquals(com.openexchange.core.infrastructure.generated.GateState.OPEN, d.gateState());
        assertTrue(d.gateReasons().isEmpty());
        assertEquals(8_192L, d.asOfLogPosition());
        final CoreEngine e = r.engine;
        final var recorded = e.coreConfig().recorded();
        final CoreConfigReportDecoder.MarketsDecoder m = d.markets();
        assertEquals(recorded.markets.length, m.count());
        int i = 0;
        while (m.hasNext()) {
            m.next();
            final var row = recorded.markets[i++];
            assertEquals(row.marketId(), m.marketId());
            assertEquals(row.symbol(), m.symbol());
            assertEquals(row.baseAssetId(), m.baseAssetId());
            assertEquals(row.quoteAssetId(), m.quoteAssetId());
            assertEquals(row.tickSize(), m.tickSize());
            assertEquals(row.minPrice(), m.minPrice());
            assertEquals(row.maxPrice(), m.maxPrice());
            assertEquals(row.lotSize(), m.lotSize());
            final var risk = e.orders().market(row.marketId());
            assertEquals(risk.minQuantity, m.minQuantity());
            assertEquals(risk.maxQuantity, m.maxQuantity());
            assertEquals(risk.minNotional, m.minNotional());
            assertEquals(risk.maxNotional, m.maxNotional());
            assertEquals(risk.configVersion, m.riskConfigVersion());
            final boolean suspended = row.marketId() == 2;
            assertEquals(suspended ? com.openexchange.core.infrastructure.generated.GateState.SUSPENDED
                    : com.openexchange.core.infrastructure.generated.GateState.OPEN, m.gateState());
            assertEquals("market " + row.marketId() + " admin bit", suspended, m.gateReasons().admin());
            assertFalse(m.gateReasons().bridgeHalt());
            assertEquals(row.marketId() == 5 ? BoolFlag.TRUE : BoolFlag.FALSE, m.circuitBreaker());
        }
        final CoreConfigReportDecoder.AssetsDecoder a = d.assets();
        assertEquals(recorded.assets.length, a.count());
        i = 0;
        while (a.hasNext()) {
            a.next();
            assertEquals(recorded.assets[i].assetId(), a.assetId());
            assertEquals(recorded.assets[i].code(), a.code());
            assertEquals(recorded.assets[i].scale(), a.scale());
            i++;
        }
        assertEquals("the frame is exactly what was encoded", MessageHeaderDecoder.ENCODED_LENGTH + d.encodedLength(), f.bytes.length);
    }
}
