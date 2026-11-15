// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.application.engine.order;

import com.openexchange.core.application.engine.CoreEngine;
import com.openexchange.core.application.engine.config.CoreLimits;
import com.openexchange.core.determinism.CoreEngineEvent;
import com.openexchange.core.domain.AdminResult;
import com.openexchange.core.domain.ConfigKind;
import com.openexchange.core.domain.CoreRejectReason;
import com.openexchange.core.domain.commands.AmendOrderCommand;
import com.openexchange.core.domain.commands.CreateOrderCommand;
import com.openexchange.core.domain.commands.DepositCommand;
import com.openexchange.core.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import java.util.List;

import static com.openexchange.core.application.engine.order.OrderRig.ONE;
import static com.openexchange.core.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The core limits record (CoreLimitsSet): the request TTL, the per-user request cap and the order-event ring capacity
 * are replicated settings with genesis defaults, not constants. The cap applies at once and evicts nothing; the TTL
 * applies to requests remembered after the record; the version is monotonic. The control of the cap row runs the same
 * check without the record and must fail with {@code fifth create accepted at cap 4}.
 */
public class CoreLimitsRecordTest {

    private static final int T_LIMITS = 77;
    private static final long TTL = CoreLimits.DEFAULT_REQUEST_TTL_MS;

    private static OrderRig funded() {
        final OrderRig r = OrderRig.open();
        r.deposit(1, USD, 1_000_000 * ONE);
        return r;
    }

    private static CoreEngineEvent.AdminAck set(final OrderRig r, final long version, final long ttl, final int cap, final int ring) {
        r.markNow();
        r.engine.applyCoreLimitsSet(770L + version, version, ttl, cap, ring, r.now);
        final CoreEngineEvent.AdminAck ack = r.first(CoreEngineEvent.AdminAck.class);
        assertEquals("the ack names CoreLimitsSet", T_LIMITS, ack.refTemplateId());
        assertEquals(770L + version, ack.correlationId());
        return ack;
    }

    private static CoreRejectReason createOnce(final OrderRig r) {
        final CreateOrderCommand c = r.limit(1, 1, 0, ONE, ONE);
        r.markNow();
        r.create(c);
        r.advance(50);
        for (final CoreEngineEvent e : r.events()) {
            if (e instanceof CoreEngineEvent.OrderReject rej) {
                return rej.reason();
            }
        }
        return CoreRejectReason.NONE;
    }

    private static CoreRejectReason amendOnce(final OrderRig r, final long orderId, final long requestLow) {
        final AmendOrderCommand c = new AmendOrderCommand();
        c.reset();
        c.correlationId = 5_200L;
        c.userId = 1;
        c.orderId = orderId;
        c.requestIdHigh = 0xA3E7DL;
        c.requestIdLow = requestLow;
        c.newPrice = 2 * ONE;
        r.markNow();
        r.orders.applyAmend(c, r.now);
        r.advance(50);
        for (final CoreEngineEvent e : r.events()) {
            if (e instanceof CoreEngineEvent.OrderReject rej) {
                return rej.reason();
            }
        }
        return CoreRejectReason.NONE;
    }

    @Test
    public void genesisIsTheDefaultRecordAtVersionZero() {
        final CoreLimits l = new CoreEngine().limits();
        assertEquals(0L, l.version());
        assertEquals(900_000L, l.requestTtlMs());
        assertEquals(256, l.requestPerUserCap());
        assertEquals(65_536, l.orderEventRingCapacity());
        assertEquals(65_536, new CoreEngine().orders().eventRing().capacity());
    }

    /** The cap row: with the record at cap 4 the fifth create and the fifth amend are refused. */
    private static void capFourRefusesTheFifth(final boolean applyTheRecord) {
        final OrderRig r = funded();
        if (applyTheRecord) {
            assertEquals(AdminResult.ACCEPTED, set(r, 1, TTL, 4, 1_024).result());
        }
        for (int i = 1; i <= 4; i++) {
            assertEquals("create " + i + " is admitted", CoreRejectReason.NONE, createOnce(r));
        }
        final CoreRejectReason fifth = createOnce(r);
        if (fifth == CoreRejectReason.NONE) {
            fail("fifth create accepted at cap 4");
        }
        assertEquals(CoreRejectReason.TOO_MANY_PENDING_REQUESTS, fifth);
        assertEquals("the refusal is not remembered", 4, r.orders.requests().countFor(1));
        assertEquals("fifth amend refused at cap 4", CoreRejectReason.TOO_MANY_PENDING_REQUESTS, amendOnce(r, 1, 9_001));
        assertEquals(4, r.orders.orders().size());
    }

    @Test
    public void capFourRefusesTheFifthCreateAndTheFifthAmend() {
        capFourRefusesTheFifth(true);
    }

    @Test
    public void controlWithoutTheRecordTheFifthCreateIsAccepted() {
        try {
            capFourRefusesTheFifth(false);
        } catch (final AssertionError expected) {
            assertEquals("fifth create accepted at cap 4", expected.getMessage());
            return;
        }
        fail("the control did not fail: the cap is not read from the record");
    }

    @Test
    public void aRaisedCapAdmitsAndALoweredCapEvictsNothing() {
        final OrderRig r = funded();
        set(r, 1, TTL, 4, 1_024);
        for (int i = 0; i < 4; i++) {
            createOnce(r);
        }
        assertEquals(CoreRejectReason.TOO_MANY_PENDING_REQUESTS, createOnce(r));
        assertEquals(AdminResult.ACCEPTED, set(r, 2, TTL, 6, 1_024).result());
        assertEquals("raised: the fifth is admitted", CoreRejectReason.NONE, createOnce(r));
        // the amend passes the cap check now (the order still waits for its create, which is what refuses it)
        assertEquals("and an amend passes the cap", CoreRejectReason.NOT_OPEN, amendOnce(r, 1, 9_002));
        assertEquals(CoreRejectReason.NONE, createOnce(r));
        assertEquals(CoreRejectReason.TOO_MANY_PENDING_REQUESTS, createOnce(r));
        assertEquals(6, r.orders.requests().countFor(1));
        assertEquals(AdminResult.ACCEPTED, set(r, 3, TTL, 2, 1_024).result());
        assertEquals("a lower cap applies at once and evicts nothing", 6, r.orders.requests().countFor(1));
        assertEquals(CoreRejectReason.TOO_MANY_PENDING_REQUESTS, createOnce(r));
        // the stored decisions still answer under the lower cap (a replay is not an admission)
        r.advance(TTL);
        r.sweep();
        assertEquals("the TTL frees the user", 0, r.orders.requests().countFor(1));
        assertEquals(CoreRejectReason.NONE, createOnce(r));
    }

    @Test
    public void aTtlChangeAppliesToNewEntriesOnly() {
        final OrderRig r = funded();
        final CreateOrderCommand before = r.limit(1, 1, 0, ONE, ONE);
        r.create(before);
        final long beforeExpiry = r.orders.requests().get(1, before.requestIdHigh, before.requestIdLow).expiresAtMs;
        assertEquals(r.now + TTL, beforeExpiry);
        r.advance(100);
        assertEquals(AdminResult.ACCEPTED, set(r, 1, 1_000L, 256, 1_024).result());
        assertEquals("an entry recorded before the record keeps its deadline", beforeExpiry,
                r.orders.requests().get(1, before.requestIdHigh, before.requestIdLow).expiresAtMs);
        final CreateOrderCommand after = r.limit(1, 1, 0, ONE, ONE);
        r.create(after);
        assertEquals(r.now + 1_000L, r.orders.requests().get(1, after.requestIdHigh, after.requestIdLow).expiresAtMs);
        // a money command is remembered with the same TTL and no cap check
        final DepositCommand d = new DepositCommand();
        d.reset();
        d.setUserId(1);
        d.setAssetId(USD);
        d.setAmount(ONE);
        d.setRequestIdHigh(9);
        d.setRequestIdLow(9);
        r.engine.applyCommand(CoreEngine.CMD_DEPOSIT, d, r.now);
        assertEquals(r.now + 1_000L, r.orders.requests().get(1, 9, 9).expiresAtMs);
        r.advance(2_000);
        r.sweep();
        assertNull("the short TTL fired", r.orders.requests().get(1, after.requestIdHigh, after.requestIdLow));
        assertNull(r.orders.requests().get(1, 9, 9));
        assertNotNull("the old entry lives to its own deadline", r.orders.requests().get(1, before.requestIdHigh, before.requestIdLow));
        // inside the old entry's window the retry is still the stored decision; after the short window the new id is new
        r.markNow();
        r.create(before);
        assertTrue(r.only(CoreEngineEvent.OrderAck.class).duplicate());
    }

    @Test
    public void theVersionIsMonotonicAndInvalidRecordsChangeNothing() {
        final OrderRig r = funded();
        final CoreEngineEvent.AdminAck first = set(r, 5, 60_000L, 10, 100);
        assertEquals(AdminResult.ACCEPTED, first.result());
        assertEquals(5L, first.configVersion());
        assertEquals("ConfigApplied{LIMITS, market 0, version 5}", new CoreEngineEvent.ConfigApplied(ConfigKind.LIMITS, 0, 5L),
                r.first(CoreEngineEvent.ConfigApplied.class));
        r.markNow();
        r.engine.applyCoreLimitsSet(1L, 5, 60_000L, 10, 100, r.now);
        assertEquals("equal version, equal values", List.of(new CoreEngineEvent.AdminAck(1L, T_LIMITS, AdminResult.DUPLICATE, 0, 5L)),
                r.events());
        r.markNow();
        r.engine.applyCoreLimitsSet(2L, 5, 60_000L, 11, 100, r.now);
        assertEquals("equal version, other values", List.of(new CoreEngineEvent.AdminAck(2L, T_LIMITS, AdminResult.STALE, 0, 5L)),
                r.events());
        r.markNow();
        r.engine.applyCoreLimitsSet(3L, 4, 1L, 1, 0, r.now);
        assertEquals("a lower version", List.of(new CoreEngineEvent.AdminAck(3L, T_LIMITS, AdminResult.STALE, 0, 5L)), r.events());
        final long[][] invalid = {{0, 10, 100}, {-1, 10, 100}, {60_000, 0, 100}, {60_000, -5, 100}, {60_000, 10, -1},
            {60_000, 10, CoreLimits.MAX_ORDER_EVENT_RING_CAPACITY + 1L}, {60_000, 10, Integer.MAX_VALUE}};
        for (final long[] v : invalid) {
            r.markNow();
            r.engine.applyCoreLimitsSet(4L, 6, v[0], (int) v[1], (int) v[2], r.now);
            assertEquals("invalid " + java.util.Arrays.toString(v), List.of(new CoreEngineEvent.AdminAck(4L, T_LIMITS,
                    AdminResult.REJECTED, CoreRejectReason.INVALID.code(), 5L)), r.events());
        }
        final CoreLimits l = r.engine.limits();
        assertEquals(5L, l.version());
        assertEquals(60_000L, l.requestTtlMs());
        assertEquals(10, l.requestPerUserCap());
        assertEquals(100, l.orderEventRingCapacity());
        assertEquals(100, r.orders.eventRing().capacity());
    }

    @Test
    public void theLargestRingIsWhatOneSnapshotSectionCarriesAndAHugeTtlNeverFires() {
        assertEquals("the bound is the image format's: a 32-bit section length, a 16-byte head, 167 bytes per event",
                (Integer.MAX_VALUE - BalanceSnapshotCodec.ORDER_EVENT_RING_HEAD_BYTES) / OrderEventRing.RECORD_BYTES,
                CoreLimits.MAX_ORDER_EVENT_RING_CAPACITY);
        final OrderRig r = funded();
        // the largest ring is accepted and costs nothing until events arrive; the longest TTL saturates instead of wrapping
        assertEquals(AdminResult.ACCEPTED, set(r, 1, Long.MAX_VALUE, Integer.MAX_VALUE, CoreLimits.MAX_ORDER_EVENT_RING_CAPACITY).result());
        final CreateOrderCommand c = r.limit(1, 1, 0, ONE, ONE);
        r.create(c);
        assertEquals("a TTL beyond the clock is never, not now", Long.MAX_VALUE,
                r.orders.requests().get(1, c.requestIdHigh, c.requestIdLow).expiresAtMs);
        r.advance(10L * 365 * 24 * 3_600_000);
        r.sweep();
        assertNotNull("still remembered ten years on", r.orders.requests().get(1, c.requestIdHigh, c.requestIdLow));
        assertEquals(1, r.orders.eventRing().count());
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final int length = BalanceSnapshotCodec.serialize(r.engine, buf, 0);
        final CoreEngine restored = new CoreEngine();
        BalanceSnapshotCodec.deserialize(buf, 0, length, restored);
        assertEquals(CoreLimits.MAX_ORDER_EVENT_RING_CAPACITY, restored.orders().eventRing().capacity());
        assertEquals(1, restored.orders().eventRing().count());
    }

    @Test
    public void theRecordAndItsDecisionsSurviveASnapshot() {
        final OrderRig r = funded();
        set(r, 3, 5_000L, 4, 16);
        for (int i = 0; i < 4; i++) {
            createOnce(r);
        }
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final int length = BalanceSnapshotCodec.serialize(r.engine, buf, 0);
        final CoreEngine restored = new CoreEngine();
        BalanceSnapshotCodec.deserialize(buf, 0, length, restored);
        restored.verifyRestoredInvariants();
        assertEquals(3L, restored.limits().version());
        assertEquals(5_000L, restored.limits().requestTtlMs());
        assertEquals(4, restored.limits().requestPerUserCap());
        assertEquals(16, restored.limits().orderEventRingCapacity());
        assertEquals(16, restored.orders().eventRing().capacity());
        assertEquals(4, restored.orders().eventRing().count());
        final ExpandableArrayBuffer again = new ExpandableArrayBuffer();
        assertEquals("the image is a function of state", length, BalanceSnapshotCodec.serialize(restored, again, 0));
        for (int i = 0; i < length; i++) {
            assertEquals("image byte " + i, buf.getByte(i), again.getByte(i));
        }
        // the same next command gets the same decision on the restored engine and on the original
        final CreateOrderCommand fifth = r.limit(1, 1, 0, ONE, ONE);
        final com.openexchange.core.determinism.RecordingCoreSink sink = new com.openexchange.core.determinism.RecordingCoreSink();
        restored.setEventSink(sink);
        restored.orders().applyCreate(fifth, r.now);
        r.markNow();
        r.create(fifth);
        assertEquals(CoreRejectReason.TOO_MANY_PENDING_REQUESTS, r.reject().reason());
        assertEquals(r.events(), sink.events());
    }
}
