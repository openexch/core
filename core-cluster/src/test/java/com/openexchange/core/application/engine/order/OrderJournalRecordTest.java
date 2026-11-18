// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.application.engine.order;

import com.openexchange.core.application.engine.CoreEngine;
import com.openexchange.core.application.engine.feed.V3Rig;
import com.openexchange.core.determinism.V3Genesis;
import com.openexchange.core.domain.CoreEventSink;
import com.openexchange.core.domain.EventKind;
import com.openexchange.core.domain.OrderJournalSink;
import com.openexchange.core.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.core.infrastructure.generated.OrderEventDecoder;
import com.openexchange.core.infrastructure.journal.OrderJournalCapture;
import com.openexchange.core.infrastructure.orderjournal.generated.OrderJournalEventDecoder;
import com.openexchange.core.infrastructure.publisher.CoreEventPublisher;
import com.openexchange.core.infrastructure.publisher.OrderEventRecordCodec;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static com.openexchange.core.application.engine.feed.V3Rig.EPOCH;
import static com.openexchange.core.application.engine.feed.V3Rig.ONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The order journal record, engine level: one record per order event through the production service-thread half
 * ({@code OrderJournal}), checked against the broadcast body the production publisher writes and against the replay
 * ring; the ME cause of every record (liquidity by order id, the ME record's epoch, sequence, position and time, and
 * zeros outside an ME apply); the hold state carried by the records; and the epoch of every record.
 */
public class OrderJournalRecordTest {

    /** Offset of the block inside a record (the SBE header). */
    private static final int H = 8;

    // ------------------------------------------------------------------------------------------ the checks

    /** The record against the broadcast body and the ring record of the same event. */
    static void assertSameEvent(final byte[] record, final byte[] broadcastBody, final byte[] ringBody) {
        final UnsafeBuffer rec = new UnsafeBuffer(record);
        final long seq = rec.getLong(H);
        for (final byte[] body : new byte[][] {broadcastBody, ringBody}) {
            if (body == null) {
                continue;
            }
            final UnsafeBuffer b = new UnsafeBuffer(body);
            final byte[] fromRecord = Arrays.copyOfRange(record, H + 24, H + 24 + OrderEventRecordCodec.RECORD_BYTES - 16);
            final byte[] fromBody = Arrays.copyOfRange(body, 16, OrderEventRecordCodec.RECORD_BYTES);
            if (b.getLong(0) != seq || b.getLong(8) != rec.getLong(H + 16) || !Arrays.equals(fromRecord, fromBody)) {
                throw new AssertionError("journal record differs from the broadcast event: journalSeq=" + seq
                        + " eventSeq=" + b.getLong(0) + " logPosition " + rec.getLong(H + 16) + "/" + b.getLong(8));
            }
        }
    }

    /**
     * The ME cause of one trade's records: TAKER on the taker's FILL, MAKER on the maker's, both naming the trade's ME
     * record, and meEpoch equal to the record's epoch.
     */
    static void assertTradeCause(final List<byte[]> fills, final long takerOrder, final long makerOrder, final long meEpoch,
                                 final long meJournalSeq, final long meLogPosition, final long meTimestampMs) {
        assertEquals("two FILL records for one trade", 2, fills.size());
        for (final byte[] f : fills) {
            final OrderJournalEventDecoder d = OrderJournalCapture.decode(f);
            if (d.liquidity() == OrderJournalSink.LIQUIDITY_NONE) {
                throw new AssertionError("fill without liquidity: order " + d.orderId() + " journalSeq " + d.journalSeq());
            }
            final long expected = d.liquidity() == OrderJournalSink.LIQUIDITY_TAKER ? takerOrder : makerOrder;
            if (d.orderId() != expected) {
                throw new AssertionError("maker and taker swapped: order " + d.orderId() + " carries liquidity "
                        + d.liquidity() + " (taker " + takerOrder + ", maker " + makerOrder + ")");
            }
            if (d.meEpoch() != meEpoch || d.meJournalSeq() != meJournalSeq || d.meLogPosition() != meLogPosition
                    || d.meTimestampMs() != meTimestampMs) {
                throw new AssertionError("fills of one batch share one ME cause or a cause differs from its entry: order "
                        + d.orderId() + " meJournalSeq " + d.meJournalSeq() + " (expected " + meJournalSeq + ")");
            }
            assertEquals("meEpoch equals the record's epoch", d.epoch(), d.meEpoch());
        }
    }

    /** A record emitted outside an ME apply carries no ME cause. */
    static void assertNoCause(final byte[] record) {
        final OrderJournalEventDecoder d = OrderJournalCapture.decode(record);
        if (d.liquidity() != 0 || d.meEpoch() != 0 || d.meJournalSeq() != 0 || d.meLogPosition() != 0
                || d.meTimestampMs() != 0) {
            throw new AssertionError("ME cause on a record with no ME cause: journalSeq " + d.journalSeq() + " kind "
                    + d.eventKind() + " meJournalSeq " + d.meJournalSeq() + " liquidity " + d.liquidity());
        }
    }

    /** Every record carries the recorded genesis. */
    static void assertEpoch(final List<byte[]> records, final long genesisId) {
        for (final byte[] r : records) {
            final OrderJournalEventDecoder d = OrderJournalCapture.decode(r);
            if (d.epoch() == 0) {
                throw new AssertionError("order journal record without a genesis: journalSeq " + d.journalSeq());
            }
            assertEquals("epoch of journalSeq " + d.journalSeq(), genesisId, d.epoch());
        }
    }

    // ------------------------------------------------------------------------------------------ the broadcast tee

    /** The broadcast bodies the production publisher wrote, by eventSeq. */
    private static final class Broadcast implements CoreEventPublisher.Egress {
        final Map<Long, byte[]> bodies = new HashMap<>();

        @Override
        public void emit(final org.agrona.MutableDirectBuffer buffer, final int offset, final int length, final int channel,
                         final CoreEventPublisher.Routing routing) {
            final MessageHeaderDecoder h = new MessageHeaderDecoder().wrap(buffer, offset);
            if (h.templateId() == OrderEventDecoder.TEMPLATE_ID) {
                final byte[] body = new byte[OrderEventRecordCodec.RECORD_BYTES];
                buffer.getBytes(offset + MessageHeaderDecoder.ENCODED_LENGTH, body);
                bodies.put(new UnsafeBuffer(body).getLong(0), body);
            }
        }
    }

    /** A sink that forwards every call to the test's recording sink and to the production publisher. */
    private static CoreEventSink tee(final CoreEventSink recording, final CoreEventSink publisher) {
        return (CoreEventSink) Proxy.newProxyInstance(CoreEventSink.class.getClassLoader(), new Class<?>[] {CoreEventSink.class},
                (proxy, method, args) -> {
                    try {
                        method.invoke(publisher, args);
                        return method.invoke(recording, args);
                    } catch (final InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static int compareAll(final CoreEngine engine, final List<byte[]> records, final Broadcast broadcast,
                                  final Set<EventKind> kinds) {
        final OrderEventRing ring = engine.orders().eventRing();
        final long oldest = engine.getEventSeq() - ring.count() + 1;
        long expectedSeq = -1;
        for (final byte[] r : records) {
            final OrderJournalEventDecoder d = OrderJournalCapture.decode(r);
            if (expectedSeq > 0) {
                assertEquals("journalSeq is dense", expectedSeq, d.journalSeq());
            }
            expectedSeq = d.journalSeq() + 1;
            byte[] ringBody = null;
            if (d.journalSeq() >= oldest) {
                ringBody = new byte[OrderEventRecordCodec.RECORD_BYTES];
                ring.copyRecord(d.journalSeq(), new UnsafeBuffer(ringBody), 0);
            }
            final byte[] body = broadcast.bodies.get(d.journalSeq());
            assertTrue("a broadcast for journalSeq " + d.journalSeq(), body != null);
            assertSameEvent(r, body, ringBody);
            kinds.add(EventKind.values()[d.eventKind()]);
        }
        assertEquals("one record per order event", engine.getEventSeq(), expectedSeq - 1);
        return records.size();
    }

    // ------------------------------------------------------------------------------------------ record vs broadcast

    /**
     * Random orders (the three fuzz drivers: behaviour 1, behaviour 3 with faults, synthetics): every record's body
     * bytes equal the broadcast body's bytes 16..166 and the ring record's, journalSeq == eventSeq, logPosition equal.
     */
    @Test
    public void journalRecordEqualsTheBroadcastBodyAndTheRingRecord() {
        final Set<EventKind> kinds = EnumSet.noneOf(EventKind.class);
        int compared = 0;
        for (long seed = 1; seed <= 3; seed++) {
            final OrderFuzzDriver d = new OrderFuzzDriver(seed);
            compared += runTee(d.r.engine, d.r.sink, () -> d.run(300), kinds);
        }
        for (long seed = 101; seed <= 103; seed++) {
            final V3CoreFuzzDriver d = new V3CoreFuzzDriver(seed, true);
            compared += runTee(d.r.engine, d.r.sink, () -> d.run(400), kinds);
        }
        for (long seed = 11; seed <= 13; seed++) {
            final SyntheticFuzzDriver d = new SyntheticFuzzDriver(seed);
            compared += runTee(d.r.engine, d.r.sink, () -> d.run(300), kinds);
        }
        final Set<EventKind> missing = EnumSet.complementOf(EnumSet.copyOf(kinds));
        System.out.println("[order-journal] records compared=" + compared + " kinds covered=" + kinds + " not covered="
                + missing);
        assertTrue("records compared", compared > 1_000);
        // HOLD_CHANGED is declared and never emitted; the others must all occur
        for (final EventKind k : EnumSet.of(EventKind.ACCEPTED, EventKind.DISPATCHED, EventKind.ACK_NEW, EventKind.FILL,
                EventKind.CANCELLED, EventKind.REJECTED, EventKind.REPLACED, EventKind.TRIGGERED, EventKind.SLICE_SUBMITTED)) {
            assertTrue("event kind " + k + " covered: " + kinds, kinds.contains(k));
        }
    }

    private static int runTee(final CoreEngine engine, final CoreEventSink recording, final Runnable run,
                              final Set<EventKind> kinds) {
        final Broadcast broadcast = new Broadcast();
        engine.setEventSink(tee(recording, new CoreEventPublisher(broadcast, () -> false)));
        final OrderJournalCapture capture = new OrderJournalCapture();
        engine.setOrderJournalSink(capture.journal);
        run.run();
        return compareAll(engine, capture.records(), broadcast, kinds);
    }

    /** Control: a second encoder with holdAmount and holdRemaining swapped is caught by the comparison. */
    @Test
    public void controlSwappedHoldFieldsAreCaught() {
        final V3CoreFuzzDriver d = new V3CoreFuzzDriver(101, false);
        final Broadcast broadcast = new Broadcast();
        d.r.engine.setEventSink(tee(d.r.sink, new CoreEventPublisher(broadcast, () -> false)));
        final OrderJournalCapture capture = new OrderJournalCapture();
        d.r.engine.setOrderJournalSink(capture.journal);
        d.run(200);
        for (final byte[] r : capture.records()) {
            final OrderJournalEventDecoder dec = OrderJournalCapture.decode(r);
            if (dec.holdAmount() == dec.holdRemaining()) {
                continue;
            }
            final byte[] swapped = r.clone();
            final UnsafeBuffer s = new UnsafeBuffer(swapped);
            s.putLong(H + OrderJournalEventDecoder.holdAmountEncodingOffset(), dec.holdRemaining());
            s.putLong(H + OrderJournalEventDecoder.holdRemainingEncodingOffset(), dec.holdAmount());
            try {
                assertSameEvent(swapped, broadcast.bodies.get(dec.journalSeq()), null);
            } catch (final AssertionError expected) {
                assertTrue(expected.getMessage(), expected.getMessage().startsWith("journal record differs from the broadcast event"));
                return;
            }
            fail("the swapped record passed the comparison");
        }
        fail("no record with holdAmount != holdRemaining: the control's precondition did not occur");
    }

    // ------------------------------------------------------------------------------------------ the ME cause

    /** The two-party book with the journal captured from the first event. */
    private static V3Rig book(final OrderJournalCapture capture) {
        final V3Rig r = V3Rig.open();
        r.engine.setOrderJournalSink(capture.journal);
        r.twoParty();
        return r;
    }

    private static List<byte[]> since(final OrderJournalCapture c, final int from) {
        final List<byte[]> all = c.records();
        return new ArrayList<>(all.subList(from, all.size()));
    }

    private static List<byte[]> fills(final List<byte[]> records) {
        final List<byte[]> f = new ArrayList<>();
        for (final byte[] r : records) {
            if (OrderJournalCapture.decode(r).eventKind() == EventKind.FILL.code()) {
                f.add(r);
            }
        }
        return f;
    }

    /**
     * A trade between two core orders: the record of takerOmsOrderId is TAKER, of makerOmsOrderId MAKER, both name the
     * trade's ME record; the next record (a create, not ME-caused) carries zeros. The ME outcome records carry the
     * outcome's cause and no liquidity.
     */
    @Test
    public void aTradesTwoFillRecordsNameTakerMakerAndTheirMeRecord() {
        final OrderJournalCapture c = new OrderJournalCapture();
        final V3Rig r = book(c);
        for (final byte[] ack : c.records()) {
            final OrderJournalEventDecoder d = OrderJournalCapture.decode(ack);
            if (d.eventKind() == EventKind.ACK_NEW.code()) {
                assertEquals("an outcome record carries no liquidity", 0, d.liquidity());
                assertEquals("an outcome record names its ME record", EPOCH, d.meEpoch());
                assertTrue("an outcome record names its ME journalSeq", d.meJournalSeq() > 0);
            }
        }
        final int mark = c.records().size();
        final long js = r.nextJseq();
        r.now += 7;
        assertTrue(r.tradeAt(js, EPOCH, 777, 1, 2, 202, 2, 1, 101, 1, 100 * ONE, ONE / 2, false));
        final List<byte[]> trade = since(c, mark);
        assertTradeCause(fills(trade), 2, 1, EPOCH, js, 777, r.now);
        assertEquals("the taker's record has the lower journalSeq (apply order)", 2,
                OrderJournalCapture.decode(fills(trade).get(0)).orderId());

        final int mark2 = c.records().size();
        r.create(r.limit(1, 0, 90 * ONE, ONE));
        final List<byte[]> after = since(c, mark2);
        assertTrue("a record after the trade", !after.isEmpty());
        for (final byte[] rec : after) {
            assertNoCause(rec);
        }
    }

    /** Control: the liquidity left 0 (a test build of the journal) fails with its text. */
    @Test
    public void controlLiquidityLeftZeroIsAFillWithoutLiquidity() {
        final OrderJournalCapture c = new OrderJournalCapture().liquidityRewrite(l -> 0);
        final V3Rig r = book(c);
        final int mark = c.records().size();
        final long js = r.nextJseq();
        assertTrue(r.tradeAt(js, EPOCH, 777, 1, 2, 202, 2, 1, 101, 1, 100 * ONE, ONE / 2, false));
        expectText("fill without liquidity", () -> assertTradeCause(fills(since(c, mark)), 2, 1, EPOCH, js, 777, r.now));
    }

    /** Control: maker applied first with liquidity by position (a test build) fails with its text. */
    @Test
    public void controlLiquidityByPositionWithTheApplyOrderSwappedIsCaught() {
        final OrderJournalCapture c = new OrderJournalCapture();
        final V3Rig r = book(c);
        final int mark = c.records().size();
        final long js = r.nextJseq();
        assertTrue(r.tradeAt(js, EPOCH, 777, 1, 2, 202, 2, 1, 101, 1, 100 * ONE, ONE / 2, false));
        final List<byte[]> f = fills(since(c, mark));
        // the test build: maker's record first, liquidity by position (first TAKER, second MAKER)
        final List<byte[]> swapped = List.of(f.get(1).clone(), f.get(0).clone());
        new UnsafeBuffer(swapped.get(0)).putByte(H + OrderJournalEventDecoder.liquidityEncodingOffset(), (byte) 1);
        new UnsafeBuffer(swapped.get(1)).putByte(H + OrderJournalEventDecoder.liquidityEncodingOffset(), (byte) 2);
        expectText("maker and taker swapped", () -> assertTradeCause(swapped, 2, 1, EPOCH, js, 777, r.now));
    }

    /** Control: a context that is not cleared leaves an ME cause on the next record (a test build) and is caught. */
    @Test
    public void controlUnclearedContextIsAnMeCauseOnARecordWithNoMeCause() {
        final OrderJournalCapture c = new OrderJournalCapture();
        final V3Rig r = book(c);
        final long js = r.nextJseq();
        // the test build: the cause set and never cleared, then a user create
        r.engine.orders().meCause().trade(EPOCH, js, 999, r.now, 2, 1);
        final int mark = c.records().size();
        r.create(r.limit(1, 0, 90 * ONE, ONE));
        r.engine.orders().meCause().clear();
        expectText("ME cause on a record with no ME cause", () -> since(c, mark).forEach(OrderJournalRecordTest::assertNoCause));
    }

    /**
     * One MeJournalTradeBatch of 64 trades through the demuxer: 128 FILL records with one log position; meJournalSeq
     * strictly increasing per trade and equal to its group element's; each pair TAKER then MAKER.
     */
    @Test
    public void aBatchOf64TradesGives128FillsWithOneCausePerElement() {
        final OrderJournalCapture c = new OrderJournalCapture();
        final V3Rig r = book(c);
        final int mark = c.records().size();
        final List<V3Rig.BatchEntry> entries = new ArrayList<>();
        final long first = r.nextJseq();
        for (int i = 0; i < 64; i++) {
            entries.add(new V3Rig.BatchEntry(first + i, EPOCH, 1_000 + i, 1 + i, 0, ONE / 32));
        }
        r.dispatchTrusted(r.tradeBatch(entries));
        final List<byte[]> f = fills(since(c, mark));
        assertEquals("128 FILL records", 128, f.size());
        final long position = OrderJournalCapture.decode(f.get(0)).logPosition();
        for (int i = 0; i < 64; i++) {
            final List<byte[]> pair = f.subList(2 * i, 2 * i + 2);
            assertTradeCause(pair, 2, 1, EPOCH, first + i, 1_000 + i, r.now);
            assertEquals("TAKER then MAKER", OrderJournalSink.LIQUIDITY_TAKER, OrderJournalCapture.decode(pair.get(0)).liquidity());
            for (final byte[] p : pair) {
                assertEquals("one log position for the batch", position, OrderJournalCapture.decode(p).logPosition());
            }
        }
        // control: the context set once per entry (every pair names the first element) is caught
        final List<byte[]> once = new ArrayList<>();
        for (final byte[] p : f.subList(2, 4)) {
            final byte[] b = p.clone();
            new UnsafeBuffer(b).putLong(H + OrderJournalEventDecoder.meJournalSeqEncodingOffset(), first);
            once.add(b);
        }
        expectText("fills of one batch share one ME cause", () -> assertTradeCause(once, 2, 1, EPOCH, first + 1, 1_001, r.now));
    }

    /**
     * The same batch below behaviour 3 (behaviour 2 recorded: the batch loop applies each element through the order
     * engine, with no admission): every pair of FILL records names its own group element.
     */
    @Test
    public void aBatchBelowBehaviour3GivesOneCausePerElement() {
        final OrderJournalCapture c = new OrderJournalCapture();
        final V3Rig r = V3Rig.v2();
        r.engine.applyOrderEntryGate(1L, com.openexchange.core.application.engine.gate.OrderEntryGate.GLOBAL,
                com.openexchange.core.domain.GateState.OPEN, com.openexchange.core.domain.GateReason.ADMIN, r.now);
        r.engine.setOrderJournalSink(c.journal);
        r.twoParty();
        final int mark = c.records().size();
        final List<V3Rig.BatchEntry> entries = new ArrayList<>();
        final long first = 500;
        for (int i = 0; i < 8; i++) {
            entries.add(new V3Rig.BatchEntry(first + i, EPOCH, 2_000 + i, 1 + i, 0, ONE / 32));
        }
        r.dispatchTrusted(r.tradeBatch(entries));
        final List<byte[]> f = fills(since(c, mark));
        assertEquals("16 FILL records below behaviour 3", 16, f.size());
        for (int i = 0; i < 8; i++) {
            final List<byte[]> pair = f.subList(2 * i, 2 * i + 2);
            for (final byte[] p : pair) {
                if (OrderJournalCapture.decode(p).meJournalSeq() != first + i) {
                    throw new AssertionError("fills of one batch share one ME cause: element " + i + " names meJournalSeq "
                            + OrderJournalCapture.decode(p).meJournalSeq() + ", expected " + (first + i));
                }
            }
            assertTradeCause(pair, 2, 1, EPOCH, first + i, 2_000 + i, r.now);
        }
    }

    // ------------------------------------------------------------------------------------------ holds

    /**
     * Random commands (behaviour 1 orders, behaviour 3 with faults and retries, amends between fill and kill): after
     * every driver step, each order whose hold remaining changed has a record in that step whose holdRemaining is the
     * new value, every live order's last record carries its hold, and locked equals the sum of the live orders' record
     * holds per (user, asset). INCONCLUSIVE (fails) when no REPLACED record changed a hold.
     */
    @Test
    public void everyHoldChangeHasAnOrderRecord() {
        long steps = 0;
        long replacedHoldChanges = 0;
        for (long seed = 1; seed <= 3; seed++) {
            final OrderFuzzDriver d = new OrderFuzzDriver(seed);
            final HoldWatch w = new HoldWatch(d.r.engine);
            for (int i = 0; i < 300; i++) {
                final int step = i;
                w.step("behaviour 1 seed " + seed + " op " + i, () -> d.step(step));
                steps++;
            }
            replacedHoldChanges += w.replacedHoldChanges;
        }
        for (long seed = 201; seed <= 204; seed++) {
            final V3CoreFuzzDriver d = new V3CoreFuzzDriver(seed, true, 4, true);
            final HoldWatch w = new HoldWatch(d.r.engine);
            for (int i = 0; i < 400; i++) {
                w.step("behaviour 4 seed " + seed + " step " + i, () -> d.run(1));
                steps++;
            }
            replacedHoldChanges += w.replacedHoldChanges;
        }
        System.out.println("[order-journal] hold property: steps=" + steps + " REPLACED records that changed a hold="
                + replacedHoldChanges);
        assertTrue("INCONCLUSIVE: no REPLACED record changed a hold remaining", replacedHoldChanges > 0);
    }

    /** Control: REPLACED records suppressed (a test build) leave a hold change with no order record. */
    @Test
    public void controlSuppressedReplacedIsAHoldChangeWithNoOrderRecord() {
        for (long seed = 201; seed <= 204; seed++) {
            final V3CoreFuzzDriver d = new V3CoreFuzzDriver(seed, true, 4, true);
            final HoldWatch w = new HoldWatch(d.r.engine);
            w.suppress = EventKind.REPLACED;
            try {
                for (int i = 0; i < 400; i++) {
                    w.step("seed " + seed + " step " + i, () -> d.run(1));
                }
            } catch (final AssertionError expected) {
                assertTrue(expected.getMessage(), expected.getMessage().startsWith("hold changed with no order record"));
                return;
            }
        }
        fail("no hold change was left without a record: the control's precondition did not occur");
    }

    /** Watches one engine: holds before and after each step against the records of the step. */
    private static final class HoldWatch {
        final CoreEngine engine;
        final List<long[]> stepRecords = new ArrayList<>(); // {orderId, holdRemaining, kind, userId, holdAssetId}
        final Map<Long, Long> lastHoldByOrder = new HashMap<>();
        long replacedHoldChanges;
        EventKind suppress;

        HoldWatch(final CoreEngine engine) {
            this.engine = engine;
            engine.setOrderJournalSink((journalSeq, epoch, logPosition, o, kind, reason, lastPrice, lastQty, tradeId,
                                        holdRemaining, updatedAtMs, meReason, liquidity, meEpoch, meJournalSeq, meLogPosition,
                                        meTimestampMs) -> {
                if (kind == suppress) {
                    return;
                }
                stepRecords.add(new long[] {o.orderId(), holdRemaining, kind.code(), o.userId(), o.holdAssetId()});
            });
        }

        private Map<Long, Long> holds() {
            final Map<Long, Long> h = new TreeMap<>();
            engine.forEachAccount(a -> a.forEachHold((orderId, assetId, remaining, oms) -> h.put(orderId, remaining)));
            return h;
        }

        void step(final String what, final Runnable step) {
            final Map<Long, Long> before = holds();
            stepRecords.clear();
            step.run();
            final Map<Long, Long> after = holds();
            final Map<Long, long[]> lastRecord = new HashMap<>();
            for (final long[] rec : stepRecords) {
                final Long prev = lastHoldByOrder.get(rec[0]);
                if (rec[2] == EventKind.REPLACED.code() && prev != null && prev != rec[1]) {
                    replacedHoldChanges++;
                }
                lastHoldByOrder.put(rec[0], rec[1]);
                lastRecord.put(rec[0], rec);
            }
            final java.util.Set<Long> ids = new java.util.TreeSet<>(before.keySet());
            ids.addAll(after.keySet());
            for (final long id : ids) {
                if (engine.orders().orders().get(id) == null && !lastHoldByOrder.containsKey(id)) {
                    continue; // a hold the core does not own as an order (a legacy hold): not an order's state
                }
                final long b = before.getOrDefault(id, 0L);
                final long a = after.getOrDefault(id, 0L);
                if (a != b) {
                    final long[] rec = lastRecord.get(id);
                    if (rec == null || rec[1] != a) {
                        throw new AssertionError("hold changed with no order record: " + what + " order " + id + " hold "
                                + b + " -> " + a + " last record " + (rec == null ? "none" : String.valueOf(rec[1])));
                    }
                }
            }
            // C10 from the journal's view: every live order's last record carries its hold, and locked is their sum
            final Map<String, Long> sum = new HashMap<>();
            engine.orders().orders().forEachByOrderId(o -> {
                final long hold = after.getOrDefault(o.orderId, 0L);
                final Long rec = lastHoldByOrder.get(o.orderId);
                if (rec == null || rec != hold) {
                    throw new AssertionError("hold changed with no order record: " + what + " live order " + o.orderId
                            + " hold " + hold + " last record " + rec);
                }
                if (engine.account(o.userId) != null && engine.account(o.userId).hasHold(o.orderId)) {
                    sum.merge(o.userId + "/" + engine.account(o.userId).holdAssetId(o.orderId), hold, Long::sum);
                }
            });
            engine.forEachAccount(acc -> {
                final long[] legacy = new long[com.openexchange.core.domain.Asset.count()];
                acc.forEachHold((orderId, assetId, remaining, oms) -> {
                    if (engine.orders().orders().get(orderId) == null && assetId < legacy.length) {
                        legacy[assetId] += remaining;
                    }
                });
                for (int asset = 0; asset < legacy.length; asset++) {
                    final long fromRecords = sum.getOrDefault(acc.userId() + "/" + asset, 0L);
                    assertEquals(what + ": locked == sum of the live orders' record holds, u=" + acc.userId() + " asset="
                            + asset, acc.locked(asset), fromRecords + legacy[asset]);
                }
            });
        }
    }

    // ------------------------------------------------------------------------------------------ the epoch

    /** After ADOPT every record carries the recorded genesisId (behaviour 3 and 4 runs, which ADOPT before any order). */
    @Test
    public void afterAdoptEveryRecordCarriesTheGenesisId() {
        int n = 0;
        for (long seed = 301; seed <= 303; seed++) {
            final V3CoreFuzzDriver d = new V3CoreFuzzDriver(seed, true, 4, true);
            final OrderJournalCapture c = new OrderJournalCapture();
            d.r.engine.setOrderJournalSink(c.journal);
            d.run(300);
            assertEpoch(c.records(), V3Genesis.GENESIS_ID);
            n += c.records().size();
        }
        assertTrue("records checked: " + n, n > 50);
    }

    /** Control: a journal that writes epoch 0 (a test build) fails with its text. */
    @Test
    public void controlEpochZeroIsARecordWithoutAGenesis() {
        final V3CoreFuzzDriver d = new V3CoreFuzzDriver(301, false);
        final OrderJournalCapture c = new OrderJournalCapture();
        d.r.engine.setOrderJournalSink(c.journal);
        d.run(50);
        final List<byte[]> zeroed = new ArrayList<>();
        for (final byte[] r : c.records()) {
            final byte[] b = r.clone();
            new UnsafeBuffer(b).putLong(H + OrderJournalEventDecoder.epochEncodingOffset(), 0L);
            zeroed.add(b);
        }
        assertTrue("records to zero", !zeroed.isEmpty());
        expectText("order journal record without a genesis", () -> assertEpoch(zeroed, V3Genesis.GENESIS_ID));
    }

    /**
     * A genesis that ADOPTs its CoreConfig first (as every real genesis does, behaviour 1 included) never emits an epoch 0
     * record: the records of a create, its cancel request and a second create after the ADOPT all carry the recorded
     * genesisId. Behaviour 1
     * still admits orders before any CoreConfig (old scenarios only); a reader treats epoch 0 as foreign.
     */
    @Test
    public void aGenesisThatAdoptsFirstNeverEmitsEpochZero() {
        final OrderRig r = new OrderRig();
        final OrderJournalCapture c = new OrderJournalCapture();
        r.engine.setOrderJournalSink(c.journal);
        V3Genesis.widen(r.engine);
        r.engine.applyCoreConfig(V3Genesis.config(1, 1), r.now);
        assertEquals("the ADOPT recorded the genesis", V3Genesis.GENESIS_ID, r.engine.recordedGenesisId());
        assertTrue("no order event at the ADOPT", c.records().isEmpty());
        r.engine.applyOrderEntryGate(1L, com.openexchange.core.application.engine.gate.OrderEntryGate.GLOBAL,
                com.openexchange.core.domain.GateState.OPEN, com.openexchange.core.domain.GateReason.ADMIN, r.now);
        r.setRisk(r.riskConfig(1, 1));
        r.deposit(1, OrderRig.USD, 1_000 * OrderRig.ONE);
        r.create(r.limit(1, 1, 0, 100 * OrderRig.ONE, OrderRig.ONE));
        r.cancel(1, r.lastAcceptedOrderId());
        r.create(r.limit(1, 1, 0, 90 * OrderRig.ONE, OrderRig.ONE));
        final List<byte[]> records = c.records();
        assertTrue("INCONCLUSIVE: one record per create expected after the ADOPT, got " + records.size(),
                records.size() >= 2);
        assertEpoch(records, V3Genesis.GENESIS_ID);
        System.out.println("[order-journal] behaviour 1 genesis, ADOPT first: " + records.size() + " records, every one with"
                + " epoch " + V3Genesis.GENESIS_ID);
    }

    // ------------------------------------------------------------------------------------------ helpers

    static void expectText(final String text, final Runnable check) {
        try {
            check.run();
        } catch (final AssertionError expected) {
            assertTrue("expected '" + text + "', got: " + expected.getMessage(), expected.getMessage().startsWith(text));
            return;
        }
        fail("the control passed: expected '" + text + "'");
    }
}
