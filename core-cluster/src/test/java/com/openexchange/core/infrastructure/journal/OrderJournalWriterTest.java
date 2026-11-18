// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.journal;

import com.openexchange.cluster.shadow.ShadowReplay;
import com.openexchange.cluster.testkit.RecordedLogFixture;
import com.openexchange.core.domain.CoreRejectReason;
import com.openexchange.core.domain.EventKind;
import com.openexchange.core.infrastructure.InfrastructureConstants;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.Test;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The order journal's ring and writer agent on their own: a ring that cannot hold one record refuses boot; the
 * readiness alarm reads a journal that records nothing although its ring is far below the fill bound; a service thread
 * waiting on a full ring leaves on interrupt; and, against a real archive, a publication that closes under the writer
 * loses no record (the writer reconnects) and a graceful close drains the ring.
 */
public class OrderJournalWriterTest {

    private static final long EPOCH = 0x5151L;
    private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(20);
    private static final int STREAM = InfrastructureConstants.ORDER_JOURNAL_STREAM_ID;

    static void emit(final OrderJournal j, final long seq) {
        j.onOrderEvent(seq, EPOCH, seq * 64, new OrderJournalStreamSchemaTest.TestOrder(), EventKind.ACCEPTED,
                CoreRejectReason.NONE, 0, 0, 0, 5, 1_000, 255, 0, 0, 0, 0, 0);
    }

    private static OrderJournalSettings settings(final Map<OrderJournalSettings.Key, String> values) {
        return OrderJournalSettings.with(values);
    }

    // ------------------------------------------------------------------------------------------ settings

    @Test
    public void aRingThatCannotHoldOneRecordRefusesBoot() {
        final OrderJournalSettings s;
        try {
            s = settings(Map.of(OrderJournalSettings.Key.CORE_ORDER_JOURNAL_RING_BYTES, "1024"));
        } catch (final IllegalArgumentException refused) {
            assertTrue(refused.getMessage(), refused.getMessage().startsWith(
                    "CORE_ORDER_JOURNAL_RING_BYTES=1024 out of range [2048,"));
            // the smallest accepted ring takes records
            final OrderJournal j = new OrderJournal(settings(Map.of(OrderJournalSettings.Key.CORE_ORDER_JOURNAL_RING_BYTES,
                    "2048")));
            emit(j, 1);
            assertEquals(1, j.appendedSeq());
            return;
        }
        String thrown = "nothing";
        try {
            emit(new OrderJournal(s), 1);
        } catch (final RuntimeException e) {
            thrown = e.toString();
        }
        fail("a ring that cannot hold one record was accepted at boot; the first order event then throws: " + thrown);
    }

    // ------------------------------------------------------------------------------------------ readiness alarm

    @Test
    public void aJournalThatRecordsNothingClosesReadinessWithTheRingFarBelowTheBound() {
        final OrderJournal j = new OrderJournal(settings(Map.of(OrderJournalSettings.Key.CORE_ORDER_JOURNAL_RING_ALARM_MS,
                "100")));
        final long t = System.nanoTime();
        assertTrue("nothing appended: not alarmed", !j.alarmed(t));
        for (int round = 1; round <= 2; round++) {
            emit(j, round);
            final long t0 = t + TimeUnit.MILLISECONDS.toNanos(1_000L * round);
            assertTrue("inside the bound: not alarmed yet", !j.alarmed(t0));
            if (!j.alarmed(t0 + TimeUnit.MILLISECONDS.toNanos(200))) {
                throw new AssertionError("member with a stalled order journal reads ready: round " + round + ", "
                        + j.ringUsedBytes() + " B in the ring (far below the fill bound), appended " + j.appendedSeq()
                        + ", recorded " + j.recordedSeq() + " for 200 ms");
            }
            j.ringBuffer().read((type, buffer, index, length) -> { }, Integer.MAX_VALUE);
            j.recordedSeq(round);
            assertTrue("recorded: the alarm clears, round " + round,
                    !j.alarmed(t0 + TimeUnit.MILLISECONDS.toNanos(300)));
        }
    }

    /** Control: the no-progress alarm off, the fill bound alone reads a journal that records nothing as ready. */
    @Test
    public void controlProgressAlarmOffReadsReady() {
        final OrderJournal j = new OrderJournal(settings(Map.of(OrderJournalSettings.Key.CORE_ORDER_JOURNAL_RING_ALARM_MS,
                "100")));
        OrderJournalCapture.progressAlarmOff(j, true);
        emit(j, 1);
        final long t = System.nanoTime();
        j.alarmed(t);
        try {
            if (!j.alarmed(t + TimeUnit.MILLISECONDS.toNanos(200))) {
                throw new AssertionError("member with a stalled order journal reads ready: control, appended "
                        + j.appendedSeq() + ", recorded " + j.recordedSeq());
            }
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(), expected.getMessage().startsWith(
                    "member with a stalled order journal reads ready"));
            return;
        }
        fail("the control closed readiness with the no-progress alarm off");
    }

    // ------------------------------------------------------------------------------------------ full ring

    @Test
    public void aServiceThreadWaitingOnAFullRingLeavesOnInterrupt() throws Exception {
        final OrderJournal j = new OrderJournal(settings(Map.of(OrderJournalSettings.Key.CORE_ORDER_JOURNAL_RING_BYTES,
                "2048")));
        final AtomicReference<Throwable> left = new AtomicReference<>();
        final Thread service = new Thread(() -> {
            try {
                for (long seq = 1; seq <= 40; seq++) {
                    emit(j, seq);
                }
            } catch (final Throwable t) {
                left.set(t);
            }
        }, "order-journal-full-ring");
        service.start();
        await(() -> j.backpressureEvents() > 0, "the ring never filled");
        service.interrupt();
        service.join(3_000);
        final boolean stuck = service.isAlive();
        // drain whatever is left so a stuck thread can finish before the next test
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        while (service.isAlive() && System.nanoTime() < deadline) {
            j.ringBuffer().read((type, buffer, index, length) -> { }, Integer.MAX_VALUE);
            Thread.sleep(1);
        }
        if (stuck) {
            throw new AssertionError("a service thread waiting on a full ring did not leave on interrupt");
        }
        assertTrue("left with an agent termination: " + left.get(), left.get() instanceof AgentTerminationException);
    }

    // ------------------------------------------------------------------------------------------ against an archive

    private static AeronArchive.Context context(final RecordedLogFixture fx) {
        return new AeronArchive.Context().aeronDirectoryName(fx.aeronDirectoryName())
                .controlRequestChannel("aeron:ipc").controlResponseChannel("aeron:ipc");
    }

    private static OrderJournalWriterAgent agent(final RecordedLogFixture fx, final OrderJournal j) {
        return new OrderJournalWriterAgent(j, () -> context(fx), "aeron:ipc?term-length=64k", STREAM, 1_000_000L);
    }

    /** Every journalSeq the archive holds on the order journal stream, over all its recordings. */
    static TreeSet<Long> recorded(final RecordedLogFixture fx) {
        final AeronArchive archive = fx.archive();
        final List<long[]> recs = new ArrayList<>();
        archive.listRecordingsForUri(0, 1_000, "aeron:ipc", STREAM,
                (c, k, recordingId, st, sp, startPosition, stopPosition, i, t, se, m, sid, str, ch, oc, src) ->
                        recs.add(new long[] {recordingId, startPosition, stopPosition}));
        final TreeSet<Long> seqs = new TreeSet<>();
        final ShadowReplay replay = new ShadowReplay(fx.aeron(), archive, SystemNanoClock.INSTANCE);
        for (final long[] r : recs) {
            final long stop = r[2] == AeronArchive.NULL_POSITION ? archive.getRecordingPosition(r[0]) : r[2];
            if (stop > r[1]) {
                replay.readAll(r[0], r[1], stop, (buffer, offset, length, h) ->
                        seqs.add(buffer.getLong(offset + 8, ByteOrder.LITTLE_ENDIAN)), System.nanoTime() + TIMEOUT_NS);
            }
        }
        return seqs;
    }

    static void assertDense(final TreeSet<Long> seqs, final long to, final String text) {
        for (long q = 1; q <= to; q++) {
            if (!seqs.contains(q)) {
                throw new AssertionError(text + ": missing journalSeq " + q + " of 1.." + to + " (archive holds "
                        + seqs.size() + " records)");
            }
        }
    }

    /** Runs the agent's duty cycle the way its runner does: an exception goes to the error handler, the loop goes on. */
    private static void pump(final OrderJournalWriterAgent a, final BooleanSupplier until, final long ms,
                             final List<Throwable> errors) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ms);
        while (!until.getAsBoolean() && System.nanoTime() < deadline) {
            try {
                if (a.doWork() == 0) {
                    Thread.sleep(1);
                }
            } catch (final RuntimeException e) {
                errors.add(e);
            }
        }
    }

    @Test
    public void aPublicationThatClosesUnderTheWriterLosesNoRecord() throws Exception {
        try (RecordedLogFixture fx = new RecordedLogFixture()) {
            final OrderJournal j = new OrderJournal(OrderJournalSettings.defaults());
            final OrderJournalWriterAgent a = agent(fx, j);
            final List<Throwable> errors = new ArrayList<>();
            for (long seq = 1; seq <= 50; seq++) {
                emit(j, seq);
            }
            pump(a, () -> j.recordedSeq() == 50, 20_000, errors);
            assertEquals("the first 50 recorded: " + errors, 50, j.recordedSeq());
            final java.lang.reflect.Field f = OrderJournalWriterAgent.class.getDeclaredField("publication");
            f.setAccessible(true);
            ((ExclusivePublication) f.get(a)).close();
            for (long seq = 51; seq <= 100; seq++) {
                emit(j, seq);
            }
            pump(a, () -> j.recordedSeq() == 100, 10_000, errors);
            try {
                assertDense(recorded(fx), 100, "order journal record lost on a closed publication");
            } finally {
                a.onClose();
            }
            System.out.println("[order-journal writer] closed publication: 1..100 recorded, errors " + errors.size()
                    + ", recorded " + j.recordedSeq());
        }
    }

    @Test
    public void aGracefulCloseDrainsTheRing() throws Exception {
        try (RecordedLogFixture fx = new RecordedLogFixture()) {
            final OrderJournal j = new OrderJournal(OrderJournalSettings.defaults());
            final OrderJournalWriterAgent a = agent(fx, j);
            for (long seq = 1; seq <= 30; seq++) {
                emit(j, seq);
            }
            a.onClose(); // the runner closes before the agent ever ran: the ring still holds 30 records
            assertDense(recorded(fx), 30, "a graceful close left records in the ring");
            assertEquals("recorded after the close", 30, j.recordedSeq());
            assertEquals("ring empty after the close", 0, j.ringUsedBytes());
        }
    }

    @Test
    public void anUnreadableTailAfterARestoreIsCountedAndShownAsUnknown() {
        try (RecordedLogFixture fx = new RecordedLogFixture()) {
            final OrderJournal j = new OrderJournal(OrderJournalSettings.defaults());
            final com.openexchange.core.infrastructure.persistence.OrderJournalRestoreRule rule =
                    new com.openexchange.core.infrastructure.persistence.OrderJournalRestoreRule(SystemNanoClock.INSTANCE,
                            20_000, new com.openexchange.core.infrastructure.persistence.OrderJournalRestoreRule.Holes() {
                                @Override
                                public void declare(final long fromExclusive, final long toInclusive) {
                                    j.declareHole(fromExclusive, toInclusive);
                                }

                                @Override
                                public void unreadable(final long restoredSeq) {
                                    j.declareUnknown(restoredSeq);
                                }
                            });
            final AeronArchive closed = fx.newArchiveClient(fx.aeron());
            closed.close();
            final var v = rule.afterRestore(fx.aeron(), closed, EPOCH, 10, "test");
            assertTrue("unreadable: " + v, v.localSeq() < 0);
            if (j.tailUnreadableTotal() != 1 || !j.holeRanges().equals("unknown(,10]")) {
                throw new AssertionError("an unreadable order journal tail is silent: unreadable "
                        + j.tailUnreadableTotal() + ", ranges " + j.holeRanges() + ", tail " + v.tail());
            }
            assertEquals("no hole declared", 0, j.holesTotal());
        }
    }

    static void await(final BooleanSupplier c, final String text) throws InterruptedException {
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        while (!c.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(text);
            }
            Thread.sleep(1);
        }
    }
}
