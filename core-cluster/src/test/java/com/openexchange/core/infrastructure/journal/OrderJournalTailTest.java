// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.journal;

import com.openexchange.cluster.testkit.RecordedLogFixture;
import com.openexchange.core.infrastructure.InfrastructureConstants;
import com.openexchange.core.infrastructure.orderjournal.generated.MessageHeaderEncoder;
import com.openexchange.core.infrastructure.orderjournal.generated.OrderJournalEventEncoder;
import com.openexchange.core.infrastructure.persistence.OrderJournalRestoreRule;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The order journal's tail reader against a real archive: the maximum journalSeq over the recordings of ONE genesis
 * (a previous genesis left in the same archive on stream 4102 is ignored, although it holds a higher sequence), read
 * through the one aligned 256-byte frame of the record; and the restore-side rule built on it, which refuses a SKIP
 * whose image eventSeq the new genesis's journal does not hold.
 */
public class OrderJournalTailTest {

    private static final String CHANNEL = "aeron:ipc?term-length=64k";
    private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(20);
    private static final int STREAM = InfrastructureConstants.ORDER_JOURNAL_STREAM_ID;

    /** One full-size order journal message (the production layout: 216 bytes) with {@code seq} and {@code epoch}. */
    static void write(final ExclusivePublication pub, final long seq, final long epoch) {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[OrderJournal.MESSAGE_BYTES]);
        new OrderJournalEventEncoder().wrapAndApplyHeader(b, 0, new MessageHeaderEncoder()).journalSeq(seq).epoch(epoch)
                .orderId(seq).logPosition(seq * 64);
        while (pub.offer(b, 0, b.capacity()) < 0) {
            Thread.onSpinWait();
        }
    }

    private static void recordAndStop(final AeronArchive archive, final long from, final long to, final long epoch) {
        final ExclusivePublication p = archive.addRecordedExclusivePublication(CHANNEL, STREAM);
        for (long s = from; s <= to; s++) {
            write(p, s, epoch);
        }
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        long rec;
        while ((rec = archive.findLastMatchingRecording(0, "aeron:ipc", STREAM, p.sessionId())) < 0
                || archive.getRecordingPosition(rec) < p.position()) {
            assertTrue("recording caught up", System.nanoTime() < deadline);
            Thread.onSpinWait();
        }
        p.close();
        while (archive.getStopPosition(rec) == AeronArchive.NULL_POSITION) {
            assertTrue("recording stopped", System.nanoTime() < deadline);
            Thread.onSpinWait();
        }
    }

    /** The check of the row: a SKIP on the new genesis must be refused while its journal is behind the image. */
    static void assertSkipRefused(final boolean skipSafe, final long imageSeq, final OrderJournalTail.Result tail) {
        if (skipSafe) {
            throw new AssertionError("skip-safe passed on a previous genesis: image eventSeq " + imageSeq + ", tail " + tail);
        }
    }

    @Test
    public void aPreviousGenesisInTheSameArchiveNeverMakesASkipLookSafe() {
        try (RecordedLogFixture fx = new RecordedLogFixture()) {
            final long previous = 0x1111L;
            final long current = 0x2222L;
            final OrderJournalTail empty = new OrderJournalTail(fx.aeron(), fx.archive(), SystemNanoClock.INSTANCE, current);
            assertTrue("no recording yet", empty.read(TIMEOUT_NS) instanceof OrderJournalTail.NoJournal);

            recordAndStop(fx.archive(), 1, 100, previous); // the previous genesis, seq up to 100
            recordAndStop(fx.archive(), 1, 10, current);   // the new genesis, seq up to 10

            final OrderJournalTail.Result tail =
                    new OrderJournalTail(fx.aeron(), fx.archive(), SystemNanoClock.INSTANCE, current).read(TIMEOUT_NS);
            assertEquals("only the new genesis counts", new OrderJournalTail.Known(10, 1, 0, 1), tail);
            final OrderJournalTail.Result old =
                    new OrderJournalTail(fx.aeron(), fx.archive(), SystemNanoClock.INSTANCE, previous).read(TIMEOUT_NS);
            assertEquals("the previous genesis reads its own", new OrderJournalTail.Known(100, 1, 0, 1), old);

            final OrderJournalRestoreRule rule = new OrderJournalRestoreRule(SystemNanoClock.INSTANCE, 20_000, null);
            final long imageSeq = 50;
            assertSkipRefused(rule.skipSafe(fx.aeron(), fx.archive(), current, imageSeq, 4_096), imageSeq, tail);
            assertTrue("the new genesis's journal holds 10", rule.skipSafe(fx.aeron(), fx.archive(), current, 10, 4_096));

            final List<long[]> holes = new ArrayList<>();
            final OrderJournalRestoreRule declaring = new OrderJournalRestoreRule(SystemNanoClock.INSTANCE, 20_000,
                    (from, to) -> holes.add(new long[] {from, to}));
            final OrderJournalRestoreRule.Verdict v = declaring.afterRestore(fx.aeron(), fx.archive(), current, imageSeq,
                    "test");
            assertTrue(v.behind());
            assertEquals("one hole (10, 50]", 1, holes.size());
            assertEquals(10L, holes.get(0)[0]);
            assertEquals(50L, holes.get(0)[1]);
            assertFalse("no hole when the journal holds the image",
                    declaring.afterRestore(fx.aeron(), fx.archive(), current, 10, "test").behind());
            assertEquals(1, holes.size());

            // control: the epoch filter off counts the previous genesis's 100 and passes the SKIP
            final OrderJournalTail.Result unfiltered = new OrderJournalTail(fx.aeron(), fx.archive(), SystemNanoClock.INSTANCE,
                    current).epochFilterOff().read(TIMEOUT_NS);
            final boolean safeWithoutFilter = unfiltered instanceof OrderJournalTail.Known k && k.seq() >= imageSeq;
            try {
                assertSkipRefused(safeWithoutFilter, imageSeq, unfiltered);
            } catch (final AssertionError expected) {
                assertTrue(expected.getMessage(), expected.getMessage().startsWith("skip-safe passed on a previous genesis"));
                return;
            }
            fail("the control passed: the unfiltered tail did not see the previous genesis");
        }
    }
}
