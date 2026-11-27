// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.journal;

import com.openexchange.cluster.journal.RecordingTail;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The money and the order journal tail readers on a real archive, against the readers they replace
 * ({@link ProbingMoneyJournalTail}, {@link ProbingOrderJournalTail}: the earlier code, kept as the oracle). In every
 * case a reader that remembers nothing, a reader that follows the recording across reads and the oracle give the same
 * result, and the two production readers raise nothing into the archive's error handler. The oracle does (it asks for
 * replays that start inside a frame); the last test shows the capture sees that.
 */
public class JournalTailEquivalenceTest {

    private static final long T = JournalTailRig.TIMEOUT_NS;
    private static final int TERM = JournalTailRig.TERM;
    private static final long EPOCH = 0x2222L;

    private JournalTailRig rig;
    private int nextStream = 7001;
    private int oracleErrors;

    @Before
    public void setUp() {
        rig = new JournalTailRig();
    }

    @After
    public void tearDown() {
        rig.close();
    }

    // ------------------------------------------------------------------ the two journals

    /** One journal: its record lengths and its three readers. */
    private abstract class Journal {
        final String name;
        /** The largest record, a smaller legal one (the same when the journal has one length), one too short to count. */
        final int full;
        final int small;
        final int tooShort;

        Journal(final String name, final int full, final int small, final int tooShort) {
            this.name = name;
            this.full = full;
            this.small = small;
            this.tooShort = tooShort;
        }

        abstract DirectBuffer message(long seq, int length);

        /** Record lengths that fill one term exactly. */
        abstract int[] exactTerm();

        /** Record lengths after which one more {@link #full} record does not fit the term. */
        abstract int[] almostATerm();

        /** A production reader that remembers nothing. */
        abstract Object fresh(int stream);

        /** A production reader that keeps what it read, across the reads of the returned supplier. */
        abstract Supplier<Object> follower(int stream);

        abstract Object oracle(int stream);

        abstract Object known(long seq, int recordings, int active);

        abstract Object noJournal(int recordings);

        abstract boolean isUnknown(Object result);

        abstract long seqOf(Object result);
    }

    private final Journal money = new Journal("money", 109, 60, 8) {
        @Override
        DirectBuffer message(final long seq, final int length) {
            final UnsafeBuffer b = new UnsafeBuffer(new byte[length]);
            b.putShort(0, (short) (length - 8));
            if (length >= 16) {
                b.putLong(MoneyJournalTail.JOURNAL_SEQ_OFFSET, seq);
            }
            return b;
        }

        @Override
        int[] exactTerm() {
            // 681 frames of 96 bytes and one of 160: 65536
            final int[] lengths = new int[682];
            java.util.Arrays.fill(lengths, small);
            lengths[681] = full;
            return lengths;
        }

        @Override
        int[] almostATerm() {
            // 682 frames of 96 bytes leave 64
            final int[] lengths = new int[682];
            java.util.Arrays.fill(lengths, small);
            return lengths;
        }

        @Override
        Object fresh(final int stream) {
            return new MoneyJournalTail(rig.aeron(), rig.archive(), SystemNanoClock.INSTANCE, "aeron:ipc", stream,
                    MoneyJournalTail.newTail()).read(T);
        }

        @Override
        Supplier<Object> follower(final int stream) {
            // as the member's housekeeping does: one RecordingTail, a new reader for every read
            final RecordingTail<Long> tail = MoneyJournalTail.newTail();
            lastFollowerTail = tail;
            return () -> new MoneyJournalTail(rig.aeron(), rig.archive(), SystemNanoClock.INSTANCE, "aeron:ipc",
                    stream, tail).read(T);
        }

        @Override
        Object oracle(final int stream) {
            return new ProbingMoneyJournalTail(rig.aeron(), rig.archive(), SystemNanoClock.INSTANCE, "aeron:ipc",
                    stream).read(T);
        }

        @Override
        Object known(final long seq, final int recordings, final int active) {
            return new MoneyJournalTail.Known(seq, recordings, active);
        }

        @Override
        Object noJournal(final int recordings) {
            return new MoneyJournalTail.NoJournal(recordings);
        }

        @Override
        boolean isUnknown(final Object result) {
            return result instanceof MoneyJournalTail.Unknown;
        }

        @Override
        long seqOf(final Object result) {
            return result instanceof MoneyJournalTail.Known k ? k.seq() : Long.MIN_VALUE;
        }
    };

    private final Journal order = new Journal("order", OrderJournal.MESSAGE_BYTES, OrderJournal.MESSAGE_BYTES, 20) {
        @Override
        DirectBuffer message(final long seq, final int length) {
            final UnsafeBuffer b = new UnsafeBuffer(new byte[length]);
            b.putShort(0, (short) (length - 8));
            if (length >= 16) {
                b.putLong(OrderJournalTail.JOURNAL_SEQ_OFFSET, seq);
            }
            if (length >= 24) {
                b.putLong(OrderJournalTail.EPOCH_OFFSET, EPOCH);
            }
            return b;
        }

        @Override
        int[] exactTerm() {
            final int[] lengths = new int[TERM / OrderJournalTail.MAX_FRAME_LENGTH];
            java.util.Arrays.fill(lengths, full);
            return lengths;
        }

        @Override
        int[] almostATerm() {
            // one frame of 64 bytes and 255 of 256 leave 192
            final int[] lengths = new int[256];
            java.util.Arrays.fill(lengths, full);
            lengths[0] = tooShort;
            return lengths;
        }

        @Override
        Object fresh(final int stream) {
            return new OrderJournalTail(rig.aeron(), rig.archive(), SystemNanoClock.INSTANCE, "aeron:ipc", stream,
                    EPOCH).read(T);
        }

        @Override
        Supplier<Object> follower(final int stream) {
            // as production does (the restore's rule): a new reader, remembering nothing, for every read
            return () -> new OrderJournalTail(rig.aeron(), rig.archive(), SystemNanoClock.INSTANCE, "aeron:ipc",
                    stream, EPOCH).read(T);
        }

        @Override
        Object oracle(final int stream) {
            return new ProbingOrderJournalTail(rig.aeron(), rig.archive(), SystemNanoClock.INSTANCE, "aeron:ipc",
                    stream, EPOCH).read(T);
        }

        @Override
        Object known(final long seq, final int recordings, final int active) {
            return new OrderJournalTail.Known(seq, recordings, active, 0);
        }

        @Override
        Object noJournal(final int recordings) {
            return new OrderJournalTail.NoJournal(recordings, 0);
        }

        @Override
        boolean isUnknown(final Object result) {
            return result instanceof OrderJournalTail.Unknown;
        }

        @Override
        long seqOf(final Object result) {
            return result instanceof OrderJournalTail.Known k ? k.seq() : Long.MIN_VALUE;
        }
    };

    private RecordingTail<Long> lastFollowerTail;

    private Journal[] journals() {
        return new Journal[] {money, order};
    }

    // ------------------------------------------------------------------ the comparison

    /**
     * Reads the stream three ways and requires one result: a reader that remembers nothing, {@code follower}, and the
     * oracle. The two production reads must leave the archive's error handler empty; what the oracle raises is counted
     * and cleared.
     */
    private Object same(final String what, final Journal j, final int stream, final Supplier<Object> follower) {
        assertTrue(what + ": the archive raised before the read " + rig.archiveErrors, rig.archiveErrors.isEmpty());
        final Object fresh = j.fresh(stream);
        final Object followed = follower.get();
        java.util.concurrent.locks.LockSupport.parkNanos(5_000_000L);
        assertTrue(j.name + " " + what + ": the archive raised " + rig.archiveErrors, rig.archiveErrors.isEmpty());
        final Object oracle = j.oracle(stream);
        oracleErrors += settledErrors();
        assertEquals(j.name + " " + what + ": a reader that remembers nothing", oracle, fresh);
        assertEquals(j.name + " " + what + ": a reader that follows the recording", oracle, followed);
        return oracle;
    }

    /**
     * What the archive raised since the last call, cleared. The archive's thread reports a refusal around the time
     * it answers the client, so it is given a moment first.
     */
    private int settledErrors() {
        java.util.concurrent.locks.LockSupport.parkNanos(10_000_000L);
        final int n = rig.archiveErrors.size();
        rig.archiveErrors.clear();
        return n;
    }

    private long appendAll(final Journal j, final JournalTailRig.Writer w, final long firstSeq, final int[] lengths) {
        long seq = firstSeq;
        for (final int length : lengths) {
            w.append(j.message(seq++, length));
        }
        return seq;
    }

    // ------------------------------------------------------------------ the cases

    /** A stream without a recording, and a recording that was stopped before a byte was written. */
    @Test
    public void noRecordingAndAnEmptyRecording() {
        for (final Journal j : journals()) {
            final int stream = nextStream++;
            final Supplier<Object> follower = j.follower(stream);
            assertEquals(j.noJournal(0), same("no recording", j, stream, follower));
            final JournalTailRig.Writer w = rig.writer(stream);
            assertEquals("an active recording without a byte", j.noJournal(1),
                    same("an empty active recording", j, stream, follower));
            assertEquals(0, w.stop());
            assertEquals(j.noJournal(1), same("an empty stopped recording", j, stream, follower));
        }
    }

    /** One record: while the recording is active, after it stopped, and for a reader built after that (a restart). */
    @Test
    public void oneRecord() {
        for (final Journal j : journals()) {
            for (final int length : new int[] {j.full, j.small}) {
                final int stream = nextStream++;
                final Supplier<Object> follower = j.follower(stream);
                final JournalTailRig.Writer w = rig.writer(stream).append(j.message(41, length)).awaitRecorded();
                assertEquals(j.known(41, 1, 1), same("one record, active", j, stream, follower));
                w.stop();
                assertEquals(j.known(41, 1, 0), same("one record, stopped", j, stream, follower));
                assertEquals(j.known(41, 1, 0), same("one record, a new reader", j, stream, j.follower(stream)));
            }
        }
    }

    /** The recording ends exactly on a term end that its records filled: the last record of that term is the tail. */
    @Test
    public void theEndOnATermEndAfterAFullTerm() {
        for (final Journal j : journals()) {
            final int stream = nextStream++;
            final Supplier<Object> follower = j.follower(stream);
            final JournalTailRig.Writer w = rig.writer(stream);
            final long next = appendAll(j, w, 1, j.exactTerm());
            assertEquals(j.name + ": the records fill the term", TERM, w.position());
            w.awaitRecorded();
            assertEquals(j.known(next - 1, 1, 1), same("a full term, active", j, stream, follower));
            // one record into the next term: the tail is that record, read from the term base
            w.append(j.message(next, j.full)).awaitRecorded();
            assertEquals(j.known(next, 1, 1), same("one record into the next term", j, stream, follower));
            // and a second full term, stopped on its end
            final long after = appendAll(j, w, next + 1, java.util.Arrays.copyOf(j.exactTerm(),
                    j.exactTerm().length - 1));
            assertEquals(j.name + ": the second term is full", 2L * TERM, w.position());
            assertEquals(2L * TERM, w.stop());
            assertEquals(j.known(after - 1, 1, 0), same("two full terms, stopped", j, stream, follower));
            assertEquals(j.known(after - 1, 1, 0), same("two full terms, a new reader", j, stream,
                    j.follower(stream)));
        }
    }

    /** The recording ends on a term end after padding: the tail is the last record before the padding. */
    @Test
    public void theEndOnATermEndAfterPadding() {
        for (final Journal j : journals()) {
            final int stream = nextStream++;
            final Supplier<Object> follower = j.follower(stream);
            final JournalTailRig.Writer w = rig.writer(stream);
            final long next = appendAll(j, w, 1, j.almostATerm());
            w.awaitRecorded();
            assertEquals(j.known(next - 1, 1, 1), same("before the padding", j, stream, follower));
            assertTrue(j.name + ": room is left, but not for a record", w.position() < TERM);
            assertEquals(j.name + ": the term is padded and rotated", io.aeron.Publication.ADMIN_ACTION,
                    w.offerOnce(j.message(next, j.full)));
            assertEquals(TERM, w.position());
            w.awaitRecorded();
            assertEquals(j.known(next - 1, 1, 1), same("on the term end after padding, active", j, stream, follower));
            assertEquals(TERM, w.stop());
            assertEquals(j.known(next - 1, 1, 0), same("on the term end after padding, stopped", j, stream, follower));
            assertEquals(j.known(next - 1, 1, 0), same("on the term end after padding, a new reader", j, stream,
                    j.follower(stream)));
        }
    }

    /** A last message too short to count: the tail is the record before it; alone in its term there is no tail. */
    @Test
    public void aTooShortLastMessage() {
        for (final Journal j : journals()) {
            int stream = nextStream++;
            Supplier<Object> follower = j.follower(stream);
            JournalTailRig.Writer w = rig.writer(stream).append(j.message(1, j.full)).append(j.message(2, j.small))
                    .awaitRecorded();
            assertEquals(j.known(2, 1, 1), same("two records", j, stream, follower));
            w.append(j.message(3, j.tooShort)).awaitRecorded();
            assertEquals(j.known(2, 1, 1), same("a too-short last message, active", j, stream, follower));
            w.append(j.message(4, j.tooShort)).stop();
            assertEquals(j.known(2, 1, 0), same("two too-short last messages, stopped", j, stream, follower));
            assertEquals(j.known(2, 1, 0), same("two too-short last messages, a new reader", j, stream,
                    j.follower(stream)));

            // only a too-short message in the term that holds the end: no tail, the result is Unknown
            stream = nextStream++;
            follower = j.follower(stream);
            w = rig.writer(stream);
            final long next = appendAll(j, w, 1, j.exactTerm());
            w.awaitRecorded();
            assertEquals(j.known(next - 1, 1, 1), same("a full term", j, stream, follower));
            w.append(j.message(next, j.tooShort)).awaitRecorded();
            final Object alone = same("only a too-short message in the last term", j, stream, follower);
            assertTrue(String.valueOf(alone), j.isUnknown(alone));
            assertTrue(String.valueOf(alone), String.valueOf(alone).contains(
                    "no journal message in the last term of recording " + w.recordingId + " [0," + (TERM + 64) + ")"));
            w.append(j.message(next + 1, j.full)).stop();
            assertEquals(j.known(next + 1, 1, 0), same("a record after it", j, stream, follower));
        }
    }

    /**
     * An active recording that grows between reads, next to a stopped recording of an earlier run: by one record, by
     * several, by more than a term, and not at all; with too-short messages among the records. Then stopped, then read
     * by a reader built afterwards.
     */
    @Test
    public void aRecordingGrowingBetweenReads() {
        for (final Journal j : journals()) {
            final int stream = nextStream++;
            final Random random = new Random(20261127L + j.full);
            // an earlier run: 30 records, stopped
            final JournalTailRig.Writer earlier = rig.writer(stream);
            long seq = 1;
            for (int i = 0; i < 30; i++) {
                earlier.append(j.message(seq++, i % 3 == 0 ? j.full : j.small));
            }
            earlier.stop();
            final Supplier<Object> follower = j.follower(stream);
            final RecordingTail<Long> tail = j == money ? lastFollowerTail : null;
            assertEquals(j.known(30, 1, 0), same("the earlier run", j, stream, follower));

            final JournalTailRig.Writer w = rig.writer(stream);
            long lastCounted = 30;
            int unchanged = 0;
            int termsCrossed = 0;
            for (int step = 0; step < 70; step++) {
                final int roll = random.nextInt(100);
                final int count = roll < 20 ? 0 : roll < 45 ? 1 : roll < 90 ? 2 + random.nextInt(12)
                        : 300 + random.nextInt(500);
                final long termBefore = w.position() / TERM;
                for (int i = 0; i < count; i++) {
                    final int kind = random.nextInt(20);
                    final int length = kind == 0 ? j.tooShort : kind < 10 ? j.small : j.full;
                    w.append(j.message(seq, length));
                    if (length != j.tooShort) {
                        lastCounted = seq;
                    }
                    seq++;
                }
                w.awaitRecorded();
                unchanged += count == 0 ? 1 : 0;
                termsCrossed += w.position() / TERM > termBefore ? 1 : 0;
                final Object result = same("step " + step + " (+" + count + " to " + w.position() + ")", j, stream,
                        follower);
                // every record of this run has a larger journalSeq than the earlier run's; a term that so far
                // holds only too-short messages has no tail (Unknown), as before
                if (!j.isUnknown(result)) {
                    // (an active recording without a byte is counted as active and not as read)
                    assertEquals(j.name + " step " + step, w.position() == 0 ? j.known(30, 1, 1)
                            : j.known(lastCounted, 2, 1), result);
                }
            }
            assertTrue(j.name + ": reads at an end that had not moved, " + unchanged, unchanged >= 5);
            assertTrue(j.name + ": reads after the end moved into a later term, " + termsCrossed, termsCrossed >= 3);
            if (tail != null) {
                assertTrue("the follower resumed after its last message, " + tail.resumedReads(),
                        tail.resumedReads() >= 20);
                assertTrue("the follower replayed nothing for an end that had not moved, " + tail.unchanged(),
                        tail.unchanged() >= unchanged);
                assertTrue("the follower read from a term base when the end left the term, " + tail.firstLooks(),
                        tail.firstLooks() >= 2 + termsCrossed);
            }
            w.append(j.message(seq, j.full)).stop();
            assertEquals(j.known(seq, 2, 0), same("stopped", j, stream, follower));
            assertEquals(j.known(seq, 2, 0), same("a new reader", j, stream, j.follower(stream)));
        }
        assertTrue("the oracle's refused replays reach the capture (it sees what production must not raise): "
                + oracleErrors, oracleErrors > 0);
    }

    /**
     * A writer appends while the readers read: every read is a known tail that never goes back and never passes
     * what was written, and nothing is raised into the archive's error handler. Once the writer is done the readers
     * and the oracle agree.
     */
    @Test
    public void readsWhileTheRecordingGrows() throws InterruptedException {
        for (final Journal j : journals()) {
            final int stream = nextStream++;
            final JournalTailRig.Writer w = rig.writer(stream).append(j.message(1, j.full)).awaitRecorded();
            final AtomicLong written = new AtomicLong(1);
            final AtomicBoolean stop = new AtomicBoolean();
            final AtomicReference<Throwable> failed = new AtomicReference<>();
            final Thread writer = new Thread(() -> {
                try {
                    long seq = 2;
                    while (!stop.get() && seq < 40_000) {
                        w.append(j.message(seq, seq % 3 == 0 ? j.small : j.full));
                        written.set(seq++);
                        if (seq % 64 == 0) {
                            Thread.sleep(1);
                        }
                    }
                } catch (final Throwable t) {
                    failed.set(t);
                }
            }, "journal-tail-test-writer");
            writer.start();
            final Supplier<Object> follower = j.follower(stream);
            long last = 0;
            int reads = 0;
            int moved = 0;
            try {
                while (reads < 300 && writer.isAlive()) {
                    final Object followed = follower.get();
                    final Object fresh = j.fresh(stream);
                    final long ceiling = written.get();
                    for (final Object r : new Object[] {followed, fresh}) {
                        assertTrue(j.name + " read " + reads + ": " + r, j.seqOf(r) != Long.MIN_VALUE);
                        assertTrue(j.name + " read " + reads + ": " + r + " went back from " + last,
                                j.seqOf(r) >= last);
                        last = j.seqOf(r);
                    }
                    // what was read was offered: the writer publishes a seq after its offer and offers the next
                    // only after that, so at most the one in flight (ceiling + 1) can be read beyond the count read
                    // after both reads
                    assertTrue(j.name + " read " + reads + ": " + last + " > written " + ceiling + " + 1",
                            last <= ceiling + 1);
                    moved += j.seqOf(followed) > 1 ? 1 : 0;
                    reads++;
                }
            } finally {
                stop.set(true);
                writer.join(20_000);
            }
            assertEquals(null, failed.get());
            assertTrue(j.name + ": reads saw the recording grow, " + moved + " of " + reads, moved > 10);
            java.util.concurrent.locks.LockSupport.parkNanos(10_000_000L);
            assertTrue(j.name + ": the archive raised " + rig.archiveErrors, rig.archiveErrors.isEmpty());
            w.awaitRecorded();
            assertEquals(j.known(written.get(), 1, 1), same("after the writer", j, stream, follower));
            w.stop();
            assertEquals(j.known(written.get(), 1, 0), same("after the writer, stopped", j, stream, follower));
        }
    }

    /**
     * Several recordings of earlier runs and an active one: the tail is the maximum over them, also when the
     * largest journalSeq is in a recording that is not the newest (a run that wrote further and was followed by
     * a run that started lower), and a reader built afterwards agrees.
     */
    @Test
    public void severalRecordingsTheMaximumOverThem() {
        for (final Journal j : journals()) {
            final int stream = nextStream++;
            final Supplier<Object> follower = j.follower(stream);
            rig.writer(stream).append(j.message(5, j.full)).append(j.message(6, j.small)).stop();
            final JournalTailRig.Writer high = rig.writer(stream);
            appendAll(j, high, 100, j.exactTerm());
            high.append(j.message(9_000, j.full)).stop();
            assertEquals(j.known(9_000, 2, 0), same("two stopped recordings", j, stream, follower));
            final JournalTailRig.Writer lower = rig.writer(stream).append(j.message(50, j.full)).awaitRecorded();
            assertEquals("the largest seq is in an older recording", j.known(9_000, 3, 1),
                    same("an active recording below an older one", j, stream, follower));
            lower.append(j.message(9_001, j.small)).stop();
            assertEquals(j.known(9_001, 3, 0), same("the newest passes it", j, stream, follower));
            assertEquals(j.known(9_001, 3, 0), same("a new reader", j, stream, j.follower(stream)));
        }
    }

    /**
     * The order journal ignores a recording whose last record carries another genesis (counted as foreign), next
     * to recordings of the expected one; a recording that switches to the expected genesis counts.
     */
    @Test
    public void anOrderRecordingOfAnotherGenesisIsForeign() {
        final int stream = nextStream++;
        final long other = EPOCH + 1;
        final Supplier<Object> follower = order.follower(stream);
        final JournalTailRig.Writer foreign = rig.writer(stream);
        foreign.append(orderMessage(700, other)).append(orderMessage(701, other)).stop();
        assertEquals(new OrderJournalTail.NoJournal(1, 1), same("only a foreign recording", order, stream, follower));
        rig.writer(stream).append(orderMessage(3, EPOCH)).stop();
        assertEquals("the foreign recording's larger seq does not count", new OrderJournalTail.Known(3, 1, 0, 1),
                same("a foreign and an own recording", order, stream, follower));
        // the last record decides: a recording of the expected genesis whose last record is foreign is foreign
        rig.writer(stream).append(orderMessage(4, EPOCH)).append(orderMessage(900, other)).stop();
        assertEquals(new OrderJournalTail.Known(3, 1, 0, 2), same("a recording ending foreign", order, stream,
                follower));
        // and one that ends on the expected genesis counts, whatever came before it
        rig.writer(stream).append(orderMessage(901, other)).append(orderMessage(5, EPOCH)).stop();
        assertEquals(new OrderJournalTail.Known(5, 2, 0, 2), same("a recording ending own", order, stream, follower));
    }

    private DirectBuffer orderMessage(final long seq, final long epoch) {
        final UnsafeBuffer b = (UnsafeBuffer) order.message(seq, order.full);
        b.putLong(OrderJournalTail.EPOCH_OFFSET, epoch);
        return b;
    }

    /**
     * A purge of whole segments moves a recording's start past what the follower remembered (the end moved into a
     * later term, and the start to that term's base): the next read looks at the recording again from the new start,
     * and agrees with a reader that remembers nothing and with the oracle.
     */
    @Test
    public void aSegmentPurgeMovesTheStartPastTheRememberedMessage() {
        final int stream = nextStream++;
        final Supplier<Object> follower = money.follower(stream);
        final RecordingTail<Long> tail = lastFollowerTail;
        final JournalTailRig.Writer w = rig.writer(stream);
        long next = appendAll(money, w, 1, money.exactTerm());
        next = appendAll(money, w, next, java.util.Arrays.copyOf(money.almostATerm(), 300));
        w.awaitRecorded();
        assertEquals(money.known(next - 1, 1, 1), same("a term and a half", money, stream, follower));
        final long remembered = tail.resumePosition(w.recordingId);
        assertTrue("the follower remembers a position in the second term: " + remembered,
                remembered > TERM && remembered < 2L * TERM);
        // into the third term, then the first two segments go: the start is the third term's base
        next = appendAll(money, w, next, money.almostATerm());
        next = appendAll(money, w, next, java.util.Arrays.copyOf(money.almostATerm(), 100));
        w.awaitRecorded();
        assertTrue("the end is in the third term: " + w.position(), w.position() > 2L * TERM);
        rig.archive().purgeSegments(w.recordingId, 2L * TERM);
        assertEquals(2L * TERM, rig.archive().getStartPosition(w.recordingId));
        final long firstLooks = tail.firstLooks();
        assertEquals(money.known(next - 1, 1, 1), same("after the purge", money, stream, follower));
        assertEquals("the follower looked at the recording again from its new start", firstLooks + 1,
                tail.firstLooks());
        assertTrue("and remembers a position above the new start", tail.resumePosition(w.recordingId) > 2L * TERM);
        w.append(money.message(next, money.full)).stop();
        assertEquals(money.known(next, 1, 0), same("after the purge, stopped", money, stream, follower));

        // the order journal, read the way production reads it, over the same kind of purge
        final int orderStream = nextStream++;
        final JournalTailRig.Writer o = rig.writer(orderStream);
        long seq = appendAll(order, o, 1, order.exactTerm());
        seq = appendAll(order, o, seq, order.exactTerm());
        seq = appendAll(order, o, seq, java.util.Arrays.copyOf(order.exactTerm(), 10));
        o.awaitRecorded();
        rig.archive().purgeSegments(o.recordingId, 2L * TERM);
        assertEquals(order.known(seq - 1, 1, 1), same("order, after a purge", order, orderStream,
                order.follower(orderStream)));
    }

    /**
     * A recording purged whole between two reads: the follower forgets it ({@code retain}) and the answer is over the
     * recordings that are left; when the purged one held the largest seq the tail goes down with it.
     */
    @Test
    public void aRecordingPurgedWholeBetweenReadsIsForgotten() {
        final int stream = nextStream++;
        final Supplier<Object> follower = money.follower(stream);
        final RecordingTail<Long> tail = lastFollowerTail;
        final JournalTailRig.Writer a = rig.writer(stream);
        a.append(money.message(10, money.full)).stop();
        final JournalTailRig.Writer b = rig.writer(stream);
        b.append(money.message(20, money.full)).stop();
        final JournalTailRig.Writer c = rig.writer(stream);
        c.append(money.message(15, money.full)).stop();
        assertEquals(money.known(20, 3, 0), same("three recordings", money, stream, follower));
        assertTrue(tail.resumePosition(b.recordingId) > 0);
        rig.archive().purgeRecording(b.recordingId);
        assertEquals("the tail goes down with the purged recording", money.known(15, 2, 0),
                same("one purged whole", money, stream, follower));
        assertEquals("the follower forgot the purged recording", -1, tail.resumePosition(b.recordingId));
        assertTrue("and kept the others", tail.resumePosition(a.recordingId) > 0
                && tail.resumePosition(c.recordingId) > 0);
        rig.archive().purgeRecording(a.recordingId);
        rig.archive().purgeRecording(c.recordingId);
        assertEquals(money.noJournal(0), same("all purged", money, stream, follower));
        assertEquals(-1, tail.resumePosition(c.recordingId));
    }

    /**
     * The housekeeping keeps its {@code RecordingTail} across a reconnect of its archive client: the reads through
     * the new client resume from what the old one delivered, replay nothing for an end that did not move, and agree
     * with a fresh reader and the oracle.
     */
    @Test
    public void theSharedTailSurvivesAnArchiveReconnect() {
        final int stream = nextStream++;
        final RecordingTail<Long> tail = MoneyJournalTail.newTail();
        final Supplier<Object> follower = () -> new MoneyJournalTail(rig.aeron(), rig.archive(),
                SystemNanoClock.INSTANCE, "aeron:ipc", stream, tail).read(T);
        rig.writer(stream).append(money.message(7, money.full)).stop();
        final JournalTailRig.Writer w = rig.writer(stream);
        long next = appendAll(money, w, 100, java.util.Arrays.copyOf(money.almostATerm(), 200));
        w.awaitRecorded();
        assertEquals(money.known(next - 1, 2, 1), same("before the reconnect", money, stream, follower));
        final long firstLooks = tail.firstLooks();
        final long unchanged = tail.unchanged();
        rig.reconnectArchive();
        assertEquals(money.known(next - 1, 2, 1), same("after the reconnect", money, stream, follower));
        assertEquals("no first look after the reconnect", firstLooks, tail.firstLooks());
        assertTrue("the ends that did not move replayed nothing", tail.unchanged() >= unchanged + 2);
        final long resumed = tail.resumedReads();
        next = appendAll(money, w, next, java.util.Arrays.copyOf(money.almostATerm(), 50));
        w.awaitRecorded();
        assertEquals(money.known(next - 1, 2, 1), same("grown after the reconnect", money, stream, follower));
        assertEquals("resumed from what the old client delivered", resumed + 1, tail.resumedReads());
        assertEquals(firstLooks, tail.firstLooks());
    }
}
