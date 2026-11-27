// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.journal;

import com.openexchange.cluster.journal.RecordingTail;
import com.openexchange.core.infrastructure.InfrastructureConstants;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The tail read budget: the rule on its own, then many stopped recordings with 4m terms read by the money journal
 * tail reader, whose first looks replay about a megabyte each. With the budget the replayed bytes of any interval
 * one window long stay within {@code 2 * max(budget, one replay)}, and the read takes as many windows as the bytes
 * need; without it (the negative control) the same read replays everything at once. A read whose deadline passes
 * while it waits answers Unknown with the deadline flag, and the next read through the same {@link RecordingTail}
 * resumes where it stopped and equals a fresh full read.
 */
public class TailReadBudgetTest {

    private static final String CHANNEL_4M = "aeron:ipc?term-length=4m";
    private static final int STREAM = InfrastructureConstants.MONEY_JOURNAL_STREAM_ID;
    private static final int RECORDINGS = 16;
    /** 6000 money messages of 109 bytes, a 160-byte frame each: 960000 bytes, so two fit a 2 MiB window, not three. */
    private static final int MESSAGES = 6_000;
    private static final long BUDGET = 2L * 1024 * 1024;
    private static final long WINDOW_MS = 600;

    private JournalTailRig rig;
    private long expectedMax;

    @Before
    public void setUp() {
        rig = new JournalTailRig();
    }

    @After
    public void tearDown() {
        rig.close();
    }

    // ------------------------------------------------------------------ the rule

    @Test
    public void aWindowGrantsItsBudgetThenWaitsForTheNext() {
        final TailReadBudget b = new TailReadBudget(1_000, 300, SystemNanoClock.INSTANCE);
        final long far = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        final long t0 = System.nanoTime();
        assertTrue(b.acquire(600, far));
        assertTrue("fits what is left", b.acquire(400, far));
        assertTrue("nothing to replay is never held", b.acquire(0, System.nanoTime() - 1));
        assertTrue("the next request waits for the next window", b.acquire(1, far));
        final long waitedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        assertTrue("waited about a window: " + waitedMs, waitedMs >= 290);
        // a request larger than the budget is granted alone, as the first of a window
        assertFalse("a full window refuses past the deadline", b.acquire(5_000, System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(50)));
        assertTrue("granted as the next window's first", b.acquire(5_000, far));
        assertEquals(1_000 + 1 + 5_000, b.grantedBytesTotal());
        // an estimate that was too high gives the rest back
        b.settle(5_000, 1_000);
        assertEquals(1_000 + 1 + 1_000, b.grantedBytesTotal());
        assertTrue(b.waitedMsTotal() >= 290);
        assertEquals("budget=1000B/300ms", b.toString());
    }

    @Test
    public void theEstimateIsWhatTheFirstLookOrTheResumeReplays() {
        final int term = 64 * 1024;
        assertEquals("a first look of one term's recording", 1000, TailReadBudget.estimate(0, 1000, term, -1));
        assertEquals("from the base of the term holding the end", 100,
                TailReadBudget.estimate(0, 2L * term + 100, term, -1));
        assertEquals("a remembered position above the floor", 40,
                TailReadBudget.estimate(0, 2L * term + 100, term, 2L * term + 60));
        assertEquals("a remembered position below the floor is not used", 100,
                TailReadBudget.estimate(0, 2L * term + 100, term, term + 60));
        assertEquals("a start above the term base", 50, TailReadBudget.estimate(2L * term + 50, 2L * term + 100,
                term, -1));
        assertEquals(0, TailReadBudget.estimate(10, 10, term, -1));
    }

    // ------------------------------------------------------------------ many recordings, 4m terms

    /** {@link #RECORDINGS} stopped recordings of {@link #MESSAGES} money messages each, journalSeq rising. */
    private void writeRecordings() {
        long seq = 1;
        for (int r = 0; r < RECORDINGS; r++) {
            final JournalTailRig.Writer w = rig.writer(STREAM, CHANNEL_4M);
            for (int i = 0; i < MESSAGES; i++) {
                final UnsafeBuffer m = new UnsafeBuffer(new byte[109]);
                m.putLong(MoneyJournalTail.JOURNAL_SEQ_OFFSET, seq++);
                w.append(m);
            }
            assertEquals(MESSAGES * 160L, w.stop());
        }
        expectedMax = seq - 1;
    }

    /** One read of the whole journal; the budget's granted bytes are sampled every millisecond meanwhile. */
    private record Run(MoneyJournalTail.Result result, long elapsedMs, List<long[]> samples, long replayed,
                       long granted) {
    }

    private Run read(final TailReadBudget budget, final long timeoutNs) throws InterruptedException {
        final RecordingTail<Long> tail = MoneyJournalTail.newTail();
        final List<long[]> samples = new ArrayList<>();
        final AtomicBoolean done = new AtomicBoolean();
        final Thread sampler = new Thread(() -> {
            while (!done.get()) {
                final long[] s = {System.nanoTime(), budget.grantedBytesTotal()};
                synchronized (samples) {
                    samples.add(s);
                }
                java.util.concurrent.locks.LockSupport.parkNanos(1_000_000L);
            }
        }, "tail-budget-sampler");
        sampler.start();
        final long t0 = System.nanoTime();
        final MoneyJournalTail.Result r;
        try {
            r = new MoneyJournalTail(rig.aeron(), rig.archive(), SystemNanoClock.INSTANCE, tail, budget).read(timeoutNs);
        } finally {
            done.set(true);
            sampler.join();
        }
        final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        samples.add(new long[] {System.nanoTime(), budget.grantedBytesTotal()});
        return new Run(r, elapsedMs, samples, tail.replayBytes(), budget.grantedBytesTotal());
    }

    /** The most bytes granted within any interval of one window between two samples. */
    private static long maxPerWindow(final List<long[]> samples) {
        final long windowNs = TimeUnit.MILLISECONDS.toNanos(WINDOW_MS);
        long max = 0;
        int j = 0;
        for (int i = 0; i < samples.size(); i++) {
            if (j < i) {
                j = i;
            }
            while (j + 1 < samples.size() && samples.get(j + 1)[0] - samples.get(i)[0] <= windowNs) {
                j++;
            }
            max = Math.max(max, samples.get(j)[1] - samples.get(i)[1]);
        }
        return max;
    }

    @Test(timeout = 120_000)
    public void manyFirstLooksOnFourMegabyteTermsStayWithinTheBudget() throws InterruptedException {
        writeRecordings();
        final long oneReplay = MESSAGES * 160L;
        final long total = RECORDINGS * oneReplay;

        // the negative control: without a budget the whole journal is replayed within one window
        final Run free = read(new TailReadBudget(Long.MAX_VALUE / 4, WINDOW_MS, SystemNanoClock.INSTANCE),
                TimeUnit.SECONDS.toNanos(60));
        assertEquals(new MoneyJournalTail.Known(expectedMax, RECORDINGS, 0), free.result());
        assertEquals("every recording was a first look of its whole length", total, free.replayed());
        final long freePeak = maxPerWindow(free.samples());
        System.out.println("tail read without a budget: " + free.elapsedMs() + " ms, " + freePeak
                + " bytes in the busiest " + WINDOW_MS + " ms");
        assertTrue("the control replays more than the bound allows within one window: " + freePeak,
                freePeak > 2 * Math.max(BUDGET, oneReplay));

        // with the budget: two recordings per window, so the read needs RECORDINGS / 2 windows
        final TailReadBudget budget = new TailReadBudget(BUDGET, WINDOW_MS, SystemNanoClock.INSTANCE);
        final Run held = read(budget, TimeUnit.SECONDS.toNanos(60));
        assertEquals("the budget changes when the bytes are replayed, never the answer", free.result(),
                held.result());
        assertEquals(total, held.replayed());
        assertEquals("the budget settled at what was actually replayed", total, held.granted());
        final long peak = maxPerWindow(held.samples());
        System.out.println("tail read with " + budget + ": " + held.elapsedMs() + " ms, " + peak
                + " bytes in the busiest " + WINDOW_MS + " ms, waited " + budget.waitedMsTotal() + " ms");
        assertTrue("any interval of one window holds at most two windows' grants: " + peak,
                peak <= 2 * Math.max(BUDGET, oneReplay));
        final long windows = RECORDINGS / 2;
        assertTrue("the read took the windows the bytes need: " + held.elapsedMs() + " ms",
                held.elapsedMs() >= (windows - 1) * WINDOW_MS - 10);
        // most of that time is waiting: a window's own replays take a fraction of it
        assertTrue("and waited for them: " + budget.waitedMsTotal(),
                budget.waitedMsTotal() >= (windows - 1) * WINDOW_MS / 2);
    }

    @Test(timeout = 120_000)
    public void aReadThatRunsOutOfTimeWaitingIsResumedByTheNextAndEqualsAFreshRead() throws InterruptedException {
        writeRecordings();
        final Run fresh = read(TailReadBudget.unbounded(), TimeUnit.SECONDS.toNanos(60));
        assertEquals(new MoneyJournalTail.Known(expectedMax, RECORDINGS, 0), fresh.result());

        final TailReadBudget budget = new TailReadBudget(BUDGET, WINDOW_MS, SystemNanoClock.INSTANCE);
        final RecordingTail<Long> tail = MoneyJournalTail.newTail();
        // about two windows' time: the read answers some recordings, then its deadline passes while it waits
        final MoneyJournalTail first = new MoneyJournalTail(rig.aeron(), rig.archive(), SystemNanoClock.INSTANCE,
                tail, budget);
        final MoneyJournalTail.Result part = first.read(TimeUnit.MILLISECONDS.toNanos(WINDOW_MS + WINDOW_MS / 2));
        assertTrue(String.valueOf(part), part instanceof MoneyJournalTail.Unknown u && u.deadline());
        assertTrue(String.valueOf(part), ((MoneyJournalTail.Unknown) part).reason().contains("tail read budget"));
        final int answered = first.answered();
        assertTrue("part of the recordings were answered: " + answered, answered >= 2 && answered < RECORDINGS);
        final long firstLooks = tail.firstLooks();
        assertEquals("one first look per answered recording", answered, firstLooks);

        // the next read through the same tail: the answered recordings replay nothing, the rest are first looks
        final MoneyJournalTail second = new MoneyJournalTail(rig.aeron(), rig.archive(), SystemNanoClock.INSTANCE,
                tail, budget);
        final MoneyJournalTail.Result all = second.read(TimeUnit.SECONDS.toNanos(60));
        assertEquals("the resumed read equals a fresh full read", fresh.result(), all);
        assertEquals(RECORDINGS, second.answered());
        assertEquals("the answered recordings were not read again", answered, tail.unchanged());
        assertEquals("only the rest were first looks", RECORDINGS, tail.firstLooks());
        assertEquals("every recording replayed once", RECORDINGS * MESSAGES * 160L, tail.replayBytes());
    }

    @Test
    public void aDeadlineFlagOnlyForRunningOutOfTime() {
        assertFalse(new MoneyJournalTail.Unknown("archive down").deadline());
        assertFalse(new OrderJournalTail.Unknown("archive down").deadline());
        assertTrue(TailReadBudget.isDeadline(new TailReadBudget.DeadlineException("x")));
        assertFalse(TailReadBudget.isDeadline(new IllegalStateException("x")));
    }
}
