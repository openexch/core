// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.infrastructure.InfrastructureConstants;
import com.openexchange.core.infrastructure.journal.JournalTailRig;
import com.openexchange.core.infrastructure.journal.MoneyJournalTail;
import com.openexchange.core.infrastructure.journal.TailReadBudget;
import io.aeron.archive.client.AeronArchive;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The restore's money journal tail read under the member's tail read budget: one {@code RecordingTail} per boot, and
 * an attempt that runs out of {@code SHADOW_LOAD_TIMEOUT_MS} after it made progress is read again, resuming, while
 * {@code JOURNAL_TAIL_RESTORE_BUDGET_MS} lasts. The restore then reaches the answer a fresh unbounded read gives; with
 * a restore budget of one attempt (the control) it stays Unknown, as a single read did before.
 */
public class RestoreJournalTailRetryTest {

    private static final int RECORDINGS = 12;
    /** 960000 bytes a recording: two fit the 2 MiB window, so one 1 s attempt answers about four. */
    private static final int MESSAGES = 6_000;

    private JournalTailRig rig;
    private long expectedMax;

    @Before
    public void setUp() {
        rig = new JournalTailRig();
        long seq = 1;
        for (int r = 0; r < RECORDINGS; r++) {
            final JournalTailRig.Writer w = rig.writer(InfrastructureConstants.MONEY_JOURNAL_STREAM_ID,
                    InfrastructureConstants.MONEY_JOURNAL_CHANNEL);
            for (int i = 0; i < MESSAGES; i++) {
                final UnsafeBuffer m = new UnsafeBuffer(new byte[109]);
                m.putLong(MoneyJournalTail.JOURNAL_SEQ_OFFSET, seq++);
                w.append(m);
            }
            w.stop();
        }
        expectedMax = seq - 1;
    }

    @After
    public void tearDown() {
        rig.close();
    }

    private static ShadowRestore restore(final SnapshotModeSettings settings, final SnapshotModeCounters counters) {
        return new ShadowRestore(new ShadowRestore.Inputs(new File("."), new AeronArchive.Context(), 0, 0),
                new CoreLogApplier(() -> { }), settings, counters, new com.openexchange.cluster.shadow.ShadowCounters(),
                new com.openexchange.cluster.shadow.ShadowVerdicts(), ShadowRestore.Faults.testing(), term -> { },
                NoopCoreEventSink.INSTANCE, text -> { }, ShadowRestore.StartupPull.disabled());
    }

    private static SnapshotModeSettings settings(final String restoreBudgetMs) {
        return SnapshotModeSettings.with(Map.of(
                SnapshotModeSettings.Key.SHADOW_LOAD_TIMEOUT_MS, "1000",
                SnapshotModeSettings.Key.JOURNAL_TAIL_READ_BUDGET_BYTES, Long.toString(2L * 1024 * 1024),
                SnapshotModeSettings.Key.JOURNAL_TAIL_READ_WINDOW_MS, "500",
                SnapshotModeSettings.Key.JOURNAL_TAIL_RESTORE_BUDGET_MS, restoreBudgetMs));
    }

    @Test(timeout = 120_000)
    public void aTimedOutReadIsResumedWithinTheRestoreBudgetAndEqualsAFreshRead() {
        final MoneyJournalTail.Result fresh = new MoneyJournalTail(rig.aeron(), rig.archive(),
                SystemNanoClock.INSTANCE).read(TimeUnit.SECONDS.toNanos(60));
        assertEquals(new MoneyJournalTail.Known(expectedMax, RECORDINGS, 0), fresh);

        final SnapshotModeSettings s = settings("30000");
        final SnapshotModeCounters counters = new SnapshotModeCounters();
        final TailReadBudget budget = s.newTailReadBudget();
        final ShadowRestore restore = restore(s, counters).tailReadBudget(budget);
        final MoneyJournalTail.Result r = restore.readJournalTail(rig.aeron(), rig.archive());
        assertEquals("the resumed reads reach what a fresh unbounded read gives", fresh, r);
        assertTrue("it took more than one attempt: " + counters.get(SnapshotModeCounters.C.JOURNAL_TAIL_READ_RETRIES_TOTAL),
                counters.get(SnapshotModeCounters.C.JOURNAL_TAIL_READ_RETRIES_TOTAL) >= 2);
        assertTrue("the duration is exported: " + counters.get(SnapshotModeCounters.C.JOURNAL_TAIL_READ_MS),
                counters.get(SnapshotModeCounters.C.JOURNAL_TAIL_READ_MS) >= 2_000);
        assertEquals("every recording was replayed once over the attempts", RECORDINGS * MESSAGES * 160L,
                budget.grantedBytesTotal());
        assertEquals(expectedMax, counters.get(SnapshotModeCounters.C.JOURNAL_TAIL_SEQ));

        // the second read of the same boot (after a pull) uses the same tail: nothing is replayed again
        final MoneyJournalTail.Result again = restore.readJournalTail(rig.aeron(), rig.archive());
        assertEquals(fresh, again);
        assertEquals(RECORDINGS * MESSAGES * 160L, budget.grantedBytesTotal());
    }

    @Test(timeout = 120_000)
    public void withARestoreBudgetOfOneAttemptTheReadStaysUnknown() {
        final SnapshotModeSettings s = settings("1000");
        final SnapshotModeCounters counters = new SnapshotModeCounters();
        final MoneyJournalTail.Result r = restore(s, counters).tailReadBudget(s.newTailReadBudget())
                .readJournalTail(rig.aeron(), rig.archive());
        assertTrue(String.valueOf(r), r instanceof MoneyJournalTail.Unknown u && u.deadline());
        assertEquals(0, counters.get(SnapshotModeCounters.C.JOURNAL_TAIL_READ_RETRIES_TOTAL));
        assertEquals("SKIP is not taken on it", -1, counters.get(SnapshotModeCounters.C.JOURNAL_TAIL_SEQ));
    }
}
