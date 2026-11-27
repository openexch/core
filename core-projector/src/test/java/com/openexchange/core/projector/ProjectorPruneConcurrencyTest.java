// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import org.junit.After;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Age-based retention running against a working projector, on a real archive and a real Postgres. A thread calls
 * {@code money_history_prune_v1} in small batches with a rising cutoff, from the projector's first compare on, while
 * the projector compares the journal's first 3000 records with their stored rows (a re-recorded journal) and inserts
 * 3000 new ones. The delete and the compare meet on the same rows; the projector must never park, the delete must never
 * fail, and what is left is exactly the journal above the mark.
 */
public class ProjectorPruneConcurrencyTest {

    private static final long T0 = 1_700_000_000_000L;
    private static final int FIRST = 3_000;
    private static final int LAST = 6_000;

    private final ProjectorProcesses procs = new ProjectorProcesses();

    @After
    public void cleanUp() throws Exception {
        procs.close();
    }

    @Test
    public void deletesWhileTheProjectorComparesAndInserts() throws Exception {
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", procs.pgBin() != null);
        procs.startPostgres();
        procs.migrate("V003__money_history_retention.sql");
        try (InProcessJournalSource source = new InProcessJournalSource(procs);
             InProcessProjector p = new InProcessProjector(procs, "core", List.of(source.endpoint())).start()) {
            source.newRecording().publish(JournalRecords.range(1, FIRST));
            InProcessProjector.await(() -> p.hasRows(FIRST), 120_000, () -> "first projection: " + p.state.render());

            final AtomicBoolean stop = new AtomicBoolean();
            final AtomicLong deleted = new AtomicLong();
            final AtomicLong calls = new AtomicLong();
            final AtomicReference<Throwable> pruneFailure = new AtomicReference<>();
            final Thread pruner = new Thread(() -> {
                long cutoff = T0;
                try (Connection c = p.dataSource.getConnection(); Statement st = c.createStatement()) {
                    while (!stop.get() && p.state.duplicateRecords == 0) {
                        Thread.onSpinWait(); // start with the compare, not before it
                    }
                    while (!stop.get()) {
                        cutoff += 16; // the records' times are T0 + journalSeq: the delete moves with the compare
                        try (ResultSet rs = st.executeQuery("SELECT journal_deleted FROM money_history_prune_v1("
                                + cutoff + ", 64)")) {
                            rs.next();
                            deleted.addAndGet(rs.getLong(1));
                            calls.incrementAndGet();
                        }
                    }
                } catch (final Throwable t) {
                    pruneFailure.set(t);
                }
            }, "test-pruner");
            pruner.start();
            try {
                // a member restart: the journal is recorded again from its first record, then goes on
                source.newRecording();
                for (int from = 1; from <= LAST; from += 250) {
                    source.publish(JournalRecords.range(from, from + 249));
                }
                InProcessProjector.await(() -> p.state.halted || maxSeq(p) == LAST, 120_000,
                        () -> "the projector did not reach the journal's end: " + p.state.render());
            } finally {
                stop.set(true);
                pruner.join(10_000);
            }
            if (pruneFailure.get() != null) {
                throw new AssertionError("a delete failed next to the projector", pruneFailure.get());
            }
            if (p.state.halted) {
                throw new AssertionError("parked while retention deleted next to it: " + p.state.render());
            }
            assertEquals(0, p.state.payloadConflicts);
            final long through = p.scalar("SELECT through_seq FROM money_journal_pruned");
            assertTrue("the delete ran: " + calls.get() + " calls, mark " + through, through > 0 && deleted.get() == through);
            assertEquals("what is left is the journal above the mark, without a hole", LAST - through, p.rows());
            assertEquals(through + 1, p.scalar("SELECT min(journal_seq) FROM money_journal"));
            assertEquals("the totals hold every deleted deposit and withdrawal", p.scalar(
                            "SELECT count(*) FROM generate_series(1, " + through + ") s WHERE s % 4 IN (0, 1, 2)"),
                    p.scalar("SELECT coalesce(sum(movements), 0) FROM money_journal_pruned_totals"));
            assertTrue("the compare met both stored rows and rows the delete had just removed: " + p.state.render(),
                    p.state.prunedRecords > 0 && p.state.prunedRecords < p.state.duplicateRecords);
            System.out.println("[TEST] prune calls " + calls.get() + ", deleted through " + through + ", compared "
                    + p.state.duplicateRecords + ", accepted as deleted " + p.state.prunedRecords);
        }
        // and a restart replays the whole journal over the pruned history
        try (InProcessJournalSource again = new InProcessJournalSource(procs)) {
            again.newRecording().publish(JournalRecords.range(1, LAST));
            try (InProcessProjector p = new InProcessProjector(procs, "core", List.of(again.endpoint())).start()) {
                InProcessProjector.await(() -> p.state.halted || p.state.duplicateRecords >= LAST, 120_000,
                        () -> "the replay over the pruned history: " + p.state.render());
                Thread.sleep(300);
                if (p.state.halted) {
                    throw new AssertionError("parked on the replay after the concurrent deletes: " + p.state.render());
                }
                assertEquals(p.scalar("SELECT through_seq FROM money_journal_pruned"), p.state.prunedRecords);
            }
        }
    }

    private static long maxSeq(final InProcessProjector p) {
        try {
            return p.scalar("SELECT coalesce(max(journal_seq), 0) FROM money_journal");
        } catch (final Exception e) {
            return -1;
        }
    }
}
