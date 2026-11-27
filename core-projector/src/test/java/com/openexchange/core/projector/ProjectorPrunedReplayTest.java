// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import org.junit.After;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

/**
 * The projector after age-based retention deleted old history (migration V003), on a real archive and a real Postgres:
 * a restart replays the journal from its first record, finds no stored row for the deleted movements and goes on;
 * every other missing row and every differing row still parks it.
 *
 * <p>The journal is {@link JournalRecords#range} 1..8 (cluster time 1_700_000_000_000 + journalSeq; 4 and 8 are opening
 * balances, which carry no time). The delete runs with cutoff "+6", so it removes 1..5 and its mark is
 * (through 5, cutoff +6).
 *
 * <p>Control: {@code -Dprojector.control=pruned-off} runs the agent without the retention mark (the behaviour before it
 * existed); {@link #aReplayAfterADeleteGoesOn} and {@link #aJournalThatStartsInsideOrAboveTheDeletedRowsGoesOn} then
 * fail with {@code parked on a movement retention deleted}, and the other tests fail on the count of accepted records
 * (the projector parks on journalSeq 1, before the row each of them damaged).
 */
public class ProjectorPrunedReplayTest {

    private static final long T0 = 1_700_000_000_000L;

    private final ProjectorProcesses procs = new ProjectorProcesses();

    @After
    public void cleanUp() throws Exception {
        procs.close();
    }

    @Test
    public void aReplayAfterADeleteGoesOn() throws Exception {
        try (InProcessJournalSource source = projectedAndPruned()) {
            try (InProcessProjector p = projector(source).start()) {
                InProcessProjector.await(() -> p.state.halted || p.state.duplicateRecords >= 8, 60_000,
                        () -> "the restart neither replayed the journal nor parked: " + p.state.render());
                source.publish(JournalRecords.record(9));
                InProcessProjector.await(() -> p.state.halted || p.hasRows(4), 60_000,
                        () -> "the next record was not projected: " + p.state.render());
                assertNotParked(p);
                assertEquals("the five deleted movements were accepted without a stored row", 5, p.state.prunedRecords);
                assertEquals(0, p.state.payloadConflicts);
                assertEquals("nothing deleted comes back", "6,7,8,9", seqs());
                assertEquals("the delete's mark is untouched", "5/" + (T0 + 6), mark());
            }
            // and again: the same journal, the same answer
            try (InProcessProjector p = projector(source).start()) {
                InProcessProjector.await(() -> p.state.halted || p.state.duplicateRecords >= 9, 60_000,
                        () -> "the second restart neither replayed the journal nor parked: " + p.state.render());
                assertNotParked(p);
                assertEquals(5, p.state.prunedRecords);
                assertEquals("6,7,8,9", seqs());
            }
        }
    }

    @Test
    public void aJournalThatStartsInsideOrAboveTheDeletedRowsGoesOn() throws Exception {
        try (InProcessJournalSource whole = projectedAndPruned()) {
            // a member whose journal was purged behind the projector: it starts at 4 (inside the deleted rows)
            try (InProcessJournalSource from4 = new InProcessJournalSource(procs)) {
                from4.newRecording().publish(JournalRecords.range(4, 9));
                try (InProcessProjector p = projector(from4).start()) {
                    InProcessProjector.await(() -> p.state.halted || p.hasRows(4), 60_000,
                            () -> "journal from 4: " + p.state.render());
                    assertNotParked(p);
                    assertEquals("4 and 5 have no stored row, 6..8 were compared", 2, p.state.prunedRecords);
                    assertEquals(5, p.state.duplicateRecords);
                }
            }
            // and one that starts at 7, above every deleted row and above the first stored one
            try (InProcessJournalSource from7 = new InProcessJournalSource(procs)) {
                from7.newRecording().publish(JournalRecords.range(7, 10));
                try (InProcessProjector p = projector(from7).start()) {
                    InProcessProjector.await(() -> p.state.halted || p.hasRows(5), 60_000,
                            () -> "journal from 7: " + p.state.render());
                    assertNotParked(p);
                    assertEquals("nothing below the journal's start is read", 0, p.state.prunedRecords);
                    assertEquals(3, p.state.duplicateRecords);
                    assertEquals("6,7,8,9,10", seqs());
                }
            }
        }
    }

    @Test
    public void aMissingRowAboveTheMarkStillParks() throws Exception {
        parks("DELETE FROM money_journal_leg WHERE journal_seq = 7; DELETE FROM money_journal WHERE journal_seq = 7", 7,
                "a row above the mark that retention did not delete");
    }

    @Test
    public void aMissingRowTheMarkNamesButWhoseTimeIsNotBelowTheCutoffStillParks() throws Exception {
        // a mark moved by hand over rows deleted by hand: journalSeq 6 carries cluster time +6, the cutoff is +6
        parks("UPDATE money_journal_pruned SET through_seq = 7; DELETE FROM money_journal_leg WHERE journal_seq IN (6, 7); "
                + "DELETE FROM money_journal WHERE journal_seq IN (6, 7)", 6, "a row at the mark with a time at the cutoff");
    }

    @Test
    public void aMissingRowUnderAMarkThatIsNotRightBelowTheFirstStoredRowStillParks() throws Exception {
        parks("UPDATE money_journal_pruned SET through_seq = 8, max_before_ms = " + (T0 + 100), 1,
                "a deleted row under a mark no delete can have written");
    }

    /**
     * The history is emptied by hand and rebuilt from the journal's first record under the mark of the earlier one.
     * The mark is stale: a row that is then lost below it parks the projector.
     */
    @Test
    public void aLostRowBelowAStaleMarkOfARebuiltHistoryParks() throws Exception {
        try (InProcessJournalSource source = projectedAndPruned()) {
            try (InProcessProjector p = projector(source)) {
                exec(p, "DELETE FROM money_journal_leg; DELETE FROM money_journal");
                p.start();
                InProcessProjector.await(() -> p.state.halted || p.hasRows(8), 60_000,
                        () -> "the rebuild from journalSeq 1: " + p.state.render());
                assertNotParked(p);
                assertEquals("the mark of the earlier history is still there", "5/" + (T0 + 6), mark());
            }
            try (InProcessProjector p = projector(source)) {
                exec(p, "DELETE FROM money_journal_leg WHERE journal_seq = 3; DELETE FROM money_journal WHERE journal_seq = 3");
                p.start();
                InProcessProjector.await(() -> p.state.halted || p.state.duplicateRecords >= 8, 60_000,
                        () -> "the restart neither replayed the journal nor parked: " + p.state.render());
                Thread.sleep(500);
                if (!p.state.halted) {
                    throw new AssertionError("a lost row below a stale mark skipped: " + p.state.render());
                }
                assertEquals("PAYLOAD_CONFLICT", p.state.haltReason);
                assertEquals("the stale mark accepted nothing", 0, p.state.prunedRecords);
            }
        }
    }

    /**
     * A journal of a new genesis read over the pruned database that was not emptied: its opening balances carry no
     * time and sit below the mark, so they are accepted; its first timed record is later than every cutoff and parks.
     * (The money journal's records carry no genesis id, so the time is what tells the two journals apart.)
     */
    @Test
    public void aJournalOfANewGenesisOverThePrunedHistoryParksOnItsFirstTimedRecord() throws Exception {
        try (InProcessJournalSource old = projectedAndPruned();
             InProcessJournalSource genesis = new InProcessJournalSource(procs)) {
            genesis.newRecording().publish(
                    JournalRecords.opening(1, 7_001, 0, 500_000_000L, 4_096),
                    JournalRecords.opening(2, 7_002, 1, 900_000_000L, 4_096),
                    JournalRecords.deposit(3, 7_001, 0, 100_000_000L, 600_000_000L, T0 + 86_400_000L, 4_096),
                    JournalRecords.deposit(4, 7_002, 0, 100_000_000L, 100_000_000L, T0 + 86_400_001L, 8_192));
            try (InProcessProjector p = projector(genesis).start()) {
                InProcessProjector.await(() -> p.state.halted || p.state.duplicateRecords >= 4, 60_000,
                        () -> "the new genesis journal was neither read nor parked on: " + p.state.render());
                Thread.sleep(500);
                if (!p.state.halted) {
                    throw new AssertionError("a journal of another genesis accepted over a pruned history: "
                            + p.state.render());
                }
                assertEquals("PAYLOAD_CONFLICT", p.state.haltReason);
                assertEquals("the two openings (no time) below the mark", 2, p.state.prunedRecords);
                assertEquals("nothing of the other genesis is written", "6,7,8", seqs());
            }
        }
    }

    /** The mark exists and the projector's login may not read it: parked with its own reason, not an epoch per second. */
    @Test
    public void aMarkTheLoginMayNotReadParks() throws Exception {
        try (InProcessJournalSource source = projectedAndPruned()) {
            procs.query("CREATE ROLE other_login LOGIN; GRANT SELECT, INSERT ON money_journal, money_journal_leg TO other_login; "
                    + "SELECT 1");
            try (InProcessProjector p = new InProcessProjector(procs, "core", List.of(source.endpoint()), true, true,
                    ProjectorConfig.DEFAULT_GAP_UNFILLED_PARK_MS, "other_login").start()) {
                InProcessProjector.await(() -> p.state.halted || p.state.epochs > 2, 60_000,
                        () -> "neither parked nor retried: " + p.state.render());
                if (!p.state.halted) {
                    throw new AssertionError("an unreadable retention mark is retried forever: " + p.state.render());
                }
                assertEquals("RETENTION_MARK", p.state.haltReason);
                assertEquals(0, p.state.payloadConflicts);
                assertEquals(1, p.state.epochs);
            }
        }
    }

    @Test
    public void aMissingLegOfAKeptMovementStillParks() throws Exception {
        parks("DELETE FROM money_journal_leg WHERE journal_seq = 7 AND leg_no = 1", 7, "a leg row of a kept movement");
    }

    @Test
    public void aDifferingRowStillParks() throws Exception {
        parks("UPDATE money_journal SET amount = amount + 1 WHERE journal_seq = 6", 6, "a stored amount that differs");
    }

    /**
     * A history another delete cut, adopted by its owner: the last deleted movement (2) and the first kept one (3)
     * share a millisecond. With the cutoff one above that time the replay goes on; with that time itself the deleted
     * movement is not below the cutoff and parks (the cutoff is exclusive).
     */
    @Test
    public void anAdoptedHistoryWhoseLastDeletedAndFirstKeptMovementShareAMillisecond() throws Exception {
        assertEquals("goes on", "", adoptedReplay(T0 + 5 + 1));
    }

    @Test
    public void anAdoptionCutoffAtTheSharedMillisecondParksOnTheDeletedMovement() throws Exception {
        assertEquals("PAYLOAD_CONFLICT", adoptedReplay(T0 + 5));
    }

    /** Project 1..4 (times +1, +5, +5, +9), delete 1 and 2 by hand, adopt with {@code cutoff}, replay: the halt reason. */
    private String adoptedReplay(final long cutoff) throws Exception {
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", procs.pgBin() != null);
        procs.startPostgres();
        try (InProcessJournalSource source = new InProcessJournalSource(procs)) {
            source.newRecording().publish(
                    JournalRecords.deposit(1, 1_001, 1, 100L, 100L, T0 + 1, 4_096),
                    JournalRecords.deposit(2, 1_001, 1, 100L, 200L, T0 + 5, 8_192),
                    JournalRecords.deposit(3, 1_001, 1, 100L, 300L, T0 + 5, 12_288),
                    JournalRecords.deposit(4, 1_001, 1, 100L, 400L, T0 + 9, 16_384));
            try (InProcessProjector p = projector(source).start()) {
                InProcessProjector.await(() -> p.hasRows(4), 60_000, () -> "first projection: " + p.state.render());
            }
            procs.migrate("V003__money_history_retention.sql");
            try (InProcessProjector p = projector(source)) {
                exec(p, "DELETE FROM money_journal_leg WHERE journal_seq <= 2; DELETE FROM money_journal WHERE journal_seq <= 2");
                assertEquals("2", procs.query("SELECT adopted_through FROM money_history_adopt_v1(" + cutoff + ")"));
                p.start();
                InProcessProjector.await(() -> p.state.halted || p.state.duplicateRecords >= 4, 60_000,
                        () -> "the replay over the adopted history: " + p.state.render());
                Thread.sleep(500);
                if (!p.state.halted) {
                    assertEquals("both deleted movements were accepted", 2, p.state.prunedRecords);
                    assertEquals("3,4", seqs());
                }
                return p.state.haltReason;
            }
        }
    }

    /** After the delete, damage the stored history with {@code damage}; the restart must park on {@code atSeq}. */
    private void parks(final String damage, final long atSeq, final String what) throws Exception {
        try (InProcessJournalSource source = projectedAndPruned();
             InProcessProjector p = projector(source)) {
            exec(p, damage);
            final String damaged = p.store.canonicalDump();
            p.start();
            InProcessProjector.await(() -> p.state.halted || p.state.duplicateRecords >= 8, 60_000,
                    () -> "the restart neither replayed the journal nor parked: " + p.state.render());
            Thread.sleep(500);
            if (!p.state.halted) {
                throw new AssertionError("money record with " + what + " skipped: " + p.state.render());
            }
            assertEquals("PAYLOAD_CONFLICT", p.state.haltReason);
            assertEquals(1, p.state.payloadConflicts);
            assertEquals("the records below it that retention deleted were accepted", Math.min(atSeq - 1, 5),
                    p.state.prunedRecords);
            assertEquals("nothing rewritten, nothing projected", damaged, p.store.canonicalDump());
        }
    }

    /** Project 1..8, apply V003 in place, delete with cutoff +6: rows 6, 7, 8 are left and the mark is (5, +6). */
    private InProcessJournalSource projectedAndPruned() throws Exception {
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", procs.pgBin() != null);
        procs.startPostgres();
        final InProcessJournalSource source = new InProcessJournalSource(procs);
        try {
            source.newRecording().publish(JournalRecords.range(1, 8));
            try (InProcessProjector p = projector(source).start()) {
                InProcessProjector.await(() -> p.hasRows(8), 60_000, () -> "first projection: " + p.state.render());
            }
            procs.migrate("V003__money_history_retention.sql");
            assertEquals("5/8", procs.query("SELECT journal_deleted || '/' || legs_deleted FROM money_history_prune_v1("
                    + (T0 + 6) + ", 100)"));
            assertEquals("6,7,8", seqs());
            assertEquals("5/" + (T0 + 6), mark());
            return source;
        } catch (final Exception | AssertionError e) {
            source.close();
            throw e;
        }
    }

    private static void assertNotParked(final InProcessProjector p) {
        if (p.state.halted) {
            throw new AssertionError("parked on a movement retention deleted: " + p.state.render());
        }
    }

    private static void exec(final InProcessProjector p, final String sql) throws Exception {
        try (Connection c = p.dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private String seqs() throws Exception {
        return procs.query("SELECT string_agg(journal_seq::text, ',' ORDER BY journal_seq) FROM money_journal");
    }

    private String mark() throws Exception {
        return procs.query("SELECT through_seq || '/' || max_before_ms FROM money_journal_pruned");
    }

    private InProcessProjector projector(final InProcessJournalSource source) {
        final InProcessProjector p = new InProcessProjector(procs, "core", List.of(source.endpoint()), true, true,
                ProjectorConfig.DEFAULT_GAP_UNFILLED_PARK_MS);
        if ("pruned-off".equals(System.getProperty("projector.control"))) {
            p.agent.withoutPrunedAccept();
        }
        return p;
    }
}
