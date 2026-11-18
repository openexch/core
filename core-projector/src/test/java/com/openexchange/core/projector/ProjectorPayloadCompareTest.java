// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import org.junit.After;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Two projections of one money journal, a changed duplicate, and a version 1 record, on a real archive and a real
 * Postgres.
 *
 * <p>Control: {@code -Dprojector.control=compare-off} runs the agent with the duplicate comparison off (the behaviour
 * before it existed); {@link #aChangedDuplicateHaltsWithPayloadConflict} then fails with
 * {@code conflicting money record skipped}, and the two missing-row tests fail with
 * {@code missing in Postgres skipped}.
 */
public class ProjectorPayloadCompareTest {

    private final ProjectorProcesses procs = new ProjectorProcesses();

    @After
    public void cleanUp() throws Exception {
        procs.close();
    }

    @Test
    public void twoProjectionsOfOneJournalHaveEqualCanonicalDumps() throws Exception {
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", procs.pgBin() != null);
        procs.startPostgres();
        procs.createDatabase("core2");
        try (InProcessJournalSource source = new InProcessJournalSource(procs)) {
            // two recordings, as after a member restart: the second re-records 1..6 byte for byte, then continues
            source.newRecording().publish(JournalRecords.range(1, 6));
            source.newRecording().publish(JournalRecords.range(1, 9));
            final String a;
            try (InProcessProjector p = projector("core", source).start()) {
                InProcessProjector.await(() -> p.hasRows(9), 60_000, () -> "first projection: " + p.state.render());
                a = p.store.canonicalDump();
                assertDumpCoversEveryJournalColumn(p);
            }
            Thread.sleep(20);
            try (InProcessProjector p = projector("core2", source).start()) {
                InProcessProjector.await(() -> p.hasRows(9), 60_000, () -> "second projection: " + p.state.render());
                assertEquals("two projections of one journal: equal canonical dumps", a, p.store.canonicalDump());
                assertTrue("the projections differ only in their insert time", 0 < p.scalar(
                        "SELECT COUNT(*) FROM money_journal WHERE ingested_at IS NOT NULL"));
            }
            assertTrue("the dump carries the log position: " + a,
                    a.contains("-- money_journal journal_seq,log_position,") && a.contains("\n9\t36864\t"));
        }
    }

    @Test
    public void aChangedDuplicateHaltsWithPayloadConflict() throws Exception {
        changedDuplicate(JournalRecords.withdraw(2, 1_002, 1, 10_000_001L, 180_000_000L, 1_700_000_000_002L, 4_096 * 2),
                "an amount");
    }

    @Test
    public void aDuplicateWithAnotherLogPositionHaltsWithPayloadConflict() throws Exception {
        changedDuplicate(JournalRecords.withdraw(2, 1_002, 1, 10_000_000L, 180_000_000L, 1_700_000_000_002L, 4_096 * 2 + 1),
                "the log position");
    }

    @Test
    public void aDuplicateWhoseLegRowIsMissingHaltsWithPayloadConflict() throws Exception {
        missingStoredRow("DELETE FROM money_journal_leg WHERE journal_seq = 3 AND leg_no = 2", "a leg row");
    }

    @Test
    public void aDuplicateWhoseHeaderRowIsMissingHaltsWithPayloadConflict() throws Exception {
        missingStoredRow("DELETE FROM money_journal_leg WHERE journal_seq = 2; "
                + "DELETE FROM money_journal WHERE journal_seq = 2", "the header row");
    }

    @Test
    public void aVersion1RecordIsRefused() throws Exception {
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", procs.pgBin() != null);
        procs.startPostgres();
        try (InProcessJournalSource source = new InProcessJournalSource(procs);
             InProcessProjector p = projector("core", source).start()) {
            source.newRecording().publish(JournalRecords.range(1, 3));
            source.publish(JournalRecords.asVersion1(JournalRecords.record(4)), JournalRecords.record(5));
            InProcessProjector.await(() -> p.state.halted || p.hasRows(5), 60_000,
                    () -> "neither refused nor projected: " + p.state.render());
            Thread.sleep(300);
            if (!p.state.halted) {
                throw new AssertionError("money journal version 1 record projected: " + p.state.render());
            }
            assertEquals("VERSION", p.state.haltReason);
            assertEquals(1, p.state.versionRefusals);
            assertEquals("everything before the refused record is projected, nothing past it", 3, p.rows());
        }
    }

    private void changedDuplicate(final byte[] changed2, final String what) throws Exception {
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", procs.pgBin() != null);
        procs.startPostgres();
        try (InProcessJournalSource source = new InProcessJournalSource(procs);
             InProcessProjector p = projector("core", source).start()) {
            source.newRecording().publish(JournalRecords.range(1, 4));
            InProcessProjector.await(() -> p.hasRows(4), 60_000, () -> "first recording: " + p.state.render());
            final String before = p.store.canonicalDump();
            // the next recording re-records 1..4 with journalSeq 2 changed (one field), then a new record 5
            final byte[][] next = JournalRecords.range(1, 5);
            next[1] = changed2;
            source.newRecording().publish(next);
            InProcessProjector.await(() -> p.state.halted || p.hasRows(5), 60_000,
                    () -> "neither halted nor projected the next record: " + p.state.render());
            Thread.sleep(500);
            if (!p.state.halted) {
                throw new AssertionError("conflicting money record skipped (" + what + " of journalSeq 2 changed): "
                        + p.state.render());
            }
            assertEquals("PAYLOAD_CONFLICT", p.state.haltReason);
            assertEquals(1, p.state.payloadConflicts);
            assertEquals("nothing changed, nothing past the conflict projected", before, p.store.canonicalDump());
        }
    }

    /** Project 1..4, delete stored rows below the high-water, restart on the same journal: the replay finds them. */
    private void missingStoredRow(final String delete, final String what) throws Exception {
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", procs.pgBin() != null);
        procs.startPostgres();
        try (InProcessJournalSource source = new InProcessJournalSource(procs)) {
            source.newRecording().publish(JournalRecords.range(1, 4));
            try (InProcessProjector p = projector("core", source).start()) {
                InProcessProjector.await(() -> p.hasRows(4), 60_000, () -> "first projection: " + p.state.render());
            }
            final String damaged;
            try (InProcessProjector p = projector("core", source)) {
                try (Connection c = p.dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(delete)) {
                    ps.execute();
                }
                damaged = p.store.canonicalDump();
                p.start();
                InProcessProjector.await(() -> p.state.halted || p.state.duplicateRecords >= 4, 60_000,
                        () -> "the restart replayed neither the journal nor parked: " + p.state.render());
                Thread.sleep(500);
                if (!p.state.halted) {
                    throw new AssertionError("money record with " + what + " missing in Postgres skipped: "
                            + p.state.render());
                }
                assertEquals("PAYLOAD_CONFLICT", p.state.haltReason);
                assertEquals(1, p.state.payloadConflicts);
                assertEquals("nothing rewritten, nothing projected", damaged, p.store.canonicalDump());
            }
        }
    }

    private static InProcessProjector projector(final String database, final InProcessJournalSource source) {
        final boolean compare = !"compare-off".equals(System.getProperty("projector.control"));
        return new InProcessProjector(source.procs, database, List.of(source.endpoint()), compare, true,
                ProjectorConfig.DEFAULT_GAP_UNFILLED_PARK_MS);
    }

    /** Every column of both tables is in the canonical dump, or is a declared wall-clock column. */
    private static void assertDumpCoversEveryJournalColumn(final InProcessProjector p) throws Exception {
        for (final String table : PostgresMoneyJournalStore.CANONICAL_DUMP.keySet()) {
            final List<String> columns = new ArrayList<>();
            try (Connection c = p.dataSource.getConnection();
                 PreparedStatement ps = c.prepareStatement("SELECT column_name FROM information_schema.columns "
                         + "WHERE table_name = ? ORDER BY ordinal_position")) {
                ps.setString(1, table);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        columns.add(rs.getString(1));
                    }
                }
            }
            final List<String> covered = new ArrayList<>(PostgresMoneyJournalStore.CANONICAL_DUMP.get(table));
            covered.addAll(PostgresMoneyJournalStore.WALL_CLOCK_COLUMNS.get(table));
            assertEquals(table + ": dumped plus wall-clock columns are every column", columns.stream().sorted().toList(),
                    covered.stream().sorted().toList());
        }
    }
}
