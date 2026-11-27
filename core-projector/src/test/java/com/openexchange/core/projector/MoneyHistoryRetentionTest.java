// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Migration V003 on a throwaway Postgres: applied in place to a database that holds history (V001 and V002 with
 * rows), then {@code money_history_prune_v1} itself: what one call deletes and keeps, its batch limit, a second call
 * over its own result, the refusals (a hole, a missing leg, a first row that is not right above the mark, a cutoff in
 * the future), the mark it writes, the totals of what it deleted (every sum of the history stays the same), the reset
 * on TRUNCATE, the adoption of a history another delete cut, the pruner role's rights, and what the balance movements
 * view shows afterwards.
 */
public class MoneyHistoryRetentionTest {

    private static final String V003 = "V003__money_history_retention.sql";
    private static final long A = 11L;
    private static final long B = 22L;

    private ProjectorProcesses procs;
    private HikariDataSource core;
    private PostgresMoneyJournalStore store;

    @Before
    public void setUp() throws Exception {
        procs = new ProjectorProcesses();
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", procs.pgBin() != null);
        procs.startPostgres();
        procs.migrate("V002__balance_movements_view.sql");
        final HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(procs.jdbcUrl("core"));
        cfg.setUsername("core");
        cfg.setMaximumPoolSize(3);
        core = new HikariDataSource(cfg);
        store = new PostgresMoneyJournalStore(core);
    }

    @After
    public void tearDown() throws Exception {
        if (core != null) {
            core.close();
        }
        if (procs != null) {
            procs.close();
        }
    }

    /** Two openings, then timed movements: 3..5 old (1000..1200), 6 and 7 recent (5000, 5100). */
    private void seedHistory() throws Exception {
        store.project(List.of(
                MoneyJournalRecord.opening(1, A, 0, 1_000L, 100L),
                MoneyJournalRecord.opening(2, B, 1, 9L, 100L),
                MoneyJournalRecord.deposit(3, A, 1, 5L, 5L, 1_000L, 100L),
                MoneyJournalRecord.settle(4, 77L, 1, 100L, 2L, A, B, true, 7L, 800L, 7L, 200L, 1_100L, 200L),
                MoneyJournalRecord.withdraw(5, A, 0, 50L, 750L, 1_200L, 300L),
                MoneyJournalRecord.deposit(6, B, 0, 10L, 210L, 5_000L, 400L),
                MoneyJournalRecord.settle(7, 78L, 1, 100L, 1L, A, B, true, 8L, 650L, 6L, 310L, 5_100L, 500L)));
    }

    @Test
    public void appliedInPlaceToADatabaseWithHistoryUnderOneShortLock() throws Exception {
        seedHistory();
        final String before = store.canonicalDump();
        final String sql;
        try (var in = getClass().getResourceAsStream("/db/migration/" + V003)) {
            sql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        try (Connection migration = core.getConnection()) {
            migration.setAutoCommit(false);
            try (Statement st = migration.createStatement()) {
                st.execute(sql);
            }
            // the locks the open migration transaction holds on the two history tables
            final List<String> locks = new ArrayList<>();
            try (Statement st = migration.createStatement(); ResultSet rs = st.executeQuery(
                    "SELECT c.relname || ':' || l.mode FROM pg_locks l JOIN pg_class c ON c.oid = l.relation "
                            + "WHERE l.pid = pg_backend_pid() AND c.relname IN ('money_journal', 'money_journal_leg') "
                            + "ORDER BY 1")) {
                while (rs.next()) {
                    locks.add(rs.getString(1));
                }
            }
            assertEquals("one lock, on money_journal, that lets reads through", List.of("money_journal:ShareRowExclusiveLock"),
                    locks);
            try (Connection other = core.getConnection(); Statement st = other.createStatement()) {
                st.execute("SET lock_timeout = '300ms'");
                try (ResultSet rs = st.executeQuery("SELECT count(*) FROM gateway_balance_movements_v1")) {
                    assertTrue(rs.next());
                    assertEquals("reads go on while the migration is open", 13, rs.getInt(1));
                }
                try {
                    st.execute("INSERT INTO money_journal (journal_seq, log_position, movement_type) VALUES (8, 600, 1)");
                    fail("an insert did not wait for the open migration");
                } catch (final SQLException e) {
                    assertEquals("the projector's insert waits (here: its lock timeout): " + e.getMessage(), "55P03",
                            e.getSQLState());
                } finally {
                    st.execute("RESET lock_timeout");
                }
            }
            migration.commit();
            migration.setAutoCommit(true);
        }
        assertEquals("the migration rewrites no row", before, store.canonicalDump());
        assertEquals("0/0/true", procs.query("SELECT through_seq || '/' || max_before_ms || '/' || (updated_at IS NULL) "
                + "FROM money_journal_pruned"));
        assertEquals("nothing was deleted yet", PostgresMoneyJournalStore.PrunedMark.NONE, store.prunedMark());
        // and it works on the rows that were there before it
        assertEquals("5/8", prune(2_000L, 100));
        assertEquals(List.of(6L, 7L), seqs());
    }

    @Test
    public void deletesTheOldestInBatchesKeepsTheNewestAndIsANoOpOverItsOwnResult() throws Exception {
        seedHistory();
        procs.migrate(V003);
        assertEquals("the two openings are as old as the first timed movement", "2/2", prune(2_000L, 2));
        assertEquals("2/2000", mark());
        assertEquals("the batch limit: two movements, a deposit's leg and a settle's four", "2/5", prune(2_000L, 2));
        assertEquals("4/2000", mark());
        assertEquals("1/1", prune(2_000L, 100));
        assertEquals("5/2000", mark());
        assertEquals(List.of(6L, 7L), seqs());
        assertEquals("nothing older is left", "0/0", prune(2_000L, 100));
        assertEquals("a call that deletes nothing leaves the mark", "5/2000", mark());
        assertEquals("a lower cutoff deletes nothing", "0/0", prune(500L, 100));
        // everything is old now: the newest movement still stays
        assertEquals("1/1", prune(1_000_000L, 100));
        assertEquals(List.of(7L), seqs());
        assertEquals("6/1000000", mark());
        assertEquals("0/0", prune(1_000_000L, 100));
        assertEquals("the projector's resume point is where it was", 7L, store.highWater());
        assertEquals(4L, scalar("SELECT count(*) FROM money_journal_leg"));
        assertEquals(new PostgresMoneyJournalStore.PrunedMark(6, 1_000_000L), store.prunedMark());
        assertEquals("a zero batch deletes nothing", "0/0", prune(1_000_000L, 0));
    }

    @Test
    public void stopsAtTheFirstMovementThatIsNotOldEvenWhenALaterOneIs() throws Exception {
        store.project(List.of(
                MoneyJournalRecord.deposit(1, A, 0, 5L, 5L, 1_000L, 100L),
                MoneyJournalRecord.deposit(2, A, 0, 5L, 10L, 9_000L, 200L),
                MoneyJournalRecord.deposit(3, A, 0, 5L, 15L, 1_000L, 300L), // a time that went back
                MoneyJournalRecord.deposit(4, A, 0, 5L, 20L, 9_500L, 400L)));
        procs.migrate(V003);
        assertEquals("1/1", prune(2_000L, 100));
        assertEquals(List.of(2L, 3L, 4L), seqs());
        assertEquals("0/0", prune(2_000L, 100));
    }

    @Test
    public void openingBalancesStayWhileTheNextTimedMovementIsNotOld() throws Exception {
        store.project(List.of(
                MoneyJournalRecord.opening(1, A, 0, 1_000L, 100L),
                MoneyJournalRecord.opening(2, B, 1, 9L, 100L),
                MoneyJournalRecord.deposit(3, A, 1, 5L, 5L, 5_000L, 100L),
                MoneyJournalRecord.deposit(4, A, 1, 5L, 10L, 6_000L, 200L)));
        procs.migrate(V003);
        assertEquals("0/0", prune(2_000L, 100));
        assertEquals("a batch that ends inside the openings", "0/0", prune(2_000L, 1));
        assertEquals("0/0", mark());
        assertEquals("an opening goes once the next timed movement is old", "1/1", prune(5_500L, 1));
        assertEquals("1/5500", mark());
        assertEquals("2/2", prune(5_500L, 100));
        assertEquals("3/5500", mark());
        assertEquals(List.of(4L), seqs());
    }

    @Test
    public void refusesAroundAHoleAMissingLegAndAMissingFirstRow() throws Exception {
        seedHistory();
        procs.migrate(V003);
        final String whole = store.canonicalDump();

        exec("DELETE FROM money_journal_leg WHERE journal_seq = 4 AND leg_no = 2");
        final String legGone = store.canonicalDump();
        refused("a settle with three legs", 2_000L, 100, "journal_seq 4 does not have exactly its legs");
        assertEquals("nothing deleted", legGone, store.canonicalDump());
        assertEquals("0/0", mark());
        assertEquals("the run below the damaged movement is deleted by a batch that ends before it", "3/3",
                prune(2_000L, 3));

        exec("DELETE FROM money_journal_leg WHERE journal_seq = 4; DELETE FROM money_journal WHERE journal_seq = 4");
        refused("the first stored row is not right above the mark", 2_000L, 100, "the first stored journal_seq is 5, expected 4");
        assertEquals("3/2000", mark());
        assertEquals(List.of(5L, 6L, 7L), seqs());

        // a hole inside the run
        exec("TRUNCATE money_journal_leg, money_journal");
        seedHistory();
        assertEquals("the same rows again", whole, store.canonicalDump());
        exec("DELETE FROM money_journal_leg WHERE journal_seq = 3; DELETE FROM money_journal WHERE journal_seq = 3");
        refused("a hole", 2_000L, 100, "a hole");
        assertEquals("0/0", mark());

        refused("a cutoff in the future", Long.MAX_VALUE / 2, 100, "in the future");
    }

    @Test
    public void truncateResetsTheMarkAndADeleteByAnyoneElseDoesNotMoveIt() throws Exception {
        seedHistory();
        procs.migrate(V003);
        assertEquals("5/8", prune(2_000L, 100));
        assertEquals("5/2000", mark());
        exec("DELETE FROM money_journal_leg WHERE journal_seq = 6; DELETE FROM money_journal WHERE journal_seq = 6");
        assertEquals("only the function writes the mark", "5/2000", mark());
        exec("TRUNCATE money_journal_leg, money_journal");
        assertEquals("0/0", mark());
        assertEquals(PostgresMoneyJournalStore.PrunedMark.NONE, store.prunedMark());
    }

    @Test
    public void theMarkAnswersOnlyWhileTheFirstStoredRowIsRightAboveIt() throws Exception {
        seedHistory();
        procs.migrate(V003);
        assertEquals("5/8", prune(2_000L, 100));
        assertEquals(new PostgresMoneyJournalStore.PrunedMark(5, 2_000L), store.prunedMark());
        exec("UPDATE money_journal_pruned SET through_seq = 4");
        assertEquals("a mark below the first stored row", PostgresMoneyJournalStore.PrunedMark.NONE, store.prunedMark());
        exec("UPDATE money_journal_pruned SET through_seq = 6");
        assertEquals("a mark over a stored row", PostgresMoneyJournalStore.PrunedMark.NONE, store.prunedMark());
        refused("a stored row at the mark", 1_000_000L, 100, "journal_seq 6 is stored at or below the mark 6");
        exec("UPDATE money_journal_pruned SET through_seq = 5; "
                + "DELETE FROM money_journal_leg; DELETE FROM money_journal");
        assertEquals("an emptied history", PostgresMoneyJournalStore.PrunedMark.NONE, store.prunedMark());
    }

    @Test
    public void theTotalsKeepEverySumOfTheHistoryTheSameAcrossDeletes() throws Exception {
        seedHistory();
        procs.migrate(V003);
        final String sums = "SELECT string_agg(t || ':' || a || ':' || s || ':' || n, ',' ORDER BY t, a) FROM ("
                + "SELECT t, a, sum(amt) AS s, sum(n) AS n FROM ("
                + "SELECT movement_type AS t, asset_id AS a, amount::numeric AS amt, 1 AS n FROM money_journal "
                + "WHERE movement_type IN (1, 2, 3) "
                + "UNION ALL SELECT movement_type, asset_id, amount, movements FROM money_journal_pruned_totals) u "
                + "GROUP BY t, a) x";
        final String whole = procs.query(sums);
        assertEquals("openings, deposits and the withdrawal of the seeded history",
                "1:0:1000:1,1:1:9:1,2:0:10:1,2:1:5:1,3:0:50:1", whole);
        assertEquals("nothing deleted yet", "", totals());
        assertEquals("2/2", prune(2_000L, 2));
        assertEquals("after the first batch", whole, procs.query(sums));
        assertEquals("the deleted openings are in the totals", "1:0:1000:1,1:1:9:1", totals());
        assertEquals("3/6", prune(2_000L, 100));
        assertEquals("after the second batch", whole, procs.query(sums));
        assertEquals("1:0:1000:1,1:1:9:1,2:1:5:1,3:0:50:1", totals());
        assertEquals("one settle deleted, none of its amounts summed", "1/0", procs.query(
                "SELECT settles || '/' || totals_unknown_through FROM money_journal_pruned"));
        assertEquals("1/1", prune(1_000_000L, 100));
        assertEquals("after the last batch", whole, procs.query(sums));
        assertEquals("1:0:1000:1,1:1:9:1,2:0:10:1,2:1:5:1,3:0:50:1", totals());
        // a second deposit of an asset adds to its row, exactly, beyond what a BIGINT sum holds
        exec("TRUNCATE money_journal_leg, money_journal");
        assertEquals("a rebuild starts with no totals", "", totals());
        store.project(List.of(
                MoneyJournalRecord.deposit(1, A, 0, Long.MAX_VALUE, Long.MAX_VALUE, 1_000L, 100L),
                MoneyJournalRecord.deposit(2, B, 0, Long.MAX_VALUE, Long.MAX_VALUE, 1_001L, 200L),
                MoneyJournalRecord.deposit(3, B, 0, 7L, 7L, 1_002L, 300L),
                MoneyJournalRecord.deposit(4, B, 0, 1L, 8L, 9_000L, 400L)));
        assertEquals("1/1", prune(2_000L, 1));
        assertEquals("2/2", prune(2_000L, 100));
        assertEquals("2:0:18446744073709551621:3", totals());
    }

    @Test
    public void whatTheHistoryStartsWithAfterACall() throws Exception {
        seedHistory();
        procs.migrate(V003);
        final String kept = "SELECT journal_deleted || ' ' || first_kept_seq || ' ' || coalesce(first_kept_ms::text, '-') "
                + "|| ' ' || newest_seq FROM money_history_prune_v1(%d, %d)";
        assertEquals("nothing old enough: the openings wait for the first timed movement", "0 1 1000 7",
                procs.query(String.format(kept, 500L, 100)));
        assertEquals("2 3 1000 7", procs.query(String.format(kept, 2_000L, 2)));
        assertEquals("3 6 5000 7", procs.query(String.format(kept, 2_000L, 100)));
        assertEquals("0 6 5000 7", procs.query(String.format(kept, 2_000L, 100)));
        assertEquals("a zero batch still reports", "0 6 5000 7", procs.query(String.format(kept, 2_000L, 0)));
    }

    @Test
    public void aHistoryAnotherDeleteCutIsAdoptedByItsOwnerOnly() throws Exception {
        seedHistory();
        procs.migrate(V003);
        exec("CREATE ROLE retention_login LOGIN; GRANT oe_history_pruner TO retention_login");
        // what a delete that knew nothing of the mark left: rows from 3, the mark at 0
        exec("DELETE FROM money_journal_leg WHERE journal_seq <= 2; DELETE FROM money_journal WHERE journal_seq <= 2");
        refused("a history that starts above the mark", 2_000L, 100, "see money_history_adopt_v1");
        try (Connection c = DriverManager.getConnection(procs.jdbcUrl("core"), "retention_login", "");
             Statement st = c.createStatement()) {
            st.execute("SELECT * FROM money_history_adopt_v1(1500)");
            fail("the pruner role can adopt");
        } catch (final SQLException e) {
            assertEquals(e.getMessage(), "42501", e.getSQLState());
        }
        adoptRefused("a cutoff in the future", Long.MAX_VALUE / 2, "must be a past time");
        exec("DELETE FROM money_journal_leg WHERE journal_seq = 5; DELETE FROM money_journal WHERE journal_seq = 5");
        adoptRefused("a hole among the kept rows", 1_500L, "(a hole)");
        exec("TRUNCATE money_journal_leg, money_journal");
        seedHistory();
        exec("DELETE FROM money_journal_leg WHERE journal_seq <= 2; DELETE FROM money_journal WHERE journal_seq <= 2; "
                + "DELETE FROM money_journal_leg WHERE journal_seq = 4 AND leg_no = 3");
        adoptRefused("a kept settle with three legs", 1_500L, "journal_seq 4 has an unknown type or not exactly its legs");
        exec("INSERT INTO money_journal_leg (journal_seq, leg_no, user_id, asset_role, balance_after) VALUES (4, 3, 22, 1, 200)");
        assertEquals("2/5", procs.query("SELECT adopted_through || '/' || movements_kept FROM money_history_adopt_v1(1500)"));
        assertEquals("the mark is right below the first stored row, and the totals below it are unknown", "2/1500/2",
                procs.query("SELECT through_seq || '/' || max_before_ms || '/' || totals_unknown_through "
                        + "FROM money_journal_pruned"));
        assertEquals(new PostgresMoneyJournalStore.PrunedMark(2, 1_500L), store.prunedMark());
        assertEquals("", totals());
        adoptRefused("a second adoption", 1_500L, "the mark is already at journal_seq 2");
        // retention goes on from there, with totals for what it deletes itself
        assertEquals("3/6", prune(2_000L, 100));
        assertEquals("5/2000/2", procs.query("SELECT through_seq || '/' || max_before_ms || '/' || totals_unknown_through "
                + "FROM money_journal_pruned"));
        assertEquals("2:1:5:1,3:0:50:1", totals());
        // a history that starts at 1 has nothing to adopt
        exec("TRUNCATE money_journal_leg, money_journal");
        seedHistory();
        adoptRefused("a whole history", 1_500L, "starts at journal_seq 1");
    }

    @Test
    public void thePrunerRoleCallsTheFunctionAndTouchesNoTable() throws Exception {
        seedHistory();
        procs.migrate(V003);
        exec("CREATE ROLE retention_login LOGIN; GRANT oe_history_pruner TO retention_login; "
                + "CREATE ROLE stranger_login LOGIN");
        try (Connection c = DriverManager.getConnection(procs.jdbcUrl("core"), "retention_login", "");
             Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT journal_deleted, legs_deleted FROM money_history_prune_v1(2000, 2)")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getLong(1));
            }
            for (final String sql : List.of("SELECT 1 FROM money_journal LIMIT 1", "SELECT 1 FROM money_journal_leg LIMIT 1",
                    "SELECT 1 FROM money_journal_pruned", "DELETE FROM money_journal WHERE journal_seq = 7",
                    "UPDATE money_journal_pruned SET through_seq = 6", "TRUNCATE money_journal_leg, money_journal",
                    "SELECT 1 FROM money_journal_pruned_totals", "SELECT * FROM money_history_adopt_v1(1500)")) {
                try {
                    st.execute(sql);
                    fail("the pruner role can run: " + sql);
                } catch (final SQLException e) {
                    assertEquals(sql + ": " + e.getMessage(), "42501", e.getSQLState());
                }
            }
        }
        try (Connection c = DriverManager.getConnection(procs.jdbcUrl("core"), "stranger_login", "");
             Statement st = c.createStatement()) {
            st.execute("SELECT * FROM money_history_prune_v1(2000, 2)");
            fail("a role outside oe_history_pruner can call the delete");
        } catch (final SQLException e) {
            assertEquals(e.getMessage(), "42501", e.getSQLState());
        }
        assertEquals("the adoption is granted to nobody", "", procs.query("SELECT coalesce(string_agg(grantee, ','), '') "
                + "FROM information_schema.routine_privileges WHERE routine_name = 'money_history_adopt_v1' "
                + "AND grantee <> 'core'"));
        assertEquals("the function is granted to the group only",
                "oe_history_pruner", procs.query("SELECT string_agg(grantee, ',' ORDER BY grantee) "
                        + "FROM information_schema.routine_privileges WHERE routine_name = 'money_history_prune_v1' "
                        + "AND grantee <> 'core'"));
    }

    @Test
    public void theMovementsViewAfterADelete() throws Exception {
        seedHistory();
        procs.migrate(V003);
        assertEquals("B's quote leg of settle 7 follows B's deposit 6", "100",
                procs.query("SELECT delta FROM gateway_balance_movements_v1 WHERE journal_seq = 7 AND leg_no = 3"));
        assertEquals("A's base leg of settle 7 follows A's leg of settle 4", "1",
                procs.query("SELECT delta FROM gateway_balance_movements_v1 WHERE journal_seq = 7 AND leg_no = 0"));
        assertEquals("5/8", prune(2_000L, 100));
        assertEquals("only the kept movements are listed", "6,7,7,7,7", procs.query(
                "SELECT string_agg(journal_seq::text, ',' ORDER BY journal_seq, leg_no) FROM gateway_balance_movements_v1"));
        assertEquals("the earlier leg is kept: the delta is what it was", "100",
                procs.query("SELECT delta FROM gateway_balance_movements_v1 WHERE journal_seq = 7 AND leg_no = 3"));
        assertEquals("the earlier leg is gone: no delta, never a guess", "",
                procs.query("SELECT delta FROM gateway_balance_movements_v1 WHERE journal_seq = 7 AND leg_no = 0"));
        assertEquals("the balance after is the journal's, with or without the earlier leg", "8",
                procs.query("SELECT balance_after FROM gateway_balance_movements_v1 WHERE journal_seq = 7 AND leg_no = 0"));
    }

    private String prune(final long beforeMs, final int maxRows) throws Exception {
        return procs.query("SELECT journal_deleted || '/' || legs_deleted FROM money_history_prune_v1(" + beforeMs + ", "
                + maxRows + ")");
    }

    private void refused(final String what, final long beforeMs, final int maxRows, final String messagePart)
            throws Exception {
        try (Connection c = core.getConnection(); Statement st = c.createStatement()) {
            st.execute("SELECT * FROM money_history_prune_v1(" + beforeMs + ", " + maxRows + ")");
            fail("the delete ran over " + what);
        } catch (final SQLException e) {
            assertTrue(what + ": " + e.getMessage(), e.getMessage().contains("money history retention refused")
                    && e.getMessage().contains(messagePart));
        }
    }

    private void adoptRefused(final String what, final long maxBeforeMs, final String messagePart) throws Exception {
        try (Connection c = core.getConnection(); Statement st = c.createStatement()) {
            st.execute("SELECT * FROM money_history_adopt_v1(" + maxBeforeMs + ")");
            fail("the adoption ran over " + what);
        } catch (final SQLException e) {
            assertTrue(what + ": " + e.getMessage(), e.getMessage().contains("money history adoption refused")
                    && e.getMessage().contains(messagePart));
        }
    }

    private String totals() throws Exception {
        return procs.query("SELECT coalesce(string_agg(movement_type || ':' || asset_id || ':' || amount || ':' || movements, "
                + "',' ORDER BY movement_type, asset_id), '') FROM money_journal_pruned_totals");
    }

    private String mark() throws Exception {
        return procs.query("SELECT through_seq || '/' || max_before_ms FROM money_journal_pruned");
    }

    private List<Long> seqs() throws Exception {
        final List<Long> out = new ArrayList<>();
        try (Connection c = core.getConnection(); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT journal_seq FROM money_journal ORDER BY journal_seq")) {
            while (rs.next()) {
                out.add(rs.getLong(1));
            }
        }
        return out;
    }

    private long scalar(final String sql) throws Exception {
        try (Connection c = core.getConnection(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private void exec(final String sql) throws Exception {
        try (Connection c = core.getConnection(); Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }
}
