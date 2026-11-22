// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * gateway_balance_movements_v1 (V002) over a throwaway Postgres with V001 and V002 applied, rows written by the
 * projector's own store: one row per leg of the user, the kind of each movement, the settle legs' assets resolved
 * through money_market_asset, the delta of a settle or opening leg derived from the same user's previous leg of the
 * same asset, and the reader role able to read that view and nothing else.
 */
public class BalanceMovementsViewTest {

    private static final long A = 11L;
    private static final long B = 22L;
    private static final int USD = 0;
    private static final int BTC = 1;

    private ProjectorProcesses procs;
    private HikariDataSource core;

    @Before
    public void setUp() throws Exception {
        procs = new ProjectorProcesses();
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", procs.pgBin() != null);
        procs.startPostgres();
        procs.migrate("V002__balance_movements_view.sql");
        final HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(procs.jdbcUrl("core"));
        cfg.setUsername("core");
        cfg.setMaximumPoolSize(2);
        core = new HikariDataSource(cfg);
        final PostgresMoneyJournalStore store = new PostgresMoneyJournalStore(core);
        store.project(List.of(
                MoneyJournalRecord.opening(1, A, USD, 1_000L, 100L),
                MoneyJournalRecord.deposit(2, A, BTC, 5L, 5L, 1_000L, 200L),
                MoneyJournalRecord.deposit(3, B, BTC, 5L, 5L, 1_100L, 300L),
                // A buys 2 BTC from B for 200 USD on market 1 (BTC-USD)
                MoneyJournalRecord.settle(4, 77L, 1, 100L, 2L, A, B, true, 7L, 800L, 3L, 200L, 1_200L, 400L),
                MoneyJournalRecord.withdraw(5, A, USD, 50L, 750L, 1_300L, 500L)));
        try (Connection c = core.getConnection()) {
            c.createStatement().execute("CREATE ROLE gw_money_login LOGIN; GRANT oe_money_reader TO gw_money_login");
        }
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

    @Test
    public void movementsOfEachUserWithResolvedAssetsAndDeltas() throws Exception {
        try (Connection reader = DriverManager.getConnection(procs.jdbcUrl("core"), "gw_money_login", "")) {
            assertEquals(List.of(
                            "5/0 withdrawal asset 0 delta -50 after 750 at 1300 pos 500 trade null market null",
                            "4/1 settle asset 0 delta -200 after 800 at 1200 pos 400 trade 77 market 1",
                            "4/0 settle asset 1 delta 2 after 7 at 1200 pos 400 trade 77 market 1",
                            "2/0 deposit asset 1 delta 5 after 5 at 1000 pos 200 trade null market null",
                            "1/0 opening asset 0 delta 1000 after 1000 at null pos 100 trade null market null"),
                    movements(reader, A));
            assertEquals(List.of(
                            "4/3 settle asset 0 delta null after 200 at 1200 pos 400 trade 77 market 1",  // no earlier leg survives: no guess
                            "4/2 settle asset 1 delta -2 after 3 at 1200 pos 400 trade 77 market 1",
                            "3/0 deposit asset 1 delta 5 after 5 at 1100 pos 300 trade null market null"),
                    movements(reader, B));
        }
    }

    @Test
    public void theReaderRoleReadsTheViewAndNothingElse() throws Exception {
        try (Connection reader = DriverManager.getConnection(procs.jdbcUrl("core"), "gw_money_login", "")) {
            for (final String relation : List.of("money_journal", "money_journal_leg", "money_market_asset",
                    "money_journal_leg_asset")) {
                try {
                    reader.createStatement().executeQuery("SELECT 1 FROM " + relation + " LIMIT 1").close();
                    throw new AssertionError("the reader role can read " + relation);
                } catch (final SQLException e) {
                    assertEquals(relation + ": " + e.getMessage(), "42501", e.getSQLState());
                }
            }
            boolean refused = false;
            try {
                reader.createStatement().execute("DELETE FROM gateway_balance_movements_v1");
            } catch (final SQLException e) {
                refused = true; // no privilege, and the view is not updatable either
            }
            assertTrue("the reader role can write through the view", refused);
        }
        try (Connection c = core.getConnection();
             ResultSet rs = c.createStatement().executeQuery(
                     "SELECT table_name, privilege_type FROM information_schema.role_table_grants "
                             + "WHERE grantee = 'oe_money_reader' ORDER BY table_name, privilege_type")) {
            final List<String> grants = new ArrayList<>();
            while (rs.next()) {
                grants.add(rs.getString(1) + ":" + rs.getString(2));
            }
            assertEquals(List.of("gateway_balance_movements_v1:SELECT"), grants);
        }
    }

    @Test
    public void aSettleOfAMarketTheTableLacksHasNoAssetAndNoDelta() throws Exception {
        new PostgresMoneyJournalStore(core).project(List.of(
                MoneyJournalRecord.settle(6, 78L, 99, 100L, 1L, A, B, true, 8L, 700L, 2L, 300L, 1_400L, 600L)));
        try (Connection reader = DriverManager.getConnection(procs.jdbcUrl("core"), "gw_money_login", "");
             PreparedStatement ps = reader.prepareStatement(
                     "SELECT count(*) FROM gateway_balance_movements_v1 WHERE journal_seq = 6 "
                             + "AND asset_id IS NULL AND delta IS NULL")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("four legs of an unknown market: asset and delta NULL, never a guess", 4, rs.getInt(1));
            }
        }
    }

    private static List<String> movements(final Connection c, final long userId) throws SQLException {
        final List<String> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT journal_seq, leg_no, kind, asset_id, delta, balance_after, at_ms, log_position, trade_id, "
                        + "market_id FROM gateway_balance_movements_v1 WHERE user_id = ? "
                        + "ORDER BY journal_seq DESC, leg_no DESC")) {
            ps.setLong(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(rs.getLong(1) + "/" + rs.getInt(2) + " " + rs.getString(3) + " asset " + rs.getString(4)
                            + " delta " + rs.getString(5) + " after " + rs.getLong(6) + " at " + rs.getString(7)
                            + " pos " + rs.getLong(8) + " trade " + rs.getString(9) + " market " + rs.getString(10));
                }
            }
        }
        return out;
    }
}
