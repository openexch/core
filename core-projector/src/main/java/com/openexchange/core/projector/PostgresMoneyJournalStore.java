// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PostgreSQL-backed money-journal store: one batched, atomic transaction projects a run of
 * {@link MoneyJournalRecord}s into {@code money_journal} (header, one row per journalSeq) and
 * {@code money_journal_leg} (the exploded balance-after facts). Mirrors the OMS
 * PostgresBalanceReadModelStore batched-upsert idiom (setAutoCommit(false) + addBatch +
 * executeBatch + commit/rollback).
 *
 * Idempotent by construction: header inserts are {@code ON CONFLICT (journal_seq) DO NOTHING} and
 * leg inserts are {@code ON CONFLICT (journal_seq, leg_no) DO NOTHING}, so re-projecting an already
 * committed journalSeq (replay overlap, epoch restart, retry after a partial-looking failure)
 * inserts nothing new. Postgres is the projector's authoritative high-water: nothing is skipped
 * on a failure, the whole transaction rolls back and is retried from the (unchanged) PG high-water.
 * A record at or below the high-water is not inserted at all: the agent compares it with
 * {@link #load stored rows} and halts on a difference.
 *
 * Age-based retention (migration V003, {@code money_history_prune_v1}) deletes the oldest movements, never the newest:
 * the high-water stays. {@link #prunedMark} is what that delete recorded, the one fact that lets the agent tell a row
 * retention deleted from a row that is lost.
 *
 * The canonical dump ({@link #CANONICAL_DUMP}, {@link #canonicalDump}) names every column that is a function of the
 * journal bytes, in a fixed order, ordered by primary key; {@link #WALL_CLOCK_COLUMNS} are the only columns it leaves
 * out. Two projections of one journal have equal canonical dumps.
 */
public final class PostgresMoneyJournalStore {

    private static final String INSERT_HEADER = """
            INSERT INTO money_journal
                (journal_seq, log_position, movement_type, cluster_time_ms, user_id, asset_id, amount, balance_after,
                 trade_id, market_id, price, quantity, buyer_user_id, seller_user_id, taker_is_buy)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (journal_seq) DO NOTHING
            """;

    private static final String INSERT_LEG = """
            INSERT INTO money_journal_leg
                (journal_seq, leg_no, user_id, asset_id, asset_role, balance_after, delta)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (journal_seq, leg_no) DO NOTHING
            """;

    private static final String MAX_SEQ = "SELECT COALESCE(MAX(journal_seq), 0) FROM money_journal";

    /** Table -> its dumped columns in dump order (the first column, or two, are its primary key). */
    public static final Map<String, List<String>> CANONICAL_DUMP = Map.of(
            "money_journal", List.of("journal_seq", "log_position", "movement_type", "cluster_time_ms", "user_id",
                    "asset_id", "amount", "balance_after", "trade_id", "market_id", "price", "quantity",
                    "buyer_user_id", "seller_user_id", "taker_is_buy"),
            "money_journal_leg", List.of("journal_seq", "leg_no", "user_id", "asset_id", "asset_role",
                    "balance_after", "delta"));
    /** Table -> its primary key, the dump's order. */
    public static final Map<String, String> CANONICAL_ORDER = Map.of(
            "money_journal", "journal_seq",
            "money_journal_leg", "journal_seq, leg_no");
    /** The columns that are not a function of the journal (insert time): never dumped, never compared. */
    public static final Map<String, List<String>> WALL_CLOCK_COLUMNS = Map.of(
            "money_journal", List.of("ingested_at"),
            "money_journal_leg", List.of());

    private static final String LOAD_HEADERS = """
            SELECT journal_seq, log_position, movement_type, cluster_time_ms, user_id, asset_id, amount, balance_after,
                   trade_id, market_id, price, quantity, buyer_user_id, seller_user_id, taker_is_buy
            FROM money_journal WHERE journal_seq = ANY (?)
            """;

    private static final String LOAD_LEGS = """
            SELECT journal_seq, leg_no, user_id, asset_id, asset_role, balance_after, delta
            FROM money_journal_leg WHERE journal_seq = ANY (?) ORDER BY journal_seq, leg_no
            """;

    /**
     * The retention mark, only while the first stored journalSeq is right above it (one statement, one snapshot): that
     * is what every delete leaves behind. A mark over a history that was emptied and rebuilt, or over rows that came
     * back below it, answers nothing.
     */
    private static final String PRUNED_MARK = """
            SELECT p.through_seq, p.max_before_ms FROM money_journal_pruned p
            WHERE p.singleton AND p.through_seq + 1 = (SELECT MIN(j.journal_seq) FROM money_journal j)
            """;
    /** SQLSTATE undefined_table: a schema older than V003 has no retention mark, so nothing was deleted by it. */
    private static final String UNDEFINED_TABLE = "42P01";
    /** SQLSTATE insufficient_privilege: the mark exists and this login may not read it (V003 applied as another role). */
    public static final String INSUFFICIENT_PRIVILEGE = "42501";

    /**
     * What age-based retention deleted: every journalSeq at or below {@code throughSeq} (a run without a hole from the
     * first stored movement), each with a cluster time below {@code maxBeforeMs}, the greatest cutoff of a delete.
     * {@link #NONE} when nothing was deleted.
     */
    public record PrunedMark(long throughSeq, long maxBeforeMs) {
        public static final PrunedMark NONE = new PrunedMark(0, 0);

        /**
         * True when retention deleted the stored rows of {@code r}: its journalSeq is at or below the mark and its
         * cluster time, when it has one (an opening balance has none), is below the greatest cutoff.
         */
        public boolean covers(final MoneyJournalRecord r) {
            return r.journalSeq() >= 1 && r.journalSeq() <= throughSeq
                    && (r.clusterTimeMs() == null || r.clusterTimeMs() < maxBeforeMs);
        }
    }

    private final HikariDataSource dataSource;

    public PostgresMoneyJournalStore(final HikariDataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** Resume high-water: MAX(journal_seq) already projected, or 0 on an empty ledger. */
    public long highWater() throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(MAX_SEQ);
             ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong(1) : 0L;
        }
    }

    /**
     * Project a run of records in journalSeq order in one transaction: all headers, then all legs
     * (headers first so the leg foreign key resolves). Either the whole batch commits or it rolls
     * back and the caller retries; the high-water never advances past an uncommitted record.
     */
    public void project(final List<MoneyJournalRecord> records) throws SQLException {
        if (records.isEmpty()) {
            return;
        }
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement header = conn.prepareStatement(INSERT_HEADER);
                 PreparedStatement leg = conn.prepareStatement(INSERT_LEG)) {
                for (final MoneyJournalRecord r : records) {
                    bindHeader(header, r);
                    header.addBatch();
                }
                header.executeBatch();
                for (final MoneyJournalRecord r : records) {
                    for (final MoneyJournalRecord.Leg l : r.legs()) {
                        bindLeg(leg, r.journalSeq(), l);
                        leg.addBatch();
                    }
                }
                leg.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    /**
     * The stored records of {@code journalSeqs}, rebuilt from their header and leg rows; a journalSeq with no header
     * row is absent from the answer.
     */
    public Map<Long, MoneyJournalRecord> load(final List<Long> journalSeqs) throws SQLException {
        final Map<Long, MoneyJournalRecord> out = new HashMap<>();
        if (journalSeqs.isEmpty()) {
            return out;
        }
        try (Connection conn = dataSource.getConnection()) {
            final java.sql.Array seqs = conn.createArrayOf("bigint", journalSeqs.toArray());
            final Map<Long, List<MoneyJournalRecord.Leg>> legs = new HashMap<>();
            try (PreparedStatement ps = conn.prepareStatement(LOAD_LEGS)) {
                ps.setArray(1, seqs);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        legs.computeIfAbsent(rs.getLong(1), k -> new ArrayList<>()).add(new MoneyJournalRecord.Leg(
                                rs.getShort(2), rs.getLong(3), nullableInt(rs, 4), nullableShort(rs, 5), rs.getLong(6),
                                nullableLong(rs, 7)));
                    }
                }
            }
            try (PreparedStatement ps = conn.prepareStatement(LOAD_HEADERS)) {
                ps.setArray(1, seqs);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        final long seq = rs.getLong(1);
                        final boolean taker = rs.getBoolean(15);
                        final Boolean takerIsBuy = rs.wasNull() ? null : taker;
                        out.put(seq, MoneyJournalRecord.stored(seq, rs.getLong(2), rs.getShort(3), nullableLong(rs, 4),
                                nullableLong(rs, 5), nullableInt(rs, 6), nullableLong(rs, 7), nullableLong(rs, 8),
                                nullableLong(rs, 9), nullableInt(rs, 10), nullableLong(rs, 11), nullableLong(rs, 12),
                                nullableLong(rs, 13), nullableLong(rs, 14), takerIsBuy,
                                legs.getOrDefault(seq, List.of())));
                    }
                }
            }
        }
        return out;
    }

    /**
     * The retention mark as committed now. Read it AFTER {@link #load}: the delete writes the mark in the transaction
     * that removes the rows, so a row that was gone at the load is covered by a mark read later, and a mark read
     * earlier could miss a delete that ran in between. A schema without the mark answers {@link PrunedMark#NONE}; a
     * mark this login may not read throws ({@link #INSUFFICIENT_PRIVILEGE}), which the agent parks on.
     */
    public PrunedMark prunedMark() throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(PRUNED_MARK);
             ResultSet rs = ps.executeQuery()) {
            return rs.next() ? new PrunedMark(rs.getLong(1), rs.getLong(2)) : PrunedMark.NONE;
        } catch (SQLException e) {
            if (UNDEFINED_TABLE.equals(e.getSQLState())) {
                return PrunedMark.NONE;
            }
            throw e;
        }
    }

    /**
     * The canonical dump: per table in name order, its {@link #CANONICAL_DUMP} columns of every row in primary key
     * order, one line per row, tab separated, {@code \N} for NULL. Equal for two projections of one journal.
     */
    public String canonicalDump() throws SQLException {
        final StringBuilder sb = new StringBuilder();
        try (Connection conn = dataSource.getConnection()) {
            for (final String table : new java.util.TreeSet<>(CANONICAL_DUMP.keySet())) {
                final List<String> columns = CANONICAL_DUMP.get(table);
                sb.append("-- ").append(table).append(' ').append(String.join(",", columns)).append('\n');
                try (PreparedStatement ps = conn.prepareStatement("SELECT " + String.join(", ", columns) + " FROM "
                        + table + " ORDER BY " + CANONICAL_ORDER.get(table));
                     ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        for (int i = 1; i <= columns.size(); i++) {
                            final String v = rs.getString(i);
                            sb.append(i > 1 ? "\t" : "").append(v == null ? "\\N" : v);
                        }
                        sb.append('\n');
                    }
                }
            }
        }
        return sb.toString();
    }

    private static void bindHeader(final PreparedStatement ps, final MoneyJournalRecord r)
            throws SQLException {
        ps.setLong(1, r.journalSeq());
        ps.setLong(2, r.logPosition());
        ps.setShort(3, r.movementType());
        setNullableLong(ps, 4, r.clusterTimeMs());
        setNullableLong(ps, 5, r.userId());
        setNullableInt(ps, 6, r.assetId());
        setNullableLong(ps, 7, r.amount());
        setNullableLong(ps, 8, r.balanceAfter());
        setNullableLong(ps, 9, r.tradeId());
        setNullableInt(ps, 10, r.marketId());
        setNullableLong(ps, 11, r.price());
        setNullableLong(ps, 12, r.quantity());
        setNullableLong(ps, 13, r.buyerUserId());
        setNullableLong(ps, 14, r.sellerUserId());
        if (r.takerIsBuy() == null) {
            ps.setNull(15, Types.BOOLEAN);
        } else {
            ps.setBoolean(15, r.takerIsBuy());
        }
    }

    private static Long nullableLong(final ResultSet rs, final int i) throws SQLException {
        final long v = rs.getLong(i);
        return rs.wasNull() ? null : v;
    }

    private static Integer nullableInt(final ResultSet rs, final int i) throws SQLException {
        final int v = rs.getInt(i);
        return rs.wasNull() ? null : v;
    }

    private static Short nullableShort(final ResultSet rs, final int i) throws SQLException {
        final short v = rs.getShort(i);
        return rs.wasNull() ? null : v;
    }

    private static void bindLeg(final PreparedStatement ps, final long journalSeq,
                                final MoneyJournalRecord.Leg l) throws SQLException {
        ps.setLong(1, journalSeq);
        ps.setShort(2, l.legNo());
        ps.setLong(3, l.userId());
        setNullableInt(ps, 4, l.assetId());
        if (l.assetRole() == null) {
            ps.setNull(5, Types.SMALLINT);
        } else {
            ps.setShort(5, l.assetRole());
        }
        ps.setLong(6, l.balanceAfter());
        setNullableLong(ps, 7, l.delta());
    }

    private static void setNullableLong(final PreparedStatement ps, final int idx, final Long v)
            throws SQLException {
        if (v == null) {
            ps.setNull(idx, Types.BIGINT);
        } else {
            ps.setLong(idx, v);
        }
    }

    private static void setNullableInt(final PreparedStatement ps, final int idx, final Integer v)
            throws SQLException {
        if (v == null) {
            ps.setNull(idx, Types.INTEGER);
        } else {
            ps.setInt(idx, v);
        }
    }
}
