-- SPDX-License-Identifier: Apache-2.0
-- Balance movements read view: what a user's own balance did, one row per balance-after leg of the money journal,
-- for display (the gateway's GET /api/v2/balances/movements). Applied by hand after V001, like V001.
--
-- Display only. Nothing may replay settlement, rebuild a balance or decide a money movement from this view: the core
-- money journal and the core's state are the only authority. The view reads the projected rows as they are; it never
-- writes, and the reader role below can read nothing else.
--
-- Columns of gateway_balance_movements_v1 (amounts are 8dp fixed-point BIGINT, times epoch ms of core cluster time):
--   user_id        the leg's user (the gateway filters on it: a caller only ever reads its own rows)
--   journal_seq    the money journal's dense sequence of the movement
--   leg_no         the leg within the movement; (journal_seq, leg_no) is unique and is the paging key, newest first
--   log_position   the core log position of the entry that produced the movement
--   kind           opening, deposit, withdrawal or settle (money_journal.movement_type 1..4)
--   asset_id       the leg's asset: the leg's own asset_id, or for a settle leg (asset_role 0 BASE, 1 QUOTE) the
--                  market's base or quote asset from money_market_asset. NULL only for a market that table lacks
--   delta          the signed change of the leg's (available + locked) total: the leg's own delta where the journal
--                  gives one (deposit +amount, withdraw -amount); for an opening, balance_after minus the previous
--                  leg's, or minus 0 when there is none; for a settle, balance_after minus the same user's previous
--                  leg of the same asset, and NULL when no previous leg survives (age retention may have deleted it,
--                  so a 0 would be a guess)
--   balance_after  the (available + locked) total after the movement
--   at_ms          the movement's core cluster time; NULL for opening
--   trade_id, market_id   the trade and market of a settle; NULL otherwise

-- The base and quote asset of each market id. A settle record names its market only, and the money journal holds no
-- market definitions, so this table carries the core's compiled market table (core-common Market.ALL; a market's id
-- and assets never change). A market added to the core needs its row here in a later migration;
-- MoneyMarketAssetSeedTest fails until the rows below equal Market.ALL.
CREATE TABLE money_market_asset (
    market_id      INT PRIMARY KEY,
    base_asset_id  INT NOT NULL,
    quote_asset_id INT NOT NULL CHECK (quote_asset_id <> base_asset_id)
);

INSERT INTO money_market_asset (market_id, base_asset_id, quote_asset_id) VALUES
    (1, 1, 0),
    (2, 2, 0),
    (3, 3, 0),
    (4, 4, 0),
    (5, 5, 0);

-- A user's legs newest first (the page) and the previous leg of a settle or opening leg (its delta).
CREATE INDEX idx_money_journal_leg_user_seq ON money_journal_leg (user_id, journal_seq, leg_no);

-- Every leg with its asset resolved and its header's fields. Not granted to anyone: the movements view reads it with
-- its owner's rights.
CREATE VIEW money_journal_leg_asset AS
    SELECT l.journal_seq, l.leg_no, l.user_id,
           COALESCE(l.asset_id,
                    CASE l.asset_role WHEN 0 THEN m.base_asset_id WHEN 1 THEN m.quote_asset_id END) AS asset_id,
           l.balance_after, l.delta,
           j.log_position, j.movement_type, j.cluster_time_ms, j.trade_id, j.market_id
      FROM money_journal_leg l
      JOIN money_journal j ON j.journal_seq = l.journal_seq
      LEFT JOIN money_market_asset m ON m.market_id = j.market_id AND l.asset_id IS NULL;

CREATE VIEW gateway_balance_movements_v1 AS
    SELECT a.user_id, a.journal_seq, a.leg_no, a.log_position,
           CASE a.movement_type WHEN 1 THEN 'opening' WHEN 2 THEN 'deposit' WHEN 3 THEN 'withdrawal'
                                WHEN 4 THEN 'settle' END AS kind,
           a.asset_id,
           CASE WHEN a.delta IS NOT NULL THEN a.delta
                WHEN a.asset_id IS NULL THEN NULL
                WHEN a.movement_type = 1 THEN a.balance_after - COALESCE((
                    SELECT p.balance_after
                      FROM money_journal_leg_asset p
                     WHERE p.user_id = a.user_id
                       AND p.asset_id = a.asset_id
                       AND (p.journal_seq, p.leg_no) < (a.journal_seq, a.leg_no)
                     ORDER BY p.journal_seq DESC, p.leg_no DESC
                     LIMIT 1), 0)
                ELSE a.balance_after - (
                    SELECT p.balance_after
                      FROM money_journal_leg_asset p
                     WHERE p.user_id = a.user_id
                       AND p.asset_id = a.asset_id
                       AND (p.journal_seq, p.leg_no) < (a.journal_seq, a.leg_no)
                     ORDER BY p.journal_seq DESC, p.leg_no DESC
                     LIMIT 1)
           END AS delta,
           a.balance_after, a.cluster_time_ms AS at_ms, a.trade_id, a.market_id
      FROM money_journal_leg_asset a;

-- The reader role: SELECT on gateway_balance_movements_v1 and nothing else. The gateway's login role is made a member
-- of it (GRANT oe_money_reader TO <gateway role>). Roles belong to the server, not the database: a concurrent create
-- by another database's migration is not an error. Creating it needs CREATEROLE; without it, create the role first
-- (CREATE ROLE oe_money_reader NOLOGIN).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'oe_money_reader') THEN
        BEGIN
            CREATE ROLE oe_money_reader NOLOGIN;
        EXCEPTION WHEN duplicate_object OR unique_violation THEN
            NULL;
        END;
    END IF;
END
$$;
GRANT SELECT ON gateway_balance_movements_v1 TO oe_money_reader;
