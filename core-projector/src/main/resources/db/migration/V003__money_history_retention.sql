-- SPDX-License-Identifier: Apache-2.0
-- Age-based retention of the money history: the delete, its mark, the totals of what it deleted, and the role that may
-- call it. Applied after V002, like V001 and V002, in one transaction, AS THE PROJECTOR'S OWN LOGIN (the owner of
-- money_journal: the functions run as their owner and the projector reads the mark as the owner), on an empty database
-- or in place on one that holds history.
--
-- The core's state and the core money journal are the authority for every balance; the gateway's movements view is
-- display only (V002). Two kinds of reader still compute from these rows, and a delete must not change their answer:
--   - the projector compares every journal record a source delivers again with its stored rows, and a missing row
--     parks it. The mark below is the one exact fact that tells "deleted by retention" from "lost";
--   - a reconciliation that sums the history (the conservation check: per asset, deposits less withdrawals equals the
--     core's total) would lose the deleted movements. The totals table below keeps their sums, so that
--     sum over the stored rows + totals is the same number before and after a delete.
-- A reader that needs every single movement (a replay of each movement from the first, a per-movement match) cannot
-- be served after a delete: it must read the mark and say that it cannot judge, never report a loss or a pass.
--
-- What one call deletes: money_history_prune_v1(before_ms, max_rows) deletes the OLDEST movements, a run of
-- journal_seq without a hole that starts at the first stored one, each movement whole (its money_journal row and all
-- of its money_journal_leg rows, in one transaction). The run ends before the first of:
--   - the newest movement (MAX(journal_seq)): it is never deleted. The projector resumes right above it, so nothing at
--     or above the projector's resume point is ever touched;
--   - the first movement whose cluster_time_ms is at or above before_ms. Every deleted movement that has a time has
--     one below before_ms; this holds movement by movement and does not need the time to rise along the journal;
--   - max_rows movements.
-- A movement without a time (the opening balances, which the core writes only at the very start of a journal, in the
-- same log entry as the first timed movement) is as old as the next movement that has a time: it is deleted only when
-- that one is below before_ms.
--   Before it deletes, the call checks what it is about to delete: no stored movement at or below the mark, the run
-- starts right above the mark (journal_seq 1 on a database that was never pruned), has no hole, and every movement of
-- it has a known type and exactly its own legs (four for a settle, one otherwise; checked movement by movement).
-- Anything else raises and deletes nothing: retention never deletes around a hole, so a hole stays where the
-- projector's compare finds it. A before_ms in the future of the database clock raises as well. There is no lower
-- bound on the window: how long history must be kept is the operator's rule, not a fact this schema knows.
--   It returns the rows deleted (journal_deleted, legs_deleted; zero movements means nothing older is left) and what
-- the history starts with after the call: first_kept_seq, first_kept_ms (the cluster time of the first kept movement,
-- or of the next one that has a time) and newest_seq, so the caller can see a history that stays old.
--
-- The mark: money_journal_pruned.through_seq is the greatest journal_seq a call has deleted. Because calls delete a
-- run without a hole from the first stored movement, it says exactly which rows retention removed: every journal_seq
-- at or below it, and no other. max_before_ms is the greatest before_ms of a call that deleted something. Both are
-- written in the transaction that deletes the rows. Only the functions of this file write the mark; a DELETE by anyone
-- else leaves it alone, so rows deleted any other way are missing rows to the projector, as before. TRUNCATE of
-- money_journal (a rebuild of the history) resets the mark and the totals in the same transaction.
--   The money journal's records carry no genesis or epoch id and the projector is given none, so the mark cannot name
-- the journal it belongs to. What ties it to the stored rows is checked instead: the mark answers only while the
-- first stored journal_seq is right above it, and a record with a time is accepted only below max_before_ms (a
-- journal of a later genesis read over this database carries later times).
--
-- The totals: money_journal_pruned_totals holds, per movement type and asset, the exact sum of amount and the number
-- of the deleted movements that carry an asset and an amount (opening 1, deposit 2, withdraw 3). A settle moves
-- nothing in or out of an asset (what the buyer gets the seller gives), so it adds to no sum; the number of deleted
-- settles is money_journal_pruned.settles. The sums are NUMERIC with no fraction: integer arithmetic that cannot
-- overflow, where a BIGINT sum of BIGINT amounts could. They are written in the transaction that deletes the rows, so
-- in any one snapshot: sum(amount) over money_journal + amount of the totals = the sum over the whole history.
--   money_journal_pruned.totals_unknown_through > 0 says that the movements at or below it were deleted without
-- totals (money_history_adopt_v1, below): a reader that sums the history must then report that it cannot judge.
--
-- The projector after a delete: it writes only above MAX(journal_seq), which a call never deletes, so it never writes
-- a deleted movement again. A record at or below its high-water that a source delivers again is compared with its
-- stored rows; when it has no stored money_journal row it is accepted without one only when all of these hold, read
-- in one snapshot after the rows were read:
--   - its journal_seq is at or below through_seq;
--   - its cluster_time_ms, when it has one, is below max_before_ms;
--   - the first stored journal_seq is through_seq + 1 (what every call leaves behind; a mark over a history that was
--     emptied and rebuilt, or over rows that came back, answers nothing).
-- Any other missing row, any missing leg row and any stored row that differs park the projector, as before.
--
-- A database whose old rows were deleted by another delete (not this function): its first journal_seq is above 1 and
-- its mark is 0, so the function refuses. money_history_adopt_v1(max_before_ms), run once by the owner, checks that
-- the stored rows have no hole from the first to the newest and that every movement has its legs, then sets the mark
-- right below the first stored row with the cutoff the operator states (every missing movement was older than it) and
-- records that their totals are unknown. Only the owner may call it.
--
-- Locks this file takes on existing relations: CREATE TRIGGER takes SHARE ROW EXCLUSIVE on money_journal until the
-- transaction commits. That waits for the projector's running insert batch and holds its next one for the length of
-- this transaction; reads (the balance movements view) go on. Nothing here scans, rewrites or indexes a table, so the
-- time does not grow with the history. The wait for that lock is bounded below: behind a long transaction the
-- migration fails after five seconds and changes nothing (run it again), instead of queueing the projector's inserts
-- behind its own wait. The delete needs no new index: it reads and deletes by the primary keys
-- (money_journal.journal_seq, money_journal_leg (journal_seq, leg_no)), which also serve the foreign key check.
--
-- Rollback: once a call has deleted rows, a projector build older than this file must not run against this database.
-- It does not read the mark, so the first time a source delivers a deleted movement again it parks.

-- Bounds of this migration's own transaction (no effect, and a warning, when the file is run outside one).
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE money_journal_pruned (
    singleton              BOOLEAN     NOT NULL PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    through_seq            BIGINT      NOT NULL DEFAULT 0 CHECK (through_seq >= 0),  -- every journal_seq at or below: deleted
    max_before_ms          BIGINT      NOT NULL DEFAULT 0,                           -- the greatest cutoff of a delete
    settles                BIGINT      NOT NULL DEFAULT 0 CHECK (settles >= 0),      -- deleted settle movements
    totals_unknown_through BIGINT      NOT NULL DEFAULT 0                            -- at or below: deleted without totals
                           CHECK (totals_unknown_through >= 0 AND totals_unknown_through <= through_seq),
    updated_at             TIMESTAMPTZ                                               -- NULL: nothing deleted yet
);
INSERT INTO money_journal_pruned (singleton) VALUES (TRUE);

CREATE TABLE money_journal_pruned_totals (
    movement_type SMALLINT       NOT NULL CHECK (movement_type IN (1, 2, 3)),
    asset_id      INT            NOT NULL,
    amount        NUMERIC(60, 0) NOT NULL,                       -- the exact sum of money_journal.amount deleted
    movements     BIGINT         NOT NULL CHECK (movements > 0), -- how many movements that is
    PRIMARY KEY (movement_type, asset_id)
);

-- A rebuild empties the history: nothing of the new one is deleted yet.
CREATE FUNCTION money_journal_note_truncated() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    INSERT INTO money_journal_pruned (singleton) VALUES (TRUE)
        ON CONFLICT (singleton) DO UPDATE SET through_seq = 0, max_before_ms = 0, settles = 0,
            totals_unknown_through = 0, updated_at = NULL;
    DELETE FROM money_journal_pruned_totals;
    RETURN NULL;
END
$$;
REVOKE ALL ON FUNCTION money_journal_note_truncated() FROM PUBLIC;
CREATE TRIGGER money_journal_truncated AFTER TRUNCATE ON money_journal
    FOR EACH STATEMENT EXECUTE FUNCTION money_journal_note_truncated();

-- The delete (see above). STRICT: a NULL argument deletes nothing. One call at a time: the mark row is its lock.
CREATE FUNCTION money_history_prune_v1(before_ms BIGINT, max_rows INTEGER,
    OUT journal_deleted BIGINT, OUT legs_deleted BIGINT,
    OUT first_kept_seq BIGINT, OUT first_kept_ms BIGINT, OUT newest_seq BIGINT)
    LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    now_ms    BIGINT := (extract(epoch FROM clock_timestamp()) * 1000::numeric)::bigint;
    through   BIGINT;
    lo        BIGINT;
    hi        BIGINT;
    held      BIGINT;
    stop_seq  BIGINT;
    hi_time   BIGINT;
    next_time BIGINT;
    odd       BIGINT;
    want_legs BIGINT;
    bad_seq   BIGINT;
    n_settles BIGINT;
BEGIN
    journal_deleted := 0;
    legs_deleted := 0;
    IF before_ms > now_ms THEN
        RAISE EXCEPTION 'money history retention refused: before_ms % is in the future of the database clock (%)',
            before_ms, now_ms;
    END IF;
    -- the tables before the mark row, the order TRUNCATE and its trigger take them in
    LOCK TABLE money_journal_leg, money_journal IN ROW EXCLUSIVE MODE;
    SELECT p.through_seq INTO through FROM money_journal_pruned p WHERE p.singleton FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'money history retention refused: money_journal_pruned has no row, so what was deleted is unknown';
    END IF;
    <<work>>
    BEGIN
        IF max_rows <= 0 THEN
            EXIT work;
        END IF;
        SELECT j.journal_seq INTO bad_seq FROM money_journal j WHERE j.journal_seq <= through ORDER BY j.journal_seq LIMIT 1;
        IF FOUND THEN
            RAISE EXCEPTION 'money history retention refused: journal_seq % is stored at or below the mark % (the history was rebuilt under a mark of an earlier one); nothing deleted',
                bad_seq, through;
        END IF;
        SELECT min(j.journal_seq) INTO lo FROM money_journal j WHERE j.journal_seq > through;
        SELECT max(j.journal_seq) INTO newest_seq FROM money_journal j;
        IF newest_seq IS NULL OR lo >= newest_seq THEN
            EXIT work; -- empty, or only the newest movement
        END IF;
        IF lo <> through + 1 THEN
            RAISE EXCEPTION 'money history retention refused: the first stored journal_seq is %, expected % (right above the deleted ones); rows are missing, or they were deleted by another delete (see money_history_adopt_v1)',
                lo, through + 1;
        END IF;
        -- the oldest max_rows movements below the newest one
        SELECT max(c.journal_seq), count(*) INTO hi, held
          FROM (SELECT j.journal_seq FROM money_journal j WHERE j.journal_seq > through AND j.journal_seq < newest_seq
                 ORDER BY j.journal_seq LIMIT max_rows) c;
        IF held <> hi - lo + 1 THEN
            RAISE EXCEPTION 'money history retention refused: % movements stored in journal_seq % to %, expected % (a hole)',
                held, lo, hi, hi - lo + 1;
        END IF;
        -- end before the first movement that is not older than the cutoff
        SELECT min(j.journal_seq) INTO stop_seq FROM money_journal j
         WHERE j.journal_seq BETWEEN lo AND hi AND j.cluster_time_ms >= before_ms;
        IF stop_seq IS NOT NULL THEN
            hi := stop_seq - 1;
        END IF;
        IF hi < lo THEN
            EXIT work;
        END IF;
        -- a movement without a time is as old as the next one that has a time
        SELECT j.cluster_time_ms INTO hi_time FROM money_journal j WHERE j.journal_seq = hi;
        IF hi_time IS NULL THEN
            SELECT j.cluster_time_ms INTO next_time FROM money_journal j
             WHERE j.journal_seq > hi AND j.cluster_time_ms IS NOT NULL ORDER BY j.journal_seq LIMIT 1;
            IF next_time IS NULL OR next_time >= before_ms THEN
                SELECT max(j.journal_seq) INTO hi FROM money_journal j
                 WHERE j.journal_seq BETWEEN lo AND hi AND j.cluster_time_ms IS NOT NULL;
                IF hi IS NULL THEN
                    EXIT work;
                END IF;
            END IF;
        END IF;
        SELECT count(*) FILTER (WHERE j.movement_type NOT IN (1, 2, 3, 4)),
               count(*) FILTER (WHERE j.movement_type = 4),
               coalesce(sum(CASE j.movement_type WHEN 4 THEN 4 ELSE 1 END), 0)
          INTO odd, n_settles, want_legs
          FROM money_journal j WHERE j.journal_seq BETWEEN lo AND hi;
        IF odd > 0 THEN
            RAISE EXCEPTION 'money history retention refused: % movements of an unknown type in journal_seq % to %',
                odd, lo, hi;
        END IF;
        -- every movement of the run has exactly its own legs
        SELECT min(j.journal_seq) INTO bad_seq
          FROM money_journal j
          LEFT JOIN (SELECT l.journal_seq, count(*) AS legs FROM money_journal_leg l
                      WHERE l.journal_seq BETWEEN lo AND hi GROUP BY l.journal_seq) g ON g.journal_seq = j.journal_seq
         WHERE j.journal_seq BETWEEN lo AND hi
           AND coalesce(g.legs, 0) <> CASE j.movement_type WHEN 4 THEN 4 ELSE 1 END;
        IF bad_seq IS NOT NULL THEN
            RAISE EXCEPTION 'money history retention refused: the movement journal_seq % does not have exactly its legs (rows are missing); nothing deleted',
                bad_seq;
        END IF;
        SELECT min(j.journal_seq) INTO bad_seq FROM money_journal j
         WHERE j.journal_seq BETWEEN lo AND hi AND j.movement_type IN (1, 2, 3) AND (j.asset_id IS NULL OR j.amount IS NULL);
        IF bad_seq IS NOT NULL THEN
            RAISE EXCEPTION 'money history retention refused: the movement journal_seq % has no asset or no amount; nothing deleted',
                bad_seq;
        END IF;
        -- the sums of what goes, in the transaction that deletes it
        INSERT INTO money_journal_pruned_totals AS t (movement_type, asset_id, amount, movements)
            SELECT j.movement_type, j.asset_id, sum(j.amount::numeric), count(*)
              FROM money_journal j
             WHERE j.journal_seq BETWEEN lo AND hi AND j.movement_type IN (1, 2, 3)
             GROUP BY j.movement_type, j.asset_id
            ON CONFLICT (movement_type, asset_id) DO UPDATE
               SET amount = t.amount + EXCLUDED.amount, movements = t.movements + EXCLUDED.movements;
        DELETE FROM money_journal_leg l WHERE l.journal_seq BETWEEN lo AND hi;
        GET DIAGNOSTICS legs_deleted = ROW_COUNT;
        DELETE FROM money_journal j WHERE j.journal_seq BETWEEN lo AND hi;
        GET DIAGNOSTICS journal_deleted = ROW_COUNT;
        IF journal_deleted <> hi - lo + 1 OR legs_deleted <> want_legs THEN
            RAISE EXCEPTION 'money history retention refused: journal_seq % to % held % movements and % legs, expected % and % (rows are missing); nothing deleted',
                lo, hi, journal_deleted, legs_deleted, hi - lo + 1, want_legs;
        END IF;
        UPDATE money_journal_pruned p
           SET through_seq = hi, max_before_ms = GREATEST(p.max_before_ms, before_ms), settles = p.settles + n_settles,
               updated_at = clock_timestamp()
         WHERE p.singleton;
        through := hi;
    END work;
    -- what the history starts with now
    SELECT max(j.journal_seq) INTO newest_seq FROM money_journal j;
    SELECT j.journal_seq, j.cluster_time_ms INTO first_kept_seq, first_kept_ms FROM money_journal j
     WHERE j.journal_seq > through ORDER BY j.journal_seq LIMIT 1;
    IF first_kept_seq IS NOT NULL AND first_kept_ms IS NULL THEN
        SELECT j.cluster_time_ms INTO first_kept_ms FROM money_journal j
         WHERE j.journal_seq > first_kept_seq AND j.cluster_time_ms IS NOT NULL ORDER BY j.journal_seq LIMIT 1;
    END IF;
END
$$;

-- The adoption of a history whose old rows another delete removed (see above). Owner only.
CREATE FUNCTION money_history_adopt_v1(max_before_ms BIGINT, OUT adopted_through BIGINT, OUT movements_kept BIGINT)
    LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    now_ms  BIGINT := (extract(epoch FROM clock_timestamp()) * 1000::numeric)::bigint;
    through BIGINT;
    lo      BIGINT;
    newest  BIGINT;
    bad_seq BIGINT;
BEGIN
    IF max_before_ms <= 0 OR max_before_ms > now_ms THEN
        RAISE EXCEPTION 'money history adoption refused: max_before_ms % must be a past time of the database clock (%)',
            max_before_ms, now_ms;
    END IF;
    LOCK TABLE money_journal_leg, money_journal IN ROW EXCLUSIVE MODE;
    SELECT p.through_seq INTO through FROM money_journal_pruned p WHERE p.singleton FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'money history adoption refused: money_journal_pruned has no row';
    END IF;
    IF through <> 0 THEN
        RAISE EXCEPTION 'money history adoption refused: the mark is already at journal_seq %', through;
    END IF;
    SELECT min(j.journal_seq), max(j.journal_seq), count(*) INTO lo, newest, movements_kept FROM money_journal j;
    IF lo IS NULL OR lo <= 1 THEN
        RAISE EXCEPTION 'money history adoption refused: the history is empty or starts at journal_seq 1 (nothing to adopt)';
    END IF;
    IF movements_kept <> newest - lo + 1 THEN
        RAISE EXCEPTION 'money history adoption refused: % movements stored in journal_seq % to %, expected % (a hole)',
            movements_kept, lo, newest, newest - lo + 1;
    END IF;
    SELECT min(j.journal_seq) INTO bad_seq
      FROM money_journal j
      LEFT JOIN (SELECT l.journal_seq, count(*) AS legs FROM money_journal_leg l GROUP BY l.journal_seq) g
             ON g.journal_seq = j.journal_seq
     WHERE j.movement_type NOT IN (1, 2, 3, 4)
        OR coalesce(g.legs, 0) <> CASE j.movement_type WHEN 4 THEN 4 ELSE 1 END;
    IF bad_seq IS NOT NULL THEN
        RAISE EXCEPTION 'money history adoption refused: the movement journal_seq % has an unknown type or not exactly its legs',
            bad_seq;
    END IF;
    adopted_through := lo - 1;
    UPDATE money_journal_pruned p
       SET through_seq = adopted_through, max_before_ms = money_history_adopt_v1.max_before_ms,
           totals_unknown_through = adopted_through, updated_at = clock_timestamp()
     WHERE p.singleton;
END
$$;
REVOKE ALL ON FUNCTION money_history_adopt_v1(BIGINT) FROM PUBLIC;

-- The definer functions resolve names in pg_catalog, then the schema this file is applied in (the projection's own,
-- never a name a caller supplies), pg_temp last.
DO $$
BEGIN
    EXECUTE format('ALTER FUNCTION money_journal_note_truncated() SET search_path = pg_catalog, %I, pg_temp',
        current_schema());
    EXECUTE format('ALTER FUNCTION money_history_prune_v1(BIGINT, INTEGER) SET search_path = pg_catalog, %I, pg_temp',
        current_schema());
    EXECUTE format('ALTER FUNCTION money_history_adopt_v1(BIGINT) SET search_path = pg_catalog, %I, pg_temp',
        current_schema());
END
$$;

-- The pruner role: EXECUTE on the delete and nothing else (no right on any table; the function runs as its owner).
-- The retention job's login role is made a member (GRANT oe_history_pruner TO <role>). Roles belong to the server, not
-- the database: a concurrent create by another database's migration is not an error. Creating it needs CREATEROLE;
-- without it, create the role first (CREATE ROLE oe_history_pruner NOLOGIN).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'oe_history_pruner') THEN
        BEGIN
            CREATE ROLE oe_history_pruner NOLOGIN;
        EXCEPTION WHEN duplicate_object OR unique_violation THEN
            NULL;
        END;
    END IF;
END
$$;
REVOKE ALL ON FUNCTION money_history_prune_v1(BIGINT, INTEGER) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION money_history_prune_v1(BIGINT, INTEGER) TO oe_history_pruner;
