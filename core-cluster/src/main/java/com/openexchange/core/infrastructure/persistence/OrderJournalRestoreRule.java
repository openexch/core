// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.infrastructure.journal.OrderJournalTail;
import com.openexchange.core.infrastructure.journal.TailReadBudget;
import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import org.agrona.concurrent.NanoClock;

import java.util.concurrent.TimeUnit;

/**
 * The order journal against the state a member restores. Nothing is added to the snapshot take (no wait, no failure in
 * onTakeSnapshot, in either snapshot mode); the invariant is carried here, on the restore side:
 *
 * <ul>
 *   <li>SKIP ({@link #skipSafe}): taken only when this member's journal of the image's genesis holds the image's
 *   eventSeq. Otherwise the SKIP is abandoned and the member falls to a path that replays the log, which regenerates
 *   the records.</li>
 *   <li>Every other restore ({@link #afterRestore}): the local tail D is compared with the restored eventSeq S before
 *   any record is emitted. D below S is a hole this member cannot regenerate: one CRITICAL line with (D, S], counted
 *   and shown on {@code /health}; the member keeps running (its money state is correct, the hole is a source fault
 *   for the reader).</li>
 * </ul>
 *
 * <p>The tail is read through this member's own archive only (bounded by {@code SHADOW_LOAD_TIMEOUT_MS}, every replay
 * held to the member's {@link TailReadBudget}), and only recordings whose last record carries the image's genesis
 * count.</p>
 */
public final class OrderJournalRestoreRule {

    /** Where a declared hole goes (the member's order journal; a test recorder). */
    public interface Holes {
        Holes NONE = (from, to) -> { };

        void declare(long fromExclusive, long toInclusive);

        /** The local tail could not be read after a restore to {@code restoredSeq}: no hole declared, none excluded. */
        default void unreadable(final long restoredSeq) {
        }
    }

    /** What a check found. */
    public record Verdict(boolean behind, long localSeq, long restoredSeq, String tail) {
    }

    /** Test seam only: the hole is not declared (the detection's negative control). */
    static volatile boolean detectionOff;
    /** Test seam only: the tail reader counts recordings of every genesis (the epoch filter's negative control). */
    static volatile boolean epochFilterOff;

    private final NanoClock clock;
    private final long timeoutNs;
    private final Holes holes;
    private final boolean enabled;
    private final TailReadBudget budget;

    /** A rule whose reads are not held to a replay budget: for tests. A member passes its {@link TailReadBudget}. */
    public OrderJournalRestoreRule(final NanoClock clock, final long timeoutMs, final Holes holes) {
        this(clock, timeoutMs, TailReadBudget.unbounded(), holes, true);
    }

    /** @param budget the member's one tail read budget; every read of the local tail waits for room in it */
    public OrderJournalRestoreRule(final NanoClock clock, final long timeoutMs, final TailReadBudget budget,
                                   final Holes holes) {
        this(clock, timeoutMs, budget, holes, true);
    }

    private OrderJournalRestoreRule(final NanoClock clock, final long timeoutMs, final TailReadBudget budget,
                                    final Holes holes, final boolean enabled) {
        this.budget = java.util.Objects.requireNonNull(budget, "budget");
        this.clock = clock;
        this.timeoutNs = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        this.holes = holes == null ? Holes.NONE : holes;
        this.enabled = enabled;
    }

    /**
     * The rule of a service that has no order journal wired (an in-process test service; every production member wires
     * one in {@code AeronCluster}): there is no journal to be behind, so it neither refuses a SKIP nor declares a hole.
     */
    public static OrderJournalRestoreRule withoutJournal() {
        return new OrderJournalRestoreRule(org.agrona.concurrent.SystemNanoClock.INSTANCE, 0,
                TailReadBudget.unbounded(), Holes.NONE, false);
    }

    /** The local tail of {@code genesisId}'s journal against the image's {@code eventSeq}. */
    public Verdict check(final Aeron aeron, final AeronArchive archive, final long genesisId, final long eventSeq) {
        if (eventSeq <= 0) {
            return new Verdict(false, 0L, eventSeq, "image has no order event");
        }
        final OrderJournalTail reader = new OrderJournalTail(aeron, archive, clock, genesisId, budget);
        if (epochFilterOff) {
            reader.epochFilterOff();
        }
        final OrderJournalTail.Result r = reader.read(timeoutNs);
        if (r instanceof OrderJournalTail.Known k) {
            return new Verdict(k.seq() < eventSeq, k.seq(), eventSeq, r.toString());
        }
        if (r instanceof OrderJournalTail.NoJournal) {
            return new Verdict(true, 0L, eventSeq, r.toString());
        }
        return new Verdict(true, -1L, eventSeq, r.toString());
    }

    /** SKIP: safe only when the local journal of this genesis holds the image's eventSeq. */
    public boolean skipSafe(final Aeron aeron, final AeronArchive archive, final long genesisId, final long eventSeq,
                            final long shadowPosition) {
        if (!enabled) {
            return true;
        }
        final Verdict v = check(aeron, archive, genesisId, eventSeq);
        if (v.behind()) {
            System.out.println("[shadow restore] SKIP abandoned: order journal behind (SKIP candidate P_s=" + shadowPosition
                    + ", image eventSeq " + eventSeq + ", local order journal " + v.tail() + ")");
            return false;
        }
        return true;
    }

    /**
     * After a non-SKIP restore, before the log replay emits anything: declare (D, S] when the local journal is behind.
     * An unreadable tail declares nothing and says so (it is not evidence of a hole).
     */
    public Verdict afterRestore(final Aeron aeron, final AeronArchive archive, final long genesisId, final long eventSeq,
                                final String path) {
        if (!enabled) {
            return new Verdict(false, -1L, eventSeq, "no order journal wired");
        }
        final Verdict v = check(aeron, archive, genesisId, eventSeq);
        if (v.localSeq() < 0) {
            unreadable(eventSeq, path, v.tail());
            return v;
        }
        if (v.behind() && !detectionOff) {
            holes.declare(v.localSeq(), eventSeq);
            if (v.localSeq() == 0) {
                // no record of this genesis here at all: a member restored from an image it did not journal (a wiped
                // member, a replica bootstrap) legitimately starts its journal above 1; readers see it as below range
                System.err.println("order journal on this member starts at " + (eventSeq + 1) + ": no record of (0, "
                        + eventSeq + "] here after the " + path + " restore (a restore from an image this member did not"
                        + " journal)");
            } else {
                System.err.println("CRITICAL order journal hole on this member: (" + v.localSeq() + ", " + eventSeq
                        + "] after the " + path + " restore; this member's journal continues at " + (eventSeq + 1));
            }
        } else {
            System.out.println("[order journal] after the " + path + " restore: local order journal " + v.tail()
                    + " holds the image's eventSeq " + eventSeq);
        }
        return v;
    }

    /** An unreadable tail after a restore: counted and shown as unknown, never silent. */
    public void unreadable(final long eventSeq, final String path, final String why) {
        if (!enabled) {
            return;
        }
        holes.unreadable(eventSeq);
        System.err.println("ERROR order journal tail unreadable after the " + path + " restore (" + why + "): no hole"
                + " declared, none excluded up to " + eventSeq);
    }
}
