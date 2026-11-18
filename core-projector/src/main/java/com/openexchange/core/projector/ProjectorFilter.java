// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

/**
 * The projector's money-critical projection decisions, kept PURE (no Aeron, no JDBC) so every
 * boundary case is unit-testable. Simpler than the settlement bridge's filter: the core money
 * journal carries ONE dense sequence, {@code journalSeq} (starts at 1, no gaps, no dupes), so
 * there is a single rule instead of the bridge's trade/terminal split.
 *
 * One instance per resume epoch: on startup and on every epoch the projector reads the Postgres
 * high-water ({@code MAX(journal_seq)}, 0 on an empty ledger) and builds a fresh filter from it.
 * The filter itself never needs persistence.
 *
 * Rules (in decision order):
 *  1. GAP is latched: after a detected gap nothing more is projected by this filter (correctness
 *     over liveness). A dense gap is a lost money movement on the source read; projecting past it
 *     would silently mis-state the ledger forever. The agent decides what a gap means (another
 *     source, or a park).
 *  2. {@code journalSeq <= highWater} was already projected (startup overlap, replay overlap across
 *     recordings, restart-duplicate) -> DUPLICATE: never inserted, always compared with what was
 *     stored (a difference is a determinism fault, never skipped silently).
 *  3. A projected record must be EXACTLY {@code highWater + 1}. Anything greater is a GAP
 *     (or, with haltOnGap=false, PROJECT anyway after counting -> an explicit operator override).
 */
public final class ProjectorFilter {

    public enum Action { PROJECT, DUPLICATE, GAP }

    private final boolean haltOnGap;

    private long highWater;
    private boolean gapped;
    private long duplicates;
    private long gapsDetected;

    /**
     * @param highWater the Postgres MAX(journal_seq) at epoch start (0 on an empty ledger)
     * @param haltOnGap latch GAP on a dense journalSeq gap (production default true)
     */
    public ProjectorFilter(final long highWater, final boolean haltOnGap) {
        this.highWater = highWater;
        this.haltOnGap = haltOnGap;
    }

    public Action onRecord(final long journalSeq) {
        if (gapped) {
            return Action.GAP;
        }
        if (journalSeq <= highWater) {
            duplicates++;
            return Action.DUPLICATE;
        }
        if (journalSeq != highWater + 1) {
            gapsDetected++;
            if (haltOnGap) {
                gapped = true;
                return Action.GAP;
            }
            // Operator override: project past the gap, loudly counted.
        }
        highWater = journalSeq;
        return Action.PROJECT;
    }

    /** True once a gap latched this filter. */
    public boolean gapped() {
        return gapped;
    }

    public long highWater() {
        return highWater;
    }

    public long duplicates() {
        return duplicates;
    }

    public long gapsDetected() {
        return gapsDetected;
    }
}
