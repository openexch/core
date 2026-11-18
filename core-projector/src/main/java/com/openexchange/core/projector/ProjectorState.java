// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

/**
 * Cross-thread observable projector state: written by the projector agent thread, read by the
 * metrics server and the periodic status log. Volatile single-writer fields, no locks needed
 * (mirrors the settlement bridge's BridgeState).
 */
public final class ProjectorState {

    public volatile boolean halted;
    /** Why the projector parked (empty while it runs): a gap on every source, a payload conflict, a refused version. */
    public volatile String haltReason = "";
    /** True while a Postgres connection is usable (flips false on a flush failure / connect error). */
    public volatile boolean connectedToPg;
    public volatile String journalSource = "";
    /** Postgres high-water (MAX(journal_seq)) as read at the start of the current epoch. */
    public volatile long epochHighWater;
    /** journalSeq of the last record durably committed to Postgres. */
    public volatile long lastJournalSeq;
    public volatile long rowsProjected;
    /** Records at or below the high-water: never inserted, each compared with its stored rows. */
    public volatile long duplicateRecords;
    /** Duplicates whose stored rows differed (or were missing): each one parks the projector. */
    public volatile long payloadConflicts;
    /** Epochs ended on a gap of one source so that the next source is read before parking. */
    public volatile long sourceSwitches;
    /** Records of a journal version this projector does not read (each one parks the projector). */
    public volatile long versionRefusals;
    public volatile long gapsDetected;
    /** True while a gap one source showed is neither filled by another source nor parked on. */
    public volatile boolean gapPending;
    /** The Postgres high-water right below the pending gap (meaningful while {@link #gapPending}). */
    public volatile long gapPendingHighWater;
    public volatile long epochs;
    public volatile long errors;
    /** Journal-source stalls/losses detected (image lost, never connected, stopped-and-stuck). */
    public volatile long sourceStalls;
    /** Bytes recorded in the followed journal recording not yet delivered to the projector. */
    public volatile long sourceBacklogBytes;
    /** Postgres flush failures (a flush that threw); each keeps the batch pending for retry. */
    public volatile long pgFlushFailures;

    public String render() {
        return "projector{halted=" + halted + (halted ? " reason=" + haltReason : "") + " pgConnected=" + connectedToPg
                + " source=" + journalSource
                + " epochHwm=" + epochHighWater
                + " lastJournalSeq=" + lastJournalSeq
                + " rowsProjected=" + rowsProjected + " duplicates=" + duplicateRecords
                + " conflicts=" + payloadConflicts + " switches=" + sourceSwitches
                + " versionRefusals=" + versionRefusals
                + " gaps=" + gapsDetected + (gapPending ? " gapPendingAbove=" + gapPendingHighWater : "")
                + " epochs=" + epochs + " errors=" + errors
                + " stalls=" + sourceStalls + " backlog=" + sourceBacklogBytes
                + " pgFlushFailures=" + pgFlushFailures + "}";
    }
}
