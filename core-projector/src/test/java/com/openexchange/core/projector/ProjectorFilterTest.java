// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import org.junit.Test;

import static com.openexchange.core.projector.ProjectorFilter.Action.GAP;
import static com.openexchange.core.projector.ProjectorFilter.Action.PROJECT;
import static com.openexchange.core.projector.ProjectorFilter.Action.DUPLICATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Pins the projector's money-critical projection semantics on the single dense journalSeq: resume
 * duplicate (compared, never inserted) at or below the Postgres high-water, exact +1 advance, dense-gap GAP
 * (latched), and the explicit
 * haltOnGap=false operator override. These rules are what make the projector safe to crash, restart
 * and re-run purely from the PG high-water.
 */
public class ProjectorFilterTest {

    @Test
    public void freshLedgerProjectsFromJournalSeqOne() {
        ProjectorFilter f = new ProjectorFilter(0L, true);
        assertEquals(PROJECT, f.onRecord(1L));
        assertEquals(PROJECT, f.onRecord(2L));
        assertEquals(PROJECT, f.onRecord(3L));
        assertEquals(3L, f.highWater());
        assertEquals(0L, f.gapsDetected());
        assertFalse(f.gapped());
    }

    @Test
    public void resumeMarksAtOrBelowHighWaterAsDuplicateAndProjectsTheNext() {
        // Postgres already has journalSeq 1..100.
        ProjectorFilter f = new ProjectorFilter(100L, true);
        assertEquals(DUPLICATE, f.onRecord(1L));    // replay-from-start overlap
        assertEquals(DUPLICATE, f.onRecord(100L));  // the boundary itself is already projected
        assertEquals(PROJECT, f.onRecord(101L));
        assertEquals(PROJECT, f.onRecord(102L));
        assertEquals(2L, f.duplicates());
        assertEquals(102L, f.highWater());
    }

    @Test
    public void duplicateReplayAcrossRecordingsIsADuplicate() {
        ProjectorFilter f = new ProjectorFilter(0L, true);
        assertEquals(PROJECT, f.onRecord(1L));
        assertEquals(PROJECT, f.onRecord(2L));
        // A stopped-recording replay re-delivers old records before the active one: dedupe by seq.
        assertEquals(DUPLICATE, f.onRecord(1L));
        assertEquals(DUPLICATE, f.onRecord(2L));
        assertEquals(PROJECT, f.onRecord(3L));
        assertEquals(2L, f.duplicates());
        assertEquals(0L, f.gapsDetected());
    }

    @Test
    public void denseGapLatchesForever() {
        ProjectorFilter f = new ProjectorFilter(10L, true);
        assertEquals(PROJECT, f.onRecord(11L));
        // 13 skips 12: a lost money movement. Nothing may be projected past it.
        assertEquals(GAP, f.onRecord(13L));
        assertTrue(f.gapped());
        assertEquals(1L, f.gapsDetected());
        // Latched: even the "missing" 12 arriving later must not resume a halted filter.
        assertEquals(GAP, f.onRecord(12L));
        assertEquals(GAP, f.onRecord(14L));
        assertEquals(11L, f.highWater());
    }

    @Test
    public void retentionRaceSurfacesAsAGapNotASilentSkip() {
        // PG at journalSeq 50; the earliest retained journal record is 60 (journal purged past the
        // projector's needs, or projector down too long): that MUST halt, not silently skip.
        ProjectorFilter f = new ProjectorFilter(50L, true);
        assertEquals(GAP, f.onRecord(60L));
        assertTrue(f.gapped());
        assertEquals(50L, f.highWater());
    }

    @Test
    public void haltOnGapFalseProjectsPastTheGapButCountsIt() {
        ProjectorFilter f = new ProjectorFilter(10L, false);
        assertEquals(PROJECT, f.onRecord(11L));
        assertEquals(PROJECT, f.onRecord(13L)); // operator override: project, count
        assertEquals(1L, f.gapsDetected());
        assertFalse(f.gapped());
        assertEquals(13L, f.highWater());
        // The late 12 is now a duplicate-by-ordering: a duplicate (compared with its stored row if one exists).
        assertEquals(DUPLICATE, f.onRecord(12L));
    }
}
