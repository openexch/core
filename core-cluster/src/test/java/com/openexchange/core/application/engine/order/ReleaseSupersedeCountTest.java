// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.application.engine.order;

import com.openexchange.core.application.engine.feed.V3Rig;
import com.openexchange.core.determinism.CoreEngineEvent;
import com.openexchange.core.application.engine.CoreEngine;
import com.openexchange.core.domain.DispatchKind;
import org.junit.Test;

import java.util.List;

import static com.openexchange.core.application.engine.order.ReleaseTestSupport.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * The superseded-release counter: a release mint that replaces a backlog entry still at stage RELEASE takes the old
 * release seq out of the pending index, so that seq is never re-offered to the ME. The counter only counts; dispatch and
 * release behaviour are unchanged. It is node-local like the minted counters: derived from the apply path, equal on
 * every member that applied the same log, and not part of the snapshot.
 */
public class ReleaseSupersedeCountTest {

    /** Resting buy (UP_TO minted), amend (UP_TO supersedes UP_TO), cancel and end (ORDER supersedes UP_TO). */
    private static V3Rig twoSupersedes() {
        final V3Rig r = funded();
        final long h = restingBuy(r, 101);
        r.amend(1, h, 0, 2 * ONE);
        assertTrue(r.outcome(h, 1, AMEND, APPLIED, 303, 0, false));
        cancel(r, 1, h);
        assertTrue(r.terminal(h, 303, 1, V3Rig.ME_CANCELLED, 0));
        return r;
    }

    @Test
    public void everySupersedeOfAPendingReleaseCountsOnce() {
        final V3Rig r = funded();
        final long h = restingBuy(r, 101);
        final OrderEngine oe = r.engine.orders();
        assertEquals("a first mint replaces nothing", 0L, oe.releaseSupersededCount());
        final long first = entry(r, h).dispatchSeq;

        r.amend(1, h, 0, 2 * ONE);
        assertTrue(r.outcome(h, 1, AMEND, APPLIED, 303, 0, false));
        assertFalse("the old seq left the index", oe.dispatchIndex().contains(first));
        assertEquals("UP_TO over UP_TO", 1L, oe.releaseSupersededCount());

        cancel(r, 1, h);
        assertTrue(r.terminal(h, 303, 1, V3Rig.ME_CANCELLED, 0));
        assertEquals(ORDER, entry(r, h).scope);
        assertEquals("ORDER over UP_TO", 2L, oe.releaseSupersededCount());
        assertEquals("minted = live entries + superseded", oe.releaseMintedUpToCount() + oe.releaseMintedOrderCount(),
                oe.backlog().size() + oe.releaseSupersededCount());
        checkInvariants(r, "supersede count");
    }

    @Test
    public void countingChangesNoDispatch() {
        final V3Rig r = funded();
        final long h = restingBuy(r, 101);
        r.mark();
        r.amend(1, h, 0, 2 * ONE);
        assertTrue(r.outcome(h, 1, AMEND, APPLIED, 303, 0, false));
        cancel(r, 1, h);
        assertTrue(r.terminal(h, 303, 1, V3Rig.ME_CANCELLED, 0));
        final List<CoreEngineEvent.MeDispatch> rel = dispatches(r, DispatchKind.RELEASE_ID);
        assertEquals("each mint is broadcast once, the superseded ones included", 2, rel.size());
        assertTrue(rel.get(1).dispatchSeq() > rel.get(0).dispatchSeq());
        assertEquals("one entry per order", 1, r.engine.orders().backlog().size());
    }

    @Test
    public void aMintAfterAnOrphanCancelIsNotASupersede() {
        final V3Rig r = funded();
        final long h = restingBuy(r, 101);
        cancel(r, 1, h);
        assertTrue(r.terminal(h, 101, 1, V3Rig.ME_CANCELLED, 0));
        assertNull(r.order(h));
        final OrderEngine oe = r.engine.orders();
        final long before = oe.releaseSupersededCount();
        assertEquals("ORDER over the resting UP_TO", 1L, before);
        assertTrue(r.releaseEntry(h, (1L << 2) | 3, ORDER, R_LEG_LIVE));
        assertEquals(ReleaseBacklog.STAGE_ORPHAN_CANCEL, entry(r, h).stage);
        assertTrue(r.outcome(h, 2, CANCEL, APPLIED, 101, 0, false));
        assertEquals("E1 minted an ORDER", ReleaseBacklog.STAGE_RELEASE, entry(r, h).stage);
        assertEquals("the replaced seq was an orphan cancel the ME answered, not a pending release", before,
                oe.releaseSupersededCount());
    }

    @Test
    public void deterministicAcrossMembersAndNodeLocalAcrossRestore() {
        final V3Rig a = twoSupersedes();
        final V3Rig b = twoSupersedes();
        assertEquals("the same log, the same count", a.engine.orders().releaseSupersededCount(),
                b.engine.orders().releaseSupersededCount());
        assertEquals(2L, a.engine.orders().releaseSupersededCount());
        final CoreEngine restored = roundTrip(a.engine, "the counter is not in the image");
        assertEquals("node-local like the minted counters", restored.orders().releaseMintedUpToCount(),
                restored.orders().releaseSupersededCount());
        assertEquals(0L, restored.orders().releaseSupersededCount());
        assertEquals(0L, restored.orders().releaseMintedOrderCount());
    }
}
