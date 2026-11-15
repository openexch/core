// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.application.engine.feed.V3Rig;
import com.openexchange.core.determinism.CoreEngineEvent;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import java.security.MessageDigest;
import java.util.HexFormat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * RequestOrderEvents, RequestUserBalances and RequestCoreConfig are read-only: any number of them, in any shape,
 * leaves the snapshot image (compared by SHA-256) unchanged and broadcasts nothing. The control applies a
 * CoreLimitsSet between the two digests, which is not read-only, and the digests must differ.
 */
public class GatewayQueriesAreReadOnlyTest {

    private final CoreFrameEncoders frames = new CoreFrameEncoders();

    private static String imageDigest(final V3Rig r) throws Exception {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final int length = BalanceSnapshotCodec.serialize(r.engine, buf, 0);
        final MessageDigest sha = MessageDigest.getInstance("SHA-256");
        sha.update(buf.byteArray(), 0, length);
        return HexFormat.of().formatHex(sha.digest());
    }

    private static V3Rig busy() {
        final V3Rig r = V3Rig.openAt(4);
        r.engine.applyCoreLimitsSet(1L, 1L, 900_000L, 256, 6, r.now); // a small ring, wrapped by the story below
        r.twoParty();
        r.sellHits(1, V3Rig.ONE);
        r.create(r.limit(1, 0, 90 * V3Rig.ONE, V3Rig.ONE));
        r.create(r.limit(2, 1, 110 * V3Rig.ONE, V3Rig.ONE));
        assertTrue("precondition: the ring wrapped", r.engine.getEventSeq() > 6);
        assertEquals(6, r.engine.orders().eventRing().count());
        return r;
    }

    @Test
    public void anyNumberOfQueriesLeavesTheSnapshotDigestUnchanged() throws Exception {
        final V3Rig r = busy();
        final String before = imageDigest(r);
        final long last = r.engine.getEventSeq();
        r.mark();
        int queries = 0;
        for (int round = 0; round < 50; round++) {
            r.now += 1_000; // queries carry later timestamps than the last write
            for (final long from : new long[] {-1, 0, last - 6, last - 3, last, last + 5, Long.MAX_VALUE}) {
                for (final int max : new int[] {-1, 0, 1, 2, 1_024, Integer.MAX_VALUE}) {
                    r.dispatchTrusted(r.load(frames.orderEvents(round, from, max)));
                    queries++;
                }
            }
            for (final long user : new long[] {0, 1, 2, 3, 999_999, -5}) {
                r.dispatchTrusted(r.load(frames.userBalances(round, user)));
                queries++;
            }
            r.dispatchTrusted(r.load(frames.coreConfigRequest(round)));
            queries++;
        }
        assertEquals("the image is unchanged after " + queries + " queries", before, imageDigest(r));
        assertEquals("no frame was dropped", 0L, r.demuxer.decodeRejectCount() + r.demuxer.unknownTemplateCount());
        for (final CoreEngineEvent e : r.events()) {
            assertTrue("a query emits only its own reply, got " + e.render(), e instanceof CoreEngineEvent.OrderEventReplay
                    || e instanceof CoreEngineEvent.OrderEventsEnd || e instanceof CoreEngineEvent.UserBalances
                    || e instanceof CoreEngineEvent.CoreConfigReport || e instanceof CoreEngineEvent.OrderReject);
        }
        assertEquals("the event sequence did not move", last, r.engine.getEventSeq());
        assertTrue(CoreSbeDemuxer.isQuery(54) && CoreSbeDemuxer.isQuery(55) && CoreSbeDemuxer.isQuery(56));
        assertFalse("the limits record is not a query", CoreSbeDemuxer.isQuery(77));
    }

    @Test
    public void controlAWriteBetweenTheDigestsChangesThem() throws Exception {
        final V3Rig r = busy();
        final String before = imageDigest(r);
        r.dispatchTrusted(r.load(frames.limits(1L, 2L, 900_000L, 256, 6)));
        assertFalse("the digest sees a state change", before.equals(imageDigest(r)));
    }
}
