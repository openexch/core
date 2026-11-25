// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.application.engine.CoreEngine;
import com.openexchange.core.application.projection.SettlementProjector;
import com.openexchange.core.determinism.RecordingCoreSink;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Two members on different jars (money-schema 10, and the next jar standing in as 11) fed one log: v10 commands, v11
 * frames, the raise, more v11 frames. Until the raise both write the same snapshot bytes at every position, because the
 * v11 frames are dropped from the header on both. At the raise the member on the earlier jar stops itself with the
 * node-local text; the other one continues and applies v11.
 */
public class MixedJarWireDeterminismTest {

    private static final long ONE = 100_000_000L;

    private static final class Member {
        final CoreEngine engine = new CoreEngine();
        final CoreSbeDemuxer demuxer;
        final List<String> stops = new ArrayList<>();

        Member(final int jarMax) {
            engine.setEventSink(new RecordingCoreSink());
            engine.coreConfig().setSupportedMaxWireVersion(jarMax);
            engine.setConfigFailFast(() -> stops.add(engine.coreConfig().crossCheck()));
            demuxer = new CoreSbeDemuxer(engine, new SettlementProjector(engine), jarMax);
        }

        void apply(final byte[] frame, final long ts) {
            demuxer.dispatchTrusted(new UnsafeBuffer(frame), 0, frame.length, ts);
        }

        byte[] image() {
            final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
            final int length = BalanceSnapshotCodec.serialize(engine, buf, 0);
            return Arrays.copyOf(buf.byteArray(), length);
        }
    }

    private static String sha256(final byte[] b) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(b));
    }

    /** Equal bytes and equal SHA-256, or a failure that names the position. */
    static void assertSameImages(final int position, final byte[] a, final byte[] b) throws Exception {
        assertArrayEquals("two jars wrote different snapshot bytes at log position " + position, a, b);
        assertEquals("SHA-256 at log position " + position, sha256(a), sha256(b));
    }

    @Test
    public void beforeTheRaiseTheJarsAgreeByteForByteAndAtTheRaiseTheEarlierJarStops() throws Exception {
        final WireVersionFrames f = new WireVersionFrames();
        final List<byte[]> beforeRaise = new ArrayList<>();
        beforeRaise.add(f.coreConfig(10, 1, 1, 1, 10));
        long corr = 100;
        for (int i = 0; i < 6; i++) {
            final long user = 1 + (i % 3);
            beforeRaise.add(f.deposit(10, corr++, user, 0, (10 + i) * ONE));
            beforeRaise.add(f.deposit(11, corr++, user, 0, 1_000 * ONE)); // the next producer, too early
            beforeRaise.add(f.withdraw(10, corr++, user, 0, ONE));
            beforeRaise.add(f.withdraw(11, corr++, user, 0, 5 * ONE)); // too early, too
        }
        beforeRaise.add(f.coreConfig(11, corr++, 2, 1, 11)); // a raise encoded above the ceiling: dropped by both

        final Member atTen = new Member(10);
        final Member atEleven = new Member(11);
        long ts = 1_700_000_000_000L;
        int position = 0;
        for (final byte[] frame : beforeRaise) {
            atTen.apply(frame, ts);
            atEleven.apply(frame, ts);
            ts++;
            position++;
            assertSameImages(position, atTen.image(), atEleven.image());
        }
        assertEquals(13L, atTen.demuxer.aboveCeilingDropCount());
        assertEquals(13L, atEleven.demuxer.aboveCeilingDropCount());
        assertEquals(List.of(), atTen.stops);

        // the raise, encoded at v10
        final byte[] raise = f.coreConfig(10, corr++, 2, 1, 11);
        atTen.apply(raise, ts);
        atEleven.apply(raise, ts);
        ts++;
        assertEquals("the earlier jar stops at the raise",
                List.of("recorded maxWireVersion=11 is above this jar's money-schema version 10"), atTen.stops);
        assertEquals(List.of(), atEleven.stops);
        assertSameImages(++position, atTen.image(), atEleven.image());

        // the next jar continues and applies v11
        final byte[] before = atEleven.image();
        atEleven.apply(f.deposit(11, corr++, 1, 0, 3 * ONE), ts++);
        atEleven.apply(f.withdraw(11, corr, 1, 0, ONE), ts);
        assertNotEquals("v11 frames now move state", sha256(before), sha256(atEleven.image()));
        assertEquals(13L, atEleven.demuxer.aboveCeilingDropCount());
    }
}
