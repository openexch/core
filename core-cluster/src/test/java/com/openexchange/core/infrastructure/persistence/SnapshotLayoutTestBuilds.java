// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.cluster.SnapshotLayouts;
import com.openexchange.core.application.engine.CoreEngine;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Builds that do not exist yet, for the layout tests: the build after this one, which adopts a next layout (v7) at
 * behaviour 5 and still carries v6, and the build after that, which has retired v6.
 *
 * <p>The v7 here is a stand-in, not a design: the v6 body under tag -7 with one trailing marker long. It is enough
 * to prove the mechanism, because the tests only need a second layout whose bytes differ and whose reader refuses
 * anything else.</p>
 */
public final class SnapshotLayoutTestBuilds {

    public static final long LAYOUT_V7_TAG = -7L;
    public static final long LAYOUT_V8_TAG = -8L;
    /** The behaviour version that adopts the stand-in v7. */
    public static final int V7_BEHAVIOR = 5;
    private static final long V7_TRAILER = 0x7777_7777_7777_7777L;

    /** The layout this build writes and reads. */
    public static final BalanceSnapshotCodec.Layout V6 = BalanceSnapshotCodec.LAYOUTS.current().codec();

    /** The stand-in next layout. */
    public static final BalanceSnapshotCodec.Layout V7 = new BalanceSnapshotCodec.Layout() {
        @Override
        public int write(final CoreEngine engine, final MutableDirectBuffer dst, final int offset) {
            final int length = V6.write(engine, dst, offset);
            dst.putLong(offset, LAYOUT_V7_TAG);
            dst.putLong(offset + length, V7_TRAILER);
            return length + 8;
        }

        @Override
        public BalanceSnapshotCodec.Decoded read(final DirectBuffer src, final int offset, final int length,
                                                 final CoreEngine engine) {
            if (length < 16 || src.getLong(offset) != LAYOUT_V7_TAG || src.getLong(offset + length - 8) != V7_TRAILER) {
                throw new IllegalStateException("balance snapshot: not a v7 image");
            }
            final byte[] body = new byte[length - 8];
            src.getBytes(offset, body);
            final UnsafeBuffer asV6 = new UnsafeBuffer(body);
            asV6.putLong(0, BalanceSnapshotCodec.LAYOUT_V6_TAG);
            final BalanceSnapshotCodec.Decoded d = V6.read(asV6, 0, body.length, engine);
            return new BalanceSnapshotCodec.Decoded(d.lastAppliedTradeId, d.consumePosition, d.journalSeq, d.lastLogTime,
                    d.accountCount, d.holdCount, d.sectionCount, d.bytesConsumed + 8);
        }

        @Override
        public long peekEventSeq(final DirectBuffer src, final int payloadOffset, final int payloadLength) {
            return src.getLong(payloadOffset + BalanceSnapshotCodec.EVENT_SEQ_OFFSET);
        }
    };

    /** This build's behaviours plus the one that adopts v7. */
    public static final int[] NEXT_BEHAVIORS = {1, 2, 3, 4, V7_BEHAVIOR};

    /** The build after this one: writes v6 up to behaviour 4 and v7 from behaviour 5, reads both. */
    public static final SnapshotLayouts<BalanceSnapshotCodec.Layout> NEXT_BUILD =
            SnapshotLayouts.<BalanceSnapshotCodec.Layout>of("balance snapshot")
                    .current(7, LAYOUT_V7_TAG, V7_BEHAVIOR, V7)
                    .previous(6, BalanceSnapshotCodec.LAYOUT_V6_TAG, 1L, V6)
                    .retired(5, BalanceSnapshotCodec.LAYOUT_V5_TAG)
                    .retired(4, BalanceSnapshotCodec.LAYOUT_V4_TAG)
                    .build();

    /** Two builds on: v6 is retired. Its v8 codec is never reached by the tests. */
    public static final SnapshotLayouts<BalanceSnapshotCodec.Layout> BUILD_AFTER_NEXT =
            SnapshotLayouts.<BalanceSnapshotCodec.Layout>of("balance snapshot")
                    .current(8, LAYOUT_V8_TAG, V7_BEHAVIOR + 1L, V7)
                    .previous(7, LAYOUT_V7_TAG, V7_BEHAVIOR, V7)
                    .retired(6, BalanceSnapshotCodec.LAYOUT_V6_TAG)
                    .build();

    private SnapshotLayoutTestBuilds() {
    }

    /** What the shadow restore's peek does with a full image carrying {@code tag}. */
    public static void shadowPeekRequireFullImage(final long tag) {
        new ShadowRestore.Peek(0, 0, false, tag).requireFullImage();
    }
}
