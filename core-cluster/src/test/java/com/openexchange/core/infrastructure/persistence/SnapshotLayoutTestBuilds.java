// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.cluster.SnapshotLayouts;
import com.openexchange.core.application.engine.CoreEngine;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Builds that do not exist yet, for the layout tests: the build after this one, which adopts a next layout (v8) at
 * behaviour 5 and still carries v7, and the build after that, which has retired v7.
 *
 * <p>The v8 here is a stand-in, not a design: the v7 body under tag -8 with one trailing marker long. It is enough
 * to prove the mechanism, because the tests only need a second layout whose bytes differ and whose reader refuses
 * anything else.</p>
 */
public final class SnapshotLayoutTestBuilds {

    public static final long LAYOUT_V8_TAG = -8L;
    public static final long LAYOUT_V9_TAG = -9L;
    /** The behaviour version that adopts the stand-in v8. */
    public static final int V8_BEHAVIOR = 5;
    private static final long V8_TRAILER = 0x7777_7777_7777_7777L;

    /** The layout this build writes and reads. */
    public static final BalanceSnapshotCodec.Layout V7 = BalanceSnapshotCodec.LAYOUTS.current().codec();

    /** The stand-in next layout. */
    public static final BalanceSnapshotCodec.Layout V8 = new BalanceSnapshotCodec.Layout() {
        @Override
        public int write(final CoreEngine engine, final MutableDirectBuffer dst, final int offset) {
            final int length = V7.write(engine, dst, offset);
            dst.putLong(offset, LAYOUT_V8_TAG);
            dst.putLong(offset + length, V8_TRAILER);
            return length + 8;
        }

        @Override
        public BalanceSnapshotCodec.Decoded read(final DirectBuffer src, final int offset, final int length,
                                                 final CoreEngine engine) {
            if (length < 16 || src.getLong(offset) != LAYOUT_V8_TAG || src.getLong(offset + length - 8) != V8_TRAILER) {
                throw new IllegalStateException("balance snapshot: not a v8 image");
            }
            final byte[] body = new byte[length - 8];
            src.getBytes(offset, body);
            final UnsafeBuffer asV7 = new UnsafeBuffer(body);
            asV7.putLong(0, BalanceSnapshotCodec.LAYOUT_V7_TAG);
            final BalanceSnapshotCodec.Decoded d = V7.read(asV7, 0, body.length, engine);
            return new BalanceSnapshotCodec.Decoded(d.lastAppliedTradeId, d.consumePosition, d.journalSeq, d.lastLogTime,
                    d.accountCount, d.holdCount, d.sectionCount, d.bytesConsumed + 8);
        }

        @Override
        public long peekEventSeq(final DirectBuffer src, final int payloadOffset, final int payloadLength) {
            return src.getLong(payloadOffset + BalanceSnapshotCodec.EVENT_SEQ_OFFSET);
        }
    };

    /** This build's behaviours plus the one that adopts v8. */
    public static final int[] NEXT_BEHAVIORS = {1, 2, 3, 4, V8_BEHAVIOR};

    /** The build after this one: writes v7 up to behaviour 4 and v8 from behaviour 5, reads both. */
    public static final SnapshotLayouts<BalanceSnapshotCodec.Layout> NEXT_BUILD =
            SnapshotLayouts.<BalanceSnapshotCodec.Layout>of("balance snapshot")
                    .current(8, LAYOUT_V8_TAG, V8_BEHAVIOR, V8)
                    .previous(7, BalanceSnapshotCodec.LAYOUT_V7_TAG, 1L, V7)
                    .retired(6, BalanceSnapshotCodec.LAYOUT_V6_TAG)
                    .retired(5, BalanceSnapshotCodec.LAYOUT_V5_TAG)
                    .retired(4, BalanceSnapshotCodec.LAYOUT_V4_TAG)
                    .build();

    /** Two builds on: v7 is retired. Its v9 codec is never reached by the tests. */
    public static final SnapshotLayouts<BalanceSnapshotCodec.Layout> BUILD_AFTER_NEXT =
            SnapshotLayouts.<BalanceSnapshotCodec.Layout>of("balance snapshot")
                    .current(9, LAYOUT_V9_TAG, V8_BEHAVIOR + 1L, V8)
                    .previous(8, LAYOUT_V8_TAG, V8_BEHAVIOR, V8)
                    .retired(7, BalanceSnapshotCodec.LAYOUT_V7_TAG)
                    .build();

    private SnapshotLayoutTestBuilds() {
    }

    /** What the shadow restore's peek does with a full image carrying {@code tag}. */
    public static void shadowPeekRequireFullImage(final long tag) {
        new ShadowRestore.Peek(0, 0, false, tag).requireFullImage();
    }
}
