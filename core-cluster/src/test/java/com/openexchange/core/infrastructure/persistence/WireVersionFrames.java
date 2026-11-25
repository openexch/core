// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.domain.Asset;
import com.openexchange.core.domain.Market;
import com.openexchange.core.infrastructure.generated.CoreConfigEncoder;
import com.openexchange.core.infrastructure.generated.DepositEncoder;
import com.openexchange.core.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.core.infrastructure.generated.WithdrawEncoder;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Ingress frames for the wire ceiling tests, stamped with any header version. A frame "at v11" is a v10 body under a
 * v11 header: money-schema evolution is append-only, so that is exactly what a v11 producer's frame looks like to a
 * v10 reader, and a jar that takes v11 decodes it at the header's version.
 */
public final class WireVersionFrames {

    private static final int HEADER = MessageHeaderEncoder.ENCODED_LENGTH;
    public static final long GENESIS = 0x5EEDL;

    private final UnsafeBuffer buf = new UnsafeBuffer(new byte[4096]);
    private final MessageHeaderEncoder header = new MessageHeaderEncoder();

    private byte[] stamp(final int bodyLength, final int version) {
        buf.putShort(6, (short) version, ByteOrder.LITTLE_ENDIAN);
        return Arrays.copyOf(buf.byteArray(), HEADER + bodyLength);
    }

    public byte[] deposit(final int version, final long corr, final long user, final int asset, final long amount) {
        buf.setMemory(0, buf.capacity(), (byte) 0);
        final int len = new DepositEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr).userId(user)
                .assetId(asset).amount(amount).requestIdHigh(0).requestIdLow(corr).encodedLength();
        return stamp(len, version);
    }

    public byte[] withdraw(final int version, final long corr, final long user, final int asset, final long amount) {
        buf.setMemory(0, buf.capacity(), (byte) 0);
        final int len = new WithdrawEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr).userId(user)
                .assetId(asset).amount(amount).requestIdHigh(0).requestIdLow(corr).encodedLength();
        return stamp(len, version);
    }

    /** The compiled tables as a CoreConfig carrying {@code ceiling}, under a {@code version} header. */
    public byte[] coreConfig(final int version, final long corr, final long configVersion, final long behavior,
                             final int ceiling) {
        buf.setMemory(0, buf.capacity(), (byte) 0);
        final CoreConfigEncoder e = new CoreConfigEncoder().wrapAndApplyHeader(buf, 0, header).correlationId(corr)
                .configVersion(configVersion).behaviorVersion(behavior).genesisId(GENESIS).maxWireVersion(ceiling);
        final CoreConfigEncoder.MarketsEncoder m = e.marketsCount(Market.ALL.length);
        for (final Market d : Market.ALL) {
            m.next().marketId(d.marketId()).symbol(d.symbol()).baseAssetId(d.baseAsset().id())
                    .quoteAssetId(d.quoteAsset().id()).tickSize(1).minPrice(1).maxPrice(1_000_000_000_000_000L).lotSize(0);
        }
        final CoreConfigEncoder.AssetsEncoder a = e.assetsCount(Asset.values().length);
        for (final Asset d : Asset.values()) {
            a.next().assetId(d.id()).code(d.name()).scale((short) 8);
        }
        return stamp(e.encodedLength(), version);
    }
}
