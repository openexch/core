// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import com.openexchange.core.infrastructure.journal.generated.BoolFlag;
import com.openexchange.core.infrastructure.journal.generated.JournalDepositEncoder;
import com.openexchange.core.infrastructure.journal.generated.JournalOpeningBalanceEncoder;
import com.openexchange.core.infrastructure.journal.generated.JournalSettleEncoder;
import com.openexchange.core.infrastructure.journal.generated.JournalWithdrawEncoder;
import com.openexchange.core.infrastructure.journal.generated.MessageHeaderEncoder;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;
import java.util.Arrays;

/** Money journal records as the core writes them, one message each. */
final class JournalRecords {

    private static final int H = MessageHeaderEncoder.ENCODED_LENGTH;

    static byte[] opening(final long seq, final long userId, final int assetId, final long amount,
                          final long logPosition) {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[128]);
        final JournalOpeningBalanceEncoder e = new JournalOpeningBalanceEncoder()
                .wrapAndApplyHeader(b, 0, new MessageHeaderEncoder())
                .journalSeq(seq).userId(userId).assetId(assetId).amount(amount).logPosition(logPosition);
        return Arrays.copyOf(b.byteArray(), H + e.encodedLength());
    }

    static byte[] deposit(final long seq, final long userId, final int assetId, final long amount,
                          final long balanceAfter, final long clusterTimeMs, final long logPosition) {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[128]);
        final JournalDepositEncoder e = new JournalDepositEncoder().wrapAndApplyHeader(b, 0, new MessageHeaderEncoder())
                .journalSeq(seq).userId(userId).assetId(assetId).amount(amount).balanceAfter(balanceAfter)
                .clusterTimeMs(clusterTimeMs).logPosition(logPosition);
        return Arrays.copyOf(b.byteArray(), H + e.encodedLength());
    }

    static byte[] withdraw(final long seq, final long userId, final int assetId, final long amount,
                           final long balanceAfter, final long clusterTimeMs, final long logPosition) {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[128]);
        final JournalWithdrawEncoder e = new JournalWithdrawEncoder().wrapAndApplyHeader(b, 0, new MessageHeaderEncoder())
                .journalSeq(seq).userId(userId).assetId(assetId).amount(amount).balanceAfter(balanceAfter)
                .clusterTimeMs(clusterTimeMs).logPosition(logPosition);
        return Arrays.copyOf(b.byteArray(), H + e.encodedLength());
    }

    static byte[] settle(final long seq, final long tradeId, final long price, final long quantity,
                         final long clusterTimeMs, final long logPosition) {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[128]);
        final JournalSettleEncoder e = new JournalSettleEncoder().wrapAndApplyHeader(b, 0, new MessageHeaderEncoder())
                .journalSeq(seq).tradeId(tradeId).marketId(1).price(price).quantity(quantity)
                .buyerUserId(200).sellerUserId(300).takerIsBuy(BoolFlag.TRUE)
                .buyerBaseAfter(11 * seq).buyerQuoteAfter(22 * seq).sellerBaseAfter(33 * seq).sellerQuoteAfter(44 * seq)
                .clusterTimeMs(clusterTimeMs).logPosition(logPosition);
        return Arrays.copyOf(b.byteArray(), H + e.encodedLength());
    }

    /**
     * {@code record} as a version 1 writer wrote it: the header says version 1 and the version 1 block length, and the
     * block ends before the log position (the last field of every message).
     */
    static byte[] asVersion1(final byte[] record) {
        final byte[] v1 = Arrays.copyOf(record, record.length - Long.BYTES);
        final UnsafeBuffer b = new UnsafeBuffer(v1);
        b.putShort(0, (short) (b.getShort(0, ByteOrder.LITTLE_ENDIAN) - Long.BYTES), ByteOrder.LITTLE_ENDIAN);
        b.putShort(6, (short) 1, ByteOrder.LITTLE_ENDIAN); // version
        return v1;
    }

    /** A dense journal 1..n of every kind, each record a pure function of its journalSeq. */
    static byte[] record(final long seq) {
        final long position = 4_096 * seq;
        return switch ((int) (seq % 4)) {
            case 1 -> deposit(seq, 1_000 + seq, 1, 100_000_000L * seq, 200_000_000L * seq, 1_700_000_000_000L + seq,
                    position);
            case 2 -> withdraw(seq, 1_000 + seq, 1, 10_000_000L, 90_000_000L * seq, 1_700_000_000_000L + seq, position);
            case 3 -> settle(seq, 500 + seq, 1_000_000L, 2_000_000L * seq, 1_700_000_000_000L + seq, position);
            default -> opening(seq, 1_000 + seq, 2, 7_000_000L * seq, position);
        };
    }

    static byte[][] range(final long from, final long to) {
        final byte[][] out = new byte[(int) (to - from + 1)][];
        for (long s = from; s <= to; s++) {
            out[(int) (s - from)] = record(s);
        }
        return out;
    }

    private JournalRecords() {
    }
}
