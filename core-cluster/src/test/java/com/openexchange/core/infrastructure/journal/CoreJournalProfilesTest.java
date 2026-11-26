// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.journal;

import com.openexchange.cluster.journal.JournalProfile;
import com.openexchange.core.infrastructure.InfrastructureConstants;
import com.openexchange.core.infrastructure.journal.generated.JournalDepositEncoder;
import com.openexchange.core.infrastructure.journal.generated.JournalOpeningBalanceEncoder;
import com.openexchange.core.infrastructure.journal.generated.JournalSettleEncoder;
import com.openexchange.core.infrastructure.journal.generated.JournalWithdrawEncoder;
import com.openexchange.core.infrastructure.journal.generated.MessageHeaderEncoder;
import com.openexchange.core.infrastructure.orderjournal.generated.OrderJournalEventEncoder;
import io.aeron.logbuffer.FrameDescriptor;
import org.agrona.BitUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The two journal profiles the kit's retention reads the core's journals by, held against records written by the
 * journals' own codecs: the key of every template comes back, one record is one frame within the profile's largest,
 * and the constants are the ones the writers and the tail readers use.
 */
public class CoreJournalProfilesTest {

    private static final int DATA_HEADER = 32;
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[512]);

    private static JournalProfile.Key key(final JournalProfile p, final UnsafeBuffer buffer, final int length)
            throws Exception {
        assertTrue("one record is one frame within the profile's largest",
                BitUtil.align(length + DATA_HEADER, FrameDescriptor.FRAME_ALIGNMENT) <= p.maxFrameLength());
        return p.key(buffer, 0, length, length + DATA_HEADER);
    }

    @Test
    public void theOrderProfileReadsARecordOfTheOrderJournalCodec() throws Exception {
        final OrderJournalEventEncoder e = new OrderJournalEventEncoder().wrapAndApplyHeader(buffer, 0,
                new com.openexchange.core.infrastructure.orderjournal.generated.MessageHeaderEncoder());
        e.journalSeq(4_711).epoch(24_301).logPosition(987_654_321L).orderId(5).userId(6);
        final int length = com.openexchange.core.infrastructure.orderjournal.generated.MessageHeaderEncoder
                .ENCODED_LENGTH + e.encodedLength();
        assertEquals(OrderJournal.MESSAGE_BYTES, length);
        final JournalProfile.Key k = key(CoreJournalProfiles.ORDER, buffer, length);
        assertEquals(4_711, k.journalSeq());
        assertEquals(24_301, k.epoch());
        assertEquals(987_654_321L, k.logPosition());
        assertEquals("the one frame of the tail reader", OrderJournalTail.MAX_FRAME_LENGTH,
                BitUtil.align(length + DATA_HEADER, FrameDescriptor.FRAME_ALIGNMENT));
        assertEquals(OrderJournalTail.MAX_FRAME_LENGTH, CoreJournalProfiles.ORDER.maxFrameLength());
        assertEquals(InfrastructureConstants.ORDER_JOURNAL_STREAM_ID, CoreJournalProfiles.ORDER.streamId());
        assertEquals(5, CoreJournalProfiles.ORDER.schemaId());
        assertTrue(CoreJournalProfiles.ORDER.hasEpoch());
        assertEquals("order", CoreJournalProfiles.ORDER.journal());
    }

    @Test
    public void theMoneyProfileReadsEveryTemplateOfTheMoneyJournalCodec() throws Exception {
        final MessageHeaderEncoder header = new MessageHeaderEncoder();
        final JournalOpeningBalanceEncoder opening = new JournalOpeningBalanceEncoder().wrapAndApplyHeader(buffer, 0,
                header);
        opening.journalSeq(1).userId(2).assetId(3).amount(4).logPosition(1_001);
        check(1, 1_001, MessageHeaderEncoder.ENCODED_LENGTH + opening.encodedLength());

        final JournalDepositEncoder deposit = new JournalDepositEncoder().wrapAndApplyHeader(buffer, 0, header);
        deposit.journalSeq(2).userId(2).assetId(3).amount(4).balanceAfter(5).clusterTimeMs(6).logPosition(1_002);
        check(2, 1_002, MessageHeaderEncoder.ENCODED_LENGTH + deposit.encodedLength());

        final JournalWithdrawEncoder withdraw = new JournalWithdrawEncoder().wrapAndApplyHeader(buffer, 0, header);
        withdraw.journalSeq(3).userId(2).assetId(3).amount(4).balanceAfter(5).clusterTimeMs(6).logPosition(1_003);
        check(3, 1_003, MessageHeaderEncoder.ENCODED_LENGTH + withdraw.encodedLength());

        final JournalSettleEncoder settle = new JournalSettleEncoder().wrapAndApplyHeader(buffer, 0, header);
        settle.journalSeq(4).tradeId(9).marketId(1).price(2).quantity(3).buyerUserId(4).sellerUserId(5)
                .buyerBaseAfter(6).buyerQuoteAfter(7).sellerBaseAfter(8).sellerQuoteAfter(9).clusterTimeMs(10)
                .logPosition(1_004);
        final int settleLength = MessageHeaderEncoder.ENCODED_LENGTH + settle.encodedLength();
        check(4, 1_004, settleLength);
        assertEquals("the largest money record is the tail reader's frame", MoneyJournalTail.MAX_FRAME_LENGTH,
                BitUtil.align(settleLength + DATA_HEADER, FrameDescriptor.FRAME_ALIGNMENT));

        assertEquals(MoneyJournalTail.MAX_FRAME_LENGTH, CoreJournalProfiles.MONEY.maxFrameLength());
        assertEquals(InfrastructureConstants.MONEY_JOURNAL_STREAM_ID, CoreJournalProfiles.MONEY.streamId());
        assertEquals(3, CoreJournalProfiles.MONEY.schemaId());
        assertFalse("the money journal's records carry no epoch", CoreJournalProfiles.MONEY.hasEpoch());
        assertEquals("money", CoreJournalProfiles.MONEY.journal());
    }

    private void check(final long seq, final long logPosition, final int length) throws Exception {
        final JournalProfile.Key k = key(CoreJournalProfiles.MONEY, buffer, length);
        assertEquals(seq, k.journalSeq());
        assertEquals(logPosition, k.logPosition());
        assertEquals(0, k.epoch());
    }

    /** A version 1 money record ends before the log position: the profile answers "none", never a wrong number. */
    @Test
    public void aVersionOneMoneyRecordHasNoLogPosition() throws Exception {
        final JournalDepositEncoder deposit = new JournalDepositEncoder().wrapAndApplyHeader(buffer, 0,
                new MessageHeaderEncoder());
        deposit.journalSeq(7).userId(2).assetId(3).amount(4).balanceAfter(5).clusterTimeMs(6).logPosition(1_007);
        final int v1Block = JournalDepositEncoder.BLOCK_LENGTH - Long.BYTES;
        buffer.putShort(0, (short) v1Block, java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.putShort(6, (short) 1, java.nio.ByteOrder.LITTLE_ENDIAN);
        final JournalProfile.Key k = key(CoreJournalProfiles.MONEY, buffer, MessageHeaderEncoder.ENCODED_LENGTH + v1Block);
        assertEquals(7, k.journalSeq());
        assertEquals(JournalProfile.NO_LOG_POSITION, k.logPosition());
    }

    @Test
    public void aRecordOfTheOtherJournalIsNotARecord() {
        final OrderJournalEventEncoder e = new OrderJournalEventEncoder().wrapAndApplyHeader(buffer, 0,
                new com.openexchange.core.infrastructure.orderjournal.generated.MessageHeaderEncoder());
        e.journalSeq(1).epoch(2).logPosition(3);
        try {
            CoreJournalProfiles.MONEY.key(buffer, 0, 100, 132);
        } catch (final JournalProfile.NotARecord expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("schema id 5, the journal's is 3"));
            return;
        }
        throw new AssertionError("the money profile read an order journal record");
    }
}
