// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.journal;

import com.openexchange.cluster.journal.JournalProfile;
import com.openexchange.core.infrastructure.InfrastructureConstants;
import com.openexchange.core.infrastructure.journal.generated.JournalDepositDecoder;
import com.openexchange.core.infrastructure.journal.generated.JournalOpeningBalanceDecoder;
import com.openexchange.core.infrastructure.journal.generated.JournalSettleDecoder;
import com.openexchange.core.infrastructure.journal.generated.JournalWithdrawDecoder;
import com.openexchange.core.infrastructure.orderjournal.generated.OrderJournalEventDecoder;

import java.util.List;
import java.util.Map;

/**
 * The core's two journals as the kit's journal retention reads them: where each is recorded and where its records
 * carry {@code journalSeq}, the epoch and the log position. Every offset comes from the generated decoder of the
 * record, so a schema change moves it here with the codec.
 *
 * <p>The money journal's records carry no epoch: its retention identity is the reader file's genesis id against the
 * member's. A version 1 money record has no log position. The order journal's records carry the recorded genesis id
 * as their epoch.</p>
 */
public final class CoreJournalProfiles {

    /** The money journal's name in settings, reader files and metrics. */
    public static final String MONEY_NAME = "money";
    /** The order journal's name in settings, reader files and metrics. */
    public static final String ORDER_NAME = "order";

    private static final int HEADER = com.openexchange.core.infrastructure.journal.generated.MessageHeaderDecoder
            .ENCODED_LENGTH;
    private static final int ORDER_HEADER = com.openexchange.core.infrastructure.orderjournal.generated
            .MessageHeaderDecoder.ENCODED_LENGTH;

    /** Stream 4101, schema 3: no epoch, the log position at the end of every version 2 block. */
    public static final JournalProfile MONEY = new JournalProfile(MONEY_NAME,
            InfrastructureConstants.MONEY_JOURNAL_STREAM_ID, "aeron:ipc", JournalSettleDecoder.SCHEMA_ID,
            HEADER + JournalSettleDecoder.journalSeqEncodingOffset(), JournalProfile.NO_EPOCH,
            Map.of(JournalOpeningBalanceDecoder.TEMPLATE_ID, HEADER + JournalOpeningBalanceDecoder.logPositionEncodingOffset(),
                    JournalDepositDecoder.TEMPLATE_ID, HEADER + JournalDepositDecoder.logPositionEncodingOffset(),
                    JournalWithdrawDecoder.TEMPLATE_ID, HEADER + JournalWithdrawDecoder.logPositionEncodingOffset(),
                    JournalSettleDecoder.TEMPLATE_ID, HEADER + JournalSettleDecoder.logPositionEncodingOffset()),
            JournalSettleDecoder.logPositionSinceVersion(), MoneyJournalTail.MAX_FRAME_LENGTH);

    /** Stream 4102, schema 5: one 256-byte frame per record, {@code journalSeq}, epoch and log position at its head. */
    public static final JournalProfile ORDER = new JournalProfile(ORDER_NAME,
            InfrastructureConstants.ORDER_JOURNAL_STREAM_ID, "aeron:ipc", OrderJournalEventDecoder.SCHEMA_ID,
            ORDER_HEADER + OrderJournalEventDecoder.journalSeqEncodingOffset(),
            ORDER_HEADER + OrderJournalEventDecoder.epochEncodingOffset(),
            Map.of(OrderJournalEventDecoder.TEMPLATE_ID, ORDER_HEADER + OrderJournalEventDecoder.logPositionEncodingOffset()),
            0, OrderJournalTail.MAX_FRAME_LENGTH);

    /** Both, in the order the retention passes them. */
    public static final List<JournalProfile> ALL = List.of(MONEY, ORDER);

    private CoreJournalProfiles() {
    }
}
