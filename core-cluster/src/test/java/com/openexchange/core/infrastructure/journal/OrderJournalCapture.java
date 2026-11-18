// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.journal;

import com.openexchange.core.infrastructure.orderjournal.generated.MessageHeaderDecoder;
import com.openexchange.core.infrastructure.orderjournal.generated.OrderJournalEventDecoder;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.IntUnaryOperator;

/**
 * The production service-thread half of the order journal ({@link OrderJournal}) with its ring drained by the test
 * instead of the writer agent: what a test reads is exactly what the writer would record. Also the test seams of the
 * journal package (the liquidity rewrite of the row controls).
 */
public final class OrderJournalCapture {

    public final OrderJournal journal;
    private final List<byte[]> records = new ArrayList<>();

    public OrderJournalCapture() {
        this(OrderJournalSettings.with(Map.of(OrderJournalSettings.Key.CORE_ORDER_JOURNAL_RING_BYTES, "4194304")));
    }

    public OrderJournalCapture(final OrderJournalSettings settings) {
        this.journal = new OrderJournal(settings);
    }

    /** Test build seam: rewrite the liquidity byte (a control that leaves it 0, or writes node-local high bits). */
    public OrderJournalCapture liquidityRewrite(final IntUnaryOperator rewrite) {
        journal.liquidityRewrite = rewrite;
        return this;
    }

    /** Test build seam on any journal (a cluster test's members). */
    public static void liquidityRewrite(final OrderJournal journal, final IntUnaryOperator rewrite) {
        journal.liquidityRewrite = rewrite;
    }

    /** Test build seam: the no-progress readiness alarm off (the fill bound alone decides). */
    public static void progressAlarmOff(final OrderJournal journal, final boolean off) {
        journal.progressAlarmOff = off;
    }

    /** Drain the ring; every record drained so far, in ring order. */
    public List<byte[]> records() {
        journal.ringBuffer().read((type, buffer, index, length) -> {
            final byte[] b = new byte[length];
            buffer.getBytes(index, b);
            records.add(b);
        }, Integer.MAX_VALUE);
        return records;
    }

    /** A decoder over one record (header checked: schema 5, template 1). */
    public static OrderJournalEventDecoder decode(final byte[] record) {
        final UnsafeBuffer b = new UnsafeBuffer(record);
        final MessageHeaderDecoder h = new MessageHeaderDecoder().wrap(b, 0);
        if (h.schemaId() != OrderJournalEventDecoder.SCHEMA_ID || h.templateId() != OrderJournalEventDecoder.TEMPLATE_ID) {
            throw new AssertionError("not an order journal record: schema " + h.schemaId() + " template " + h.templateId());
        }
        return new OrderJournalEventDecoder().wrapAndApplyHeader(b, 0, new MessageHeaderDecoder());
    }
}
