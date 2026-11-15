// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.publisher;

import com.openexchange.core.domain.CoreOrderView;
import com.openexchange.core.domain.CoreRejectReason;
import com.openexchange.core.domain.EventKind;
import com.openexchange.core.infrastructure.generated.OrderEventEncoder;

/**
 * The one place that writes the body of a wire OrderEvent. The live broadcast and the replay ring both encode through
 * it, so a replayed record is the broadcast record byte for byte by construction. The encoder must already be wrapped
 * where the body goes.
 */
public final class OrderEventRecordCodec {

    /** Bytes of one OrderEvent body. Every byte is a field: there is no padding to leave stale. */
    public static final int RECORD_BYTES = OrderEventEncoder.BLOCK_LENGTH;

    private OrderEventRecordCodec() {
    }

    public static void encode(final OrderEventEncoder e, final long eventSeq, final long logPosition, final CoreOrderView o,
                              final EventKind kind, final CoreRejectReason reason, final long lastPrice, final long lastQty,
                              final long tradeId, final long holdRemaining, final long updatedAtMs, final int meReason) {
        e.eventSeq(eventSeq).logPosition(logPosition).orderId(o.orderId()).userId(o.userId()).marketId(o.marketId())
                .eventKind(com.openexchange.core.infrastructure.generated.EventKind.get((short) kind.code()))
                .status(com.openexchange.core.infrastructure.generated.OrderStatus.get((short) o.statusCode()))
                .reason(com.openexchange.core.infrastructure.generated.CoreRejectReason.get((short) reason.code()))
                .side(com.openexchange.core.infrastructure.generated.Side.get((short) o.sideCode()))
                .orderType(com.openexchange.core.infrastructure.generated.OrderType.get((short) o.orderTypeCode()))
                .timeInForce(com.openexchange.core.infrastructure.generated.TimeInForce.get((short) o.timeInForceCode()))
                .price(o.price()).stopPrice(o.stopPrice()).quantity(o.quantity()).filledQty(o.filledQty())
                .remainingQty(o.remainingQty()).lastPrice(lastPrice).lastQty(lastQty).tradeId(tradeId)
                .meOrderId(o.meOrderId()).holdAssetId(o.holdAssetId()).holdAmount(o.holdAmount()).holdRemaining(holdRemaining)
                .clientOrderIdHigh(o.clientOrderIdHigh()).clientOrderIdLow(o.clientOrderIdLow())
                .createdAtMs(o.createdAtMs()).updatedAtMs(updatedAtMs)
                .meReason((short) meReason); // the engine's code on engine-caused events, else 255
    }
}
