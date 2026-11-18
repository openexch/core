// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.domain;

/**
 * Outbound port for the order journal: one call per order event, on every member, leader or not, from the single
 * exit of an order event (after the replay ring append). The production adapter ({@code OrderJournal}) encodes the
 * broadcast body with the one OrderEvent codec and adds the journal's own fields; tests capture the calls directly.
 *
 * <p>Every argument is a function of the log: {@code journalSeq} is the engine's eventSeq, {@code epoch} the recorded
 * CoreConfig genesisId, {@code logPosition} the applying entry's position, and the ME cause ({@code liquidity},
 * {@code meEpoch}, {@code meJournalSeq}, {@code meLogPosition}, {@code meTimestampMs}) the fields of the ME record
 * whose apply emitted the event, all zero when no ME record is being applied. Invoked synchronously on the service
 * thread; an implementation may block (lossless) but must never drop.</p>
 */
public interface OrderJournalSink {

    /** Liquidity codes of the record. */
    int LIQUIDITY_NONE = 0;
    int LIQUIDITY_TAKER = 1;
    int LIQUIDITY_MAKER = 2;

    /** Nothing journaled (unit tests that do not look at the journal). */
    OrderJournalSink NO_OP = (journalSeq, epoch, logPosition, order, kind, reason, lastPrice, lastQty, tradeId, holdRemaining,
                              updatedAtMs, meReason, liquidity, meEpoch, meJournalSeq, meLogPosition, meTimestampMs) -> {
    };

    void onOrderEvent(long journalSeq, long epoch, long logPosition, CoreOrderView order, EventKind kind,
                      CoreRejectReason reason, long lastPrice, long lastQty, long tradeId, long holdRemaining,
                      long updatedAtMs, int meReason, int liquidity, long meEpoch, long meJournalSeq, long meLogPosition,
                      long meTimestampMs);
}
