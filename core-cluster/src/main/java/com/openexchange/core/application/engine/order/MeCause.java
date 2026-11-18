// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.application.engine.order;

/**
 * The ME record being applied, for the order journal: set by each trade, terminal and outcome apply from the fields
 * of its log entry and cleared when that apply returns (per group element for a trade batch), so every order event
 * emitted inside the apply names the ME record that caused it and every other event carries zeros. Not replicated
 * and not snapshotted: it never outlives one apply, and its values come only from the log entry, so it is the same on
 * every member and on every replay.
 */
public final class MeCause {

    private boolean active;
    private long meEpoch;
    private long meJournalSeq;
    private long meLogPosition;
    private long meTimestampMs;
    private long takerOrderId;
    private long makerOrderId;

    /** A trade: the taker and maker core order ids decide liquidity. */
    public void trade(final long meEpoch, final long meJournalSeq, final long egressSeq, final long meTimestampMs,
                      final long takerOmsOrderId, final long makerOmsOrderId) {
        set(meEpoch, meJournalSeq, egressSeq, meTimestampMs);
        this.takerOrderId = takerOmsOrderId;
        this.makerOrderId = makerOmsOrderId;
    }

    /** A terminal or an outcome: no liquidity. */
    public void record(final long meEpoch, final long meJournalSeq, final long egressSeq, final long meTimestampMs) {
        set(meEpoch, meJournalSeq, egressSeq, meTimestampMs);
        this.takerOrderId = 0L;
        this.makerOrderId = 0L;
    }

    private void set(final long meEpoch, final long meJournalSeq, final long egressSeq, final long meTimestampMs) {
        this.active = true;
        this.meEpoch = meEpoch;
        this.meJournalSeq = meJournalSeq;
        this.meLogPosition = egressSeq;
        this.meTimestampMs = meTimestampMs;
    }

    public void clear() {
        active = false;
        meEpoch = 0L;
        meJournalSeq = 0L;
        meLogPosition = 0L;
        meTimestampMs = 0L;
        takerOrderId = 0L;
        makerOrderId = 0L;
    }

    public boolean active() {
        return active;
    }

    public long meEpoch() {
        return meEpoch;
    }

    public long meJournalSeq() {
        return meJournalSeq;
    }

    public long meLogPosition() {
        return meLogPosition;
    }

    public long meTimestampMs() {
        return meTimestampMs;
    }

    /** TAKER, MAKER or NONE for an event of {@code orderId}; only a FILL with a real trade carries liquidity. */
    public int liquidity(final long orderId, final boolean fillWithTrade) {
        if (!active || !fillWithTrade) {
            return 0;
        }
        if (orderId == takerOrderId) {
            return 1;
        }
        if (orderId == makerOrderId) {
            return 2;
        }
        return 0;
    }
}
