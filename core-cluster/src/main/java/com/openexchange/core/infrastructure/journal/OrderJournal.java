// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.journal;

import com.openexchange.core.domain.CoreOrderView;
import com.openexchange.core.domain.CoreRejectReason;
import com.openexchange.core.domain.EventKind;
import com.openexchange.core.domain.OrderJournalSink;
import com.openexchange.core.infrastructure.Logger;
import com.openexchange.core.infrastructure.generated.OrderEventEncoder;
import com.openexchange.core.infrastructure.orderjournal.generated.MessageHeaderEncoder;
import com.openexchange.core.infrastructure.orderjournal.generated.OrderJournalEventEncoder;
import com.openexchange.core.infrastructure.publisher.OrderEventRecordCodec;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.IntUnaryOperator;

/**
 * Service-thread half of the order journal: one record per order event into an in-memory SPSC ring, drained by
 * {@link OrderJournalWriterAgent} into a recorded publication on the member's own archive. A copy of the money
 * journal's shape ({@link MoneyJournal}): the money journal's classes are not shared on purpose.
 *
 * <p>Lossless: on a full ring the service thread waits, it never sheds, and logs an ERROR line every
 * {@code CORE_ORDER_JOURNAL_STALL_LOG_MS} while it waits. The wait backs off to a park and ends only when the ring
 * takes the record, or with an {@link AgentTerminationException} when the thread is interrupted or the journal is
 * closed (a member that is shutting down must be able to close). A full ring means the local archive has stalled; the
 * member then stops applying (on a follower it lags; on the leader order entry stops until the archive drains), and
 * its readiness reads false through the alarm ({@link #alarmed}).</p>
 *
 * <p>The record: the journal head (journalSeq = eventSeq, epoch = the recorded genesisId, the log position), the
 * OrderEvent body bytes 16..166 written by {@link OrderEventRecordCodec} (the one codec of the broadcast and the replay
 * ring), then liquidity and the ME cause. Every input is a function of the log, so every member writes the same
 * bytes.</p>
 *
 * <p>Single writer: the sink runs only on the cluster service thread. Counters are volatile for the metrics side.</p>
 */
public final class OrderJournal implements OrderJournalSink {

    private static final Logger log = Logger.getLogger(OrderJournal.class);

    /** Ring message type id (journal-internal; the SBE header inside the payload is authoritative). */
    public static final int MSG_TYPE_ORDER_EVENT = 1;
    /** Bytes of one journal message: SBE header + the OrderJournalEvent block. */
    public static final int MESSAGE_BYTES = MessageHeaderEncoder.ENCODED_LENGTH + OrderJournalEventEncoder.BLOCK_LENGTH;
    /** Where the OrderEvent body bytes 16..166 start inside the block (after journalSeq, epoch, logPosition). */
    public static final int BODY_COPY_AT = 24;
    /** The first OrderEvent body byte copied (eventSeq and logPosition are the journal's own head fields). */
    public static final int BODY_COPY_FROM = 16;
    public static final int BODY_COPY_LENGTH = OrderEventRecordCodec.RECORD_BYTES - BODY_COPY_FROM;

    private final OneToOneRingBuffer ringBuffer;
    private final int ringCapacity;
    private final long stallLogNs;
    private final int alarmPct;
    private final long alarmNs;

    private final UnsafeBuffer body = new UnsafeBuffer(ByteBuffer.allocateDirect(OrderEventRecordCodec.RECORD_BYTES));
    private final OrderEventEncoder bodyEncoder = new OrderEventEncoder();
    private final UnsafeBuffer scratch = new UnsafeBuffer(ByteBuffer.allocateDirect(MESSAGE_BYTES));
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final OrderJournalEventEncoder encoder = new OrderJournalEventEncoder();

    /** Test seam only (row controls): rewrites the liquidity byte before it is written. Production: identity. */
    IntUnaryOperator liquidityRewrite = IntUnaryOperator.identity();

    private volatile long appendedSeq;
    private volatile long backpressureEvents;
    private volatile long recordedSeq;
    private volatile long connectFailures;
    private volatile long lagAtSnapshotEvents;
    private volatile long holesTotal;
    private volatile long tailUnreadableTotal;
    private volatile long publicationLosses;
    private final List<String> holes = new ArrayList<>();
    private volatile boolean closed;

    // the alarm windows: sampled from several threads without a lock; a race moves a window start by one sample
    private volatile long aboveSinceNs = -1L;
    private volatile long stuckSinceNs = -1L;
    private volatile long stuckAtRecordedSeq = -1L;
    /** Test seam only (the control of the no-progress alarm): the fill bound alone decides. */
    volatile boolean progressAlarmOff;

    public OrderJournal(final OrderJournalSettings settings) {
        this.ringCapacity = settings.ringBytes();
        this.ringBuffer = new OneToOneRingBuffer(new UnsafeBuffer(
                ByteBuffer.allocateDirect(ringCapacity + RingBufferDescriptor.TRAILER_LENGTH)));
        this.stallLogNs = TimeUnit.MILLISECONDS.toNanos(settings.stallLogMs());
        this.alarmPct = settings.ringAlarmPct();
        this.alarmNs = TimeUnit.MILLISECONDS.toNanos(settings.ringAlarmMs());
    }

    /** The ring the writer agent drains. */
    public OneToOneRingBuffer ringBuffer() {
        return ringBuffer;
    }

    @Override
    public void onOrderEvent(final long journalSeq, final long epoch, final long logPosition, final CoreOrderView order,
                             final EventKind kind, final CoreRejectReason reason, final long lastPrice, final long lastQty,
                             final long tradeId, final long holdRemaining, final long updatedAtMs, final int meReason,
                             final int liquidity, final long meEpoch, final long meJournalSeq, final long meLogPosition,
                             final long meTimestampMs) {
        encode(journalSeq, epoch, logPosition, order, kind, reason, lastPrice, lastQty, tradeId, holdRemaining,
                updatedAtMs, meReason, liquidity, meEpoch, meJournalSeq, meLogPosition, meTimestampMs);
        blockingWrite(journalSeq);
    }

    /** Encode one message into the scratch (package-private: the record tests read it). */
    void encode(final long journalSeq, final long epoch, final long logPosition, final CoreOrderView order,
                final EventKind kind, final CoreRejectReason reason, final long lastPrice, final long lastQty,
                final long tradeId, final long holdRemaining, final long updatedAtMs, final int meReason,
                final int liquidity, final long meEpoch, final long meJournalSeq, final long meLogPosition,
                final long meTimestampMs) {
        bodyEncoder.wrap(body, 0);
        OrderEventRecordCodec.encode(bodyEncoder, journalSeq, logPosition, order, kind, reason, lastPrice, lastQty,
                tradeId, holdRemaining, updatedAtMs, meReason);
        encoder.wrapAndApplyHeader(scratch, 0, headerEncoder)
                .journalSeq(journalSeq)
                .epoch(epoch)
                .logPosition(logPosition);
        scratch.putBytes(MessageHeaderEncoder.ENCODED_LENGTH + BODY_COPY_AT, body, BODY_COPY_FROM, BODY_COPY_LENGTH);
        encoder.liquidity((short) (liquidityRewrite.applyAsInt(liquidity) & 0xFF))
                .meEpoch(meEpoch)
                .meJournalSeq(meJournalSeq)
                .meLogPosition(meLogPosition)
                .meTimestampMs(meTimestampMs);
    }

    /** The last encoded message (header included), {@link #MESSAGE_BYTES} long. */
    UnsafeBuffer scratch() {
        return scratch;
    }

    private void blockingWrite(final long journalSeq) {
        if (ringBuffer.write(MSG_TYPE_ORDER_EVENT, scratch, 0, MESSAGE_BYTES)) {
            appendedSeq = journalSeq;
            return;
        }
        backpressureEvents = backpressureEvents + 1;
        final long startNs = System.nanoTime();
        long nextLogNs = startNs + stallLogNs;
        final IdleStrategy idle = new BackoffIdleStrategy(100, 10, 1_000, TimeUnit.MILLISECONDS.toNanos(1));
        while (!ringBuffer.write(MSG_TYPE_ORDER_EVENT, scratch, 0, MESSAGE_BYTES)) {
            final long nowNs = System.nanoTime();
            if (closed || Thread.currentThread().isInterrupted()) {
                throw new AgentTerminationException("order journal ring full at close: journalSeq=" + journalSeq
                        + " not written after " + TimeUnit.NANOSECONDS.toMillis(nowNs - startNs) + " ms (recorded="
                        + recordedSeq + ")");
            }
            if (nowNs - nextLogNs >= 0) {
                nextLogNs = nowNs + stallLogNs;
                log.error("ORDER JOURNAL STALLED: ring full for %d ms (journalSeq=%d, recorded=%d) - the journal archive"
                                + " is not draining; this member stops applying until it does",
                        TimeUnit.NANOSECONDS.toMillis(nowNs - startNs), journalSeq, recordedSeq);
            }
            idle.idle();
        }
        appendedSeq = journalSeq;
    }

    /** Ends a full-ring wait on the service thread (it leaves with an {@link AgentTerminationException}). */
    public void close() {
        closed = true;
    }

    // ------------------------------------------------------------------ readiness alarm (any thread)

    /** Ring bytes in use now. */
    public long ringUsedBytes() {
        return ringBuffer.size();
    }

    /**
     * True when, for {@code CORE_ORDER_JOURNAL_RING_ALARM_MS}, either the ring has stayed above
     * {@code CORE_ORDER_JOURNAL_RING_ALARM_PCT} of its capacity, or records were waiting (appended, not yet recorded by
     * the archive) while the recorded sequence did not move. The second catches a writer that cannot connect, a dead
     * writer and a stalled archive at any flow, long before the ring fills. Sampled by every caller (the probe, the
     * writer, the service thread's background work), without a lock; each window starts at the first sample in its
     * state and ends at the first one out of it.
     */
    public boolean alarmed(final long nowNs) {
        final boolean filled = fillAlarmed(nowNs);
        final boolean stuck = progressAlarmed(nowNs);
        return filled || stuck;
    }

    private boolean fillAlarmed(final long nowNs) {
        final boolean above = ringUsedBytes() * 100L > (long) alarmPct * ringCapacity;
        if (!above) {
            aboveSinceNs = -1L;
            return false;
        }
        long since = aboveSinceNs;
        if (since < 0) {
            since = nowNs;
            aboveSinceNs = since;
        }
        return nowNs - since >= alarmNs;
    }

    private boolean progressAlarmed(final long nowNs) {
        final long recorded = recordedSeq;
        if (progressAlarmOff || appendedSeq <= recorded) {
            stuckSinceNs = -1L;
            return false;
        }
        long since = stuckSinceNs;
        if (since < 0 || stuckAtRecordedSeq != recorded) {
            since = nowNs;
            stuckAtRecordedSeq = recorded;
            stuckSinceNs = since;
        }
        return nowNs - since >= alarmNs;
    }

    // ------------------------------------------------------------------ restore-side holes

    /** A range (from, to] this member cannot regenerate: declared once per restore that finds it. */
    public synchronized void declareHole(final long fromExclusive, final long toInclusive) {
        holes.add("(" + fromExclusive + "," + toInclusive + "]");
        holesTotal = holesTotal + 1;
    }

    /**
     * A restore whose local tail could not be read: no hole declared and none excluded. Shown as {@code unknown} in the
     * ranges (up to the restored sequence) and counted, so an unreadable tail is never silent.
     */
    public synchronized void declareUnknown(final long restoredSeq) {
        holes.add("unknown(," + restoredSeq + "]");
        tailUnreadableTotal = tailUnreadableTotal + 1;
    }

    public synchronized String holeRanges() {
        return holes.isEmpty() ? "none" : String.join(" ", holes);
    }

    public long tailUnreadableTotal() {
        return tailUnreadableTotal;
    }

    public long publicationLosses() {
        return publicationLosses;
    }

    void publicationLost() {
        publicationLosses = publicationLosses + 1;
    }

    public long holesTotal() {
        return holesTotal;
    }

    // ------------------------------------------------------------------ metrics

    public long appendedSeq() {
        return appendedSeq;
    }

    public long backpressureEvents() {
        return backpressureEvents;
    }

    public long recordedSeq() {
        return recordedSeq;
    }

    void recordedSeq(final long seq) {
        recordedSeq = seq;
    }

    public long connectFailures() {
        return connectFailures;
    }

    void connectFailed() {
        connectFailures = connectFailures + 1;
    }

    /** Called at every snapshot on the service thread: a read of two counters, never a wait. */
    public void sampleLagAtSnapshot() {
        lagAtSnapshotEvents = Math.max(0L, appendedSeq - recordedSeq);
    }

    public long lagAtSnapshotEvents() {
        return lagAtSnapshotEvents;
    }
}
