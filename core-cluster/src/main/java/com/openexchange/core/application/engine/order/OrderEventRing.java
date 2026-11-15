// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.application.engine.order;

import com.openexchange.core.domain.CoreOrderView;
import com.openexchange.core.domain.CoreRejectReason;
import com.openexchange.core.domain.EventKind;
import com.openexchange.core.domain.OrderEventRecords;
import com.openexchange.core.infrastructure.generated.OrderEventEncoder;
import com.openexchange.core.infrastructure.publisher.OrderEventRecordCodec;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The newest order events, kept so that a consumer that lost its session can ask for what it missed. Replicated state:
 * every member appends the same record at the same apply (followers and a restoring member included, which emit
 * nothing), and the ring rides the snapshot, so the replay answer is the same on every member and after every restore.
 *
 * <p>A record is the wire OrderEvent body, {@link #RECORD_BYTES} bytes, packed: no per-event object. Storage is
 * allocated in chunks as events arrive, so an idle core holds nothing and a full ring holds {@code capacity x
 * RECORD_BYTES} bytes. The capacity is the limits record's setting; 0 keeps nothing.</p>
 *
 * <p>Sequences are dense: the record of {@code eventSeq} follows the record of {@code eventSeq - 1}. The ring holds
 * the {@link #count()} newest, ending at the engine's current eventSeq. A sequence jump (never produced by the engine)
 * empties the ring rather than leaving a hole a replay could read across.</p>
 */
public final class OrderEventRing implements OrderEventRecords {

    public static final int RECORD_BYTES = OrderEventRecordCodec.RECORD_BYTES;
    private static final int CHUNK_RECORDS = 256;

    private final OrderEventEncoder encoder = new OrderEventEncoder();
    private int capacity;
    private int count;
    private long newestSeq;
    private UnsafeBuffer[] chunks;

    public OrderEventRing(final int capacity) {
        setCapacity(capacity);
    }

    private void setCapacity(final int newCapacity) {
        if (newCapacity < 0) {
            throw new IllegalArgumentException("order event ring capacity " + newCapacity);
        }
        capacity = newCapacity;
        chunks = new UnsafeBuffer[(int) (((long) newCapacity + CHUNK_RECORDS - 1) / CHUNK_RECORDS)];
        count = 0;
    }

    public int capacity() {
        return capacity;
    }

    /** Records held; the oldest is {@code eventSeq - count + 1} for the engine's current eventSeq. */
    public int count() {
        return count;
    }

    @Override
    public int recordLength() {
        return RECORD_BYTES;
    }

    private UnsafeBuffer chunkFor(final int slot) {
        final int c = slot / CHUNK_RECORDS;
        UnsafeBuffer b = chunks[c];
        if (b == null) {
            final int records = Math.min(CHUNK_RECORDS, capacity - c * CHUNK_RECORDS);
            b = new UnsafeBuffer(new byte[records * RECORD_BYTES]);
            chunks[c] = b;
        }
        return b;
    }

    private int slotOf(final long eventSeq) {
        return (int) Long.remainderUnsigned(eventSeq, capacity);
    }

    private static int offsetIn(final int slot) {
        return (slot % CHUNK_RECORDS) * RECORD_BYTES;
    }

    /** Account for {@code eventSeq}: returns false when nothing is kept (capacity 0). */
    private boolean admit(final long eventSeq) {
        if (count > 0 && eventSeq != newestSeq + 1) {
            count = 0; // a jump: nothing older can be replayed contiguously
        }
        newestSeq = eventSeq;
        if (capacity == 0) {
            return false;
        }
        if (count < capacity) {
            count++;
        }
        return true;
    }

    /** Append the event the engine is emitting, with exactly the bytes the broadcast frame carries. */
    public void append(final long eventSeq, final long logPosition, final CoreOrderView o, final EventKind kind,
                       final CoreRejectReason reason, final long lastPrice, final long lastQty, final long tradeId,
                       final long holdRemaining, final long updatedAtMs, final int meReason) {
        if (!admit(eventSeq)) {
            return;
        }
        final int slot = slotOf(eventSeq);
        encoder.wrap(chunkFor(slot), offsetIn(slot));
        OrderEventRecordCodec.encode(encoder, eventSeq, logPosition, o, kind, reason, lastPrice, lastQty, tradeId, holdRemaining,
                updatedAtMs, meReason);
    }

    /** Restore one record (ascending, dense). */
    public void restoreRecord(final long eventSeq, final DirectBuffer src, final int offset) {
        if (!admit(eventSeq)) {
            throw new IllegalStateException("order event ring of capacity 0 cannot hold a record");
        }
        final int slot = slotOf(eventSeq);
        chunkFor(slot).putBytes(offsetIn(slot), src, offset, RECORD_BYTES);
    }

    @Override
    public void copyRecord(final long eventSeq, final MutableDirectBuffer dst, final int offset) {
        if (count == 0 || eventSeq > newestSeq || eventSeq <= newestSeq - count) {
            throw new IllegalArgumentException("order event " + eventSeq + " is not retained (newest " + newestSeq
                    + ", count " + count + ")");
        }
        final int slot = slotOf(eventSeq);
        dst.putBytes(offset, chunkFor(slot), offsetIn(slot), RECORD_BYTES);
    }

    /**
     * Change the capacity, keeping the newest events that fit (all of them when the ring grows). 0 drops everything.
     */
    public void resize(final int newCapacity) {
        if (newCapacity == capacity) {
            return;
        }
        final int keep = Math.min(count, newCapacity);
        final long newest = newestSeq;
        final UnsafeBuffer saved = new UnsafeBuffer(new byte[Math.max(1, keep) * RECORD_BYTES]);
        for (int i = 0; i < keep; i++) {
            copyRecord(newest - keep + 1 + i, saved, i * RECORD_BYTES);
        }
        setCapacity(newCapacity);
        for (int i = 0; i < keep; i++) {
            restoreRecord(newest - keep + 1 + i, saved, i * RECORD_BYTES);
        }
        newestSeq = newest;
    }
}
