// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.domain;

import org.agrona.MutableDirectBuffer;

/**
 * Read-only view of retained order events for a replay reply. Each record is the body of the wire OrderEvent,
 * byte for byte as it was emitted, {@link #recordLength()} bytes long. The publisher copies records into reply frames;
 * it never interprets them.
 */
public interface OrderEventRecords {

    /** Bytes per record. */
    int recordLength();

    /** Copy the record of {@code eventSeq} to {@code dst} at {@code offset}. The seq must be retained. */
    void copyRecord(long eventSeq, MutableDirectBuffer dst, int offset);
}
