// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.application.engine.config;

/**
 * The replicated core limits record: how long a requestId decision is remembered, how many remembered requests one
 * user may hold before a create or an amend is refused, and how many order events the replay ring keeps. These decide
 * admission and snapshot bytes, so they are a logged ADMIN record (CoreLimitsSet), never a member's environment: every
 * replica changes them at the same log position and a replay of that log changes them there too. The record is
 * snapshotted. A core that has never seen a CoreLimitsSet runs on the genesis record, version 0.
 */
public final class CoreLimits {

    public static final long DEFAULT_REQUEST_TTL_MS = 15 * 60_000L;
    public static final int DEFAULT_REQUEST_PER_USER_CAP = 256;
    public static final int DEFAULT_ORDER_EVENT_RING_CAPACITY = 65_536;

    /**
     * The largest ring whose retained events fit in one snapshot section's length field: that field is a 32-bit count
     * of bytes and a retained event is 167 bytes behind a 16-byte head. It bounds that one field only, not the image: the
     * whole image is addressed with 32-bit offsets, so a ring near this bound next to the other sections would still not
     * serialize, and shrinking a ring that large copies the kept events into a temporary on the service thread. A
     * capacity above it is refused when it is set; capacities that are actually used sit far below it.
     */
    public static final int MAX_ORDER_EVENT_RING_CAPACITY = (Integer.MAX_VALUE - 16) / 167;

    /** The outcome of one CoreLimitsSet against the recorded state. */
    public enum Decision { INVALID, DUPLICATE, STALE, ACCEPT }

    private long version;
    private long requestTtlMs = DEFAULT_REQUEST_TTL_MS;
    private int requestPerUserCap = DEFAULT_REQUEST_PER_USER_CAP;
    private int orderEventRingCapacity = DEFAULT_ORDER_EVENT_RING_CAPACITY;

    public long version() {
        return version;
    }

    public long requestTtlMs() {
        return requestTtlMs;
    }

    public int requestPerUserCap() {
        return requestPerUserCap;
    }

    public int orderEventRingCapacity() {
        return orderEventRingCapacity;
    }

    /** Structural validity of a record (also the restore check). */
    public static boolean valid(final long requestTtlMs, final int requestPerUserCap, final int orderEventRingCapacity) {
        return requestTtlMs >= 1 && requestPerUserCap >= 1 && orderEventRingCapacity >= 0
                && orderEventRingCapacity <= MAX_ORDER_EVENT_RING_CAPACITY;
    }

    /** The decision for a command, reading only the command and the recorded state. Mutates nothing. */
    public Decision decide(final long configVersion, final long ttlMs, final int perUserCap, final int ringCapacity) {
        if (!valid(ttlMs, perUserCap, ringCapacity)) {
            return Decision.INVALID;
        }
        if (configVersion == version && configVersion != 0 && ttlMs == requestTtlMs && perUserCap == requestPerUserCap
                && ringCapacity == orderEventRingCapacity) {
            return Decision.DUPLICATE;
        }
        if (configVersion <= version) {
            return Decision.STALE;
        }
        return Decision.ACCEPT;
    }

    /** Record an accepted command (or a restored record). */
    public void set(final long configVersion, final long ttlMs, final int perUserCap, final int ringCapacity) {
        version = configVersion;
        requestTtlMs = ttlMs;
        requestPerUserCap = perUserCap;
        orderEventRingCapacity = ringCapacity;
    }
}
