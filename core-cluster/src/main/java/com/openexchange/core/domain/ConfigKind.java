// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.domain;

/** Which config record a ConfigApplied names. Codes equal the wire values. */
public enum ConfigKind {
    CORE(0),
    RISK(1),
    BREAKER(2),
    GATE(3),
    /** The core limits record (request TTL, per-user request cap, order-event ring capacity). */
    LIMITS(4);

    private final int code;

    ConfigKind(final int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}
