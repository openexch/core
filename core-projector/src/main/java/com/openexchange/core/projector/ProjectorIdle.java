// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.util.Locale;

/**
 * The money projector's idle mode: the stack profile's one idle setting ({@code busy_spin | backoff | sleep}), as the
 * core members read it from {@code TRANSPORT_IDLE_MODE}, here {@code PROJECTOR_IDLE_MODE} (default {@code backoff}):
 * <ul>
 *   <li>{@code busy_spin}: {@link BusySpinIdleStrategy}.</li>
 *   <li>{@code backoff}: {@link BackoffIdleStrategy}; the embedded driver keeps Aeron's own defaults.</li>
 *   <li>{@code sleep}: {@link SleepingMillisIdleStrategy}(1), the members' {@code sleep}: up to 1 ms of wake-up latency
 *       for near-zero idle CPU; nothing of the process spins or yields.</li>
 * </ul>
 * It sets the agent's idle, the archive clients' idle and, in embedded driver mode, every idle strategy of the
 * projector's own media driver. An external driver is not the projector's to set.
 * Parsing is strict: an unknown value refuses the start.
 */
public final class ProjectorIdle {

    public static final String MODE_ENV = "PROJECTOR_IDLE_MODE";

    public enum Mode { BUSY_SPIN, BACKOFF, SLEEP }

    private ProjectorIdle() {
    }

    public static Mode fromEnv() {
        return parseMode(System.getenv(MODE_ENV));
    }

    /** Unset or empty is the default (backoff); anything else must name a mode, case-insensitive. */
    static Mode parseMode(final String value) {
        if (value == null || value.isEmpty()) {
            return Mode.BACKOFF;
        }
        try {
            return Mode.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid " + MODE_ENV + " '" + value + "' (expected busy_spin|backoff|sleep)");
        }
    }

    /** A new strategy for one thread (strategies are stateful; never share one between threads). */
    public static IdleStrategy newStrategy(final Mode mode) {
        switch (mode) {
            case BUSY_SPIN:
                return new BusySpinIdleStrategy();
            case SLEEP:
                return new SleepingMillisIdleStrategy(1);
            case BACKOFF:
            default:
                return new BackoffIdleStrategy();
        }
    }

    /**
     * The embedded driver's idle strategies for {@code mode}. {@code backoff} sets nothing: the driver keeps exactly
     * the defaults it had before the setting existed. The other modes set all five, so no thread of the driver keeps
     * a default of its own.
     */
    static MediaDriver.Context applyTo(final MediaDriver.Context context, final Mode mode) {
        if (mode == Mode.BACKOFF) {
            return context;
        }
        return context
                .sharedIdleStrategy(newStrategy(mode))
                .conductorIdleStrategy(newStrategy(mode))
                .senderIdleStrategy(newStrategy(mode))
                .receiverIdleStrategy(newStrategy(mode))
                .sharedNetworkIdleStrategy(newStrategy(mode));
    }
}
