// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.bridge;

import java.util.Locale;

/**
 * Which media driver the {@link CoreDump} tool runs on, set like the projector's driver mode
 * ({@code PROJECTOR_DRIVER_MODE}, core-projector {@code ProjectorDriver}):
 * <ul>
 *   <li>{@code COREDUMP_DRIVER_MODE=embedded} (dev/demo): the tool launches its own driver on
 *       {@code /dev/shm/aeron-aedump-<pid>} and deletes that directory when it exits.</li>
 *   <li>{@code COREDUMP_DRIVER_MODE=external} (production): the tool attaches to the aeronmd at
 *       {@code AERON_DIR} and launches none.</li>
 * </ul>
 * The mode has no default: the tool is run by hand next to a stack whose driver choice the operator must
 * name, so an unset or empty mode refuses the start, as does an unknown mode, external without
 * {@code AERON_DIR}, or {@code AERON_DIR} set in embedded mode (an operator who set it expects the
 * external driver; embedded would silently ignore it).
 */
public final class CoreDumpDriver {

    public static final String MODE_ENV = "COREDUMP_DRIVER_MODE";
    public static final String AERON_DIR_ENV = "AERON_DIR";

    public enum Mode { EXTERNAL, EMBEDDED }

    public final Mode mode;
    /** The external driver's directory; null in embedded mode (the embedded driver's dir is per pid). */
    public final String aeronDir;

    private CoreDumpDriver(final Mode mode, final String aeronDir) {
        this.mode = mode;
        this.aeronDir = aeronDir;
    }

    public static CoreDumpDriver fromEnv() {
        return parse(System.getenv(MODE_ENV), System.getenv(AERON_DIR_ENV));
    }

    static CoreDumpDriver parse(final String modeValue, final String aeronDirValue) {
        final Mode mode = parseMode(modeValue);
        final boolean dirSet = aeronDirValue != null && !aeronDirValue.isEmpty();
        if (mode == Mode.EXTERNAL) {
            if (!dirSet || aeronDirValue.isBlank()) {
                throw new IllegalArgumentException(MODE_ENV + "=external needs " + AERON_DIR_ENV
                        + " (the external media driver's directory)");
            }
            return new CoreDumpDriver(mode, aeronDirValue.trim());
        }
        if (dirSet) {
            throw new IllegalArgumentException(AERON_DIR_ENV + "='" + aeronDirValue + "' is set but " + MODE_ENV
                    + " is embedded (the embedded driver never uses it): set " + MODE_ENV + "=external or unset "
                    + AERON_DIR_ENV);
        }
        return new CoreDumpDriver(mode, null);
    }

    /** Required: unset or empty refuses the start; otherwise it must name a mode, case-insensitive. */
    static Mode parseMode(final String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(MODE_ENV + " is not set (expected external|embedded): name the"
                    + " media driver this tool runs on");
        }
        try {
            return Mode.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid " + MODE_ENV + " '" + value + "' (expected external|embedded)");
        }
    }

    @Override
    public String toString() {
        return mode == Mode.EXTERNAL ? "EXTERNAL (" + AERON_DIR_ENV + "=" + aeronDir + ")" : "EMBEDDED";
    }
}
