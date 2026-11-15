// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.determinism;

import com.openexchange.core.application.engine.CoreEngine;
import com.openexchange.core.domain.ConfigDecision;
import com.openexchange.core.domain.commands.CoreConfigCommand;

/**
 * The one way every feed-bound test boots. It sets the jar's supported behaviour set
 * through the existing test seam, sends CoreConfig
 * 1 -> 2 -> 3 with one {@code genesisId} while GLOBAL is SUSPENDED (genesis state), then the ME's FRESH EngineConfig 68 at
 * journalSeq 1 with that genesisId as the epoch, which binds the feed.
 */
public final class V3Genesis {

    public static final long GENESIS_ID = 0x5EEDL;
    /** The ME EngineConfig the FRESH record carries (S-c ranges). */
    public static final long ME_CONFIG_VERSION = 1L;
    public static final long ME_BEHAVIOR = 2L;
    public static final long ME_CAPACITY = 4096L;
    public static final int ME_MAX_WIRE = 12;

    private V3Genesis() {
    }

    /** The compiled tables at {@code version} / {@code behavior}, carrying {@link #GENESIS_ID}. */
    public static CoreConfigCommand config(final long version, final long behavior) {
        final CoreConfigCommand c = CoreScenarioRunner.coreConfigFor(version, behavior, 1L, 1L, 1_000_000_000_000_000L, 0L,
                7_000L + version);
        c.genesisId = GENESIS_ID;
        return c;
    }

    public static void widen(final CoreEngine engine) {
        engine.coreConfig().setSupportedBehaviorVersions(1, 2, 3, 4);
    }

    /** CoreConfig 1 -> 2 -> 3 (ADOPT, SWITCH, SWITCH); the feed stays unbound. */
    public static void switchTo3(final CoreEngine engine, final long now) {
        widen(engine);
        for (long v = 1; v <= 3; v++) {
            engine.applyCoreConfig(config(v, v), now);
        }
        if (engine.coreConfig().behaviorVersion() != 3) {
            throw new IllegalStateException("V3Genesis: CoreConfig 3 was not recorded (behaviour "
                    + engine.coreConfig().behaviorVersion() + ")");
        }
    }

    /** The ME FRESH EngineConfig at journalSeq 1, egressSeq {@code egressSeq}. */
    public static boolean bind(final CoreEngine engine, final long egressSeq, final long now) {
        return engine.onMeEngineConfig(egressSeq, 1L, GENESIS_ID, ME_CONFIG_VERSION, ME_BEHAVIOR, ME_CAPACITY, ME_MAX_WIRE,
                ConfigDecision.FRESH, now, id -> { });
    }

    /** {@link #switchTo3} then {@link #bind} at egressSeq 1. */
    public static void boot(final CoreEngine engine, final long now) {
        switchTo3(engine, now);
        bind(engine, 1L, now);
    }

    /**
     * {@link #boot} then one CoreConfig per step up to {@code behavior} (3 = {@link #boot} alone). The steps above 3
     * have no precondition, so the order relative to the bind does not matter; they are taken after it.
     */
    public static void boot(final CoreEngine engine, final long now, final long behavior) {
        if (behavior < 3) {
            throw new IllegalArgumentException("a feed-bound genesis starts at behaviour 3, got " + behavior);
        }
        boot(engine, now);
        for (long v = 4; v <= behavior; v++) {
            engine.applyCoreConfig(config(v, v), now);
        }
        if (engine.coreConfig().behaviorVersion() != behavior) {
            throw new IllegalStateException("genesis: behaviour " + behavior + " was not recorded (behaviour "
                    + engine.coreConfig().behaviorVersion() + ")");
        }
    }
}
