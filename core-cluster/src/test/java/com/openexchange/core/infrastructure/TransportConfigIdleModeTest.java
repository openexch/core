// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.junit.After;
import org.junit.Assume;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** {@code TRANSPORT_IDLE_MODE} (here through its system property): each mode picks its strategy, an unknown one refuses. */
public class TransportConfigIdleModeTest {

    private static final String PROP = "transport.idle.mode";

    @After
    public void clear() {
        System.clearProperty(PROP);
    }

    private static void assumeEnvUnset() {
        Assume.assumeTrue("TRANSPORT_IDLE_MODE set in the environment wins over the property",
                System.getenv("TRANSPORT_IDLE_MODE") == null);
    }

    @Test
    public void eachModePicksItsStrategy() {
        assumeEnvUnset();
        assertEquals(TransportConfig.IdleMode.BACKOFF, TransportConfig.idleMode());
        assertTrue(TransportConfig.idleStrategySupplier().get() instanceof BackoffIdleStrategy);

        System.setProperty(PROP, "busy_spin");
        assertTrue(TransportConfig.idleStrategySupplier().get() instanceof BusySpinIdleStrategy);

        System.setProperty(PROP, "sleep");
        assertEquals(TransportConfig.IdleMode.SLEEP, TransportConfig.idleMode());
        assertTrue(TransportConfig.idleStrategySupplier().get() instanceof SleepingMillisIdleStrategy);
    }

    @Test
    public void unknownModeRefuses() {
        assumeEnvUnset();
        System.setProperty(PROP, "yield");
        try {
            TransportConfig.idleMode();
            fail("yield accepted");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("(expected busy_spin|backoff|sleep)"));
        }
    }
}
