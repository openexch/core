// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

/** {@link ProjectorIdle}: strict parse, and the strategy each mode gives the agent and the embedded driver. */
public class ProjectorIdleTest {

    @Test
    public void unsetOrEmptyIsBackoff() {
        assertEquals(ProjectorIdle.Mode.BACKOFF, ProjectorIdle.parseMode(null));
        assertEquals(ProjectorIdle.Mode.BACKOFF, ProjectorIdle.parseMode(""));
    }

    @Test
    public void threeValuesParseCaseInsensitively() {
        assertEquals(ProjectorIdle.Mode.BUSY_SPIN, ProjectorIdle.parseMode("busy_spin"));
        assertEquals(ProjectorIdle.Mode.BUSY_SPIN, ProjectorIdle.parseMode(" BUSY_SPIN "));
        assertEquals(ProjectorIdle.Mode.BACKOFF, ProjectorIdle.parseMode("Backoff"));
        assertEquals(ProjectorIdle.Mode.SLEEP, ProjectorIdle.parseMode("sleep"));
    }

    @Test
    public void anUnknownValueIsRefusedWithTheExactMessage() {
        for (final String bad : new String[] {"busy-spin", "spin", "yield", "true", " ", "sleep,backoff"}) {
            final IllegalArgumentException e = assertThrows(bad, IllegalArgumentException.class,
                    () -> ProjectorIdle.parseMode(bad));
            assertEquals("Invalid PROJECTOR_IDLE_MODE '" + bad + "' (expected busy_spin|backoff|sleep)",
                    e.getMessage());
        }
    }

    @Test
    public void theAgentIdlesOnTheModesStrategy() {
        assertEquals(BackoffIdleStrategy.class, agent(null).idleStrategy().getClass());
        assertEquals(BackoffIdleStrategy.class, agent(ProjectorIdle.Mode.BACKOFF).idleStrategy().getClass());
        assertEquals(BusySpinIdleStrategy.class, agent(ProjectorIdle.Mode.BUSY_SPIN).idleStrategy().getClass());
        assertEquals(SleepingMillisIdleStrategy.class, agent(ProjectorIdle.Mode.SLEEP).idleStrategy().getClass());
    }

    @Test
    public void theEmbeddedDriverIdlesOnTheModesStrategyOnEveryThread() {
        for (final ProjectorIdle.Mode mode : new ProjectorIdle.Mode[] {
                ProjectorIdle.Mode.BUSY_SPIN, ProjectorIdle.Mode.SLEEP}) {
            final Class<?> expected = mode == ProjectorIdle.Mode.BUSY_SPIN
                    ? BusySpinIdleStrategy.class : SleepingMillisIdleStrategy.class;
            final MediaDriver.Context context = ProjectorIdle.applyTo(new MediaDriver.Context(), mode);
            final List<IdleStrategy> all = List.of(context.sharedIdleStrategy(), context.conductorIdleStrategy(),
                    context.senderIdleStrategy(), context.receiverIdleStrategy(), context.sharedNetworkIdleStrategy());
            for (final IdleStrategy s : all) {
                assertEquals(mode.name(), expected, s.getClass());
            }
            assertEquals(5, all.stream().distinct().count()); // one instance per thread
        }
    }

    @Test
    public void backoffLeavesTheDriverOnItsDefaults() {
        final MediaDriver.Context context = new MediaDriver.Context();
        assertSame(context, ProjectorIdle.applyTo(context, ProjectorIdle.Mode.BACKOFF));
        assertNull(context.sharedIdleStrategy());
        assertNull(context.conductorIdleStrategy());
        assertNull(context.senderIdleStrategy());
        assertNull(context.receiverIdleStrategy());
        assertNull(context.sharedNetworkIdleStrategy());
    }

    private static ProjectorAgent agent(final ProjectorIdle.Mode mode) {
        final ProjectorConfig config = new ProjectorConfig(List.of("localhost:1"), 10, "jdbc:none", "u", "p",
                true, 0, "localhost");
        return mode == null
                ? new ProjectorAgent(config, "/nonexistent", new ProjectorState(), null)
                : new ProjectorAgent(config, "/nonexistent", new ProjectorState(), null, null, mode);
    }
}
