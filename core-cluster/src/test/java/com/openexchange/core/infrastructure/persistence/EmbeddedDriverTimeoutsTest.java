// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * The embedded member driver's client liveness and publication unblock timeouts are settings with defaults
 * ({@code TRANSPORT_DRIVER_CLIENT_LIVENESS_MS}, {@code TRANSPORT_DRIVER_PUBLICATION_UNBLOCK_MS}): the boot guard parses
 * and range-checks them before any driver starts, and refuses an unblock timeout at or below the liveness (Aeron's own
 * rule, which would otherwise fail later inside the driver launch).
 */
public class EmbeddedDriverTimeoutsTest {

    private static Function<String, String> env(final String... pairs) {
        final Map<String, String> m = new HashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            m.put(pairs[i], pairs[i + 1]);
        }
        return m::get;
    }

    /** A kernel that grants every buffer in full. */
    private record FakeKernel(long rmemMax, long wmemMax) implements com.openexchange.cluster.SocketBufferProbe.Kernel {
        @Override
        public long grantedRcvbuf(final long requested) {
            return Math.min(requested, rmemMax);
        }

        @Override
        public long grantedSndbuf(final long requested) {
            return Math.min(requested, wmemMax);
        }

        @Override
        public String sysctl(final String name) {
            return Long.toString(com.openexchange.cluster.SocketBufferProbe.RMEM_MAX.equals(name) ? rmemMax : wmemMax);
        }
    }

    private static String refusal(final Function<String, String> env) {
        return assertThrows(StandbySnapshotBootGuard.Refused.class,
                () -> StubModeBootGuard.enforceTransport(env, new FakeKernel(1L << 30, 1L << 30))).getMessage();
    }

    @Test
    public void theBootGuardRefusesAClientLivenessOutsideItsRange() {
        assertEquals("CORE BOOT GUARD: TRANSPORT_DRIVER_CLIENT_LIVENESS_MS must be 1000..600000 ms: 500",
                refusal(env("TRANSPORT_DRIVER_CLIENT_LIVENESS_MS", "500")));
        assertEquals("CORE BOOT GUARD: TRANSPORT_DRIVER_CLIENT_LIVENESS_MS is not an integer: 5s",
                refusal(env("TRANSPORT_DRIVER_CLIENT_LIVENESS_MS", "5s")));
    }

    @Test
    public void theBootGuardRefusesAnUnblockTimeoutNotAboveTheClientLiveness() {
        assertEquals("CORE BOOT GUARD: TRANSPORT_DRIVER_PUBLICATION_UNBLOCK_MS 10000 must exceed"
                        + " TRANSPORT_DRIVER_CLIENT_LIVENESS_MS 12000 (Aeron refuses the driver otherwise)",
                refusal(env("TRANSPORT_DRIVER_CLIENT_LIVENESS_MS", "12000")));
        assertEquals("CORE BOOT GUARD: TRANSPORT_DRIVER_PUBLICATION_UNBLOCK_MS must be 2000..1200000 ms: 1200001",
                refusal(env("TRANSPORT_DRIVER_PUBLICATION_UNBLOCK_MS", "1200001")));
    }

    @Test
    public void theDefaultsKeepFiveAndTenSeconds() {
        final StubModeBootGuard.Transport t = StubModeBootGuard.enforceTransport(env(), new FakeKernel(1L << 30, 1L << 30));
        assertEquals(new com.openexchange.core.infrastructure.TransportConfig.DriverTimeouts(5_000, 10_000),
                t.driverTimeouts());
        assertEquals(com.openexchange.core.infrastructure.TransportConfig.DriverTimeouts.defaults(), t.driverTimeouts());
    }

    @Test
    public void theSettingsReachTheEmbeddedDriverContext() {
        final StubModeBootGuard.Transport t = StubModeBootGuard.enforceTransport(env(
                "TRANSPORT_DRIVER_CLIENT_LIVENESS_MS", "15000", "TRANSPORT_DRIVER_PUBLICATION_UNBLOCK_MS", "20000"),
                new FakeKernel(1L << 30, 1L << 30));
        final io.aeron.driver.MediaDriver.Context context = AeronCluster.embeddedDriverTimeouts(
                new io.aeron.driver.MediaDriver.Context(), t.driverTimeouts());
        assertEquals(15_000_000_000L, context.clientLivenessTimeoutNs());
        assertEquals(20_000_000_000L, context.publicationUnblockTimeoutNs());
        // at the limits
        assertEquals(1_000L, com.openexchange.core.infrastructure.TransportConfig.DriverTimeouts.parse(env(
                "TRANSPORT_DRIVER_CLIENT_LIVENESS_MS", "1000")).clientLivenessMs());
        assertEquals(600_000L, com.openexchange.core.infrastructure.TransportConfig.DriverTimeouts.parse(env(
                "TRANSPORT_DRIVER_CLIENT_LIVENESS_MS", "600000", "TRANSPORT_DRIVER_PUBLICATION_UNBLOCK_MS",
                "1200000")).clientLivenessMs());
    }
}
