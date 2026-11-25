// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.bridge;

import java.util.List;

/**
 * Env-driven connection settings of the core client tools in this jar ({@code dump} and
 * {@code set-money-journal}): where the core cluster is and where its replies come back to.
 */
public final class BridgeConfig {

    /** Core cluster member addresses (CSV) + port base for the ingress endpoint math. */
    public final List<String> coreClusterAddresses;
    public final int corePortBase;
    /** The tool's own egress listen endpoint (must differ from any other core client on the host). */
    public final String coreEgressEndpoint;
    /**
     * The tool's own routable host: the address the core leader sends egress replies back to.
     * "localhost" only works co-resident; a cross-host tool must advertise its routable IP.
     * Defaults to the host part of {@link #coreEgressEndpoint}.
     */
    public final String localHost;

    public BridgeConfig(
            final List<String> coreClusterAddresses,
            final int corePortBase,
            final String coreEgressEndpoint,
            final String localHost) {
        this.coreClusterAddresses = coreClusterAddresses;
        this.corePortBase = corePortBase;
        this.coreEgressEndpoint = coreEgressEndpoint;
        this.localHost = localHost;
    }

    public static BridgeConfig fromEnv() {
        final String egressEndpoint = envOr("BRIDGE_CORE_EGRESS_ENDPOINT", "127.0.0.1:9394");
        return new BridgeConfig(
                List.of(envOr("BRIDGE_CORE_CLUSTER_ADDRESSES", "127.0.0.1").split(",")),
                Integer.parseInt(envOr("BRIDGE_CORE_PORT_BASE", "9300")),
                egressEndpoint,
                envOr("BRIDGE_LOCAL_HOST", hostOf(egressEndpoint)));
    }

    /** Host part of a "host:port" endpoint (used to derive localHost from the egress endpoint). */
    private static String hostOf(final String endpoint) {
        final int colon = endpoint.lastIndexOf(':');
        return colon > 0 ? endpoint.substring(0, colon) : endpoint;
    }

    private static String envOr(final String key, final String fallback) {
        final String v = System.getenv(key);
        return v == null || v.isBlank() ? fallback : v;
    }
}
