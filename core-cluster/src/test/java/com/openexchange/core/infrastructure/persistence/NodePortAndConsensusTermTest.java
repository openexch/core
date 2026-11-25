// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.cluster.ClusterPorts;
import org.junit.After;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Where a member's probe listens and what term its consensus channel takes, as {@code ClusterConfig.create} wires
 * them: the probe on the member ports' rule (the same port on every host at the default stride), an override that
 * disagrees refused before any Aeron component exists, and the consensus channel on
 * {@code TRANSPORT_CONSENSUS_TERM_LENGTH} instead of the driver's 16m.
 */
public class NodePortAndConsensusTermTest {

    private static final List<String> HOSTS = List.of("localhost", "localhost", "localhost");

    @After
    public void tearDown() {
        System.clearProperty("cluster.port.stride");
        System.clearProperty("core.node.port");
        System.clearProperty("transport.consensus.term.length");
        ClusterPorts.resetStrideForTest();
    }

    /** One member per host (stride 0): members 0 and 2 serve the probe on the same port. The old rule gave 9700/9702. */
    @Test
    public void membersZeroAndTwoShareTheProbePortAtTheDefaultStride() {
        assertEquals(0, ClusterPorts.stride());
        assertEquals(9306, ClusterConfig.nodeEndpointPort(0, 9300));
        assertEquals(ClusterConfig.nodeEndpointPort(0, 9300), ClusterConfig.nodeEndpointPort(2, 9300));
        final CoreClusteredService two = new CoreClusteredService();
        ClusterConfig.create(2, HOSTS, 9300, two);
        assertEquals("the port create hands the service", 9306, two.nodeEndpointPort());
    }

    /** Co-located members (stride 100) get distinct probe ports, one stride apart, from create. */
    @Test
    public void aSharedHostStrideGivesEachMemberItsOwnProbePort() {
        System.setProperty("cluster.port.stride", "100");
        ClusterPorts.resetStrideForTest();
        for (int id = 0; id < 3; id++) {
            final CoreClusteredService s = new CoreClusteredService();
            ClusterConfig.create(id, HOSTS, 9300, s);
            assertEquals(9306 + id * 100, s.nodeEndpointPort());
        }
    }

    /** An agreeing override is accepted; one that disagrees refuses the boot in create, naming both ports. */
    @Test
    public void aConflictingOverrideRefusesTheBoot() {
        System.setProperty("cluster.port.stride", "100");
        ClusterPorts.resetStrideForTest();
        System.setProperty("core.node.port", "9406");
        final CoreClusteredService ok = new CoreClusteredService();
        ClusterConfig.create(1, HOSTS, 9300, ok);
        assertEquals(9406, ok.nodeEndpointPort());

        System.setProperty("core.node.port", "9701");
        try {
            ClusterConfig.create(1, HOSTS, 9300, new CoreClusteredService());
            fail("a disagreeing CORE_NODE_PORT must refuse the boot");
        } catch (final IllegalArgumentException expected) {
            assertEquals("CORE_NODE_PORT=9701 disagrees with the node endpoint port 9406 (port base 9300 + node 1 x stride"
                    + " 100 + 6); unset it, or fix the port base or CLUSTER_PORT_STRIDE", expected.getMessage());
        }
    }

    /** The consensus channel names its term: 1m by default, the setting when set, a bad value refused. */
    @Test
    public void theConsensusChannelTakesTheConfiguredTerm() {
        assertEquals("aeron:udp?term-length=1m", ClusterConfig.create(0, HOSTS, 9300, new CoreClusteredService())
                .consensusModuleContext().consensusChannel());
        System.setProperty("transport.consensus.term.length", "4m");
        assertEquals("aeron:udp?term-length=4m", ClusterConfig.create(0, HOSTS, 9300, new CoreClusteredService())
                .consensusModuleContext().consensusChannel());
        System.setProperty("transport.consensus.term.length", "16mb");
        try {
            ClusterConfig.create(0, HOSTS, 9300, new CoreClusteredService());
            fail("an unparsable term must refuse the boot");
        } catch (final IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().startsWith("TRANSPORT_CONSENSUS_TERM_LENGTH '16mb'"));
        }
    }
}
