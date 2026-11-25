// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.cluster.ClusterPorts;
import com.openexchange.core.cluster.ShadowRestoreExternalDriverProcessTest.ChildFaultInstaller;
import com.openexchange.core.cluster.ShadowRestoreExternalDriverProcessTest.Clients;
import com.openexchange.core.cluster.ShadowRestoreExternalDriverProcessTest.Node;
import com.openexchange.core.domain.Asset;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import com.openexchange.core.infrastructure.persistence.WireVersionFrames;
import io.aeron.cluster.ClusterControl;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * The wire bump story with member processes (the production {@code Main}) on external {@code aeronmd} drivers. Two
 * members are rolled to the next jar (money-schema 11, the seam installed in the child through JDWP before its restore),
 * one is left on the earlier jar. A v11 probe before the raise is an above-ceiling drop on all three
 * ({@code core_ingress_above_ceiling_drops_total} 1 on each). At the raise the member left behind stops itself: it
 * exits non-zero with the node-local text in its log. It is then restarted on the next jar, rejoins, and every member
 * reports the same {@code core_snapshot_state_hash} at one snapshot position.
 */
public class WireBumpRollingExternalDriverProcessTest {

    private static final String R6 = "recorded maxWireVersion=11 is above this jar's money-schema version 10";
    private static final long ONE = ShadowClusterRig.ONE;

    private final ShadowRestoreExternalDriverProcessTest outer = new ShadowRestoreExternalDriverProcessTest();
    private final List<Clients> open = new ArrayList<>();

    @After
    public void cleanUp() throws Exception {
        for (final Clients c : open) {
            c.close();
        }
        outer.cleanUp();
        System.clearProperty("cluster.port.stride");
        ClusterPorts.resetStrideForTest();
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** Full cluster snapshots, so the state hash gauge has a value. */
    private static final Map<String, String> ENV = Map.of("SNAPSHOT_MODE", "off");

    /** Starts {@code n}'s engine on the next jar: the seam goes in at the service's onStart, before restore. */
    private static void startOnNextJar(final Node n) throws Exception {
        final int jdwp = freePort();
        n.startEngine(ENV, List.of("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=127.0.0.1:" + jdwp));
        try (ChildFaultInstaller ignored = new ChildFaultInstaller(jdwp, "wireJar", 11)) {
            n.awaitReady(180_000);
        }
        n.awaitLine("stands in for a money-schema 11 jar", 10_000);
    }

    private static String metrics(final Node n) throws IOException {
        final HttpURLConnection c = (HttpURLConnection)URI.create("http://127.0.0.1:" + n.probePort + "/metrics").toURL()
                .openConnection();
        c.setConnectTimeout(1_000);
        c.setReadTimeout(2_000);
        try (var in = c.getInputStream()) {
            return new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    private static long metric(final Node n, final String name) throws IOException {
        final String v = ServiceProbe.seriesValue(metrics(n), name);
        return v == null ? Long.MIN_VALUE : Long.parseLong(v.contains(".") ? v.substring(0, v.indexOf('.')) : v);
    }

    private static void awaitMetric(final List<Node> nodes, final String name, final long value) throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (true) {
            final StringBuilder seen = new StringBuilder();
            boolean all = true;
            for (final Node n : nodes) {
                final long v = metric(n, name);
                seen.append("member ").append(n.nodeId).append('=').append(v).append(' ');
                all &= v == value;
            }
            if (all) {
                return;
            }
            assertTrue(name + " must read " + value + " on every member: " + seen, System.nanoTime() < deadline);
            Thread.sleep(100);
        }
    }

    private Clients clients(final List<Node> nodes) throws InterruptedException {
        final Clients c = outer.new Clients(nodes);
        open.add(c);
        return c;
    }

    private static void offer(final Clients c, final byte[] frame) {
        CoreRestoreRig.offer(c.admin, new UnsafeBuffer(frame), frame.length, c.all);
    }

    @Test(timeout = 900_000)
    public void aMemberLeftOnTheEarlierJarStopsAtTheRaiseAndRejoinsOnTheNextJar() throws Exception {
        System.setProperty("cluster.port.stride", Integer.toString(ClusterPorts.SHARED_HOST_STRIDE));
        ClusterPorts.resetStrideForTest();
        final int portBase = 27600;
        final List<Node> nodes = List.of(outer.new Node("wire-bump-0-", portBase, 0, 3),
                outer.new Node("wire-bump-1-", portBase, 1, 3), outer.new Node("wire-bump-2-", portBase, 2, 3));
        for (final Node n : nodes) {
            n.startDriver();
            n.startEngine(ENV, List.of());
        }
        for (final Node n : nodes) {
            n.awaitReady(180_000);
            assertTrue("external driver mode on member " + n.nodeId, n.log().contains("Driver mode EXTERNAL"));
        }
        Clients c = clients(nodes);
        c.adopt();
        c.deposits(50);
        awaitMetric(nodes, "core_max_wire_version", 10);
        awaitMetric(nodes, "core_supported_min_wire_version", 10);
        awaitMetric(nodes, "core_supported_max_wire_version", 10);

        // members 0 and 1 roll to the next jar; member 2 is left on the earlier one
        final Node left = nodes.get(2);
        for (final Node n : nodes.subList(0, 2)) {
            c.close();
            n.killEngine();
            startOnNextJar(n);
            c = clients(nodes);
            c.deposits(20);
        }
        for (final Node n : nodes.subList(0, 2)) {
            assertEquals(11, metric(n, "core_supported_max_wire_version"));
        }
        assertEquals(10, metric(left, "core_supported_max_wire_version"));

        // a v11 probe before the raise: dropped from the header on all three, no ack
        final WireVersionFrames f = new WireVersionFrames();
        offer(c, f.deposit(11, 0x7_0001L, 9, Asset.USD.id(), ONE));
        awaitMetric(nodes, "core_ingress_above_ceiling_drops_total", 1);
        c.deposits(10);
        assertTrue("no ack for the dropped probe", c.admin.in.maxDepositAckCorr < 0x7_0001L);

        // the raise, encoded at v10. "Raised" means applied: the recorded value on the members, not the ack (when the
        // member left behind leads, it applies the raise and stops before its egress is sent)
        final String roles = nodes.stream().map(n -> n.nodeId + "=" + n.role()).toList().toString();
        offer(c, f.coreConfig(10, 0x72L, 2, 1, 11));
        final Clients raised = c;
        // the member left behind stops itself with the node-local text; it never rejects
        assertTrue("the member on the earlier jar exits", left.engine.waitFor(60, TimeUnit.SECONDS));
        final int exit = left.engine.exitValue();
        assertNotEquals("a non-zero exit", 0, exit);
        assertTrue("the node-local text in its log: " + ShadowRestoreExternalDriverProcessTest.tail(left.log()),
                left.log().contains(R6));
        final List<Node> upgraded = nodes.subList(0, 2);
        awaitMetric(upgraded, "core_max_wire_version", 11);
        assertEquals("the raise is never rejected", 0, raised.admin.in.count(
                s -> s.startsWith("ADMINACK ref=70 result=REJECTED")));
        System.out.println("[wire-bump] roles at the raise " + roles + "; ack seen: "
                + raised.admin.in.count(s -> s.startsWith("ADMINACK ref=70 result=ACCEPTED")));
        c.close();
        c = clients(upgraded);
        final Clients after = c;

        // after the raise a v11 deposit is applied on the two members (quorum)
        offer(c, f.deposit(11, 0x7_0002L, 9, Asset.USD.id(), ONE));
        CoreRestoreRig.await(c.all, () -> after.admin.in.maxDepositAckCorr >= 0x7_0002L,
                System.currentTimeMillis() + 60_000, "the v11 deposit after the raise is acked");
        c.deposits(10);

        // recovery: the stopped member restarts on the next jar, rejoins and replays the raise without stopping
        Thread.sleep(12_000); // its mark files and driver resources time out first, as after a kill
        startOnNextJar(left);
        awaitMetric(nodes, "core_max_wire_version", 11);
        assertTrue("no stop on the rejoined member", !left.log().contains(R6));
        c.close();
        c = clients(nodes); // the rejoined member may lead: the client knows all three endpoints again
        c.deposits(10);

        // one snapshot, the same state hash everywhere
        final long before = left.latestSnapshot();
        // the leader's consensus module takes the snapshot for the cluster
        final long toggleDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        boolean toggled = false;
        while (!toggled) {
            assertTrue("the SNAPSHOT toggle on the leader", System.nanoTime() < toggleDeadline);
            for (final Node n : nodes) {
                if (n.role() == io.aeron.cluster.service.Cluster.Role.LEADER) {
                    final AtomicCounter toggle = ClusterControl.findControlToggle(n.aeron().countersReader(),
                            n.layout.consensusModuleContext().clusterId());
                    toggled = toggle != null && ClusterControl.ToggleState.SNAPSHOT.toggle(toggle);
                }
            }
            if (!toggled) {
                Thread.sleep(200);
            }
        }
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (nodes.stream().anyMatch(n -> n.latestSnapshot() <= before)) {
            assertTrue("a snapshot on every member", System.nanoTime() < deadline);
            Thread.sleep(100);
        }
        final long p = nodes.get(0).latestSnapshot();
        awaitMetric(nodes, "core_snapshot_last_log_position", p);
        final long hash = metric(nodes.get(0), "core_snapshot_state_hash");
        assertTrue("a state hash", hash != Long.MIN_VALUE);
        awaitMetric(nodes, "core_snapshot_state_hash", hash);
        System.out.println("GREEN external wire bump: probe dropped on 3, member " + left.nodeId + " stopped at the raise"
                + " (exit " + exit + "), rejoined on the next jar; state hash " + hash + " at " + p
                + " on all three");
    }
}
