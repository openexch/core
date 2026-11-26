// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.cluster.ClusterPorts;
import com.openexchange.cluster.EngineLifecycle;
import com.openexchange.core.cluster.ShadowRestoreExternalDriverProcessTest.Clients;
import com.openexchange.core.cluster.ShadowRestoreExternalDriverProcessTest.Node;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * S4 T5 (core): incompatible member processes (the production {@code Main} through {@code MemberCompatSeamMain}, which
 * only sets the member version) against a three-member g1.1 cluster, on external {@code aeronmd} drivers and on
 * embedded drivers. Each refused member exits 78 with the R-A4 text in its log and the cluster keeps serving; each is
 * then restarted on a legal build and rejoins (C5).
 * <ol>
 *   <li>g1.3 with an empty directory: it joins, meets the leader's g1.1 term and exits 78;</li>
 *   <li>g2.1 (another generation), empty directory: the same;</li>
 *   <li>g1.3 with this cluster's local state, in a forced election: the member is the only one that can win (its log
 *   is the longest of the two started), and the pre-election check stops it before it canvasses, so the healthy g1.1
 *   member that started with it is never stopped.</li>
 * </ol>
 * Negative controls ({@code -Ds4.control=}): {@code a} turns the pre-election check off in the g1.3 child of row 3:
 * it wins the election and the assertion "no healthy member stopped" fails; {@code b} gives the g1.3 child of row 1
 * Aeron's default validator: it joins, and the assertion "exit == 78" fails.
 */
public class IncompatibleMemberProcessTest {

    private static final String SEAM_MAIN = "com.openexchange.core.infrastructure.persistence.MemberCompatSeamMain";
    private static final String CONTROL = System.getProperty("s4.control", "");
    private static final String R_A4 = "FATAL (exit 78): member version incompatible: this build is ";

    private final ShadowRestoreExternalDriverProcessTest outer = new ShadowRestoreExternalDriverProcessTest();
    private final List<Clients> open = new ArrayList<>();
    private final List<Path> embeddedDirs = new ArrayList<>();

    @After
    public void cleanUp() throws Exception {
        for (final Clients c : open) {
            c.close();
        }
        outer.cleanUp();
        // the members' own embedded driver directories (the rig deletes only the directories it created)
        for (final Path d : embeddedDirs) {
            if (Files.exists(d)) {
                try (Stream<Path> walk = Files.walk(d)) {
                    for (final Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                        Files.deleteIfExists(p);
                    }
                }
            }
        }
        System.clearProperty("cluster.port.stride");
        ClusterPorts.resetStrideForTest();
    }

    @Test(timeout = 1_500_000)
    public void externalDriver() throws Exception {
        rows(28400, false);
    }

    @Test(timeout = 1_500_000)
    public void embeddedDriver() throws Exception {
        rows(28800, true);
    }

    private Map<String, String> env(final boolean embedded) {
        final Map<String, String> env = new HashMap<>();
        env.put("SNAPSHOT_MODE", "off");
        if (embedded) {
            env.put("TRANSPORT_DRIVER_MODE", "embedded");
        }
        return env;
    }

    /** Starts {@code n}'s engine at {@code version} ({@code "1.1"}) with the extra seam properties. */
    private void start(final Node n, final boolean embedded, final String version, final String... props)
            throws IOException {
        final List<String> jvm = new ArrayList<>();
        jvm.add("-Dtest.member.compat=" + version);
        if (embedded) {
            // the member's own embedded driver directory, apart from the test's client driver at aeronDir
            jvm.add("-Daeron.dir=" + n.aeronDir + "-emb");
            final Path dir = Path.of(n.aeronDir + "-emb-core-" + n.nodeId + "-driver");
            if (!embeddedDirs.contains(dir)) {
                embeddedDirs.add(dir);
            }
        }
        jvm.addAll(List.of(props));
        n.startEngine(env(embedded), jvm, SEAM_MAIN);
    }

    private Clients clients(final List<Node> nodes) throws InterruptedException {
        final Clients c = outer.new Clients(nodes);
        open.add(c);
        return c;
    }

    private void closeClients() {
        for (final Clients c : open) {
            c.close();
        }
        open.clear();
    }

    private static String ready(final Node n) throws IOException {
        final HttpURLConnection c = (HttpURLConnection)URI.create("http://127.0.0.1:" + n.probePort + "/ready").toURL()
                .openConnection();
        c.setConnectTimeout(1_000);
        c.setReadTimeout(2_000);
        final var in = c.getResponseCode() < 400 ? c.getInputStream() : c.getErrorStream();
        try (in) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static void wipeState(final Node n) throws IOException {
        final Path state = n.base.resolve("state");
        if (Files.exists(state)) {
            try (Stream<Path> walk = Files.walk(state)) {
                for (final Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                    Files.delete(p);
                }
            }
        }
    }

    /** Waits for {@code n} to exit; returns its status, or -1 when it is still running after {@code seconds}. */
    private static int exitWithin(final Node n, final int seconds) throws InterruptedException {
        return n.engine.waitFor(seconds, TimeUnit.SECONDS) ? n.engine.exitValue() : -1;
    }

    /** After an exit: the member's mark files and driver resources time out before the next start claims them. */
    private static void settle() throws InterruptedException {
        Thread.sleep(12_000);
    }

    private void assertRefused(final Node n, final String own, final String other, final String what)
            throws Exception {
        final int exit = exitWithin(n, 120);
        assertEquals("exit == 78 (" + what + "): " + ShadowRestoreExternalDriverProcessTest.tail(n.log()),
                EngineLifecycle.EXIT_CONFIG, exit);
        final String log = n.log();
        assertTrue(what + ": the R-A4 text names " + own + ": " + ShadowRestoreExternalDriverProcessTest.tail(log),
                log.contains(R_A4 + own + " (build "));
        assertTrue(what + ": the R-A4 text names " + other + ": " + ShadowRestoreExternalDriverProcessTest.tail(log),
                log.contains(other + "; a cluster runs level N and N-1 of one generation; install a compatible build,"
                        + " a restart cannot fix this"));
        assertTrue(what + ": the exit is reported as 78", log.contains("exit 78"));
    }

    private void serves(final List<Node> nodes, final int deposits) throws Exception {
        closeClients();
        clients(nodes).deposits(deposits);
    }

    private void rows(final int portBase, final boolean embedded) throws Exception {
        System.setProperty("cluster.port.stride", Integer.toString(ClusterPorts.SHARED_HOST_STRIDE));
        ClusterPorts.resetStrideForTest();
        final String p = embedded ? "s4-emb-" : "s4-ext-";
        final List<Node> nodes = List.of(outer.new Node(p + "0-", portBase, 0, 3), outer.new Node(p + "1-", portBase, 1, 3),
                outer.new Node(p + "2-", portBase, 2, 3));
        for (final Node n : nodes) {
            n.startDriver(); // embedded: the test's own client driver only; the member runs its own
            start(n, embedded, "1.1");
        }
        for (final Node n : nodes) {
            n.awaitReady(180_000);
            if (!embedded) {
                assertTrue(n.log(), n.log().contains("Driver mode EXTERNAL"));
            }
            final String body = ready(n);
            assertTrue(body, body.contains(" compat=g1.1 ") && body.contains(embedded ? " driver=embedded"
                    : " driver=external"));
        }
        final Clients first = clients(nodes);
        first.adopt();
        first.deposits(30);
        final Node a = nodes.get(2);
        final Node b = nodes.get(1);
        final Node c = nodes.get(0);
        final List<Node> twoLeft = List.of(c, b);

        // row 1: g1.3, empty directory: it meets the leader and exits 78; the cluster keeps serving
        closeClients();
        a.killEngine();
        wipeState(a);
        start(a, embedded, "1.3", "b".equals(CONTROL) ? "-Dtest.member.compat.validator=aeron" : "-Dnone=0");
        assertRefused(a, "g1.3", "is g1.1", "g1.3 with an empty directory");
        serves(twoLeft, 10);
        settle();

        // row 2: another generation, empty directory
        wipeState(a);
        start(a, embedded, "2.1");
        assertRefused(a, "g2.1", "is g1.1", "another generation");
        serves(twoLeft, 10);
        settle();

        // recovery: the legal build rejoins
        wipeState(a);
        start(a, embedded, "1.1");
        a.awaitReady(180_000);
        serves(nodes, 10);

        // row 3: g1.3 with local state in a forced election. b stops first, so a and c hold the longest log; then
        // only a (g1.3) and b (g1.1) start: a is the only member that can win
        closeClients();
        b.killEngine();
        serves(List.of(c, a), 20);
        closeClients();
        c.killEngine();
        a.killEngine();
        if ("dry".equals(CONTROL)) {
            // C14 dry run: both on g1.1, the kill window makes a the leader (the forced election is real)
            start(a, embedded, "1.1");
            start(b, embedded, "1.1");
            a.awaitReady(180_000);
            final String body = ready(a);
            System.out.println("[s4 dry run] member a after the forced election: " + body);
            assertTrue("a leads after the forced election: " + body, body.contains("role=LEADER"));
            return;
        }
        start(a, embedded, "1.3", "a".equals(CONTROL) ? "-Dtest.member.compat.prelaunch=off" : "-Dnone=0");
        start(b, embedded, "1.1");
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline && a.engine.isAlive() && b.engine.isAlive()) {
            Thread.sleep(200);
        }
        if (a.engine.isAlive() && b.engine.isAlive()) {
            Thread.sleep(5_000);
        }
        assertTrue("no healthy member stopped: the g1.1 member b " + (b.engine.isAlive() ? "runs" : "exited "
                + b.engine.exitValue()) + ": " + ShadowRestoreExternalDriverProcessTest.tail(b.log()),
                b.engine.isAlive() && !b.log().contains("FATAL"));
        assertRefused(a, "g1.3", "the cluster's last seen level is g1.1", "g1.3 with local state");
        assertFalse("stopped before any Aeron component: " + ShadowRestoreExternalDriverProcessTest.tail(a.log()),
                a.log().contains("starting: portBase") || a.log().contains("NODE: /health"));
        start(c, embedded, "1.1");
        c.awaitReady(180_000);
        b.awaitReady(180_000);
        serves(twoLeft, 10);
        settle();

        // recovery: a on the legal build rejoins with its state
        start(a, embedded, "1.1");
        a.awaitReady(180_000);
        serves(nodes, 10);
        for (final Node n : nodes) {
            assertTrue(ready(n), ready(n).contains(" compat=g1.1 "));
        }
        System.out.println("GREEN incompatible members (" + (embedded ? "embedded" : "external") + " driver): empty"
                + " g1.3, g2.1 and stateful g1.3 each exited 78, the cluster kept serving, the legal build rejoined");
    }
}
