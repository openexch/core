// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.CoreRestoreRig.Encoders;
import com.openexchange.core.cluster.CoreRestoreRig.Listener;
import com.openexchange.core.cluster.CoreRestoreRig.Session;
import com.openexchange.core.infrastructure.InfrastructureConstants;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.generated.TimeInForce;
import com.openexchange.core.infrastructure.persistence.ClusterConfig;
import com.openexchange.cluster.ClusterPorts;
import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.AeronCluster;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The order journal through the production entry point ({@code com.openexchange.core.Main}) in a child JVM, in both
 * driver modes. Each leg journals order events (read back from the member's own archive), loses its process, restarts
 * on the same directories, places more orders, and its journal continues densely from 1 (the restart replays the log,
 * regenerating what the ring held at the loss). External mode loses its MEDIA DRIVER (kill -9 of the driver process):
 * the engine exits 1 on driver loss by the existing rule. Its control runs the same leg on a test build that ignores
 * driver loss ({@link DriverLossIgnoredMain}, the production entry point with its halt swallowed): the member stays alive
 * with a dead driver and the leg fails. Embedded mode loses the whole process (kill -9; its driver is inside it).
 */
public class OrderJournalDriverLossProcessTest {

    private static final String ENGINE_MAIN = "com.openexchange.core.Main";
    private static final long ONE = 100_000_000L;
    private static final Pattern RECORDED = Pattern.compile("recordedSeq=(-?\\d+)");
    private static final Pattern HOLES = Pattern.compile("holes=(\\S+)");

    private final List<Process> processes = new ArrayList<>();
    private final List<Path> dirs = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @Before
    public void defaultStride() {
        System.clearProperty("cluster.port.stride");
        ClusterPorts.resetStrideForTest();
    }

    @After
    public void cleanUp() throws Exception {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            CloseHelper.quietClose(closeables.get(i));
        }
        for (final Process p : processes) {
            p.destroyForcibly();
            p.waitFor(10, TimeUnit.SECONDS);
        }
        for (final Path d : dirs) {
            deleteTree(d);
        }
    }

    @Test(timeout = 600_000)
    public void externalDriverKilledExitsOneAndTheJournalContinuesDenselyAfterTheRestart() throws Exception {
        externalDriverKilled(new Node("order-journal-ext-", 27800, true));
    }

    /** Control: a test build that ignores driver loss stays alive on a dead driver; the leg must catch it. */
    @Test(timeout = 600_000)
    public void controlATestBuildThatIgnoresDriverLossStaysAliveWithADeadDriver() throws Exception {
        final Node node = new Node("order-journal-ext-ignored-", 28400, true);
        node.mainClass = DriverLossIgnoredMain.class.getName();
        node.jvmArgs = List.of("-Djava.security.manager=allow");
        try {
            externalDriverKilled(node);
        } catch (final AssertionError expected) {
            final String msg = String.valueOf(expected.getMessage());
            assertTrue(msg, msg.startsWith("member alive with a dead driver"));
            final String out = node.log();
            final int lost = out.indexOf("media driver lost in EXTERNAL driver mode");
            final int swallowed = out.indexOf(DriverLossIgnoredMain.SWALLOWED);
            assertTrue("the test build swallowed the driver-loss halt (the first exit swallowed follows the loss): "
                    + tail(out), lost >= 0 && swallowed > lost);
            System.out.println("[order-journal process] control: " + msg.lines().findFirst().orElse(msg) + " | "
                    + line(out, DriverLossIgnoredMain.SWALLOWED));
            return;
        }
        fail("the test build that ignores driver loss exited after the driver kill");
    }

    private void externalDriverKilled(final Node node) throws Exception {
        node.startDriver();
        node.startEngine();
        node.awaitReady(90_000);
        Clients c = node.clients();
        c.open();
        c.place(20);
        final long before = node.awaitJournalAtRest("before the driver kill");
        c.close();
        node.closeAeron();

        node.driver.destroyForcibly(); // kill -9 of the media driver
        final long t0 = System.nanoTime();
        if (!node.engine.waitFor(45, TimeUnit.SECONDS)) {
            throw new AssertionError("member alive with a dead driver: the engine did not exit within 45 s of the driver"
                    + " kill: " + tail(node.log()));
        }
        final long exitMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        final String out = node.log();
        assertEquals("driver loss exits 1 (after " + exitMs + " ms): " + tail(out), 1, node.engine.exitValue());
        assertTrue("the existing driver-loss rule halted it: " + tail(out),
                out.contains("media driver lost in EXTERNAL driver mode"));
        Thread.sleep(12_000); // the killed engine's mark files time out before a new one may claim them

        node.startDriver();
        node.startEngine();
        node.awaitReady(120_000);
        c = node.clients();
        c.reconnectGateway();
        c.place(10);
        final long after = node.awaitJournalAtRest("after the restart");
        assertTrue("the restart journals more: " + before + " < " + after, after > before);
        assertTrue("external mode: " + tail(node.log()), node.log().contains("Driver mode EXTERNAL"));
        System.out.println("GREEN order journal, external driver killed: exit 1 after " + exitMs + " ms ("
                + line(out, "media driver lost in EXTERNAL driver mode") + "); journal 1.." + before + " before, 1.." + after
                + " dense after the restart over " + node.recordings + " recordings");
    }

    @Test(timeout = 600_000)
    public void embeddedDriverJournalsAndContinuesDenselyAfterAProcessKill() throws Exception {
        final Node node = new Node("order-journal-emb-", 27900, false);
        node.startEngine();
        node.awaitReady(90_000);
        Clients c = node.clients();
        c.open();
        c.place(20);
        final long before = node.awaitJournalAtRest("before the kill");
        c.close();
        node.closeAeron();
        node.engine.destroyForcibly(); // kill -9: the embedded driver dies with the process
        assertTrue("the engine died", node.engine.waitFor(20, TimeUnit.SECONDS));
        Thread.sleep(12_000); // the embedded driver directory's heartbeat must age past CORE_DRIVER_DIR_ACTIVE_MS

        node.startEngine();
        node.awaitReady(120_000);
        c = node.clients();
        c.reconnectGateway();
        c.place(10);
        final long after = node.awaitJournalAtRest("after the restart");
        assertTrue("the restart journals more: " + before + " < " + after, after > before);
        final String out = node.log();
        assertTrue("embedded, not external: " + tail(out), out.contains("driverMode=EMBEDDED")
                && !out.contains("Driver mode EXTERNAL"));
        System.out.println("GREEN order journal, embedded driver: journal 1.." + before + " before the kill, 1.." + after
                + " dense after the restart over " + node.recordings + " recordings");
    }

    // ---------------------------------------------------------------------------------------------------- node

    private final class Node {
        final Path base;
        final Path shm;
        final int portBase;
        final int probePort;
        final boolean external;
        final ClusterConfig layout;
        String mainClass = ENGINE_MAIN;
        List<String> jvmArgs = List.of();
        Process driver;
        Process engine;
        Aeron aeron;
        AeronArchive archive;
        int starts;
        int recordings;

        Node(final String prefix, final int portBase, final boolean external) throws IOException {
            this.base = dir(prefix);
            this.shm = shmDir(prefix + "aeron-");
            this.portBase = portBase;
            this.probePort = freePort();
            this.external = external;
            this.layout = ClusterConfig.create(0, List.of("localhost"), List.of("localhost"), portBase,
                    new TimerPerOrderControlService());
            layout.baseDir(base.resolve("state").toFile());
            layout.aeronDirectoryName(clientDir());
        }

        /** The driver directory the test JVM's clients use: the external driver's, or the one embedded mode derives. */
        String clientDir() {
            return external ? shm.toString() : shm.resolve("aeron") + "-core-0-driver";
        }

        void startDriver() throws Exception {
            final List<String> cmd = java();
            cmd.addAll(List.of("-Daeron.dir=" + shm, "-Daeron.dir.delete.on.start=true",
                    "-Daeron.dir.delete.on.shutdown=true", "-Daeron.term.buffer.sparse.file=true",
                    "-cp", System.getProperty("java.class.path"), "io.aeron.driver.MediaDriver"));
            driver = new ProcessBuilder(cmd).redirectErrorStream(true)
                    .redirectOutput(base.resolve("driver-" + starts + ".log").toFile()).start();
            processes.add(driver);
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (!Files.exists(shm.resolve("cnc.dat"))) {
                assertTrue("driver did not start", System.nanoTime() < deadline && driver.isAlive());
                Thread.sleep(50);
            }
        }

        Path logFile() {
            return base.resolve("engine-" + starts + ".log");
        }

        String log() throws IOException {
            return Files.readString(logFile());
        }

        void startEngine() throws IOException {
            starts++;
            final List<String> cmd = java();
            if (!external) {
                cmd.add("-Daeron.dir=" + shm.resolve("aeron"));
            }
            cmd.addAll(jvmArgs);
            cmd.addAll(List.of("-Dcore.auth.keys=" + TestAuth.KEYS, "-Dsnapshot.interval.minutes=0",
                    "-XX:ErrorFile=" + base.resolve("hs_err_pid%p.log"), "-cp", System.getProperty("java.class.path"),
                    mainClass));
            final ProcessBuilder pb = new ProcessBuilder(cmd);
            final Map<String, String> env = pb.environment();
            env.put("BASE_DIR", base.resolve("state").toString());
            if (external) {
                env.put("AERON_DIR", shm.toString());
            }
            env.put("TRANSPORT_DRIVER_MODE", external ? "external" : "embedded");
            env.put("TRANSPORT_IDLE_MODE", "backoff");
            env.put("TRANSPORT_LOG_TERM_LENGTH", "4m");
            env.put("TRANSPORT_LOG_RCV_WINDOW", "2m");
            env.put("TRANSPORT_LOG_SO_RCVBUF", "2m");
            env.put("TRANSPORT_LOG_SO_SNDBUF", "2m");
            env.put("TRANSPORT_SNAPSHOT_TERM_LENGTH", "4m");
            env.put("CLUSTER_NODE", "0");
            env.put("CLUSTER_ADDRESSES", "localhost");
            env.put("CLUSTER_PORT_BASE", String.valueOf(portBase));
            env.put("CORE_NODE_PORT", String.valueOf(probePort));
            env.put("SNAPSHOT_MODE", "off");
            engine = pb.redirectErrorStream(true).redirectOutput(logFile().toFile()).start();
            processes.add(engine);
        }

        void awaitReady(final long timeoutMs) throws Exception {
            final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            while (System.nanoTime() < deadline) {
                assertTrue("engine exited before /ready: " + tail(log()), engine.isAlive());
                if (get("/ready") != null) {
                    return;
                }
                Thread.sleep(100);
            }
            throw new AssertionError("no /ready 200 within " + timeoutMs + " ms: " + tail(log()));
        }

        /** The body of a 200 answer, or null. */
        String get(final String path) {
            try {
                final HttpURLConnection c = (HttpURLConnection) URI.create("http://127.0.0.1:" + probePort + path).toURL()
                        .openConnection();
                c.setConnectTimeout(500);
                c.setReadTimeout(1_000);
                if (c.getResponseCode() != 200) {
                    return null;
                }
                return new String(c.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            } catch (final IOException notYet) {
                return null;
            }
        }

        Aeron aeron() {
            if (aeron == null) {
                aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(clientDir())
                        .errorHandler(t -> System.out.println("[order-journal process] test-JVM Aeron client: " + t)));
                closeables.add(aeron);
            }
            return aeron;
        }

        AeronArchive archive() {
            if (archive == null) {
                archive = AeronArchive.connect(layout.aeronArchiveContext().clone().aeron(aeron()).ownsAeronClient(false));
                closeables.add(archive);
            }
            return archive;
        }

        void closeAeron() {
            CloseHelper.quietClose(archive);
            archive = null;
            CloseHelper.quietClose(aeron);
            aeron = null;
        }

        /** Every order journal record in this member's archive, over all its recordings: journalSeq -> epoch. */
        TreeMap<Long, Long> journal() {
            final List<long[]> recs = new ArrayList<>();
            archive().listRecordingsForUri(0, 1_000, "aeron:ipc", InfrastructureConstants.ORDER_JOURNAL_STREAM_ID,
                    (c, k, recordingId, st, sp, startPosition, stopPosition, i, t, se, m, sid, str, ch, oc, src) ->
                            recs.add(new long[] {recordingId, startPosition, stopPosition}));
            recordings = recs.size();
            final TreeMap<Long, Long> seqs = new TreeMap<>();
            final com.openexchange.cluster.shadow.ShadowReplay replay = new com.openexchange.cluster.shadow.ShadowReplay(
                    aeron(), archive(), org.agrona.concurrent.SystemNanoClock.INSTANCE);
            for (final long[] r : recs) {
                final long stop = r[2] == AeronArchive.NULL_POSITION ? archive().getRecordingPosition(r[0]) : r[2];
                if (stop > r[1]) {
                    replay.readAll(r[0], r[1], stop, (buffer, offset, length, h) -> seqs.put(
                            buffer.getLong(offset + 8, java.nio.ByteOrder.LITTLE_ENDIAN),
                            buffer.getLong(offset + 16, java.nio.ByteOrder.LITTLE_ENDIAN)), System.nanoTime() + 30_000_000_000L);
                }
            }
            return seqs;
        }

        /**
         * Waits until {@code /health} shows the same recordedSeq twice 500 ms apart and no hole, and the archive's
         * journal is dense from 1 to it with one genesis; returns it.
         */
        long awaitJournalAtRest(final String when) throws Exception {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            String last = "";
            while (System.nanoTime() < deadline) {
                final String h1 = get("/health");
                Thread.sleep(500);
                final String h2 = get("/health");
                last = h2;
                final long r1 = recorded(h1);
                final long r2 = recorded(h2);
                if (r1 > 0 && r1 == r2) {
                    final TreeMap<Long, Long> j = journal();
                    if (!j.isEmpty() && j.lastKey() == r2) {
                        long expect = 1;
                        for (final long q : j.keySet()) {
                            assertEquals(when + ": the journal is dense from 1 (" + recordings + " recordings)", expect, q);
                            expect = q + 1;
                        }
                        assertEquals(when + ": one genesis", 1, new java.util.HashSet<>(j.values()).size());
                        assertFalse(when + ": epoch 0", j.containsValue(0L));
                        final Matcher holes = HOLES.matcher(h2);
                        assertTrue(when + ": holes on /health: " + h2, holes.find());
                        assertEquals(when + ": no hole declared: " + h2, "none", holes.group(1));
                        return r2;
                    }
                }
            }
            throw new AssertionError(when + ": the order journal never came to rest; /health " + last + "; engine "
                    + tail(log()));
        }

        Clients clients() throws InterruptedException {
            return new Clients(this);
        }
    }

    private static long recorded(final String health) {
        if (health == null) {
            return -1;
        }
        final Matcher m = RECORDED.matcher(health);
        return m.find() ? Long.parseLong(m.group(1)) : -1;
    }

    /** ADMIN, TOOL and GATEWAY sessions of the test JVM on the node's driver. */
    private final class Clients implements AutoCloseable {
        final Node node;
        final List<Session> all = new ArrayList<>();
        final Encoders enc = new Encoders();
        Session gateway;
        long request = 1;

        Clients(final Node node) {
            this.node = node;
            closeables.add(this);
        }

        Session session(final SessionClass cls) throws InterruptedException {
            final Listener in = new Listener();
            final long deadline = System.currentTimeMillis() + 60_000;
            RuntimeException last = null;
            while (System.currentTimeMillis() < deadline) {
                try {
                    final AeronCluster client = AeronCluster.connect(new AeronCluster.Context()
                            .credentialsSupplier(TestAuth.credentials(cls))
                            .errorHandler(t -> System.out.println("[order-journal process] cluster client: " + t))
                            .egressListener(in)
                            .egressChannel("aeron:udp?endpoint=localhost:0")
                            .ingressChannel("aeron:udp")
                            .aeronDirectoryName(node.clientDir())
                            .ingressEndpoints("0=localhost:" + ClusterConfig.calculatePort(0, node.portBase,
                                    ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                            .messageTimeoutNs(TimeUnit.SECONDS.toNanos(5)));
                    final Session s = new Session(client, in);
                    all.add(s);
                    return s;
                } catch (final RuntimeException e) {
                    last = e;
                    Thread.sleep(250);
                }
            }
            throw new IllegalStateException("could not connect a " + cls + " session", last);
        }

        /** ADOPT, deposits, limits and the gate OPEN, then a registered GATEWAY session. */
        void open() throws InterruptedException {
            final Session admin = session(SessionClass.ADMIN);
            final Session tool = session(SessionClass.TOOL);
            final UnsafeBuffer buf = new UnsafeBuffer(new byte[4096]);
            CoreRestoreRig.offer(admin, buf, ShadowClusterRig.encodeAdopt(buf), all);
            CoreRestoreRig.await(all, () -> admin.in.count(f -> f.startsWith("ADMINACK ref=70 result=ADOPTED")) > 0,
                    System.currentTimeMillis() + 60_000, "the CoreConfig ADOPT must be acked");
            for (long corr = 1; corr <= 50; corr++) {
                CoreRestoreRig.offer(tool, enc.buf, enc.deposit(corr, 1 + (corr % 5), CoreRestoreRig.usd(), ONE), all);
            }
            CoreRestoreRig.await(all, () -> tool.in.lastDepositAckCorr >= 50, System.currentTimeMillis() + 60_000,
                    "deposit acks");
            CoreRestoreRig.offer(admin, enc.buf, enc.limits(0x77L, 1, 600_000L, 100_000, 1_024), all);
            CoreRestoreRig.offer(admin, enc.buf, enc.gateOpen(0x73L), all);
            CoreRestoreRig.await(all, () -> admin.in.count(f -> f.startsWith("ADMINACK ref=73 result=ACCEPTED")) > 0,
                    System.currentTimeMillis() + 60_000, "the ADMIN OPEN must be accepted");
            reconnectGateway();
        }

        void reconnectGateway() throws InterruptedException {
            gateway = session(SessionClass.GATEWAY);
            CoreRestoreRig.register(gateway, SessionClass.GATEWAY, false, false, all);
        }

        void place(final int n) {
            final int before = gateway.in.acks;
            for (int i = 0; i < n; i++, request++) {
                CoreRestoreRig.offer(gateway, enc.buf, enc.limit(0x6000L + request + 1_000L * node.starts,
                        1 + (request % 5), request + 1_000L * node.starts, 1, true, ONE / 10, ONE, TimeInForce.GTC, 0L), all);
            }
            CoreRestoreRig.await(all, () -> gateway.in.acks >= before + n, System.currentTimeMillis() + 60_000,
                    "order acks " + gateway.in.acks + " first reject " + gateway.in.firstReject);
        }

        @Override
        public void close() {
            for (final Session s : all) {
                CloseHelper.quietClose(s.client);
            }
            all.clear();
        }
    }

    // ---------------------------------------------------------------------------------------------------- helpers

    private static List<String> java() {
        return new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "-Xmx1g", "-Daeron.threading.mode=SHARED", "-Daeron.archive.threading.mode=SHARED",
                "-Daeron.archive.segment.file.length=67108864"));
    }

    private static String line(final String out, final String needle) {
        for (final String l : out.split("\n")) {
            if (l.contains(needle)) {
                return l.trim();
            }
        }
        return null;
    }

    private static String tail(final String out) {
        final String[] lines = out.split("\n");
        final StringBuilder sb = new StringBuilder();
        for (int i = Math.max(0, lines.length - 40); i < lines.length; i++) {
            sb.append('\n').append(lines[i]);
        }
        return sb.toString();
    }

    private Path dir(final String prefix) throws IOException {
        final Path d = Files.createTempDirectory(prefix);
        dirs.add(d);
        return d;
    }

    private Path shmDir(final String prefix) throws IOException {
        final Path shm = Path.of("/dev/shm");
        final Path d = Files.isDirectory(shm) && Files.isWritable(shm)
                ? Files.createTempDirectory(shm, prefix) : Files.createTempDirectory(prefix);
        dirs.add(d);
        return d;
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static void deleteTree(final Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }
}
