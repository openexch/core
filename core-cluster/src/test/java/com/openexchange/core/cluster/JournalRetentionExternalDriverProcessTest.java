// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.cluster.ClusterPorts;
import com.openexchange.cluster.shadow.ShadowReplay;
import com.openexchange.core.cluster.CoreRestoreRig.Encoders;
import com.openexchange.core.cluster.CoreRestoreRig.Listener;
import com.openexchange.core.cluster.CoreRestoreRig.Session;
import com.openexchange.core.cluster.ShadowRestoreExternalDriverProcessTest.Clients;
import com.openexchange.core.cluster.ShadowRestoreExternalDriverProcessTest.Node;
import com.openexchange.core.infrastructure.InfrastructureConstants;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.generated.TimeInForce;
import com.openexchange.core.infrastructure.persistence.ClusterConfig;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.AeronCluster;
import org.agrona.CloseHelper;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Journal retention on an external media driver: one member, the production entry point in a child JVM on its own
 * {@code MediaDriver} process, {@code JOURNAL_RETENTION_ORDER=readers} from the environment, 128 KiB archive segments
 * and a 64 KiB order journal term. The reader's cursor is delivered by the kit's command as a process of its own; everything
 * about a purge is read from the member's archive and its {@code /metrics}.
 *
 * <p>The story: the start position rises behind the reader; two restarts leave at most two recordings with bytes
 * and no order journal hole; then the media driver is killed right after a cursor that gives the retention work.
 * The member exits 1 by the existing driver-loss rule, and after the restart the archive is consistent (the journal
 * replays densely from its start) and the next pass goes on purging.
 */
public class JournalRetentionExternalDriverProcessTest {

    private static final int SEG = 128 * 1024;
    private static final int FRAME = 256;
    private static final long GENESIS = ShadowClusterRig.GENESIS;
    private static final String TOOL = "com.openexchange.cluster.journal.JournalReaderTool";
    private static final int USERS = 400;

    private final ShadowRestoreExternalDriverProcessTest rig = new ShadowRestoreExternalDriverProcessTest();
    private final List<Session> sessions = new ArrayList<>();

    @Before
    public void stride() {
        System.clearProperty("cluster.port.stride");
        ClusterPorts.resetStrideForTest();
    }

    @After
    public void cleanUp() throws Exception {
        closeSessions();
        rig.cleanUp();
    }

    private void closeSessions() {
        for (final Session s : sessions) {
            CloseHelper.quietClose(s.client);
        }
        sessions.clear();
    }

    private static Map<String, String> env() {
        final Map<String, String> env = new HashMap<>();
        env.put("SNAPSHOT_MODE", "off");
        env.put("JOURNAL_RETENTION_ORDER", "readers");
        env.put("JOURNAL_RETENTION_POLL_MS", "1000");
        // the log's receiver window is at least 64k and at most half its term, and a segment is never below the
        // log term: 128k segments, the smallest the boot accepts
        env.put("ARCHIVE_SEGMENT_LENGTH", "128k");
        env.put("CORE_ORDER_JOURNAL_TERM_LENGTH", "64k");
        env.put("TRANSPORT_LOG_TERM_LENGTH", "128k");
        env.put("TRANSPORT_LOG_RCV_WINDOW", "64k");
        env.put("TRANSPORT_LOG_SO_RCVBUF", "64k");
        env.put("TRANSPORT_LOG_SO_SNDBUF", "64k");
        return env;
    }

    /** The kit's command as a process of its own on the member's host; returns its output. */
    private static String tool(final int expectedExit, final String... args) throws Exception {
        final List<String> cmd = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xshare:auto", "-Xmx64m", "-cp",
                System.getProperty("java.class.path"), TOOL));
        cmd.addAll(List.of(args));
        final Path out = Files.createTempFile("journal-reader-tool-", ".log");
        try {
            final Process p = new ProcessBuilder(cmd).redirectErrorStream(true).redirectOutput(out.toFile()).start();
            assertTrue("the reader command did not finish", p.waitFor(60, TimeUnit.SECONDS));
            final String text = Files.readString(out);
            assertEquals("JournalReaderTool " + String.join(" ", args) + ": " + text, expectedExit, p.exitValue());
            return text;
        } finally {
            Files.deleteIfExists(out);
        }
    }

    private static void setReader(final Node node, final long applied) throws Exception {
        tool(0, "set", "--cluster-dir", node.clusterDir().getAbsolutePath(), "--engine", "core", "--cluster-id",
                String.valueOf(node.layout.consensusModuleContext().clusterId()), "--genesis-id",
                String.valueOf(GENESIS), "--journal", "order", "--reader", "oe-projector", "--applied-journal-seq",
                String.valueOf(applied), "--source", "test-pusher");
    }

    private Session gateway(final Node node) throws InterruptedException {
        final Listener in = new Listener();
        final long deadline = System.currentTimeMillis() + 60_000;
        RuntimeException last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                final AeronCluster client = AeronCluster.connect(new AeronCluster.Context()
                        .credentialsSupplier(TestAuth.credentials(SessionClass.GATEWAY))
                        .errorHandler(t -> System.out.println("[cjr external] cluster client: " + t))
                        .egressListener(in)
                        .egressChannel("aeron:udp?endpoint=localhost:0")
                        .ingressChannel("aeron:udp")
                        .aeronDirectoryName(node.aeronDir.toString())
                        .ingressEndpoints("0=localhost:" + ClusterConfig.calculatePort(0, node.portBase,
                                ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                        .messageTimeoutNs(TimeUnit.SECONDS.toNanos(5)));
                final Session s = new Session(client, in);
                sessions.add(s);
                CoreRestoreRig.register(s, SessionClass.GATEWAY, false, false, sessions);
                return s;
            } catch (final RuntimeException e) {
                last = e;
                Thread.sleep(250);
            }
        }
        throw new IllegalStateException("could not connect a GATEWAY session", last);
    }

    /** {@code n} resting orders, ten per funded user. */
    private void place(final Session gw, final int first, final int n) {
        final Encoders enc = new Encoders();
        final int before = gw.in.acks + gw.in.rejects;
        for (int k = 0; k < n; k++) {
            final int i = first - 1 + k;
            final long user = 1 + i / OrderLoad.ORDERS_PER_USER;
            final int slot = i % OrderLoad.ORDERS_PER_USER;
            final boolean buy = slot % 2 == 0;
            final int market = CoreRestoreRig.marketOf(user);
            CoreRestoreRig.offer(gw, enc.buf, enc.limit(i + 1, user, i + 1, market, buy,
                    CoreRestoreRig.priceOf(market, buy, slot / 2), ShadowClusterRig.ONE / 10, TimeInForce.GTC, 0L),
                    sessions);
        }
        CoreRestoreRig.await(sessions, () -> gw.in.acks + gw.in.rejects - before >= n,
                System.currentTimeMillis() + 120_000, "every create must be answered");
        assertEquals("no create refused: " + gw.in.firstReject, 0, gw.in.rejects);
    }

    /** {id, start, end, active} of the member's order journal recordings. */
    private static List<long[]> recordings(final Node node) {
        final AeronArchive archive = node.archive();
        final List<long[]> out = new ArrayList<>();
        archive.listRecordingsForUri(0, 1_000, "aeron:ipc", InfrastructureConstants.ORDER_JOURNAL_STREAM_ID,
                (c, k, recordingId, st, sp, startPosition, stopPosition, i, t, se, mm, sid, str, ch, oc, src) ->
                        out.add(new long[] {recordingId, startPosition, stopPosition, 0}));
        for (final long[] r : out) {
            if (r[2] == AeronArchive.NULL_POSITION) {
                r[2] = archive.getRecordingPosition(r[0]);
                r[3] = 1;
            }
        }
        return out;
    }

    private static long[] active(final Node node) {
        for (final long[] r : recordings(node)) {
            if (r[3] == 1) {
                return r;
            }
        }
        return null;
    }

    private static long withBytes(final Node node) {
        return recordings(node).stream().filter(r -> r[2] > r[1]).count();
    }

    private static String describe(final Node node) {
        return recordings(node).stream().map(java.util.Arrays::toString).toList().toString();
    }

    /**
     * Every order journal record the archive holds: journalSeq to copies. Read again when the member purged between
     * the listing and a replay.
     */
    private static TreeMap<Long, Integer> held(final Node node) {
        while (true) {
            final TreeMap<Long, Integer> seqs = new TreeMap<>();
            final ShadowReplay replay = new ShadowReplay(node.aeron(), node.archive(), SystemNanoClock.INSTANCE);
            try {
                for (final long[] r : recordings(node)) {
                    if (r[2] > r[1]) {
                        replay.readAll(r[0], r[1], r[2], (buffer, offset, length, h) ->
                                seqs.merge(buffer.getLong(offset + 8, ByteOrder.LITTLE_ENDIAN), 1, Integer::sum),
                                System.nanoTime() + 30_000_000_000L);
                    }
                }
                return seqs;
            } catch (final io.aeron.archive.client.ArchiveException purgedUnderTheRead) {
                // list again
            }
        }
    }

    /** The largest journalSeq the archive holds; -1 when it holds none. */
    private static long headOf(final Node node) {
        final TreeMap<Long, Integer> seqs = held(node);
        return seqs.isEmpty() ? -1 : seqs.lastKey();
    }

    private static String get(final Node node, final String path) {
        try {
            final HttpURLConnection c = (HttpURLConnection) URI.create("http://127.0.0.1:" + node.probePort + path)
                    .toURL().openConnection();
            c.setConnectTimeout(500);
            c.setReadTimeout(2_000);
            return c.getResponseCode() == 200 ? new String(c.getInputStream().readAllBytes(), StandardCharsets.UTF_8)
                    : null;
        } catch (final java.io.IOException notYet) {
            return null;
        }
    }

    private static long metric(final Node node, final String name) {
        final String body = get(node, "/metrics");
        final String series = "core_journal_retention_" + name + "{journal=\"order\"} ";
        if (body != null) {
            for (final String line : body.split("\n")) {
                if (line.startsWith(series)) {
                    return Long.parseLong(line.substring(series.length()).trim());
                }
            }
        }
        return Long.MIN_VALUE;
    }

    private static void await(final BooleanSupplier done, final long timeoutMs,
                              final java.util.function.Supplier<String> why) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (!done.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError(why.get());
            }
            Thread.sleep(100);
        }
    }

    private static long expectedStart(final long end) {
        return Math.max(0, (end & -(long) SEG) - 2L * SEG);
    }

    @Test(timeout = 900_000)
    public void theMemberPurgesItsOrderJournalOnAnExternalDriverAcrossRestartsAndADriverKill() throws Exception {
        final Node node = rig.new Node("cjr-external-", 27100);
        final Map<String, String> env = env();
        node.startDriver();
        node.startEngine(env, List.of());
        node.awaitReady(180_000);
        assertTrue("external driver mode", node.log().contains("Driver mode EXTERNAL"));
        assertTrue("the retention thread started: " + ShadowRestoreExternalDriverProcessTest.tail(node.log()),
                node.log().contains("[JOURNAL-RETENTION] started: JOURNAL_RETENTION_MONEY=off"));

        Clients clients = rig.new Clients(List.of(node));
        clients.adopt();
        final Encoders enc = new Encoders();
        CoreRestoreRig.offer(clients.admin, enc.buf, enc.gateOpen(0xAD1L), clients.all);
        final Clients opened = clients;
        CoreRestoreRig.await(clients.all, () -> opened.admin.in.count(f -> f.startsWith("ADMINACK ref=73 result=ACCEPTED"))
                > 0, System.currentTimeMillis() + 60_000, "the ADMIN gate open must be ACCEPTED");
        for (long u = 1; u <= USERS; u++) {
            CoreRestoreRig.offer(clients.tool, enc.buf, enc.deposit(0, u, CoreRestoreRig.usd(),
                    1_000_000 * ShadowClusterRig.ONE), clients.all);
            CoreRestoreRig.offer(clients.tool, enc.buf, enc.deposit(0, u,
                    CoreRestoreRig.baseAssetOf(CoreRestoreRig.marketOf(u)), 1_000 * ShadowClusterRig.ONE), clients.all);
        }
        CoreRestoreRig.offer(clients.tool, enc.buf, enc.deposit(99, 1, CoreRestoreRig.usd(), ShadowClusterRig.ONE),
                clients.all);
        CoreRestoreRig.await(clients.all, () -> opened.tool.in.lastDepositAckCorr == 99,
                System.currentTimeMillis() + 300_000, "the funding deposits must be applied");

        // registered before the reader starts, by the command as a process of its own
        setReader(node, 0);
        Session gw = gateway(node);
        place(gw, 1, 1_600);
        long head = 1_600;
        final long h0 = head;
        await(() -> active(node) != null && active(node)[2] == h0 * FRAME, 60_000,
                () -> "the member records " + h0 + ": " + describe(node));
        assertEquals("held at the reader's cursor 0", 0, active(node)[1]);

        // 1. the start position rises behind the reader
        setReader(node, head);
        await(() -> active(node)[1] == expectedStart(h0 * FRAME), 60_000,
                () -> "the order journal start position never rose on the external driver: " + describe(node));
        final long firstStart = active(node)[1];
        assertTrue(firstStart > 0);
        await(() -> metric(node, "purged_bytes_total") == firstStart && metric(node, "state") == 0, 30_000,
                () -> "the member's /metrics: purged " + metric(node, "purged_bytes_total") + " state "
                        + metric(node, "state"));
        assertEquals(head, metric(node, "safe_seq"));
        assertEquals(2, metric(node, "mode"));

        // 2. two restarts: no hole, the old recordings go, the start advances again
        int next = 1_601;
        for (int restart = 1; restart <= 2; restart++) {
            closeSessions();
            clients.close();
            node.killEngine();
            node.startEngine(env, List.of());
            node.awaitReady(180_000);
            gw = gateway(node);
            place(gw, next, 300);
            next += 300;
            head += 300;
            final long h = head;
            final int n = restart;
            await(() -> active(node) != null && active(node)[2] > 0 && headOf(node) == h, 60_000,
                    () -> "restart " + n + ": the member journals to " + h + ": " + describe(node));
            setReader(node, head);
            await(() -> withBytes(node) == 1 && active(node) != null && active(node)[1] > 0, 90_000,
                    () -> "restart " + n + ": the stopped recordings never went: " + describe(node));
            final String health = get(node, "/health");
            assertTrue("restart " + restart + ": no hole on /health: " + health, health != null
                    && health.contains("holes=none"));
            assertFalse("restart " + restart + ": no order journal hole",
                    node.log().contains("CRITICAL order journal hole"));
            assertFalse(String.valueOf(health), health.contains("journal-retention:"));
        }

        // 3. the media driver is killed right after a cursor that gives the retention work
        place(gw, next, 600);
        next += 600;
        head += 600;
        final long h1 = head;
        await(() -> headOf(node) == h1, 60_000, () -> "the member journals to " + h1 + ": " + describe(node));
        final long startBeforeKill = active(node)[1];
        closeSessions();
        clients.close();
        setReader(node, head);
        node.closeAeron();
        node.driver.destroyForcibly(); // kill -9 of the media driver, a pass is due within the second
        if (!node.engine.waitFor(60, TimeUnit.SECONDS)) {
            throw new AssertionError("member alive with a dead driver: "
                    + ShadowRestoreExternalDriverProcessTest.tail(node.log()));
        }
        assertEquals("driver loss exits 1: " + ShadowRestoreExternalDriverProcessTest.tail(node.log()), 1,
                node.engine.exitValue());
        assertTrue(node.log().contains("media driver lost in EXTERNAL driver mode"));
        Thread.sleep(12_000); // the killed engine's mark files time out before a new one may claim them
        node.startDriver();
        node.startEngine(env, List.of());
        node.awaitReady(180_000);
        // the archive is consistent: every recording lists and replays, and the journal is dense from its start
        final TreeMap<Long, Integer> after = held(node);
        long expect = after.firstKey();
        for (final long seq : after.keySet()) {
            assertEquals("the journal is dense from its start after the driver kill", expect, seq);
            expect = seq + 1;
        }
        assertEquals(head, (long) after.lastKey());
        System.out.println("[cjr external] after the driver kill: start before " + startBeforeKill + ", journal "
                + after.firstKey() + ".." + after.lastKey() + " in " + describe(node));
        // the next pass continues: the reader file is still there, the new recording passes the old ones
        gw = gateway(node);
        place(gw, next, 200);
        head += 200;
        final long h2 = head;
        await(() -> headOf(node) == h2, 60_000, () -> "the member journals to " + h2 + ": " + describe(node));
        setReader(node, head);
        await(() -> withBytes(node) == 1 && active(node) != null && active(node)[1] > 0
                && metric(node, "state") == 0 && metric(node, "safe_seq") == h2, 90_000,
                () -> "the retention did not go on after the driver kill: " + describe(node) + " state "
                        + metric(node, "state"));
        assertTrue(get(node, "/health").contains("holes=none"));
        assertTrue("shown by the command: " + tool(0, "show", "--cluster-dir", node.clusterDir().getAbsolutePath()),
                tool(0, "show", "--cluster-dir", node.clusterDir().getAbsolutePath())
                        .contains("applied_journal_seq=" + head));
        System.out.println("GREEN journal retention, external driver: purged to " + firstStart + " first; two restarts"
                + " and a driver kill later the journal holds " + held(node).firstKey() + ".." + head + " in "
                + describe(node));
    }
}
