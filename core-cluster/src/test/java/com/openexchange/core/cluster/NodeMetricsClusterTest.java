// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.domain.Asset;
import com.openexchange.core.domain.FixedPoint;
import com.openexchange.core.infrastructure.generated.DepositAckDecoder;
import com.openexchange.core.infrastructure.generated.DepositEncoder;
import com.openexchange.core.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.core.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.core.infrastructure.persistence.CoreClusteredService;
import com.openexchange.core.infrastructure.persistence.ClusterConfig;
import io.aeron.cluster.ClusteredMediaDriver;
import com.openexchange.core.infrastructure.generated.SessionClass;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The node's own {@code /metrics} answers the question "what does one ingress message cost the
 * service thread" from a real cluster: apply and drain histograms, per-template counts, role and
 * log position, all read over HTTP from the cluster-kit {@code NodeEndpoint} the service starts.
 */
public class NodeMetricsClusterTest {

    private static final int PORT_BASE = 19600; // disjoint from 19300/19400/19500
    // /health /ready /metrics: the computed port (one member, default stride), passed as an agreeing override
    private static final int NODE_PORT = PORT_BASE + com.openexchange.cluster.ClusterPorts.NODE_ENDPOINT_OFFSET;
    private static final int DEPOSITS = 300;
    private static final long USER = 4242L;

    private final UnsafeBuffer ingress = new UnsafeBuffer(new byte[256]);
    private final MessageHeaderEncoder headerEnc = new MessageHeaderEncoder();
    private final DepositEncoder depositEnc = new DepositEncoder();
    private final MessageHeaderDecoder headerDec = new MessageHeaderDecoder();
    private int acks;

    @Test
    public void metricsReportServiceThreadCostPerMessage() throws Exception {
        System.setProperty("core.node.port", Integer.toString(NODE_PORT));
        System.setProperty("snapshot.interval.minutes", "0");

        final File tmp = new File(System.getProperty("java.io.tmpdir"), "core-node-metrics-" + PORT_BASE);
        IoUtil.delete(tmp, true);
        final String aeronDir = new File(tmp, "driver").getAbsolutePath();
        final List<String> hosts = List.of("localhost");
        final CoreClusteredService service = new CoreClusteredService();
        TestAuth.install();
        final ClusterConfig cfg = ClusterConfig.create(0, hosts, PORT_BASE, service);
        cfg.memberCompat(cfg.memberCompat(), com.openexchange.cluster.MemberCompat.testRefusal("rig"));
        cfg.baseDir(new File(tmp, "node0"));
        cfg.aeronDirectoryName(aeronDir);
        cfg.idleStrategySupplier(BackoffIdleStrategy::new);
        cfg.errorHandler(Throwable::printStackTrace);
        cfg.consensusModuleContext()
                .ingressChannel("aeron:udp?term-length=64k")
                .logChannel("aeron:udp?term-length=64k")
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2));

        // sampler threads that exist before this member starts (one leaked by an earlier test in this JVM) do not count
        final java.util.Set<Thread> samplersBefore = samplerThreads();
        ClusteredMediaDriver mediaDriver = null;
        ClusteredServiceContainer container = null;
        AeronCluster client = null;
        try {
            mediaDriver = ClusteredMediaDriver.launch(
                    cfg.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true),
                    cfg.archiveContext(), cfg.consensusModuleContext());
            container = ClusteredServiceContainer.launch(cfg.clusteredServiceContext());
            client = AeronCluster.connect(new AeronCluster.Context()
                    .credentialsSupplier(TestAuth.credentials(SessionClass.TOOL))
                    .egressListener((sessionId, timestamp, buffer, offset, length, header) -> {
                        if (length >= MessageHeaderDecoder.ENCODED_LENGTH
                                && headerDec.wrap(buffer, offset).templateId() == DepositAckDecoder.TEMPLATE_ID) {
                            acks++;
                        }
                    })
                    .egressChannel("aeron:udp?endpoint=localhost:0")
                    .ingressChannel("aeron:udp?term-length=64k")
                    .aeronDirectoryName(aeronDir)
                    .ingressEndpoints(ClusterConfig.ingressEndpoints(
                            hosts, PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                    .messageTimeoutNs(TimeUnit.SECONDS.toNanos(10)));

            for (int i = 1; i <= DEPOSITS; i++) {
                deposit(client, i);
            }
            final long deadline = System.currentTimeMillis() + 20_000;
            while (acks < DEPOSITS) {
                assertTrue("only " + acks + " deposit acks within 20s", System.currentTimeMillis() < deadline);
                if (client.pollEgress() == 0) {
                    Thread.sleep(1);
                }
            }

            final HttpResponse<String> response = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + NODE_PORT + "/metrics")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            final String body = response.body();
            System.out.println("[TEST] /metrics " + body.length() + " bytes");

            assertTrue(body, value(body, "core_apply_dispatch_seconds_count") >= DEPOSITS);
            assertTrue(body, value(body, "core_apply_drain_seconds_count") >= DEPOSITS);
            assertTrue(body, value(body, "core_apply_template_dispatch_count{template=\"1\"}") >= DEPOSITS);
            assertTrue(body, doubleValue(body, "core_apply_dispatch_seconds_max") > 0);
            assertTrue(body, doubleValue(body, "core_apply_dispatch_seconds_sum") > 0);
            assertEquals(2, value(body, "core_cluster_role"));
            assertEquals(0, value(body, "core_member_id"));
            assertEquals(1, value(body, "core_accounts"));
            // The egress-queue count was renamed; core_sessions{class} is the registered-session family
            assertEquals(1, value(body, "core_egress_sessions"));
            assertTrue(body, body.contains("core_sessions{class=\"BRIDGE\"} "));
            assertEquals(0, value(body, "core_application_callback_failed"));
            assertTrue(body, value(body, "core_log_position") > 0);
            assertEquals(-1, value(body, "core_snapshot_last_bytes"));
            assertTrue(body, value(body, "core_timer_drain_seconds_count") >= 1);
            System.out.println("[TEST] apply p-max " + doubleValue(body, "core_apply_dispatch_seconds_max") * 1e6
                    + " us, mean " + doubleValue(body, "core_apply_dispatch_seconds_sum")
                    / value(body, "core_apply_dispatch_seconds_count") * 1e6 + " us over "
                    + value(body, "core_apply_dispatch_seconds_count") + " messages (dev box, SHARED driver)");

            // Aeron's duty-cycle and snapshot-duration counters of this member (embedded driver), sampled by the
            // kit's own thread once a second: wait for its first samples, then every family is in the body
            final String[] stall = {"core_consensus_module_max_cycle_time_seconds",
                "core_consensus_module_max_cycle_time_recent_seconds",
                "core_consensus_module_max_cycle_time_last_rise_age_seconds",
                "core_consensus_module_cycle_time_exceeded_total", "core_consensus_module_cycle_time_exceeded_recent",
                "core_consensus_module_cycle_time_exceeded_last_age_seconds",
                "core_service_max_cycle_time_seconds", "core_service_cycle_time_exceeded_total",
                "core_snapshot_total_max_duration_seconds", "core_snapshot_total_duration_exceeded_total",
                "core_snapshot_service_max_duration_seconds", "core_snapshot_service_duration_exceeded_total",
                "core_driver_conductor_max_cycle_time_seconds", "core_driver_conductor_cycle_time_exceeded_total",
                "core_driver_sender_max_cycle_time_seconds", "core_driver_receiver_max_cycle_time_seconds"};
            String stallBody = body;
            final long stallDeadline = System.currentTimeMillis() + 20_000;
            while (true) {
                final String scraped = stallBody;
                // the first sample is taken as the member starts, before a cycle was timed: wait for a later one
                if (java.util.Arrays.stream(stall).allMatch(name -> scraped.contains("\n" + name + " "))
                        && scraped.contains("\ncore_archive_max_cycle_time_seconds{archive_id=")
                        && doubleValue(scraped, "core_consensus_module_max_cycle_time_seconds") > 0
                        && doubleValue(scraped, "core_service_max_cycle_time_seconds") > 0) {
                    break;
                }
                assertTrue("the duty-cycle series did not appear with a timed cycle within 20s:\n" + scraped,
                        System.currentTimeMillis() < stallDeadline);
                Thread.sleep(250);
                stallBody = HttpClient.newHttpClient().send(
                        HttpRequest.newBuilder(URI.create("http://localhost:" + NODE_PORT + "/metrics")).GET().build(),
                        HttpResponse.BodyHandlers.ofString()).body();
            }
            final long samplerAge = value(stallBody, "core_stall_sampler_age_seconds");
            assertTrue("the sampler runs: " + samplerAge, samplerAge >= 0 && samplerAge <= 5);
            assertTrue(stallBody, doubleValue(stallBody, "core_consensus_module_max_cycle_time_recent_seconds") > 0);
            assertTrue(stallBody, stallBody.contains("# TYPE core_consensus_module_cycle_time_exceeded_total counter"));
            assertTrue(stallBody, stallBody.contains("# TYPE core_consensus_module_max_cycle_time_seconds gauge"));
            assertTrue(stallBody, stallBody.lines().anyMatch(
                    l -> l.startsWith("core_archive_max_cycle_time_seconds{archive_id=")
                            && l.contains(",agent=\"conductor\"} ")));
            // every series of the body still has one head: the kit's block repeats no family of the registry
            final java.util.Set<String> typed = new java.util.HashSet<>();
            for (final String line : stallBody.split("\n")) {
                if (line.startsWith("# TYPE ")) {
                    assertTrue("a family declared twice: " + line, typed.add(line.split(" ")[2]));
                }
            }
            final java.util.Set<Thread> started = samplerThreads();
            started.removeAll(samplersBefore);
            assertEquals("this member started one sampler thread of its own: " + started, 1, started.size());
            final Thread sampler = started.iterator().next();
            // and its member's stop stops it
            CloseHelper.quietClose(client);
            client = null;
            CloseHelper.quietClose(container);
            container = null;
            sampler.join(10_000);
            assertFalse("the member's stop stops its sampler thread", sampler.isAlive());
        } finally {
            CloseHelper.quietClose(client);
            CloseHelper.quietClose(container);
            CloseHelper.quietClose(mediaDriver);
            IoUtil.delete(tmp, true);
            System.clearProperty("core.node.port");
            System.clearProperty("snapshot.interval.minutes");
        }
    }

    /** The live threads named as the kit names the duty-cycle sampler's thread. */
    private static java.util.Set<Thread> samplerThreads() {
        final java.util.Set<Thread> out = new java.util.HashSet<>();
        for (final Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().equals("stall-metrics-sampler") && t.isAlive()) {
                out.add(t);
            }
        }
        return out;
    }

    private static long value(final String body, final String series) {
        return (long) doubleValue(body, series);
    }

    private static double doubleValue(final String body, final String series) {
        for (final String line : body.split("\n")) {
            if (line.startsWith(series + " ")) {
                return Double.parseDouble(line.substring(series.length() + 1).trim());
            }
        }
        throw new AssertionError("series not found: " + series + "\n" + body);
    }

    private void deposit(final AeronCluster client, final long correlationId) {
        depositEnc.wrapAndApplyHeader(ingress, 0, headerEnc)
                .correlationId(correlationId).userId(USER).assetId(Asset.USD.id())
                .amount(FixedPoint.fromDouble(1.0)).requestIdHigh(0).requestIdLow(0);
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + depositEnc.encodedLength();
        final long deadline = System.currentTimeMillis() + 10_000;
        final BackoffIdleStrategy idle = new BackoffIdleStrategy();
        while (System.currentTimeMillis() < deadline) {
            if (client.offer(ingress, 0, length) > 0) {
                return;
            }
            client.pollEgress();
            idle.idle();
        }
        throw new IllegalStateException("failed to offer ingress within timeout");
    }
}
