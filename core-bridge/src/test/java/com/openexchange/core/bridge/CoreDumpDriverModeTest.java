// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.bridge;

import com.openexchange.core.domain.Asset;
import com.openexchange.core.domain.FixedPoint;
import com.openexchange.core.infrastructure.generated.BalanceUpdateBatchDecoder;
import com.openexchange.core.infrastructure.generated.BalanceUpdateDecoder;
import com.openexchange.core.infrastructure.generated.DepositEncoder;
import com.openexchange.core.infrastructure.generated.HoldAckBatchDecoder;
import com.openexchange.core.infrastructure.generated.HoldAckDecoder;
import com.openexchange.core.infrastructure.generated.HoldEncoder;
import com.openexchange.core.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.core.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.ClusterConfig;
import com.openexchange.core.infrastructure.persistence.CoreClusteredService;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * CoreDump against a real single-node core cluster in both driver modes. The cluster's own media driver stands in
 * for the box's aeronmd: the external run attaches to it through its directory, the embedded run launches the
 * tool's own driver. Both must print the same snapshot; the embedded run must leave no directory behind, and the
 * external run must start no driver at all.
 *
 * <p>The external run's driver launcher fails the test the moment it is called, the same as a box with no tmpfs
 * for the tool. The control ({@link #externalCheckCatchesAnIgnoredSwitch}) proves that check bites: a build that
 * ignores the switch (always embedded) fails with {@link #EMBEDDED_IN_EXTERNAL}. Running the suite with
 * {@code -Dcoredump.test.ignoreSwitch=true} puts that build under the external test itself, to watch it fail.</p>
 */
public class CoreDumpDriverModeTest {

    static final String EMBEDDED_IN_EXTERNAL = "CoreDump started an embedded driver in external mode";

    private static final String TOOL_SECRET = "44".repeat(32);
    private static final int CORE_PORT_BASE = 24600;
    private static final long USER_A = 700001L;
    private static final long USER_B = 700002L;
    private static final long ORDER = 31L;

    /** The driver choice as the tool takes it; the ignored-switch build replaces it with embedded. */
    private static final UnaryOperator<CoreDumpDriver> REAL_BUILD = d -> d;
    private static final UnaryOperator<CoreDumpDriver> SWITCH_IGNORED = d -> CoreDumpDriver.parse("embedded", null);

    static {
        // CoreDump reads its key like any tool: here from the system property, a TOOL key.
        System.setProperty(com.openexchange.core.auth.CoreCredentials.PROPERTY, "2:TOOL:" + TOOL_SECRET);
    }

    private static File tmp;
    private static String clusterDriverDir;
    private static ClusteredMediaDriver coreDriver;
    private static ClusteredServiceContainer coreContainer;

    @BeforeClass
    public static void startClusterAndFund() {
        tmp = new File(System.getProperty("java.io.tmpdir"), "coredump-driver-" + CORE_PORT_BASE);
        IoUtil.delete(tmp, true);
        System.setProperty(com.openexchange.core.auth.CoreAuth.POLICY.keysProperty(),
                "2:TOOL:" + TOOL_SECRET + "::users=*");

        final ClusterConfig cfg = ClusterConfig.create(0, List.of("localhost"), CORE_PORT_BASE,
                new CoreClusteredService());
        cfg.baseDir(new File(tmp, "node0"));
        clusterDriverDir = new File(tmp, "core-driver").getAbsolutePath();
        cfg.aeronDirectoryName(clusterDriverDir);
        cfg.idleStrategySupplier(BackoffIdleStrategy::new);
        cfg.errorHandler(Throwable::printStackTrace);
        cfg.consensusModuleContext()
                .ingressChannel("aeron:udp?term-length=4m|mtu=8192")
                .sessionTimeoutNs(TimeUnit.SECONDS.toNanos(5))
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2));
        coreDriver = ClusteredMediaDriver.launch(
                cfg.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true),
                cfg.archiveContext(),
                cfg.consensusModuleContext());
        coreContainer = ClusteredServiceContainer.launch(cfg.clusteredServiceContext());

        try (FundingClient funding = new FundingClient(clusterDriverDir)) {
            funding.deposit(USER_A, Asset.BTC.id(), FixedPoint.fromDouble(1.5));
            funding.deposit(USER_B, Asset.USD.id(), FixedPoint.fromDouble(50000.0));
            funding.awaitAtLeast(funding.balanceUpdates, 2, "deposit balance updates");
            funding.hold(ORDER, USER_B, Asset.USD.id(), FixedPoint.fromDouble(1000.0));
            funding.awaitAtLeast(funding.holdAcks, 1, "hold ack");
        }
    }

    @AfterClass
    public static void stopCluster() {
        CloseHelper.quietCloseAll(coreContainer, coreDriver);
        IoUtil.delete(tmp, true);
    }

    private static BridgeConfig dumpConfig() {
        return new BridgeConfig(List.of(), List.of("localhost"), CORE_PORT_BASE, "localhost:0",
                "localhost", true, 10_000, 0);
    }

    private static String dump(final CoreDumpDriver driver, final Function<MediaDriver.Context, MediaDriver> launcher) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final int exit = CoreDump.run(driver, dumpConfig(), launcher, new PrintStream(out, true, StandardCharsets.UTF_8));
        assertEquals("CoreDump exit code", 0, exit);
        return out.toString(StandardCharsets.UTF_8).trim();
    }

    /** The embedded run: the real launcher, recording the directory it was given. */
    private static String dumpEmbedded(final AtomicReference<String> launchedDir) {
        return dump(CoreDumpDriver.parse("embedded", null), ctx -> {
            launchedDir.set(ctx.aeronDirectoryName());
            return MediaDriver.launch(ctx);
        });
    }

    /** The external run: no tmpfs for the tool, so any launch fails the test. */
    private static String dumpExternal(final UnaryOperator<CoreDumpDriver> build) {
        return dump(build.apply(CoreDumpDriver.parse("external", clusterDriverDir)), ctx -> {
            throw new AssertionError(EMBEDDED_IN_EXTERNAL);
        });
    }

    @Test
    public void embeddedAndExternalPrintTheSameSnapshot() {
        final AtomicReference<String> launchedDir = new AtomicReference<>();
        final String embedded = dumpEmbedded(launchedDir);

        assertNotNull("embedded mode launches its own driver", launchedDir.get());
        assertTrue(launchedDir.get(), launchedDir.get().startsWith("/dev/shm/aeron-aedump-"));
        assertFalse("embedded run leaves no directory in /dev/shm: " + launchedDir.get(),
                new File(launchedDir.get()).exists());

        final boolean ignoreSwitch = Boolean.getBoolean("coredump.test.ignoreSwitch");
        final String external = dumpExternal(ignoreSwitch ? SWITCH_IGNORED : REAL_BUILD);

        assertTrue(embedded, embedded.contains("{\"userId\":" + USER_A + ",\"assetId\":" + Asset.BTC.id()));
        assertTrue(embedded, embedded.contains("{\"orderId\":" + ORDER + ",\"userId\":" + USER_B));
        assertEquals("the same snapshot in both driver modes", embedded, external);
    }

    @Test
    public void externalCheckCatchesAnIgnoredSwitch() {
        final AssertionError e = assertThrows(AssertionError.class, () -> dumpExternal(SWITCH_IGNORED));
        assertEquals(EMBEDDED_IN_EXTERNAL, e.getMessage());
    }

    /** Funds the ledger through the cluster's own driver with the TOOL key, as the bridge end-to-end test does. */
    private static final class FundingClient implements AutoCloseable {
        final AtomicInteger balanceUpdates = new AtomicInteger();
        final AtomicInteger holdAcks = new AtomicInteger();
        private final AeronCluster cluster;
        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final BalanceUpdateBatchDecoder balanceBatch = new BalanceUpdateBatchDecoder();
        private final HoldAckBatchDecoder holdAckBatch = new HoldAckBatchDecoder();
        private final MessageHeaderEncoder headerEnc = new MessageHeaderEncoder();
        private final DepositEncoder depositEnc = new DepositEncoder();
        private final HoldEncoder holdEnc = new HoldEncoder();
        private final UnsafeBuffer buf = new UnsafeBuffer(new byte[128]);

        FundingClient(final String aeronDir) {
            cluster = AeronCluster.connect(new AeronCluster.Context()
                    .credentialsSupplier(new com.openexchange.core.auth.CoreCredentials(SessionClass.TOOL, 2,
                            HexFormat.of().parseHex(TOOL_SECRET)).supplier())
                    .aeronDirectoryName(aeronDir)
                    .ingressChannel("aeron:udp?term-length=4m|mtu=8192")
                    .ingressEndpoints("0=localhost:" + (CORE_PORT_BASE + 2))
                    .egressChannel("aeron:udp?endpoint=localhost:0|term-length=4m")
                    .egressListener(this::onMessage));
        }

        private void onMessage(final long sessionId, final long timestamp, final DirectBuffer buffer,
                               final int offset, final int length, final Header h) {
            header.wrap(buffer, offset);
            switch (header.templateId()) {
                case BalanceUpdateDecoder.TEMPLATE_ID:
                    balanceUpdates.incrementAndGet();
                    break;
                case BalanceUpdateBatchDecoder.TEMPLATE_ID:
                    balanceBatch.wrapAndApplyHeader(buffer, offset, header);
                    balanceUpdates.addAndGet(balanceBatch.updates().count());
                    break;
                case HoldAckDecoder.TEMPLATE_ID:
                    holdAcks.incrementAndGet();
                    break;
                case HoldAckBatchDecoder.TEMPLATE_ID:
                    holdAckBatch.wrapAndApplyHeader(buffer, offset, header);
                    holdAcks.addAndGet(holdAckBatch.acks().count());
                    break;
                default:
                    break;
            }
        }

        void deposit(final long userId, final int assetId, final long amount) {
            depositEnc.wrapAndApplyHeader(buf, 0, headerEnc)
                    .correlationId(0).userId(userId).assetId(assetId).amount(amount).requestIdHigh(0).requestIdLow(0);
            offer(MessageHeaderEncoder.ENCODED_LENGTH + depositEnc.encodedLength());
        }

        void hold(final long orderId, final long userId, final int assetId, final long amount) {
            holdEnc.wrapAndApplyHeader(buf, 0, headerEnc)
                    .correlationId(0).orderId(orderId).userId(userId).assetId(assetId).amount(amount);
            offer(MessageHeaderEncoder.ENCODED_LENGTH + holdEnc.encodedLength());
        }

        private void offer(final int length) {
            final long deadline = System.currentTimeMillis() + 10_000;
            while (cluster.offer(buf, 0, length) < 0) {
                assertTrue("funding offer timed out", System.currentTimeMillis() < deadline);
                cluster.pollEgress();
                cluster.sendKeepAlive();
                Thread.onSpinWait();
            }
        }

        void awaitAtLeast(final AtomicInteger counter, final int expected, final String what) {
            final long deadline = System.currentTimeMillis() + 15_000;
            while (counter.get() < expected) {
                assertTrue("timed out waiting for " + what + ", have " + counter.get(),
                        System.currentTimeMillis() < deadline);
                cluster.pollEgress();
                cluster.sendKeepAlive();
                try {
                    Thread.sleep(5);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
        }

        @Override
        public void close() {
            CloseHelper.quietClose(cluster);
        }
    }
}
