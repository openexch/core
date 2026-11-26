// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.cluster.EngineLifecycle;
import io.aeron.logbuffer.Header;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * S4 T11 (core): the two fail-fast seams in a real forked JVM. A recorded behaviour this jar does not run is a stop a
 * restart cannot fix: the node halts with 78 ({@code EngineLifecycle.EXIT_CONFIG}) after the unchanged CRITICAL line.
 * Every other stop keeps its 1 (an application fault after a mutation, {@code CoreApplyFailureFenceTest}'s path).
 *
 * <p>Negative control: {@code -Ds4.control=both1} wires the incompatible seam to the exit-1 halt in the child; the
 * first assertion ({@code aBehaviourTheJarLacksHalts78}) then fails on the exit status.</p>
 */
public class IncompatibleConfigExitSeamTest {

    private static final String CONTROL = System.getProperty("s4.control", "");

    private static Process child(final String mode, final Path log) throws Exception {
        final List<String> cmd = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "-cp", System.getProperty("java.class.path"),
                IncompatibleConfigExitSeamTest.class.getName(), mode, CONTROL));
        return new ProcessBuilder(cmd).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    }

    private static int run(final String mode, final StringBuilder out) throws Exception {
        final Path dir = Files.createTempDirectory("core-s4-exit-seam-");
        final Path log = dir.resolve("child.log");
        final Process p = child(mode, log);
        try {
            assertTrue("child must exit promptly", p.waitFor(30, TimeUnit.SECONDS));
            out.append(Files.readString(log));
            return p.exitValue();
        } finally {
            if (p.isAlive()) {
                p.destroyForcibly();
                p.waitFor(5, TimeUnit.SECONDS);
            }
            System.out.print(Files.readString(log));
            Files.deleteIfExists(log);
            Files.deleteIfExists(dir);
        }
    }

    @Test(timeout = 120_000)
    public void aBehaviourTheJarLacksHalts78() throws Exception {
        final StringBuilder log = new StringBuilder();
        final int exit = run("incompatible", log);
        assertEquals(log.toString(), EngineLifecycle.EXIT_CONFIG, exit);
        assertTrue(log.toString(), log.toString().contains("CORE-CONFIG CRITICAL (ADOPT): this node differs from the"
                + " cluster's declared config: behaviorVersion 1 is not supported by this jar [2, 3, 4]"));
    }

    @Test(timeout = 120_000)
    public void anApplicationFaultStillHalts1() throws Exception {
        final StringBuilder log = new StringBuilder();
        assertEquals(log.toString(), EngineLifecycle.EXIT_FATAL, run("fault", log));
        assertTrue(log.toString(), log.toString().contains("FATAL command application failure"));
    }

    @Test(timeout = 120_000)
    public void aMarketMismatchKeepsExit1() throws Exception {
        // the cross-check part a restart could fix by another config (compiled tables) keeps the existing seam
        final StringBuilder log = new StringBuilder();
        assertEquals(log.toString(), EngineLifecycle.EXIT_FATAL, run("markets", log));
        assertTrue(log.toString(), log.toString().contains("CORE-CONFIG CRITICAL (ADOPT)"));
    }

    private static void admin(final CoreClusteredService service) {
        service.engine().sessions().open(-1L, com.openexchange.cluster.auth.ClusterPrincipal.anyUser(
                com.openexchange.core.auth.CoreSessionClasses.toKit(
                        com.openexchange.core.infrastructure.generated.SessionClass.ADMIN), 0).encode());
    }

    private static void dispatch(final CoreClusteredService service, final byte[] frame) {
        final Header header = new Header(0, 16);
        header.buffer(new UnsafeBuffer(new byte[64]));
        final UnsafeBuffer b = new UnsafeBuffer(frame);
        service.onSessionMessage(null, 10, b, 0, frame.length, header);
    }

    public static void main(final String[] args) throws Exception {
        final String mode = args[0];
        final boolean both1 = args.length > 1 && "both1".equals(args[1]);
        final CoreClusteredService service = new CoreClusteredService();
        if (both1) {
            service.failFastIncompatible = CoreClusteredService::failFastHalt;
        }
        admin(service);
        switch (mode) {
            case "incompatible" -> {
                // the ADOPT records behaviour 1; this jar stands in for one that no longer runs it
                service.engine().coreConfig().setSupportedBehaviorVersions(2, 3, 4);
                dispatch(service, new WireVersionFrames().coreConfig(10, 0x70L, 1, 1, 10));
            }
            case "markets" -> {
                // a config this jar runs (behaviour and ceiling) whose market table differs from the compiled one
                final byte[] frame = new WireVersionFrames().coreConfig(10, 0x70L, 1, 1, 10);
                dispatch(service, marketsChanged(frame));
            }
            case "fault" -> {
                final java.lang.reflect.Method m = CoreApplyFailureFenceTest.class.getDeclaredMethod("main",
                        String[].class);
                final Path marker = Files.createTempFile("core-s4-hook", ".marker");
                Files.delete(marker);
                m.invoke(null, (Object)new String[] {marker.toString()});
            }
            default -> throw new IllegalArgumentException(mode);
        }
        // a seam that did not halt: exit 0 so the parent's assertion fails loudly
        System.out.println("[exit-seam] no halt");
        System.exit(0);
    }

    /** The config frame with its first market's symbol changed (the jar compares it with the compiled table). */
    private static byte[] marketsChanged(final byte[] frame) {
        final String symbol = com.openexchange.core.domain.Market.ALL[0].symbol();
        final byte[] needle = symbol.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        for (int i = 0; i + needle.length <= frame.length; i++) {
            boolean match = true;
            for (int j = 0; j < needle.length && match; j++) {
                match = frame[i + j] == needle[j];
            }
            if (match) {
                frame[i] = (byte)(frame[i] == 'Z' ? 'Y' : 'Z');
                return frame;
            }
        }
        throw new IllegalStateException("symbol " + symbol + " not found in the config frame");
    }
}
