// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.replica;

import com.openexchange.cluster.BuildInfo;
import com.openexchange.cluster.EngineLifecycle;
import com.openexchange.cluster.MemberCompat;
import com.openexchange.cluster.log.ReplayContainer;
import com.openexchange.cluster.log.ReplayStoppedException;
import com.openexchange.cluster.log.ShadowTriggerListener;
import com.openexchange.cluster.replica.ReplicaEngine;
import com.openexchange.cluster.testkit.RecordedLog;
import com.openexchange.core.infrastructure.persistence.CoreMemberCompat;
import io.aeron.cluster.VersionValidator;
import org.agrona.SemanticVersion;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * S4 T7: the core replica on a member log stamped with the members' version ({@link CoreMemberCompat#MEMBER_COMPAT},
 * g1.1). A replica two levels ahead (g1.3) is refused at the first term event and its process exits 78 through the
 * same refusal the members use ({@code MemberCompat.fatalOnRefusal}); one level ahead (g1.2) it follows.
 *
 * <p>Negative control: {@code -Ds4.control=aeronDefault} gives the follow row a binding that returns Aeron's default
 * version and validator (what the binding returned before S4): it is refused at once on the g1 log and the assertion
 * names both versions.</p>
 */
public class ReplicaIncompatibleVersionTest {

    private static final String CONTROL = System.getProperty("s4.control", "");

    /** A g1.1 member log: one term event, then a timer the replica applies. */
    static RecordedLog memberLog() {
        return RecordedLog.builder().term(0, 1_000, CoreMemberCompat.MEMBER_COMPAT.appVersion()).timer(7, 1_001)
                .build();
    }

    @Test(timeout = 120_000)
    public void aReplicaTwoLevelsAheadExits78AtTheFirstTermEvent() throws Exception {
        final Path dir = Files.createTempDirectory("core-s4-replica-");
        final Path log = dir.resolve("child.log");
        final List<String> cmd = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java")
                .toString(), "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "-cp", System.getProperty("java.class.path"), ReplicaIncompatibleVersionTest.class.getName(), "3"));
        final Process p = new ProcessBuilder(cmd).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertTrue("the replica must exit: " + Files.readString(log), p.waitFor(60, TimeUnit.SECONDS));
            final String out = Files.readString(log);
            assertEquals(out, EngineLifecycle.EXIT_CONFIG, p.exitValue());
            assertTrue(out, out.contains("Core replica FATAL (exit 78): member version incompatible: this build is g1.3"
                    + " (build "));
            assertTrue(out, out.contains("the cluster's leader, log or snapshot is g1.1; a cluster runs level N and N-1"
                    + " of one generation; install a compatible build, a restart cannot fix this"));
            assertTrue("refused at the first term event: " + out, out.contains("[replica-child] stopped at"
                    + " INCOMPATIBLE_APP_VERSION after 0 frame(s)"));
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

    @Test
    public void aReplicaOneLevelAheadFollows() {
        final List<String> refusals = new ArrayList<>();
        ReplicaEngine binding = new CoreShadowReplicaMain.Binding(() -> { }, MemberCompat.of(1, 2),
                (own, other) -> refusals.add(own + "/" + other));
        if ("aeronDefault".equals(CONTROL)) {
            binding = aeronDefault(binding);
        }
        final ReplayContainer c = new ReplayContainer(binding.engine(), binding.appVersion(), binding.validator(),
                ShadowTriggerListener.NONE);
        c.genesis();
        try {
            assertEquals(2, memberLog().replay(-1, Long.MAX_VALUE, c));
        } catch (final ReplayStoppedException e) {
            throw new AssertionError("the replica at " + MemberCompat.fromAppVersion(binding.appVersion())
                    + " was refused on the " + CoreMemberCompat.MEMBER_COMPAT + " log: " + e.getMessage(), e);
        }
        assertEquals(List.of(), refusals);
    }

    /** The binding as it was before S4: Aeron's default version (0.0.1) and validator. */
    private static ReplicaEngine aeronDefault(final ReplicaEngine b) {
        return new ReplicaEngine() {
            public int engineId() { return b.engineId(); }
            public com.openexchange.cluster.log.ShadowEngine engine() { return b.engine(); }
            public com.openexchange.cluster.shadow.ShadowIdentitySource identity() { return b.identity(); }
            public com.openexchange.cluster.shadow.ShadowSelfCheck selfCheck() { return b.selfCheck(); }
            public int appVersion() { return SemanticVersion.compose(0, 0, 1); }
            public VersionValidator validator() {
                return io.aeron.cluster.AppVersionValidator.SEMANTIC_VERSIONING_VALIDATOR;
            }
            public String postRestoreWitness(final com.openexchange.cluster.shadow.ShadowHeader h) {
                return b.postRestoreWitness(h);
            }
            public long imageGenesisId(final org.agrona.DirectBuffer image, final int length) {
                return b.imageGenesisId(image, length);
            }
        };
    }

    /** Child: the production refusal on a real lifecycle, a binding at g1.{@code args[0]}, the g1.1 member log. */
    public static void main(final String[] args) {
        final EngineLifecycle lifecycle = EngineLifecycle.install("Core replica", System.out::println);
        final CoreShadowReplicaMain.Binding binding = new CoreShadowReplicaMain.Binding(() -> { },
                MemberCompat.of(1, Integer.parseInt(args[0])),
                MemberCompat.fatalOnRefusal(lifecycle, BuildInfo.of(CoreShadowReplicaMain.Binding.class)));
        final ReplayContainer c = new ReplayContainer(binding.engine(), binding.appVersion(), binding.validator(),
                ShadowTriggerListener.NONE);
        c.genesis();
        final int[] applied = {0};
        try {
            memberLog().replay(-1, Long.MAX_VALUE, new com.openexchange.cluster.log.ClusterLogListener() {
                public void onSessionMessage(long p, long s, long t, org.agrona.DirectBuffer b, int o, int l) {
                    c.onSessionMessage(p, s, t, b, o, l);
                    applied[0]++;
                }
                public void onTimerEvent(long p, long id, long t) {
                    c.onTimerEvent(p, id, t);
                    applied[0]++;
                }
                public void onSessionOpen(long term, long p, long s, long t, int rs, String rc, byte[] pr) {
                    c.onSessionOpen(term, p, s, t, rs, rc, pr);
                    applied[0]++;
                }
                public void onSessionClose(long term, long p, long s, long t, io.aeron.cluster.codecs.CloseReason r) {
                    c.onSessionClose(term, p, s, t, r);
                    applied[0]++;
                }
                public boolean onClusterAction(long term, long p, long t, io.aeron.cluster.codecs.ClusterAction a,
                                               int f) {
                    applied[0]++;
                    return c.onClusterAction(term, p, t, a, f);
                }
                public void onNewLeadershipTermEvent(long term, long p, long t, long base, int leader, int ls,
                                                     TimeUnit u, int av) {
                    c.onNewLeadershipTermEvent(term, p, t, base, leader, ls, u, av);
                    applied[0]++;
                }
            });
        } catch (final ReplayStoppedException stopped) {
            System.out.println("[replica-child] stopped at " + stopped.reason() + " after " + applied[0] + " frame(s)");
        }
        lifecycle.awaitStop();
        lifecycle.finish();
    }
}
