// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * S4 T4 (core, embedded driver, in-process rig): a three-member roll from level N-1 (g1.1) to N (g1.2) by the R-B4
 * walk, with deposits between every step and right after every kill. Every member holds a snapshot taken at N-1 first;
 * followers roll one at a time ({@code /ready} 200 and {@code compat=g1.2} before the next); the leader is killed
 * mid-roll and comes back on N-1 until an N member leads N-1 followers, which serve; then the rest roll. No validator
 * refuses, no deposit is rejected, every {@code /ready} shows the expected {@code compat=} at each step, and every
 * member reports the same snapshot state hash at the end.
 */
public class MixedLevelRollClusterTest {

    private static final int N_MINUS_1 = 1;
    private static final int N = 2;

    /**
     * The rig's clients ride one member's media driver, which dies with that member: before {@code m} is stopped the
     * load moves to a fresh TOOL session on another live member (the old one's rejects are kept in {@code tools}).
     */
    private static void reconnectAvoiding(final ShadowClusterRig rig, final List<ShadowClusterRig.Client> tools,
                                          final ShadowClusterRig.Member m) throws InterruptedException {
        final ShadowClusterRig.Member host = rig.live().stream().filter(x -> x != m).findFirst().orElseThrow();
        final ShadowClusterRig.Client old = tools.get(0);
        assertEquals("zero rejected commands", 0, old.in.rejects);
        rig.forgetAll();
        final ShadowClusterRig.Client next = rig.connect(SessionClass.TOOL, host).withIdentity();
        next.corr = old.corr;
        tools.set(0, next);
    }

    @Test(timeout = 900_000)
    public void aRollFromNMinus1ToNUnderLoadWithTheLeaderKilledMidRoll() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(29400, "core-mixed-level-roll")) {
            final MemberLevels levels = new MemberLevels(rig, N_MINUS_1);
            rig.bootAll();
            for (final ShadowClusterRig.Member m : rig.members) {
                rig.awaitReady(m, 120_000);
                levels.awaitCompat(m, N_MINUS_1);
            }
            ShadowClusterRig.Member leader = rig.awaitLeader(null);
            final ShadowClusterRig.Client admin = rig.connect(SessionClass.ADMIN, leader);
            rig.adopt(admin);
            final List<ShadowClusterRig.Client> tools = new ArrayList<>();
            tools.add(rig.connect(SessionClass.TOOL, leader).withIdentity());
            tools.get(0).deposits(50);
            assertEquals("zero rejected admin commands", 0, admin.in.count(f -> f.contains("result=REJECTED")));

            // step 1: every member at N-1, ready, holding a snapshot taken at N-1
            rig.snapshotAll();

            // step 2: one follower to N
            final ShadowClusterRig.Member first = rig.followerOtherThan(leader, null);
            reconnectAvoiding(rig, tools, first);
            levels.reboot(first, N);
            levels.awaitCompat(first, N);
            tools.get(0).deposits(20);

            // the leader dies mid-roll and comes back on N-1, until a member at N leads followers at N-1
            for (int attempt = 0; attempt < 6 && levels.level.get(leader.id) != N; attempt++) {
                final ShadowClusterRig.Member killed = leader;
                reconnectAvoiding(rig, tools, killed);
                rig.kill(killed);
                tools.get(0).deposits(10); // across the election: the deposits carry identity and are re-offered
                rig.restart(killed);
                rig.awaitReady(killed, 120_000);
                levels.awaitCompat(killed, N_MINUS_1);
                leader = rig.awaitLeader(null);
                System.out.println("[mixed-level] after killing member " + killed.id + " the leader is member "
                        + leader.id + " at g1." + levels.level.get(leader.id) + "; roles " + rig.roles());
            }
            assertEquals("an N member leads (attempts exhausted otherwise)", N, (int)levels.level.get(leader.id));
            for (final ShadowClusterRig.Member m : rig.members) {
                if (m != leader) {
                    assertEquals("member " + m.id + " follows at N-1", N_MINUS_1, (int)levels.level.get(m.id));
                    levels.awaitCompat(m, N_MINUS_1);
                }
            }
            tools.get(0).deposits(30); // an N leader with N-1 followers serves

            // the rest roll, followers first; the leader is already at N
            for (final ShadowClusterRig.Member m : rig.members) {
                if (levels.level.get(m.id) != N) {
                    reconnectAvoiding(rig, tools, m);
                    levels.reboot(m, N);
                    levels.awaitCompat(m, N);
                    tools.get(0).deposits(10);
                }
            }

            // step 4: all at N, one snapshot, the same state on every member
            final long at = rig.snapshotAll();
            final List<String> hashes = new ArrayList<>();
            for (final ShadowClusterRig.Member m : rig.members) {
                levels.awaitCompat(m, N);
                final String body = ServiceProbe.nodeMetricsText(m.service);
                assertEquals("member " + m.id + " snapshot position", Long.toString(at),
                        ServiceProbe.seriesValue(body, "core_snapshot_last_log_position"));
                final String hash = ServiceProbe.seriesValue(body, "core_snapshot_state_hash");
                assertNotNull("member " + m.id + " state hash", hash);
                hashes.add(hash);
            }
            assertEquals("the same state hash on every member: " + hashes, 1, hashes.stream().distinct().count());
            assertEquals("no member version was refused: " + levels.refusals, 0, levels.refusals.size());
            for (final ShadowClusterRig.Client c : tools) {
                assertEquals("zero rejected commands", 0, c.in.rejects);
            }
            for (final ShadowClusterRig.Member m : rig.members) {
                assertTrue("no fail-fast on member " + m.id + ": " + m.halts, m.halts.isEmpty());
            }
            System.out.println("GREEN mixed-level roll: g1.1 -> g1.2, leader killed mid-roll, an N leader served N-1"
                    + " followers, snapshot " + at + " hash " + hashes.get(0) + " on all three");
        }
    }
}
