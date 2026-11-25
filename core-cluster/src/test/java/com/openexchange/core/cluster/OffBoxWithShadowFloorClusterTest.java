// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.cluster.OffBoxWatermark;
import com.openexchange.cluster.shadow.ProducerKind;
import com.openexchange.cluster.testkit.RecordedLog;
import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.Who;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.Written;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The off-box bound next to the shadow floor ({@code SNAPSHOT_MODE=shadow}): the purge goes to the lowest of this
 * member's shadow floor, the retention floor and the off-box position, on the leader (whose shadow floor is the
 * remote floor: what the followers' archives hold) and on a follower. The shadow floor stays the pruner's
 * watermark: the member starts, which the runtime check {@code watermarkIs(floor)} would refuse otherwise.
 *
 * <p>The set-up is {@code LeaderRemoteFloorClusterTest}'s: three members, two snapshots, two producers' ALIGNED
 * shadows at each on a follower's archive, so the leader's remote floor becomes the first snapshot's position.
 * Member 0 is the appointed leader.</p>
 */
public class OffBoxWithShadowFloorClusterTest {

    private static final long PERIOD_MS = 1000;
    private static final long GENESIS = ShadowClusterRig.GENESIS;
    private static final int SEGMENT = StubStory.SEGMENT;
    /** A position inside the first segment: a member bounded by it purges nothing. */
    private static final long X_LOW = 1_000;
    private static int nextProducerId = 900;

    private static String sources(final ShadowClusterRig rig, final Member m) {
        final List<Member> others = new ArrayList<>(rig.members);
        others.remove(m);
        return rig.sources(others.toArray(new Member[0]));
    }

    private static void story(final int portBase, final String name, final OffBoxWatermark.Mode leaderMode)
            throws Exception {
        System.setProperty("retention.recheck.max.ms", "1000");
        final long[] allowed = new long[ShadowClusterRig.MEMBERS];
        try (ShadowClusterRig rig = new ShadowClusterRig(portBase, name, 3, "64k", SEGMENT)) {
            for (final Member m : rig.members) {
                m.settings = ShadowClusterRig.shadowSettings("SHADOW_PULL_SOURCES", sources(rig, m),
                        "SHADOW_PULL_PERIOD_MS", Long.toString(PERIOD_MS), "SHADOW_SCAN_PERIOD_MS", "1000",
                        "SHADOW_DEEP_READ_PACE_MS", "0");
                final OffBoxWatermark.Mode mode = m.id == 0 ? leaderMode : OffBoxWatermark.Mode.REQUIRED;
                m.beforeLaunch = service -> ServiceProbe.offBoxMode(service, mode);
                m.afterConfig = mm -> mm.cfg.consensusModuleContext().appointedLeaderId(0);
            }
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            assertEquals("the appointed leader", 0, leader.id);
            final Member f = rig.members.get(1);
            final Member g = rig.members.get(2);
            rig.adopt(rig.connect(SessionClass.ADMIN, leader));
            final Client tool = rig.connect(SessionClass.TOOL, leader);
            tool.deposits(600);
            final long p1 = rig.snapshotAll();
            tool.deposits(600);
            final long p2 = rig.snapshotAll();
            tool.deposits(40);
            rig.awaitImagesEqual(f, leader, 60_000);
            assertTrue("the story needs a whole segment below the first snapshot: P1=" + p1, p1 > SEGMENT);
            for (final Member m : rig.members) {
                assertTrue("member " + m.id + " started: its pruner's watermark is the shadow floor (halts "
                        + m.halts + ")", m.halts.isEmpty());
                rig.await(() -> ServiceProbe.offBox(m.service) != null, 30_000, "member " + m.id + "'s pruner");
                rig.await(() -> ServiceProbe.puller(m.service) != null, 30_000, "member " + m.id + "'s puller");
            }

            // every member's watermark is inside the first segment: nobody may purge anything
            for (final Member m : rig.members) {
                OffBoxTool.set(m.clusterDir(), GENESIS, X_LOW);
                allowed[m.id] = X_LOW;
            }
            rig.duringAwait.add(() -> {
                for (final Member m : rig.live()) {
                    final long start = rig.logStart(m);
                    if (start > allowed[m.id]) {
                        throw new AssertionError("member " + m.id + ": log start " + start
                                + " passed the off-box position " + allowed[m.id]);
                    }
                }
            });

            // two producers' ALIGNED shadows at both snapshots on a follower: the followers attest, and the
            // leader's shadow floor (the remote floor) becomes P1
            final RecordedLog log = rig.captureCommitted(f);
            for (int producer = 0; producer < 2; producer++) {
                final Who who = new Who(ProducerKind.FOLLOWER, nextProducerId++);
                for (final long p : new long[] {p1, p2}) {
                    final Written w = rig.producer(f).produce(who, log, p);
                    assertTrue(w.toString(), w.ok());
                }
            }
            rig.await(() -> ServiceProbe.housekeeping(leader.service, "shadow_leader_remote_floor_position") == p1,
                    120_000, () -> "the leader's remote floor at P1=" + p1 + ": "
                            + ServiceProbe.housekeeping(leader.service, "shadow_leader_remote_floor_position"));

            // the leader: its shadow floor allows P1, its off-box position does not
            rig.pumpMs(10_000);
            assertEquals("the leader's shadow floor says P1 and its off-box position holds it at 0", 0,
                    rig.logStart(leader));
            final OffBoxWatermark leaderOffBox = ServiceProbe.offBox(leader.service);
            assertNotNull(leaderOffBox);
            assertEquals(OffBoxWatermark.State.VALID, leaderOffBox.state());
            assertTrue("the held bytes are counted: " + leaderOffBox.heldBytes(), leaderOffBox.heldBytes() > 0);

            // X rises on the leader to a position below P1: the purge follows X, not the shadow floor
            final long x = SEGMENT + 500;
            assertTrue(x < p1);
            OffBoxTool.set(leader.clusterDir(), GENESIS, x);
            allowed[leader.id] = x;
            rig.await(() -> rig.logStart(leader) == SEGMENT, 90_000, () -> "the leader purges behind X=" + x
                    + " (shadow floor P1=" + p1 + "): log start " + rig.logStart(leader));
            // X above the shadow floor: now the shadow floor is the lowest and the purge stops at it
            OffBoxTool.set(leader.clusterDir(), GENESIS, p2);
            allowed[leader.id] = p1;
            rig.await(() -> rig.logStart(leader) == p1 - p1 % SEGMENT, 90_000, () -> "the leader purges behind its"
                    + " shadow floor P1=" + p1 + " with X=" + p2 + ": log start " + rig.logStart(leader));

            // a follower: the lowest of its own shadow floor, the leader's log start and its off-box position
            rig.pumpMs(8_000);
            assertEquals("the follower's off-box position holds it at 0 while the leader moved", 0, rig.logStart(g));
            OffBoxTool.set(g.clusterDir(), GENESIS, p2);
            allowed[g.id] = p2;
            rig.pumpMs(12_000);
            final long shadowFloor = ServiceProbe.housekeeping(g.service, "shadow_floor_position");
            final long lowest = Math.min(Math.max(0, shadowFloor), Math.min(rig.logStart(leader), p2));
            assertEquals("the follower purged to the lowest of its shadow floor (" + shadowFloor + "), the leader's"
                    + " log start (" + rig.logStart(leader) + ") and its off-box position (" + p2 + ")",
                    lowest - lowest % SEGMENT, rig.logStart(g));
            System.out.println("GREEN off-box with the shadow floor: leader log start " + rig.logStart(leader)
                    + " (P1=" + p1 + "), follower " + g.id + " log start " + rig.logStart(g) + " (shadow floor "
                    + shadowFloor + ")");
        } finally {
            System.clearProperty("retention.recheck.max.ms");
        }
    }

    @Test(timeout = 600_000)
    public void thePurgeTakesTheLowestOfTheShadowFloorTheRetentionFloorAndTheOffBoxPosition() throws Exception {
        story(57000, "offbox-shadow-floor", OffBoxWatermark.Mode.REQUIRED);
    }

    /** The control: the off-box position is not applied on the leader's remote-floor path, and it purges past it. */
    @Test(timeout = 600_000)
    public void controlTheOffBoxPositionIgnoredOnTheLeadersRemoteFloorPath() throws Exception {
        try {
            story(57300, "offbox-shadow-floor-control", OffBoxWatermark.Mode.REPORT);
        } catch (final AssertionError expected) {
            assertTrue(String.valueOf(expected.getMessage()), expected.getMessage() != null
                    && expected.getMessage().startsWith("member 0: log start ")
                    && expected.getMessage().contains("passed the off-box position " + X_LOW));
            System.out.println("NC off-box ignored on the leader's remote-floor path -> " + expected.getMessage());
            return;
        }
        fail("negative control did not fail: the rule cannot see a leader that ignores its off-box position");
    }
}
