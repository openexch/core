// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.Who;
import com.openexchange.core.infrastructure.persistence.TestShadowProducer.Written;
import com.openexchange.cluster.shadow.ProducerKind;
import com.openexchange.cluster.testkit.RecordedLog;
import io.aeron.cluster.service.Cluster;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * A stable leader in {@code shadow} mode reclaims its log, and what it purged is never needed from its own disk
 * again. Three members, every member lists the other two archives ({@code SHADOW_PULL_SOURCES}), leadership does not
 * move until the test kills the leader:
 *
 * <ol>
 *   <li>Two snapshots, ONE producer's ALIGNED at each on a follower's archive (pulled by the other follower): no
 *       member attests, and the leader's log start stays 0 for several periods (one producer never anchors the
 *       leader either).</li>
 *   <li>A second producer's ALIGNED arrives at both positions (later): the followers attest
 *       ({@code shadow_attested_anchor_position}), the leader's listing sees it, and the leader's log start moves
 *       past 0 while it still leads, with {@code shadow_pull_total 0} and {@code shadow_pull_started_total 0} on it
 *       (D15: it listed, it never pulled) and no shadow recording in its archive.</li>
 *   <li>The leader is killed and restarted with a source that answers nothing: it halts with the 7.3 text (a stub, a
 *       log that starts above 0, no shadow, virtual genesis refused). It never replays the purged log.</li>
 *   <li>Restarted with its real sources it pulls a shadow first (rule 3b), restores from it, rejoins as a follower
 *       and its image equals the new leader's.</li>
 * </ol>
 *
 * <p>Three members, two kills, about 35 s on the dev box: it runs on the PR gate.</p>
 */
public class LeaderRemoteFloorClusterTest {

    private static final int PORT_BASE = 53000;
    private static final long PERIOD_MS = 1000;
    private static int nextProducerId = 800;

    private static String settingsText(final ShadowClusterRig rig, final Member m) {
        final List<Member> others = new ArrayList<>(rig.members);
        others.remove(m);
        return rig.sources(others.toArray(new Member[0]));
    }

    @Test
    public void aStableLeaderPurgesBehindRemoteShadowsAndRestartsByPullingOne() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE, "leader-remote-floor", 3, "64k",
                StubStory.SEGMENT)) {
            for (final Member m : rig.members) {
                m.settings = ShadowClusterRig.shadowSettings("SHADOW_PULL_SOURCES", settingsText(rig, m),
                        "SHADOW_PULL_PERIOD_MS", Long.toString(PERIOD_MS), "SHADOW_SCAN_PERIOD_MS", "1000",
                        "SHADOW_DEEP_READ_PACE_MS", "0");
            }
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            final Member f = rig.followerOtherThan(leader, null);
            final Member g = rig.followerOtherThan(leader, f);
            final Client admin = rig.connect(SessionClass.ADMIN, leader);
            rig.adopt(admin);
            final Client tool = rig.connect(SessionClass.TOOL, leader);
            tool.deposits(600);
            final long p1 = rig.snapshotAll();
            tool.deposits(600);
            final long p2 = rig.snapshotAll();
            tool.deposits(40);
            rig.awaitImagesEqual(f, leader, 60_000);
            assertTrue("the story needs a whole segment below the first snapshot: P1=" + p1, p1 > StubStory.SEGMENT);
            for (final Member m : rig.live()) {
                rig.await(() -> ServiceProbe.puller(m.service) != null, 30_000, "member " + m.id + "'s puller");
            }

            // 1. one producer at both positions: pulled by the other follower, attested by nobody
            final RecordedLog log = rig.captureCommitted(f);
            final Who first = new Who(ProducerKind.FOLLOWER, nextProducerId++);
            final Who second = new Who(ProducerKind.FOLLOWER, nextProducerId++);
            int produced = 0;
            for (final long p : new long[] {p1, p2}) {
                final Written w = rig.producer(f).produce(first, log, p);
                assertTrue(w.toString(), w.ok());
                // one at a time: a steady-state pull takes only positions above the newest VERIFIED local one
                final int want = ++produced;
                rig.await(() -> ServiceProbe.pull(g.service, "shadow_pull_total") >= want, 60_000,
                        () -> "member " + g.id + " pulls the shadow at " + p + ": "
                                + ServiceProbe.pull(g.service, "shadow_pull_total"));
            }
            rig.pumpMs(8 * PERIOD_MS);
            for (final Member m : new Member[] {f, g}) {
                assertEquals("member " + m.id + " attests nothing behind one producer", -1,
                        ServiceProbe.housekeeping(m.service, "shadow_attested_anchor_position"));
            }
            assertEquals("the leader's log stays whole behind one remote producer", 0, rig.logStart(leader));
            assertEquals(0, ServiceProbe.housekeeping(leader.service, "shadow_leader_remote_floor_applied_total"));
            assertTrue("the leader listed the followers' archives", ServiceProbe.housekeeping(leader.service,
                    "shadow_leader_remote_listings_total") >= 1);

            // 2. the second producer, seconds later: two witnesses on the followers, the leader purges behind them
            for (final long p : new long[] {p1, p2}) {
                final Written w = rig.producer(f).produce(second, log, p);
                assertTrue(w.toString(), w.ok());
            }
            for (final Member m : new Member[] {f, g}) {
                rig.await(() -> ServiceProbe.housekeeping(m.service, "shadow_attested_anchor_position") == p2, 90_000,
                        () -> "member " + m.id + " attests the two-producer anchor at " + p2 + ": "
                                + ServiceProbe.housekeeping(m.service, "shadow_attested_anchor_position")
                                + ", witnesses " + ServiceProbe.housekeeping(m.service, "shadow_anchor_witnesses"));
            }
            rig.await(() -> rig.logStart(leader) > 0, 90_000, () -> "the leader's log start never moved past 0: "
                    + "remote floor " + ServiceProbe.housekeeping(leader.service, "shadow_leader_remote_floor_position")
                    + ", holders " + ServiceProbe.housekeeping(leader.service, "shadow_leader_remote_holders")
                    + ", floor " + ServiceProbe.housekeeping(leader.service, "shadow_floor_position"));
            final long start = rig.logStart(leader);
            assertEquals("leadership was stable", Cluster.Role.LEADER, leader.role());
            assertTrue("the leader purged behind the margin shadow at P1=" + p1 + ", not past it: " + start,
                    start <= p1);
            assertEquals(p1, ServiceProbe.housekeeping(leader.service, "shadow_leader_remote_floor_position"));
            assertTrue(ServiceProbe.housekeeping(leader.service, "shadow_leader_remote_floor_applied_total") >= 1);
            // D15 stands: it listed, it never pulled, and its archive holds no shadow
            if (ServiceProbe.pull(leader.service, "shadow_pull_total") != 0
                    || ServiceProbe.pull(leader.service, "shadow_pull_started_total") != 0) {
                throw new AssertionError("the leader pulled: shadow_pull_total="
                        + ServiceProbe.pull(leader.service, "shadow_pull_total") + " while LEADER");
            }
            assertTrue("the leader holds no shadow: " + ShadowPullerClusterTest.shadowRecordings(rig, leader),
                    ShadowPullerClusterTest.shadowRecordings(rig, leader).isEmpty());
            System.out.println("GREEN leader remote floor: member " + leader.id + " led throughout, log start " + start
                    + " (P1=" + p1 + ", P2=" + p2 + "), shadow_pull_total=0");

            // 3. the old leader comes back with a source that answers nothing: it halts, it never replays its log
            rig.forgetAll();
            rig.kill(leader);
            final Member next = rig.awaitLeader(leader);
            final com.openexchange.core.infrastructure.persistence.SnapshotModeSettings real = leader.settings;
            leader.settings = ShadowClusterRig.shadowSettings("SHADOW_PULL_SOURCES", "localhost:" + (PORT_BASE + 990),
                    "SHADOW_PULL_PERIOD_MS", Long.toString(PERIOD_MS), "SHADOW_PULL_STARTUP_WAIT_MS", "5000");
            rig.tee.clear();
            rig.errTee.clear();
            rig.restart(leader);
            rig.await(() -> !leader.halts.isEmpty()
                            && StubStory.line(rig.errTee.lines, "FATAL shadow restore: stub at P_cm=") != null, 120_000,
                    () -> "the old leader must halt without a source; halts " + leader.halts + " stderr "
                            + rig.errTee.lines);
            final String halt = StubStory.line(rig.errTee.lines, "FATAL shadow restore: stub at P_cm=");
            assertTrue(halt, halt.contains("log starts at " + start + " > 0"));
            assertTrue(halt, halt.contains("virtual genesis refused: log recording starts at " + start));
            assertTrue(halt, halt.contains("(3) pull: tried 1 sources, 0 candidates"));
            System.out.println("GREEN old leader without a source -> " + halt.trim());

            // 4. with its sources it pulls first, restores from the pulled shadow and rejoins
            rig.kill(leader);
            leader.settings = real;
            rig.tee.clear();
            rig.errTee.clear();
            rig.restart(leader);
            rig.await(() -> !leader.killed && leader.role() == Cluster.Role.FOLLOWER && leader.halts.size() <= 1
                            && StubStory.line(rig.tee.lines, "[shadow restore] 3b pulled shadow") != null, 120_000,
                    () -> "the old leader restores from a pulled shadow and rejoins; halts " + leader.halts
                            + " stdout " + rig.tee.lines + " stderr " + rig.errTee.lines);
            final String pulled = StubStory.line(rig.tee.lines, "[shadow restore] 3b pulled shadow");
            assertNotNull(pulled);
            assertTrue("the startup pull is counted", ServiceProbe.pull(leader.service, "shadow_pull_total") >= 1);
            assertEquals("no virtual genesis on a purged log", 0,
                    ServiceProbe.rcs(leader.service, "core_virtual_genesis_total"));
            assertEquals("the purged log stays purged", start, rig.logStart(leader));
            rig.awaitImagesEqual(leader, next, 90_000);
            System.out.println("GREEN old leader restart: " + pulled.trim() + "; image equal to the new leader "
                    + next.id + "'s");
        }
    }
}
