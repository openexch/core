// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@code core_snapshot_last_log_position} and {@code core_snapshot_state_hash} on a real three-member core cluster
 * (one JVM, full snapshots): the hash is the CRC32C of the snapshot payload, a pure function of the replicated state
 * at the snapshot's log position, so every healthy member reports the same pair after a snapshot. A member restarted
 * from that snapshot reports the same pair before any new snapshot is taken, and its snapshot age stays -1 (it took
 * none). At the next snapshot the restored member writes the same bytes as its peers, and so does an old leader
 * restarted from that second image.
 *
 * <p>The control proves the comparison can fail: with a seam that mixes the member id into the hash (a node-local
 * input), the same comparison must refuse the three pairs.</p>
 */
public class SnapshotStateHashClusterTest {

    private static final int PORT_BASE = 43800;
    private static final int CONTROL_PORT_BASE = 44100;
    private static final long READY_TIMEOUT_MS = 120_000;

    /** One member's snapshot identity as its {@code /metrics} renders it, both values from one scrape. */
    record Pair(int member, String position, String hash) {
        @Override
        public String toString() {
            return "member " + member + " position=" + position + " hash=" + hash;
        }
    }

    static Pair pairOf(final Member m) {
        final String body = ServiceProbe.nodeMetricsText(m.service);
        return new Pair(m.id, ServiceProbe.seriesValue(body, "core_snapshot_last_log_position"),
                ServiceProbe.seriesValue(body, "core_snapshot_state_hash"));
    }

    /**
     * Every pair names {@code expectedPosition} and one and the same hash, an unsigned 32-bit decimal. The assertion the
     * test and its control share.
     */
    static void assertSameIdentity(final List<Pair> pairs, final long expectedPosition) {
        final Pair first = pairs.get(0);
        for (final Pair p : pairs) {
            assertEquals("snapshot log position of " + p + " (all: " + pairs + ")",
                    Long.toString(expectedPosition), p.position());
            assertNotNull("core_snapshot_state_hash missing on " + p + " (all: " + pairs + ")", p.hash());
            assertTrue("the hash is an unsigned decimal: " + p, p.hash().matches("[0-9]{1,10}")
                    && Long.parseLong(p.hash()) <= 0xFFFF_FFFFL);
            assertEquals("state hash of " + p + " differs from " + first + " at the same log position",
                    first.hash(), p.hash());
        }
    }

    private static List<Pair> pairs(final ShadowClusterRig rig) {
        final List<Pair> out = new ArrayList<>();
        for (final Member m : rig.live()) {
            out.add(pairOf(m));
        }
        return out;
    }

    @Test
    public void everyMemberReportsTheSamePositionAndHashAndARestartedMemberReportsItBeforeAnyNewSnapshot()
            throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE, "core-snapshot-state-hash")) {
            for (final Member m : rig.members) {
                m.settings = StubStory.off();
            }
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            final Member f = rig.followerOtherThan(leader, null);
            final Member g = rig.followerOtherThan(leader, f);
            for (final Member m : rig.members) {
                assertEquals("before any snapshot the hash series is omitted on member " + m.id, null,
                        pairOf(m).hash());
                assertEquals("before any snapshot the position reads -1 on member " + m.id, "-1",
                        pairOf(m).position());
            }

            final Client admin = rig.connect(SessionClass.ADMIN, g);
            rig.adopt(admin);
            final Client tool = rig.connect(SessionClass.TOOL, g);
            tool.deposits(100);
            final long snapshotPosition = rig.snapshotAll();
            final List<Pair> taken = pairs(rig);
            System.out.println("[TEST] after the snapshot at " + snapshotPosition + ": " + taken);
            assertSameIdentity(taken, snapshotPosition);
            final String hash = taken.get(0).hash();

            // Log moves on past the snapshot; the restarted member replays it, and no new snapshot is taken.
            rig.kill(f);
            tool.deposits(20);
            rig.restart(f);
            rig.awaitReady(f, READY_TIMEOUT_MS);
            tool.deposits(5);
            final Pair restarted = pairOf(f);
            System.out.println("[TEST] restarted " + restarted);
            assertEquals("no new snapshot was taken", 1, rig.snapshots(leader).size());
            assertSameIdentity(List.of(restarted, pairOf(leader), pairOf(g)), snapshotPosition);
            assertEquals("the restarted member loaded the image every member wrote", hash, restarted.hash());
            assertEquals("the restarted member took no snapshot", "-1",
                    ServiceProbe.seriesValue(ServiceProbe.nodeMetricsText(f.service), "core_snapshot_age_seconds"));
            assertTrue(f.halts.toString(), f.halts.isEmpty());

            // The restored member writes the same bytes as its peers at the next snapshot: a difference that only a
            // restore introduces would show here as a false "diverged".
            final long secondPosition = rig.snapshotAll();
            final List<Pair> second = pairs(rig);
            System.out.println("[TEST] after the snapshot at " + secondPosition + " (member " + f.id
                    + " restored from the first): " + second);
            assertSameIdentity(second, secondPosition);
            assertNotEquals("the state moved, so the hash moved", hash, second.get(0).hash());
            assertNotEquals("the restored member took this snapshot itself", "-1",
                    ServiceProbe.seriesValue(ServiceProbe.nodeMetricsText(f.service), "core_snapshot_age_seconds"));

            // The leader goes down, a new one is elected, and the old leader restarts from the second snapshot.
            rig.kill(leader);
            final Member newLeader = rig.awaitLeader(leader);
            rig.restart(leader);
            rig.awaitReady(leader, READY_TIMEOUT_MS);
            final Pair oldLeader = pairOf(leader);
            System.out.println("[TEST] restarted old leader " + oldLeader + ", new leader member " + newLeader.id);
            assertSameIdentity(List.of(oldLeader, pairOf(f), pairOf(g)), secondPosition);
            assertEquals("the old leader loaded the second image", second.get(0).hash(), oldLeader.hash());
            final Client tool2 = rig.connect(SessionClass.TOOL, newLeader);
            tool2.deposits(7);
            final long thirdPosition = rig.snapshotAll();
            final List<Pair> third = pairs(rig);
            System.out.println("[TEST] after the snapshot at " + thirdPosition + ": " + third);
            assertSameIdentity(third, thirdPosition);
            assertTrue(leader.halts.toString(), leader.halts.isEmpty());
            System.out.println("GREEN snapshot identity: position " + snapshotPosition + " hash " + hash
                    + " on all three members and on member " + f.id + " after its restart; positions "
                    + secondPosition + " and " + thirdPosition + " equal after restores from images");
        }
    }

    @Test
    public void controlANodeLocalInputInTheHashIsCaughtByTheSameComparison() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(CONTROL_PORT_BASE, "core-snapshot-state-hash-control")) {
            for (final Member m : rig.members) {
                m.settings = StubStory.off();
                m.faults = new String[] {"stateHashMixesMemberId"};
            }
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            final Member g = rig.followerOtherThan(leader, null);
            final Client admin = rig.connect(SessionClass.ADMIN, g);
            rig.adopt(admin);
            final Client tool = rig.connect(SessionClass.TOOL, g);
            tool.deposits(50);
            final long snapshotPosition = rig.snapshotAll();
            final List<Pair> taken = pairs(rig);
            System.out.println("[TEST] control after the snapshot at " + snapshotPosition + ": " + taken);
            try {
                assertSameIdentity(taken, snapshotPosition);
            } catch (final AssertionError caught) {
                assertTrue("the control must fail on the hash, not elsewhere: " + caught.getMessage(),
                        caught.getMessage().contains("differs from"));
                System.out.println("GREEN control: the member id in the hash was caught: " + caught.getMessage());
                return;
            }
            fail("a hash that mixes in the member id passed the comparison: " + taken);
        }
    }
}
