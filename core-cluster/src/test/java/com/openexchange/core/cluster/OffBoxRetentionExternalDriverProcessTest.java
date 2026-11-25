// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.cluster.ClusterPorts;
import com.openexchange.cluster.OffBoxWatermark;
import com.openexchange.core.cluster.ShadowRestoreExternalDriverProcessTest.Clients;
import com.openexchange.core.cluster.ShadowRestoreExternalDriverProcessTest.Node;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The off-box bound on external media drivers: three members, each the production entry point in a child JVM on its
 * own {@code MediaDriver} process, {@code RETENTION_OFFBOX=required} from the environment, 128 KiB log segments. The
 * engine's cadence snapshots every 128 KiB of log while deposits flow; the watermark is written on each member by
 * the kit's command as a process of its own; the log start is read from each member's archive.
 *
 * <p>Which member leads is not fixed here, so the story is written around the leader it finds: a follower never
 * purges past the leader's log start, and the leader is bounded by its own watermark alone. One rule is checked on
 * every poll: no member's log start is ever above what its own watermark file allows.</p>
 */
public class OffBoxRetentionExternalDriverProcessTest {

    private static final int SEGMENT = 128 * 1024;
    private static final long GENESIS = ShadowClusterRig.GENESIS;
    private static final long WAIT_MS = 180_000;

    /** The process rig of this package: its nodes, clients and clean-up. */
    private final ShadowRestoreExternalDriverProcessTest rig = new ShadowRestoreExternalDriverProcessTest();

    @Before
    public void stride() {
        System.setProperty("cluster.port.stride", Integer.toString(ClusterPorts.SHARED_HOST_STRIDE));
        ClusterPorts.resetStrideForTest();
    }

    @After
    public void cleanUp() throws Exception {
        rig.cleanUp();
        System.clearProperty("cluster.port.stride");
        ClusterPorts.resetStrideForTest();
    }

    private static Map<String, String> env(final String offBoxMode) {
        final Map<String, String> env = new HashMap<>();
        env.put("SNAPSHOT_MODE", "off"); // full images: this node's own floor is its newest snapshot
        env.put("RETENTION_OFFBOX", offBoxMode);
        env.put("RETENTION_RECHECK_MAX_MS", "1000");
        env.put("SNAPSHOT_LOG_BYTES", Integer.toString(SEGMENT));
        env.put("ARCHIVE_SEGMENT_LENGTH", "128k");
        env.put("TRANSPORT_LOG_TERM_LENGTH", "128k");
        env.put("TRANSPORT_LOG_RCV_WINDOW", "64k");
        env.put("TRANSPORT_LOG_SO_RCVBUF", "64k");
        env.put("TRANSPORT_LOG_SO_SNDBUF", "64k");
        return env;
    }

    private final class Story {
        final List<Node> nodes = new ArrayList<>();
        final long[] allowed = new long[3];
        final Map<String, String> env;
        Clients clients;
        Node leader;

        Story(final String prefix, final int portBase, final String offBoxMode) throws Exception {
            env = env(offBoxMode);
            for (int id = 0; id < 3; id++) {
                nodes.add(rig.new Node(prefix + id + "-", portBase, id, 3));
            }
            for (final Node n : nodes) {
                n.startDriver();
                n.startEngine(env, List.of());
            }
            for (final Node n : nodes) {
                n.awaitReady(180_000);
                assertTrue("external driver mode on member " + n.nodeId, n.log().contains("Driver mode EXTERNAL"));
            }
            leader = ShadowRestoreExternalDriverProcessTest.awaitLeader(nodes);
            clients = rig.new Clients(nodes);
            clients.adopt();
        }

        long logStart(final Node n) {
            return n.archive().getStartPosition(n.logRecordingId());
        }

        void assertNoMemberPastItsWatermark() {
            for (final Node n : nodes) {
                if (n.engine == null || !n.engine.isAlive()) {
                    continue;
                }
                final long start = logStart(n);
                if (start > allowed[n.nodeId]) {
                    throw new AssertionError("member " + n.nodeId + ": log start " + start
                            + " passed the off-box position " + allowed[n.nodeId]);
                }
            }
        }

        void set(final long x, final Node... targets) {
            for (final Node n : targets) {
                OffBoxTool.set(n.clusterDir(), GENESIS, x);
                allowed[n.nodeId] = Math.max(allowed[n.nodeId], x);
            }
        }

        void await(final BooleanSupplier done, final String what) throws Exception {
            final long deadline = System.currentTimeMillis() + WAIT_MS;
            while (true) {
                assertNoMemberPastItsWatermark();
                if (done.getAsBoolean()) {
                    return;
                }
                if (System.currentTimeMillis() > deadline) {
                    throw new AssertionError("timed out waiting for " + what + ": " + state());
                }
                Thread.sleep(200);
            }
        }

        void loadUntilSnapshotsReach(final long position) throws Exception {
            final long deadline = System.currentTimeMillis() + WAIT_MS;
            while (true) {
                assertNoMemberPastItsWatermark();
                boolean reached = true;
                for (final Node n : nodes) {
                    reached &= n.engine == null || !n.engine.isAlive() || n.latestSnapshot() >= position;
                }
                if (reached) {
                    return;
                }
                if (System.currentTimeMillis() > deadline) {
                    throw new AssertionError("timed out under load waiting for a snapshot at or above " + position
                            + ": " + state());
                }
                clients.deposits(300);
            }
        }

        void awaitLogStart(final long start, final Node... targets) throws Exception {
            await(() -> {
                for (final Node n : targets) {
                    if (logStart(n) != start) {
                        return false;
                    }
                }
                return true;
            }, "log start " + start);
        }

        void pump(final long ms) throws Exception {
            final long until = System.currentTimeMillis() + ms;
            await(() -> System.currentTimeMillis() >= until, "a pause");
        }

        Node followerOtherThan(final Node exclude) {
            for (final Node n : nodes) {
                if (n != leader && n != exclude) {
                    return n;
                }
            }
            throw new IllegalStateException("no such follower");
        }

        String state() {
            final StringBuilder sb = new StringBuilder();
            for (final Node n : nodes) {
                final boolean live = n.engine != null && n.engine.isAlive();
                sb.append("[member ").append(n.nodeId).append(n == leader ? " (leader)" : "").append(" log start ")
                        .append(live ? String.valueOf(logStart(n)) : "down").append(" allowed ")
                        .append(allowed[n.nodeId]).append(" newest snapshot ").append(n.latestSnapshot())
                        .append("] ");
            }
            return sb.toString();
        }
    }

    private static long at(final int segments) {
        return (long)segments * SEGMENT + 1_000;
    }

    private static long base(final int segments) {
        return (long)segments * SEGMENT;
    }

    @Test(timeout = 1_500_000)
    public void noMemberPurgesPastItsOwnWatermarkOnExternalDrivers() throws Exception {
        final Story s = new Story("offbox-external-", 26700, "required");
        final Node leader = s.leader;
        final Node f = s.followerOtherThan(null);
        final Node g = s.followerOtherThan(f);

        // 1. no watermark anywhere: snapshots pile up and nobody purges anything
        s.loadUntilSnapshotsReach(base(3));
        s.pump(12_000);
        for (final Node n : s.nodes) {
            assertEquals("member " + n.nodeId + " holds its whole log with no watermark", 0, s.logStart(n));
            assertTrue("member " + n.nodeId + " says why it holds", n.log().contains("off-box watermark ABSENT"));
        }

        // 2. X on every member: each purges to the segment below X and no further
        s.set(at(1), leader, f, g);
        s.awaitLogStart(base(1), leader, f, g);

        // 3. X advances on the leader and one follower; the update reaches the other follower later
        s.set(at(2), leader, f);
        s.awaitLogStart(base(2), leader, f);
        s.pump(8_000);
        assertEquals("the member that missed the update holds more log, nothing else", base(1), s.logStart(g));
        s.set(at(2), g);
        s.awaitLogStart(base(2), g);

        // 4. a follower's file is deleted: it holds where it is while the others move; restored, it catches up
        Files.delete(OffBoxWatermark.file(f.clusterDir()));
        s.allowed[f.nodeId] = base(2);
        s.loadUntilSnapshotsReach(base(5));
        s.set(at(4), leader, g);
        s.awaitLogStart(base(4), leader, g);
        assertEquals("the member without a file did not move", base(2), s.logStart(f));
        s.set(at(4), f);
        s.awaitLogStart(base(4), f);

        // 5. kill -9 of a follower, its file gone when it comes back: it holds everything it has
        s.clients.close();
        g.killEngine();
        Files.delete(OffBoxWatermark.file(g.clusterDir()));
        s.allowed[g.nodeId] = base(4);
        g.startEngine(s.env, List.of());
        g.awaitReady(180_000);
        s.clients = rig.new Clients(s.nodes);
        s.loadUntilSnapshotsReach(base(7));
        s.set(at(6), leader, f);
        s.awaitLogStart(base(6), leader, f);
        s.pump(8_000);
        assertEquals("restarted with the file gone: nothing purged", base(4), s.logStart(g));
        assertTrue("the restarted member says why it holds", g.log().contains("off-box watermark ABSENT"));
        // and it recovers when the archiver sends the watermark again
        s.set(at(6), g);
        s.awaitLogStart(base(6), g);
        System.out.println("GREEN off-box required (external drivers): " + s.state());
    }

    /**
     * The control on external drivers: every member is launched with the bound reported and not applied. The leader's
     * log start passes X and the rule fails for it. (A follower cannot show it: it never purges past the leader.)
     */
    @Test(timeout = 900_000)
    public void controlMembersThatDoNotApplyTheBoundPassTheirWatermark() throws Exception {
        final Story s = new Story("offbox-external-control-", 26900, "report");
        s.set(at(1), s.nodes.toArray(new Node[0]));
        try {
            s.loadUntilSnapshotsReach(base(4));
            s.awaitLogStart(base(3), s.leader);
            fail("negative control did not fail: the rule cannot see a member that ignores its watermark");
        } catch (final AssertionError expected) {
            assertTrue(String.valueOf(expected.getMessage()), expected.getMessage() != null
                    && expected.getMessage().contains(": log start ")
                    && expected.getMessage().contains("passed the off-box position " + at(1)));
        }
        assertTrue("the leader says the floor is not applied", s.leader.log().contains(
                "RETENTION_OFFBOX=report: the off-box floor " + at(1) + " (VALID) is not applied"));
        System.out.println("NC off-box unapplied (external drivers) -> a member passed " + at(1));
    }
}
