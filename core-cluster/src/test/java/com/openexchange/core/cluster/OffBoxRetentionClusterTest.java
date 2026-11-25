// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.cluster.OffBoxWatermark;
import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.PrintStream;
import java.nio.file.Files;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Three members under {@code RETENTION_OFFBOX=required}, embedded drivers, 64 KiB log segments. The engine's own
 * cadence takes the snapshots (64 KiB of log each) while deposits flow; the off-box watermark is written on each
 * member by the kit's command run as a process of its own; the log start is read from each member's archive.
 *
 * <p>Member 0 is the appointed leader, so which member bounds which is known: a follower never purges past the
 * leader's log start, and the leader is bounded by its own watermark alone. One rule is checked on every poll of
 * every wait in the story: no member's log start is ever above what its own watermark file allows.</p>
 */
public class OffBoxRetentionClusterTest {

    private static final int SEGMENT = StubStory.SEGMENT;
    private static final long GENESIS = ShadowClusterRig.GENESIS;
    private static final long WAIT_MS = 120_000;

    private PrintStream errBefore;
    private CoreRestoreRig.Tee err;

    @Before
    public void settings() {
        // the cadence is built with the service: a snapshot every 64 KiB of log, as SnapshotCadenceClusterTest does
        System.setProperty("snapshot.log.bytes", Integer.toString(SEGMENT));
        // a held pass is looked at again on every poll, so a watermark that rises is followed within seconds
        System.setProperty("retention.recheck.max.ms", "1000");
    }

    @After
    public void restore() {
        System.clearProperty("snapshot.log.bytes");
        System.clearProperty("retention.recheck.max.ms");
        if (errBefore != null) {
            System.setErr(errBefore);
        }
    }

    /** The cluster, the highest log start each member's file allows, and the rule checked on every poll. */
    private final class Story implements AutoCloseable {
        final ShadowClusterRig rig;
        final long[] allowed = new long[ShadowClusterRig.MEMBERS];
        Client tool;
        boolean checking;

        Story(final int portBase, final String name, final OffBoxWatermark.Mode leaderMode) {
            rig = new ShadowClusterRig(portBase, name, ShadowClusterRig.MEMBERS, "64k", SEGMENT);
            errBefore = System.err;
            err = new CoreRestoreRig.Tee(errBefore, "[RETENTION] CRITICAL");
            System.setErr(err);
            for (final Member m : rig.members) {
                final OffBoxWatermark.Mode mode = m.id == 0 ? leaderMode : OffBoxWatermark.Mode.REQUIRED;
                m.beforeLaunch = service -> ServiceProbe.offBoxMode(service, mode);
                m.afterConfig = mm -> mm.cfg.consensusModuleContext().appointedLeaderId(0);
            }
            rig.duringAwait.add(this::assertNoMemberPastItsWatermark);
        }

        Story start() throws InterruptedException {
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            assertEquals("the appointed leader", 0, leader.id);
            rig.adopt(rig.connect(SessionClass.ADMIN, leader));
            tool = rig.connect(SessionClass.TOOL, leader);
            checking = true; // every member has a log recording from here on
            return this;
        }

        Member member(final int id) {
            return rig.members.get(id);
        }

        void assertNoMemberPastItsWatermark() {
            if (!checking) {
                return;
            }
            for (final Member m : rig.live()) {
                final long start = rig.logStart(m);
                if (start > allowed[m.id]) {
                    throw new AssertionError("member " + m.id + ": log start " + start
                            + " passed the off-box position " + allowed[m.id]);
                }
            }
        }

        /** The watermark of {@code ids} becomes {@code x}, by the command, one member after the other. */
        void set(final long x, final int... ids) {
            for (final int id : ids) {
                OffBoxTool.set(member(id).clusterDir(), GENESIS, x);
                allowed[id] = Math.max(allowed[id], x);
            }
        }

        /** Deposits until {@code done}; the cadence snapshots as the log grows. */
        void loadUntil(final BooleanSupplier done, final String what) {
            final long deadline = System.currentTimeMillis() + WAIT_MS;
            while (!done.getAsBoolean()) {
                if (System.currentTimeMillis() > deadline) {
                    throw new AssertionError("timed out under load waiting for " + what + ": " + state());
                }
                tool.deposits(200);
                rig.pumpMs(100);
            }
        }

        /** Every live member holds a snapshot at or above {@code position}. */
        void loadUntilSnapshotsReach(final long position) {
            loadUntil(() -> {
                for (final Member m : rig.live()) {
                    final java.util.List<long[]> s = rig.snapshots(m);
                    if (s.isEmpty() || s.get(s.size() - 1)[0] < position) {
                        return false;
                    }
                }
                return true;
            }, "a snapshot at or above " + position + " on every member");
        }

        void awaitLogStart(final long start, final int... ids) {
            rig.await(() -> {
                for (final int id : ids) {
                    if (rig.logStart(member(id)) != start) {
                        return false;
                    }
                }
                return true;
            }, WAIT_MS, () -> "log start " + start + " on members " + java.util.Arrays.toString(ids) + ": "
                    + state());
        }

        void awaitState(final int id, final OffBoxWatermark.State state) {
            rig.await(() -> {
                final OffBoxWatermark w = ServiceProbe.offBox(member(id).service);
                return w != null && w.state() == state;
            }, WAIT_MS, () -> "member " + id + " off-box state " + state + ": " + state());
        }

        String state() {
            final StringBuilder sb = new StringBuilder();
            for (final Member m : rig.live()) {
                final OffBoxWatermark w = ServiceProbe.offBox(m.service);
                sb.append("[member ").append(m.id).append(" log start ").append(rig.logStart(m)).append(" allowed ")
                        .append(allowed[m.id]).append(" off-box ").append(w == null ? "unwired" : w.state() + "@"
                                + w.positionBytes()).append(" error ").append(ServiceProbe.prunerLastError(m.service))
                        .append(" snapshots ").append(rig.snapshots(m).size()).append("] ");
            }
            return sb.toString();
        }

        @Override
        public void close() {
            rig.close();
        }
    }

    private static long at(final int segments) {
        return (long)segments * SEGMENT + 1_000; // inside segment number `segments`: the purge stops at its base
    }

    private static long base(final int segments) {
        return (long)segments * SEGMENT;
    }

    @Test(timeout = 900_000)
    public void noMemberPurgesPastItsOwnWatermarkAndEveryHoldRecovers() throws Exception {
        try (Story s = new Story(56000, "offbox-required", OffBoxWatermark.Mode.REQUIRED).start()) {
            final ShadowClusterRig rig = s.rig;

            // 1. no watermark anywhere: snapshots pile up and nobody purges anything
            s.loadUntilSnapshotsReach(base(3));
            for (final Member m : rig.members) {
                s.awaitState(m.id, OffBoxWatermark.State.ABSENT);
            }
            rig.pumpMs(12_000);
            for (final Member m : rig.members) {
                assertEquals("member " + m.id + " holds its whole log with no watermark", 0, rig.logStart(m));
                assertTrue(ServiceProbe.prunerLastError(m.service),
                        ServiceProbe.prunerLastError(m.service).contains("off-box watermark ABSENT"));
                assertEquals("1", ServiceProbe.seriesValue(ServiceProbe.metricsText(m.service),
                        "core_retention_offbox_state"));
                assertEquals("2", ServiceProbe.seriesValue(ServiceProbe.metricsText(m.service),
                        "core_retention_offbox_mode"));
            }
            assertTrue("the hold is said out loud: " + err.lines,
                    err.lines.stream().anyMatch(l -> l.contains("off-box watermark ABSENT")));

            // 2. X on every member: each purges to the segment below X and no further
            s.set(at(1), 0, 1, 2);
            s.awaitLogStart(base(1), 0, 1, 2);
            for (final Member m : rig.members) {
                s.awaitState(m.id, OffBoxWatermark.State.VALID);
                assertEquals(Long.toString(at(1)), ServiceProbe.seriesValue(ServiceProbe.metricsText(m.service),
                        "core_retention_offbox_position_bytes"));
            }

            // 3. X advances on two members; the update reaches the third later
            s.set(at(2), 0, 1);
            s.awaitLogStart(base(2), 0, 1);
            rig.pumpMs(8_000);
            assertEquals("member 2 missed the update and holds more log, nothing else", base(1), rig.logStart(s.member(2)));
            s.set(at(2), 2);
            s.awaitLogStart(base(2), 2);

            // 4. one member's file is deleted: it holds where it is while the others move; restored, it catches up
            Files.delete(OffBoxWatermark.file(s.member(1).clusterDir()));
            s.allowed[1] = rig.logStart(s.member(1));
            s.awaitState(1, OffBoxWatermark.State.ABSENT);
            s.loadUntilSnapshotsReach(base(5));
            s.set(at(4), 0, 2);
            s.awaitLogStart(base(4), 0, 2);
            assertEquals("the member without a file did not move", base(2), rig.logStart(s.member(1)));
            assertTrue(ServiceProbe.prunerLastError(s.member(1).service),
                    ServiceProbe.prunerLastError(s.member(1).service).contains("off-box watermark ABSENT"));
            s.set(at(4), 1);
            s.awaitLogStart(base(4), 1);
            s.awaitState(1, OffBoxWatermark.State.VALID);

            // 5. a file of another genesis holds; a correct one clears it
            OffBoxTool.clear(s.member(2).clusterDir(), GENESIS);
            s.allowed[2] = rig.logStart(s.member(2));
            OffBoxTool.set(s.member(2).clusterDir(), GENESIS + 1, at(6));
            s.awaitState(2, OffBoxWatermark.State.FOREIGN);
            s.loadUntilSnapshotsReach(base(7));
            s.set(at(6), 0, 1);
            s.awaitLogStart(base(6), 0, 1);
            assertEquals("the member with a foreign file did not move", base(4), rig.logStart(s.member(2)));
            assertEquals("the command refuses to overwrite another genesis", 65,
                    OffBoxTool.run("set", "--cluster-dir", s.member(2).clusterDir().getAbsolutePath(), "--engine",
                            "core", "--cluster-id", "0", "--genesis-id", Long.toString(GENESIS),
                            "--log-position-bytes", Long.toString(at(6)), "--source", "test-archiver").exit());
            OffBoxTool.clear(s.member(2).clusterDir(), GENESIS + 1);
            s.set(at(6), 2);
            s.awaitLogStart(base(6), 2);

            // 6. the operator releases the leader's hold: its floor moves to the released position, with its line;
            //    the followers keep their own bound
            s.loadUntilSnapshotsReach(base(9));
            err.lines.clear();
            final String released = OffBoxTool.release(s.member(0).clusterDir(), GENESIS, at(8),
                    "archiver down, disk filling").output();
            assertTrue(released, released.contains("off-box hold released by operator up to " + at(8)));
            s.allowed[0] = at(8);
            s.awaitLogStart(base(8), 0);
            rig.await(() -> err.lines.stream().anyMatch(l -> l.contains("off-box hold released by operator up to "
                            + at(8) + " (durable " + at(6) + "): log in [" + at(6) + ", " + at(8) + ") may be purged"
                            + " without an off-box copy; reason: archiver down, disk filling")), WAIT_MS,
                    () -> "the release line on the leader's pass: " + err.lines);
            assertEquals(Long.toString(at(8)), ServiceProbe.seriesValue(ServiceProbe.metricsText(s.member(0).service),
                    "core_retention_offbox_released_bytes"));
            assertEquals("a follower is not released by the leader's release", base(6), rig.logStart(s.member(1)));
            assertEquals(base(6), rig.logStart(s.member(2)));
            // the archiver comes back: a set at the released position clears the release
            s.set(at(8), 0, 1, 2);
            s.awaitLogStart(base(8), 1, 2);
            rig.await(() -> "-1".equals(ServiceProbe.seriesValue(ServiceProbe.metricsText(s.member(0).service),
                    "core_retention_offbox_released_bytes")), WAIT_MS, "the release to clear on the leader");

            // 7. a restarted member keeps holding at its file; restarted without the file it holds everything
            final Member f = s.member(1);
            rig.kill(f);
            rig.restart(f);
            rig.awaitReady(f, WAIT_MS);
            s.loadUntilSnapshotsReach(base(11));
            s.set(at(10), 0, 2);
            s.awaitLogStart(base(10), 0, 2);
            assertEquals("the restarted member holds at the file it kept", base(8), rig.logStart(f));
            rig.kill(f);
            Files.delete(OffBoxWatermark.file(f.clusterDir()));
            s.allowed[1] = base(8);
            rig.restart(f);
            rig.awaitReady(f, WAIT_MS);
            s.awaitState(1, OffBoxWatermark.State.ABSENT);
            s.loadUntilSnapshotsReach(base(13));
            s.set(at(12), 0, 2);
            s.awaitLogStart(base(12), 0, 2);
            assertEquals("restarted with the file gone: nothing purged", base(8), rig.logStart(f));
            // and the last hold recovers too
            s.set(at(12), 1);
            s.awaitLogStart(base(12), 1);
            System.out.println("GREEN off-box required (embedded): " + s.state());
        }
    }

    /**
     * The control: the leader is launched with the off-box bound not applied. Its log start passes X, the rule fails
     * for that member, and the two members that apply the bound stay behind X.
     */
    @Test(timeout = 600_000)
    public void controlAMemberThatDoesNotApplyTheBoundPassesItsWatermark() throws Exception {
        try (Story s = new Story(56300, "offbox-control", OffBoxWatermark.Mode.REPORT).start()) {
            s.set(at(1), 0, 1, 2);
            try {
                s.loadUntilSnapshotsReach(base(4));
                s.awaitLogStart(base(3), 0);
                fail("negative control did not fail: the rule cannot see a member that ignores its watermark");
            } catch (final AssertionError expected) {
                assertNotNull(expected.getMessage());
                assertTrue(expected.getMessage(), expected.getMessage().startsWith("member 0: log start ")
                        && expected.getMessage().contains("passed the off-box position " + at(1)));
            }
            for (final int id : new int[] {1, 2}) {
                assertTrue("member " + id + " applies the bound", s.rig.logStart(s.member(id)) <= at(1));
            }
            System.out.println("NC off-box unapplied on the leader -> member 0 passed " + at(1));
        }
    }
}
