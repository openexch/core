// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.infrastructure.InfrastructureConstants;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.generated.TimeInForce;
import com.openexchange.core.infrastructure.journal.OrderJournalSettings;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import org.agrona.CloseHelper;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Replay slots kept for cluster catch-up. The leader's archive caps concurrent replays at
 * CORE_ARCHIVE_MAX_CONCURRENT_REPLAYS (4 here) and keeps CORE_ARCHIVE_CLUSTER_RESERVED_REPLAYS (2) for the cluster; the
 * readers' budget (the cap minus the reservation) is read from the leader's {@code /health} detail. Readers hold
 * replays on the LEADER's archive up to that budget, a follower is restarted behind the leader, and it catches up
 * within {@link #CATCH_UP_BOUND_MS}. Afterwards the reserved slots are free again and the cap is the setting (the next
 * replay past it is refused). A follower's catch-up holds one replay on the leader's archive (log replication): the
 * leader archive's active replay count is sampled through the catch-up and must rise above the readers' replays.
 *
 * <p>Aeron's archive has one cap for every client: it cannot set slots aside per client. The reservation is a budget
 * the readers are held to; the control shows what it guards against: readers that take every slot starve the
 * follower ({@code follower catch-up starved by reader replays}). That text is given only for a real starvation: the
 * follower did not catch up, every sample of the leader's active replays was at the cap, and a probe replay on the
 * leader's archive was refused at the cap. Any other failure to catch up is reported as itself.
 */
public class ArchiveReplayCapClusterTest {

    private static final long ONE = 100_000_000L;
    private static final int CAP = 4;
    private static final int RESERVED = 2;
    private static final int READER_REPLAY_STREAM = 4_901;
    /** The follower's catch-up bound; with free slots it takes well under a second here. */
    private static final long CATCH_UP_BOUND_MS = 30_000;
    private static final String READER_CHANNEL = "aeron:ipc?term-length=64k";
    private static final Pattern BUDGET = Pattern.compile("readerReplayBudget=(\\d+)");

    @Test
    public void aFollowerCatchesUpWhileReadersHoldTheirWholeBudgetOnTheLeader() throws Exception {
        story(32300, false);
    }

    /** Control: readers past their budget (every slot of the leader) leave the follower's catch-up no slot. */
    @Test
    public void controlReadersPastTheirBudgetStarveTheFollower() throws Exception {
        try {
            story(32500, true);
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(), expected.getMessage().startsWith("follower catch-up starved by reader replays"));
            System.out.println("[replay-cap] control: " + expected.getMessage());
            return;
        }
        fail("the follower caught up with every replay slot of the leader held");
    }

    /** Samples the leader archive's active replay count on its own thread: min and max over the window. */
    private static final class ReplaySampler implements AutoCloseable {
        private final io.aeron.Counter replays;
        private final Thread thread;
        private volatile boolean running = true;
        private volatile long min = Long.MAX_VALUE;
        private volatile long max = Long.MIN_VALUE;
        private volatile long samples;

        ReplaySampler(final io.aeron.Counter replays) {
            this.replays = replays;
            this.thread = new Thread(this::run, "replay-sampler");
            thread.setDaemon(true);
            thread.start();
        }

        private void run() {
            while (running) {
                final long v = replays.get();
                if (v < min) {
                    min = v;
                }
                if (v > max) {
                    max = v;
                }
                samples++;
                java.util.concurrent.locks.LockSupport.parkNanos(100_000L);
            }
        }

        @Override
        public void close() throws InterruptedException {
            running = false;
            thread.join(5_000);
        }

        @Override
        public String toString() {
            return "min " + min + ", max " + max + " over " + samples + " samples";
        }
    }

    private static Subscription hold(final ShadowClusterRig rig, final ShadowClusterRig.Member leader,
                                     final AeronArchive archive, final long recordingId, final int stream) {
        final Subscription sub = rig.aeron(leader).addSubscription(READER_CHANNEL, stream);
        archive.startReplay(recordingId, 0, AeronArchive.NULL_LENGTH, READER_CHANNEL, stream);
        return sub;
    }

    private static void story(final int port, final boolean pastBudget) throws Exception {
        System.setProperty("core.archive.max.concurrent.replays", String.valueOf(CAP));
        System.setProperty("core.archive.cluster.reserved.replays", String.valueOf(RESERVED));
        final List<Subscription> held = new ArrayList<>();
        try (ShadowClusterRig rig = new ShadowClusterRig(port, "archive-replay-reserve-" + pastBudget)) {
            final OrderJournalReplicationClusterTest.Wiring w =
                    new OrderJournalReplicationClusterTest.Wiring(rig, OrderJournalSettings.fromEnv());
            try {
                rig.bootAll();
                final ShadowClusterRig.Member leader = rig.awaitLeader(null);
                final ShadowClusterRig.Member f = rig.followerOtherThan(leader, null);
                final ShadowClusterRig.Member g = rig.followerOtherThan(leader, f);
                final ShadowClusterRig.Client admin = rig.connect(SessionClass.ADMIN, g);
                rig.adopt(admin);
                rig.connect(SessionClass.TOOL, g).deposits(100);
                final CoreRestoreRig.Encoders enc = new CoreRestoreRig.Encoders();
                admin.offer(enc.buf, enc.limits(0x77L, 1, 600_000L, 100_000, 1_024));
                admin.offer(enc.buf, enc.gateOpen(0x73L));
                rig.await(() -> admin.in.count(x -> x.startsWith("ADMINACK ref=73 result=ACCEPTED")) > 0, 30_000,
                        () -> "the ADMIN OPEN must be accepted");
                final ShadowClusterRig.Client gateway = rig.connect(SessionClass.GATEWAY, g);
                CoreRestoreRig.register(gateway.session, SessionClass.GATEWAY, false, false, rig.sessions());
                long request = 1;
                for (int i = 0; i < 20; i++, request++) {
                    gateway.offer(enc.buf, enc.limit(0x6000L + request, 1 + (request % 5), request, 1, true, ONE / 10, ONE,
                            TimeInForce.GTC, 0L));
                }
                rig.await(() -> gateway.in.acks >= 20, 30_000, () -> "acks " + gateway.in.acks);

                // the readers' budget as the leader advertises it
                final String detail = ServiceProbe.probe(leader.service).detail();
                final Matcher m = BUDGET.matcher(detail);
                assertTrue("the leader's /health detail advertises the readers' budget: " + detail, m.find());
                final int budget = Integer.parseInt(m.group(1));
                assertEquals("the budget is the cap minus the reservation: " + detail, CAP - RESERVED, budget);
                assertTrue(detail, detail.contains("CORE_ARCHIVE_CLUSTER_RESERVED_REPLAYS=" + RESERVED));

                // the readers: replays of the leader's log recording into subscriptions nobody polls
                final AeronArchive archive = rig.archive(leader);
                final long[] logRecording = {-1};
                archive.listRecordings(0, 100, (c, k, recordingId, st, sp, startPosition, stopPosition, i, t, se, mm, sid,
                                                stream, ch, oc, src) -> {
                    if (logRecording[0] < 0 && stopPosition == AeronArchive.NULL_POSITION
                            && stream != InfrastructureConstants.ORDER_JOURNAL_STREAM_ID
                            && stream != InfrastructureConstants.MONEY_JOURNAL_STREAM_ID) {
                        logRecording[0] = recordingId;
                    }
                });
                assertTrue("the leader's active log recording", logRecording[0] >= 0);
                final int readers = pastBudget ? CAP : budget;
                for (int i = 0; i < readers; i++) {
                    held.add(hold(rig, leader, archive, logRecording[0], READER_REPLAY_STREAM + i));
                }
                System.out.println("[replay-cap] cap " + CAP + ", reserved " + RESERVED + ", readers' budget " + budget
                        + "; readers hold " + readers + " replays on the leader");

                // the follower restarts behind the leader
                w.stop(f.id);
                rig.kill(f);
                for (int i = 0; i < 30; i++, request++) {
                    gateway.offer(enc.buf, enc.limit(0x6000L + request, 1 + (request % 5), request, 1, true, ONE / 10, ONE,
                            TimeInForce.GTC, 0L));
                }
                rig.await(() -> gateway.in.acks >= 50, 60_000, () -> "acks with the follower down " + gateway.in.acks);
                final long head = leader.service.engine().getEventSeq();
                // the leader archive's active replay count, sampled through the follower's catch-up
                final io.aeron.Counter replays = leader.driver.archive().context().replaySessionCounter();
                final long baseline = replays.get();
                assertEquals("the leader's active replays before the restart are the readers'", readers, baseline);
                final long t0 = System.nanoTime();
                final ReplaySampler sampler = new ReplaySampler(replays);
                AssertionError notCaughtUp = null;
                try {
                    rig.restart(f);
                    rig.awaitReady(f, CATCH_UP_BOUND_MS);
                    rig.await(() -> f.service.engine().getEventSeq() >= head, CATCH_UP_BOUND_MS,
                            () -> "follower at " + f.service.engine().getEventSeq() + " of " + head);
                } catch (final AssertionError e) {
                    notCaughtUp = e;
                } finally {
                    sampler.close();
                }
                final long catchUpMs = (System.nanoTime() - t0) / 1_000_000;
                System.out.println("[replay-cap] leader's active replays during the catch-up: " + sampler + " (readers "
                        + readers + ", cap " + CAP + ")");
                if (notCaughtUp != null) {
                    String probe;
                    try {
                        held.add(hold(rig, leader, archive, logRecording[0], READER_REPLAY_STREAM + 200));
                        probe = "accepted";
                    } catch (final ArchiveException full) {
                        probe = full.getMessage();
                    }
                    final boolean starved = sampler.min >= CAP && sampler.max <= readers
                            && probe.contains("max concurrent replays reached " + CAP);
                    if (starved) {
                        throw new AssertionError("follower catch-up starved by reader replays: " + readers
                                + " reader replays held of cap " + CAP + " (budget " + budget + "); leader's active replays "
                                + sampler + "; probe replay refused: " + probe + "; " + notCaughtUp.getMessage());
                    }
                    throw new AssertionError("follower did not catch up, not a replay starvation: leader's active replays "
                            + sampler + " (readers " + readers + ", cap " + CAP + "); probe replay: " + probe + "; "
                            + notCaughtUp.getMessage(), notCaughtUp);
                }
                assertTrue("the follower's catch-up held a replay on the leader's archive: active replays " + sampler
                        + " never rose above the readers' " + readers, sampler.max >= readers + 1);

                // the reserved slots are free again and the cap is the setting
                int accepted = 0;
                String refused = null;
                final long deadline = System.currentTimeMillis() + 10_000;
                while (refused == null) {
                    try {
                        held.add(hold(rig, leader, archive, logRecording[0], READER_REPLAY_STREAM + 100 + accepted));
                        accepted++;
                    } catch (final ArchiveException full) {
                        if (accepted < RESERVED && System.currentTimeMillis() < deadline) {
                            Thread.sleep(200); // the catch-up replay may still be closing
                            continue;
                        }
                        refused = full.getMessage();
                    }
                }
                assertEquals("the reserved slots are free after the catch-up (" + refused + ")", RESERVED, accepted);
                assertTrue("the cap is the setting: " + refused, refused.contains("max concurrent replays reached " + CAP));
                System.out.println("GREEN replay reserve: readers held " + readers + " of cap " + CAP + " (budget " + budget
                        + "); the restarted follower held a replay on the leader (active replays " + sampler + ") and caught"
                        + " up to eventSeq " + head + " in " + catchUpMs + " ms (bound "
                        + CATCH_UP_BOUND_MS + "); afterwards " + accepted + " reserved slots free, then refused: " + refused);
            } finally {
                held.forEach(CloseHelper::quietClose);
                w.stopAll();
            }
        } finally {
            System.clearProperty("core.archive.max.concurrent.replays");
            System.clearProperty("core.archive.cluster.reserved.replays");
        }
    }
}
