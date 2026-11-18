// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.cluster.NodeReadiness;
import com.openexchange.core.infrastructure.InfrastructureConstants;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.generated.TimeInForce;
import com.openexchange.core.infrastructure.journal.OrderJournal;
import com.openexchange.core.infrastructure.journal.OrderJournalSettings;
import com.openexchange.core.infrastructure.journal.OrderJournalWriterAgent;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import io.aeron.cluster.service.Cluster;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A member whose order journal stops draining (its writer agent stopped: nothing takes the ring's bytes, the effect of
 * a stalled archive on the journal) under flow, in a real three-member core with a small ring: the member reads not
 * ready inside the alarm bound, the others keep committing, and once the writer runs again the member catches up with
 * a dense journal. Done twice on a follower, then once on the LEADER held longer than the leader heartbeat timeout:
 * what the cluster then does (the blocked leader stays and order entry stops, or it is replaced) is recorded in the
 * output, not assumed. Control: the alarm off (a bound no ring can pass) reads ready while blocked.
 */
public class OrderJournalStallClusterTest {

    private static final long ONE = 100_000_000L;
    private static final long ALARM_MS = 500;

    private static final class Wiring {
        final OrderJournalSettings settings;
        final Map<Integer, OrderJournal> journal = new HashMap<>();
        final Map<Integer, AgentRunner> writer = new HashMap<>();
        final Map<Integer, ShadowClusterRig.Member> members = new HashMap<>();

        Wiring(final ShadowClusterRig rig, final int alarmPct, final boolean progressAlarmOff) {
            settings = OrderJournalSettings.with(Map.of(
                    OrderJournalSettings.Key.CORE_ORDER_JOURNAL_RING_BYTES, "65536",
                    OrderJournalSettings.Key.CORE_ORDER_JOURNAL_RING_ALARM_PCT, String.valueOf(alarmPct),
                    OrderJournalSettings.Key.CORE_ORDER_JOURNAL_RING_ALARM_MS, String.valueOf(ALARM_MS)));
            for (final ShadowClusterRig.Member m : rig.members) {
                members.put(m.id, m);
                m.beforeLaunch = svc -> {
                    final OrderJournal j = new OrderJournal(settings);
                    com.openexchange.core.infrastructure.journal.OrderJournalCapture.progressAlarmOff(j, progressAlarmOff);
                    journal.put(m.id, j);
                    svc.setOrderJournal(j, settings);
                };
                m.beforeContainer = mm -> start(mm.id);
            }
        }

        void start(final int id) {
            final ShadowClusterRig.Member m = members.get(id);
            final AgentRunner w = new AgentRunner(new BackoffIdleStrategy(), Throwable::printStackTrace, null,
                    new OrderJournalWriterAgent(journal.get(id), m.cfg.aeronArchiveContext().clone()::clone,
                            settings.channel(), InfrastructureConstants.ORDER_JOURNAL_STREAM_ID, 1_000_000L));
            writer.put(id, w);
            AgentRunner.startOnThread(w);
        }

        void stop(final int id) {
            CloseHelper.quietClose(writer.remove(id));
        }

        void stopAll() {
            new ArrayList<>(writer.keySet()).forEach(this::stop);
        }
    }

    private static final class Flow {
        final ShadowClusterRig rig;
        final CoreRestoreRig.Encoders enc = new CoreRestoreRig.Encoders();
        final ShadowClusterRig.Client gateway;
        long request = 1;

        Flow(final ShadowClusterRig rig, final ShadowClusterRig.Member via) throws InterruptedException {
            this.rig = rig;
            final ShadowClusterRig.Client admin = rig.connect(SessionClass.ADMIN, via);
            rig.adopt(admin);
            rig.connect(SessionClass.TOOL, via).deposits(500);
            admin.offer(enc.buf, enc.limits(0x77L, 1, 600_000L, 100_000, 1_024));
            admin.offer(enc.buf, enc.gateOpen(0x73L));
            rig.await(() -> admin.in.count(x -> x.startsWith("ADMINACK ref=73 result=ACCEPTED")) > 0, 30_000,
                    () -> "the ADMIN OPEN must be accepted; frames " + admin.in.frames);
            gateway = rig.connect(SessionClass.GATEWAY, via);
            CoreRestoreRig.register(gateway.session, SessionClass.GATEWAY, false, false, rig.sessions());
        }

        void offer(final int n) {
            for (int i = 0; i < n; i++, request++) {
                gateway.offer(enc.buf, enc.limit(0x6000L + request, 1 + (request % 5), request, 1, true, ONE / 100, ONE,
                        TimeInForce.GTC, 0L));
            }
        }
    }

    /** The row's check: a member blocked on its journal must not read ready. */
    static void assertNotReadyWhileBlocked(final ShadowClusterRig.Member m) {
        if (ServiceProbe.probe(m.service).ready()) {
            throw new AssertionError("member with a stalled order journal reads ready: member " + m.id + " "
                    + ServiceProbe.probe(m.service).detail());
        }
    }

    /** Readiness closed by the order journal's alarm (the probe names it first in its detail). */
    static boolean journalAlarmClosed(final ShadowClusterRig.Member m) {
        final NodeReadiness.Probe p = ServiceProbe.probe(m.service);
        return !p.ready() && p.detail().startsWith("order-journal-stalled");
    }

    private static void stallAndRelease(final ShadowClusterRig rig, final Wiring w, final Flow flow,
                                        final ShadowClusterRig.Member m, final long holdMs, final int offers,
                                        final int fillBytes, final String what) {
        final OrderJournal j = w.journal.get(m.id);
        w.stop(m.id);
        final long ackBefore = flow.gateway.in.acks;
        final long t0 = System.nanoTime();
        // 200 records fill the 64 KiB ring above half without blocking the service thread (the alarm alone decides);
        // 400 are more than it holds: the service thread blocks on the full ring
        flow.offer(offers);
        rig.await(() -> j.ringUsedBytes() > fillBytes, 30_000, () -> what + ": the ring never filled");
        // the journal's own alarm, not any not-ready answer: under the burst the kit's readiness can close (apply
        // lag above its bound) and reopen before the alarm window has run, and the hold below would then read ready
        try {
            rig.await(() -> journalAlarmClosed(m), 3 * ALARM_MS + 2_000, () -> "");
        } catch (final AssertionError neverClosed) {
            assertNotReadyWhileBlocked(m);
            throw new AssertionError(what + ": the order journal alarm never closed readiness: member " + m.id + " "
                    + ServiceProbe.probe(m.service).detail());
        }
        final long notReadyMs = (System.nanoTime() - t0) / 1_000_000;
        final List<String> roles = new ArrayList<>();
        final long holdUntil = System.currentTimeMillis() + holdMs;
        while (System.currentTimeMillis() < holdUntil) {
            assertNotReadyWhileBlocked(m);
            rig.pumpMs(250);
            final StringBuilder sb = new StringBuilder();
            for (final ShadowClusterRig.Member x : rig.members) {
                sb.append(x.id).append('=').append(x.role()).append(' ');
            }
            if (roles.isEmpty() || !roles.get(roles.size() - 1).equals(sb.toString().trim())) {
                roles.add(sb.toString().trim());
            }
        }
        final long acksDuring = flow.gateway.in.acks - ackBefore;
        System.out.println("[order-journal stall] " + what + ": the journal alarm closed readiness after " + notReadyMs
                + " ms (alarm bound " + ALARM_MS + " ms + fill); held " + holdMs + " ms; acks during the stall "
                + acksDuring + "; roles " + roles + "; ring used " + j.ringUsedBytes() + " B, recorded "
                + j.recordedSeq());
        w.start(m.id);
        final long head = flow.request - 1;
        rig.await(() -> flow.gateway.in.acks >= head, 120_000, () -> what + ": acks " + flow.gateway.in.acks + " of " + head);
        rig.await(() -> ServiceProbe.probe(m.service).ready(), 60_000,
                () -> what + ": the member never read ready again: " + ServiceProbe.probe(m.service).detail());
    }

    private static void awaitDense(final ShadowClusterRig rig, final Wiring w) {
        final long head = rig.members.get(0).service.engine().getEventSeq();
        rig.await(() -> rig.members.stream().allMatch(x -> w.journal.get(x.id).recordedSeq() == x.service.engine().getEventSeq()
                && x.service.engine().getEventSeq() == head), 120_000, () -> "every member records the head " + head);
        for (final ShadowClusterRig.Member x : rig.members) {
            final TreeMap<Long, byte[]> u = new TreeMap<>();
            OrderJournalReplicationClusterTest.union(OrderJournalReplicationClusterTest.recordings(rig, x), u,
                    String.valueOf(x.id));
            assertEquals("member " + x.id + " journal from 1", 1L, (long) u.firstKey());
            assertEquals("member " + x.id + " journal to the head", head, (long) u.lastKey());
        }
    }

    private static void story(final int port, final int alarmPct, final boolean progressAlarmOff, final boolean leaderToo)
            throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(port, "order-journal-stall-" + alarmPct)) {
            final Wiring w = new Wiring(rig, alarmPct, progressAlarmOff);
            try {
                rig.bootAll();
                final ShadowClusterRig.Member leader = rig.awaitLeader(null);
                final ShadowClusterRig.Member f = rig.followerOtherThan(leader, null);
                final ShadowClusterRig.Member g = rig.followerOtherThan(leader, f);
                final Flow flow = new Flow(rig, g);
                flow.offer(20);
                rig.await(() -> flow.gateway.in.acks >= 20, 30_000, () -> "acks " + flow.gateway.in.acks);
                // round 0: a few records and nothing recorded, the ring far below the fill bound (the no-progress alarm)
                for (int round = 0; round <= 2; round++) {
                    stallAndRelease(rig, w, flow, f, 2_000, round == 0 ? 10 : round == 1 ? 200 : 400,
                            round == 0 ? 0 : 65536 / 2, "follower round " + round + (round == 0
                                    ? " (ring far below the bound, nothing recorded)" : round == 1
                                    ? " (ring above the bound, not full)" : " (ring full, service thread blocked)"));
                    assertTrue("the others kept committing", flow.gateway.in.acks >= flow.request - 1);
                }
                awaitDense(rig, w);
                if (leaderToo) {
                    final long hold = 3 * rig.leaderHeartbeatTimeoutMs + 12_000;
 
                    stallAndRelease(rig, w, flow, leader, hold, 400, 65536 / 2, "LEADER (held " + hold + " ms, heartbeat timeout "
                            + rig.leaderHeartbeatTimeoutMs + " ms)");
                    final ShadowClusterRig.Member now = rig.awaitLeader(null);
                    System.out.println("[order-journal stall] after the LEADER stall: leader is member " + now.id
                            + (now.id == leader.id ? " (the blocked leader stayed)" : " (replaced)"));
                    awaitDense(rig, w);
                }
                System.out.println("GREEN order journal stall: the stalled member read not ready each time, the journals"
                        + " are dense after release");
            } finally {
                // a service thread blocked on a full ring cannot close: drain every ring before the rig closes
                for (final ShadowClusterRig.Member x : rig.members) {
                    if (!w.writer.containsKey(x.id) && w.journal.containsKey(x.id)) {
                        w.start(x.id);
                    }
                }
                final long deadline = System.currentTimeMillis() + 30_000;
                while (System.currentTimeMillis() < deadline
                        && w.journal.values().stream().anyMatch(j -> j.ringUsedBytes() > 0)) {
                    Thread.sleep(50);
                }
                w.stopAll();
            }
        }
    }

    @Test
    public void aStalledOrderJournalClosesReadinessAndTheMemberCatchesUp() throws Exception {
        story(31900, 50, false, true);
    }

    /**
     * Control: the alarm switched off (a fill bound of 100 percent, which a ring never passes, and the no-progress
     * alarm off in a test build) reads ready while blocked.
     */
    @Test
    public void controlAlarmOffReadsReadyWhileBlocked() throws Exception {
        try {
            story(32100, 100, true, false); // the first follower round (nothing recorded, ring far below) decides
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(),
                    expected.getMessage().startsWith("member with a stalled order journal reads ready"));
            System.out.println("[order-journal stall] control: member with a stalled order journal reads ready ("
                    + expected.getMessage() + ")");
            return;
        }
        fail("the control read not ready with the alarm off");
    }
}
