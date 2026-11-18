// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.infrastructure.InfrastructureConstants;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.generated.TimeInForce;
import com.openexchange.core.infrastructure.journal.OrderJournal;
import com.openexchange.core.infrastructure.journal.OrderJournalSettings;
import com.openexchange.core.infrastructure.journal.OrderJournalWriterAgent;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Mixed jars on a real three-member core. The previous cut's jar has no order journal; here a member on it is the same
 * build booted with no order journal wired (no ring, no writer, no restore-side rule), which is exactly what that jar
 * does. A property, recorded, with no switch:
 *
 * <ol>
 *   <li>one follower on the previous jar for N events (restored from a snapshot taken before, so it never journals
 *   them), then upgraded and restored from a snapshot taken after them: its journal has the hole (a, a+N] (declared
 *   on restore and absent from its archive); the other two members are dense;</li>
 *   <li>all three rolled back to the previous jar one at a time, M events, a snapshot, all three rolled forward: every
 *   member has the same hole (c, c+M].</li>
 * </ol>
 * The journal reports what it has: nothing fabricates a record, and every record present is byte equal across members.
 */
public class OrderJournalMixedJarsClusterTest {

    private static final long ONE = 100_000_000L;

    private static final class Jars {
        final OrderJournalSettings settings = OrderJournalSettings.defaults();
        final Map<Integer, Boolean> journalJar = new HashMap<>();
        final Map<Integer, OrderJournal> journal = new HashMap<>();
        final Map<Integer, AgentRunner> writer = new HashMap<>();

        Jars(final ShadowClusterRig rig) {
            for (final ShadowClusterRig.Member m : rig.members) {
                journalJar.put(m.id, true);
                m.beforeLaunch = svc -> {
                    journal.remove(m.id);
                    if (journalJar.get(m.id)) {
                        final OrderJournal j = new OrderJournal(settings);
                        journal.put(m.id, j);
                        svc.setOrderJournal(j, settings);
                    }
                };
                m.beforeContainer = mm -> {
                    if (journalJar.get(mm.id)) {
                        final AgentRunner w = new AgentRunner(new BackoffIdleStrategy(), Throwable::printStackTrace, null,
                                new OrderJournalWriterAgent(journal.get(mm.id), mm.cfg.aeronArchiveContext().clone()::clone,
                                        settings.channel(), InfrastructureConstants.ORDER_JOURNAL_STREAM_ID, 1_000_000L));
                        writer.put(mm.id, w);
                        AgentRunner.startOnThread(w);
                    }
                };
            }
        }

        /** Stop, then boot {@code m} on the journal jar ({@code withJournal}) or the previous one. */
        void reboot(final ShadowClusterRig rig, final ShadowClusterRig.Member m, final boolean withJournal) {
            CloseHelper.quietClose(writer.remove(m.id));
            rig.kill(m);
            journalJar.put(m.id, withJournal);
            rig.restart(m);
            rig.awaitReady(m, 120_000);
        }

        void stopAll() {
            new ArrayList<>(writer.values()).forEach(CloseHelper::quietClose);
            writer.clear();
        }
    }

    /** A GATEWAY session on any live member, placing resting limit orders. */
    private static final class Flow {
        final ShadowClusterRig rig;
        final CoreRestoreRig.Encoders enc = new CoreRestoreRig.Encoders();
        ShadowClusterRig.Client gateway;
        long request = 1;

        Flow(final ShadowClusterRig rig) {
            this.rig = rig;
        }

        void connect() throws InterruptedException {
            gateway = rig.connect(SessionClass.GATEWAY, rig.live().get(0));
            CoreRestoreRig.register(gateway.session, SessionClass.GATEWAY, false, false, rig.sessions());
        }

        /** {@code n} creates, acked; returns the leader's eventSeq after them, once every live member applied it. */
        long place(final int n) {
            final int before = gateway.in.acks;
            for (int i = 0; i < n; i++, request++) {
                gateway.offer(enc.buf, enc.limit(0x6000L + request, 1 + (request % 5), request, 1, true, ONE / 10, ONE,
                        TimeInForce.GTC, 0L));
            }
            rig.await(() -> gateway.in.acks >= before + n, 60_000, () -> "acks " + gateway.in.acks);
            final long head = rig.awaitLeader(null).service.engine().getEventSeq();
            rig.await(() -> rig.live().stream().allMatch(m -> m.service.engine().getEventSeq() == head), 60_000,
                    () -> "every member applies eventSeq " + head);
            return head;
        }
    }

    private static void awaitRecorded(final ShadowClusterRig rig, final Jars jars, final long seq) {
        rig.await(() -> rig.members.stream().allMatch(m -> jars.journal.get(m.id).recordedSeq() == seq), 60_000,
                () -> "every member records eventSeq " + seq);
    }

    /** The ranges of 1..{@code head} missing from {@code j}, as "(from,to]". */
    static List<String> missing(final TreeMap<Long, byte[]> j, final long head) {
        final List<String> out = new ArrayList<>();
        long q = 1;
        while (q <= head) {
            if (j.containsKey(q)) {
                q++;
                continue;
            }
            final long from = q - 1;
            while (q <= head && !j.containsKey(q)) {
                q++;
            }
            out.add("(" + from + "," + (q - 1) + "]");
        }
        return out;
    }

    @Test
    public void aLaggingJarLeavesItsHoleAndAWholeClusterRollLeavesTheSameHoleOnEveryMember() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(33900, "order-journal-mixed-jars")) {
            final Jars jars = new Jars(rig);
            try {
                rig.bootAll();
                final ShadowClusterRig.Member leader = rig.awaitLeader(null);
                final ShadowClusterRig.Member x = rig.followerOtherThan(leader, null);
                final ShadowClusterRig.Client admin = rig.connect(SessionClass.ADMIN, leader);
                rig.adopt(admin);
                rig.connect(SessionClass.TOOL, leader).deposits(100);
                final CoreRestoreRig.Encoders enc = new CoreRestoreRig.Encoders();
                admin.offer(enc.buf, enc.limits(0x77L, 1, 600_000L, 100_000, 1_024));
                admin.offer(enc.buf, enc.gateOpen(0x73L));
                rig.await(() -> admin.in.count(f -> f.startsWith("ADMINACK ref=73 result=ACCEPTED")) > 0, 30_000,
                        () -> "the ADMIN OPEN must be accepted");
                final Flow flow = new Flow(rig);
                flow.connect();

                // 1. every member on the journal jar
                final long a = flow.place(10);
                awaitRecorded(rig, jars, a);
                rig.snapshotAll();
                rig.forgetAll();

                // 2. one follower on the previous jar for N events, then upgraded
                jars.reboot(rig, x, false);
                flow.connect();
                final long b = flow.place(10);
                rig.snapshotAll();
                rig.forgetAll();
                jars.reboot(rig, x, true);
                final OrderJournal xj = jars.journal.get(x.id);
                System.out.println("[mixed jars] member " + x.id + " on the previous jar for (" + a + "," + b + "]; upgraded:"
                        + " holes " + xj.holeRanges() + " total " + xj.holesTotal());
                assertEquals("the lagging member declares the hole of its N events", "(" + a + "," + b + "]",
                        xj.holeRanges());
                flow.connect();
                final long c = flow.place(5);
                awaitRecorded(rig, jars, c);

                // 3. the whole cluster rolled back, M events, a snapshot, rolled forward
                rig.forgetAll();
                for (final ShadowClusterRig.Member m : rig.members) {
                    jars.reboot(rig, m, false);
                }
                flow.connect();
                final long d = flow.place(10);
                rig.snapshotAll();
                rig.forgetAll();
                for (final ShadowClusterRig.Member m : rig.members) {
                    jars.reboot(rig, m, true);
                }
                for (final ShadowClusterRig.Member m : rig.members) {
                    assertEquals("member " + m.id + " declares the roll's hole after the roll forward", "(" + c + "," + d + "]",
                            jars.journal.get(m.id).holeRanges());
                }
                flow.connect();
                final long e = flow.place(5);
                awaitRecorded(rig, jars, e);

                // what each member's archive holds
                final Map<Integer, TreeMap<Long, byte[]>> held = new TreeMap<>();
                for (final ShadowClusterRig.Member m : rig.members) {
                    final TreeMap<Long, byte[]> u = new TreeMap<>();
                    OrderJournalReplicationClusterTest.union(OrderJournalReplicationClusterTest.recordings(rig, m), u,
                            String.valueOf(m.id), false);
                    held.put(m.id, u);
                }
                final String lag = "(" + a + "," + b + "]";
                final String roll = "(" + c + "," + d + "]";
                for (final ShadowClusterRig.Member m : rig.members) {
                    final List<String> gaps = missing(held.get(m.id), e);
                    System.out.println("[mixed jars] member " + m.id + " archive holds " + held.get(m.id).size()
                            + " records of 1.." + e + ", missing " + gaps);
                    assertEquals("member " + m.id + " archive", m.id == x.id ? List.of(lag, roll) : List.of(roll), gaps);
                    for (final Map.Entry<Long, byte[]> r : held.get(m.id).entrySet()) {
                        for (final TreeMap<Long, byte[]> other : held.values()) {
                            final byte[] o = other.get(r.getKey());
                            assertTrue("a record present on two members is byte equal: journalSeq " + r.getKey(),
                                    o == null || Arrays.equals(o, r.getValue()));
                        }
                    }
                }
                System.out.println("GREEN mixed jars: N=" + (b - a) + " events on the previous jar left " + lag
                        + " on member " + x.id + " only; the whole-cluster roll with M=" + (d - c) + " events left " + roll
                        + " on every member; nothing fabricated, every present record byte equal");
            } finally {
                jars.stopAll();
            }
        }
    }
}
