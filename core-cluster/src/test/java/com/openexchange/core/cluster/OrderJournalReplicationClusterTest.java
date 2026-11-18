// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.infrastructure.InfrastructureConstants;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.generated.TimeInForce;
import com.openexchange.core.infrastructure.journal.OrderJournal;
import com.openexchange.core.infrastructure.journal.OrderJournalCapture;
import com.openexchange.core.infrastructure.journal.OrderJournalSettings;
import com.openexchange.core.infrastructure.journal.OrderJournalWriterAgent;
import com.openexchange.core.infrastructure.orderjournal.generated.MessageHeaderDecoder;
import com.openexchange.core.infrastructure.orderjournal.generated.OrderJournalEventDecoder;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.junit.Test;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.IntUnaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The order journal on every member of a real three-member core, with the production ring and writer agent on each:
 * the three journals are byte equal per journalSeq (SHA-256 over the ordered records equal), and a follower killed
 * mid-flow and restarted (log replay from its snapshot or from the start) writes a new recording that overlaps its old
 * one byte for byte and continues densely. Two independent clusters fed one log journal byte equal on all six members.
 */
public class OrderJournalReplicationClusterTest {

    private static final long ONE = 100_000_000L;
    private static final int H = MessageHeaderDecoder.ENCODED_LENGTH;

    /** The production ring and writer per member, rebuilt on every boot; an optional test-build rewrite per boot. */
    static final class Wiring {
        final OrderJournalSettings settings;
        final Map<Integer, OrderJournal> journal = new HashMap<>();
        final Map<Integer, AgentRunner> writer = new HashMap<>();
        final Map<Integer, Integer> boots = new HashMap<>();
        java.util.function.BiFunction<Integer, Integer, IntUnaryOperator> rewrite; // (memberId, boot) -> rewrite

        Wiring(final ShadowClusterRig rig) {
            this(rig, OrderJournalSettings.defaults());
        }

        Wiring(final ShadowClusterRig rig, final OrderJournalSettings settings) {
            this.settings = settings;
            for (final ShadowClusterRig.Member m : rig.members) {
                m.beforeLaunch = svc -> {
                    final OrderJournal j = new OrderJournal(settings);
                    final int boot = boots.merge(m.id, 1, Integer::sum);
                    if (rewrite != null) {
                        OrderJournalCapture.liquidityRewrite(j, rewrite.apply(m.id, boot));
                    }
                    journal.put(m.id, j);
                    svc.setOrderJournal(j, settings);
                };
                final java.util.function.Consumer<ShadowClusterRig.Member> prior = m.afterConfig;
                m.afterConfig = mm -> {
                    if (prior != null) {
                        prior.accept(mm);
                    }
                    mm.cfg.archiveReplayCap(settings); // the archive cap from the instance the service reports
                };
                m.beforeContainer = mm -> {
                    final AgentRunner w = new AgentRunner(new BackoffIdleStrategy(), Throwable::printStackTrace, null,
                            new OrderJournalWriterAgent(journal.get(mm.id), mm.cfg.aeronArchiveContext().clone()::clone,
                                    settings.channel(), InfrastructureConstants.ORDER_JOURNAL_STREAM_ID, 1_000_000L));
                    writer.put(mm.id, w);
                    AgentRunner.startOnThread(w);
                };
            }
        }

        void stop(final int memberId) {
            CloseHelper.quietClose(writer.remove(memberId));
        }

        void stopAll() {
            new ArrayList<>(writer.keySet()).forEach(this::stop);
        }
    }

    /** One member's journal: its recordings, each a map journalSeq -> record bytes. */
    static List<TreeMap<Long, byte[]>> recordings(final ShadowClusterRig rig, final ShadowClusterRig.Member m) {
        return recordings(rig, m, java.util.Set.of());
    }

    /** As above, leaving out the recordings {@code skip} names (recordings copied from another cluster's archive). */
    static List<TreeMap<Long, byte[]>> recordings(final ShadowClusterRig rig, final ShadowClusterRig.Member m,
                                                  final java.util.Set<Long> skip) {
        final io.aeron.archive.client.AeronArchive archive = rig.archive(m);
        final List<long[]> recs = new ArrayList<>();
        archive.listRecordingsForUri(0, 1_000, "aeron:ipc", InfrastructureConstants.ORDER_JOURNAL_STREAM_ID,
                (c, k, recordingId, st, sp, startPosition, stopPosition, i, t, se, mm, sid, str, ch, oc, src) ->
                {
                    if (!skip.contains(recordingId)) {
                        recs.add(new long[] {recordingId, startPosition, stopPosition});
                    }
                });
        final List<TreeMap<Long, byte[]>> out = new ArrayList<>();
        final com.openexchange.cluster.shadow.ShadowReplay replay = new com.openexchange.cluster.shadow.ShadowReplay(
                rig.aeron(m), archive, org.agrona.concurrent.SystemNanoClock.INSTANCE);
        for (final long[] r : recs) {
            final long stop = r[2] == io.aeron.archive.client.AeronArchive.NULL_POSITION
                    ? archive.getRecordingPosition(r[0]) : r[2];
            final TreeMap<Long, byte[]> seqs = new TreeMap<>();
            if (stop > r[1]) {
                replay.readAll(r[0], r[1], stop, (buffer, offset, length, h) -> {
                    final byte[] b = new byte[length];
                    buffer.getBytes(offset, b);
                    seqs.put(buffer.getLong(offset + 8, java.nio.ByteOrder.LITTLE_ENDIAN), b);
                }, System.nanoTime() + 30_000_000_000L);
            }
            out.add(seqs);
        }
        return out;
    }

    /** The union of a member's recordings; the overlap of its recordings must be byte equal. Returns the overlap count. */
    static long union(final List<TreeMap<Long, byte[]>> recs, final TreeMap<Long, byte[]> into, final String member) {
        return union(recs, into, member, true);
    }

    /** As above; {@code dense} false leaves holes to the caller (a journal that legitimately has them). */
    static long union(final List<TreeMap<Long, byte[]>> recs, final TreeMap<Long, byte[]> into, final String member,
                      final boolean dense) {
        long overlap = 0;
        for (final TreeMap<Long, byte[]> r : recs) {
            for (final Map.Entry<Long, byte[]> e : r.entrySet()) {
                final byte[] prev = into.putIfAbsent(e.getKey(), e.getValue());
                if (prev != null) {
                    overlap++;
                    if (!Arrays.equals(prev, e.getValue())) {
                        throw new AssertionError("replayed record differs from the first write: member " + member
                                + " journalSeq " + e.getKey());
                    }
                }
            }
        }
        if (!dense) {
            return overlap;
        }
        long expect = into.firstKey();
        for (final long q : into.keySet()) {
            assertEquals("member " + member + " journal dense from its first seq", expect, q);
            expect = q + 1;
        }
        return overlap;
    }

    static String sha256(final TreeMap<Long, byte[]> j) throws Exception {
        final MessageDigest d = MessageDigest.getInstance("SHA-256");
        j.values().forEach(d::update);
        return HexFormat.of().formatHex(d.digest());
    }

    private static void story(final int port, final String name,
                              final java.util.function.BiFunction<Integer, Integer, IntUnaryOperator> rewrite) throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(port, name)) {
            final Wiring w = new Wiring(rig);
            w.rewrite = rewrite;
            try {
                rig.bootAll();
                final ShadowClusterRig.Member leader = rig.awaitLeader(null);
                final ShadowClusterRig.Member f = rig.followerOtherThan(leader, null);
                final ShadowClusterRig.Member g = rig.followerOtherThan(leader, f);
                final ShadowClusterRig.Client admin = rig.connect(SessionClass.ADMIN, g);
                rig.adopt(admin);
                final ShadowClusterRig.Client tool = rig.connect(SessionClass.TOOL, g);
                tool.deposits(100);
                final CoreRestoreRig.Encoders enc = new CoreRestoreRig.Encoders();
                admin.offer(enc.buf, enc.limits(0x77L, 1, 600_000L, 100_000, 1_024));
                admin.offer(enc.buf, enc.gateOpen(0x73L));
                rig.await(() -> admin.in.count(x -> x.startsWith("ADMINACK ref=73 result=ACCEPTED")) > 0, 30_000,
                        () -> "the ADMIN OPEN must be accepted; frames " + admin.in.frames);
                final ShadowClusterRig.Client gateway = rig.connect(SessionClass.GATEWAY, g);
                CoreRestoreRig.register(gateway.session, SessionClass.GATEWAY, false, false, rig.sessions());
                long request = 1;
                for (int i = 0; i < 20; i++, request++) {
                    gateway.offer(enc.buf, enc.limit(0x6000L + request, 1 + (request % 5), request, 1, true, ONE / 10, ONE,
                            TimeInForce.GTC, 0L));
                }
                rig.await(() -> gateway.in.acks >= 20, 30_000, () -> "acks " + gateway.in.acks);
                rig.snapshotAll(); // the follower restores from a snapshot below what it has journaled
                final long atSnapshot = f.service.engine().getEventSeq();
                // mid-flow: offer, kill the follower once it has journaled past its snapshot, before every ack
                for (int i = 0; i < 20; i++, request++) {
                    gateway.offer(enc.buf, enc.limit(0x6000L + request, 1 + (request % 5), request, 1, true, ONE / 10, ONE,
                            TimeInForce.GTC, 0L));
                }
                rig.await(() -> w.journal.get(f.id).recordedSeq() > atSnapshot + 4, 30_000,
                        () -> "the follower journals past its snapshot " + atSnapshot);
                w.stop(f.id);
                rig.kill(f);
                rig.await(() -> gateway.in.acks >= 40, 60_000, () -> "acks after the kill " + gateway.in.acks);
                rig.restart(f);
                rig.awaitReady(f, 120_000);
                for (int i = 0; i < 10; i++, request++) {
                    gateway.offer(enc.buf, enc.limit(0x6000L + request, 1 + (request % 5), request, 1, true, ONE / 10, ONE,
                            TimeInForce.GTC, 0L));
                }
                rig.await(() -> gateway.in.acks >= 50, 60_000, () -> "acks after the restart " + gateway.in.acks);
                final long head = leader.service.engine().getEventSeq();
                rig.await(() -> rig.members.stream().allMatch(m -> w.journal.get(m.id).recordedSeq() == head), 60_000,
                        () -> "every member records eventSeq " + head);

                final Map<Integer, TreeMap<Long, byte[]>> journals = new TreeMap<>();
                long overlap = 0;
                for (final ShadowClusterRig.Member m : rig.members) {
                    final TreeMap<Long, byte[]> u = new TreeMap<>();
                    final long o = union(recordings(rig, m), u, String.valueOf(m.id));
                    if (m.id == f.id) {
                        overlap = o;
                    }
                    assertEquals("member " + m.id + " journals from 1", 1L, (long) u.firstKey());
                    assertEquals("member " + m.id + " journals to the head", head, (long) u.lastKey());
                    journals.put(m.id, u);
                }
                assertTrue("INCONCLUSIVE: the restarted member's recordings do not overlap", overlap > 0);
                final TreeMap<Long, byte[]> first = journals.get(leader.id);
                for (final Map.Entry<Integer, TreeMap<Long, byte[]>> e : journals.entrySet()) {
                    for (final Map.Entry<Long, byte[]> r : first.entrySet()) {
                        if (!Arrays.equals(r.getValue(), e.getValue().get(r.getKey()))) {
                            throw new AssertionError("order journal differs between members: member " + e.getKey()
                                    + " journalSeq " + r.getKey());
                        }
                    }
                    assertEquals("SHA-256 over the ordered records", sha256(first), sha256(e.getValue()));
                }
                final OrderJournalEventDecoder d = new OrderJournalEventDecoder();
                d.wrapAndApplyHeader(new org.agrona.concurrent.UnsafeBuffer(first.firstEntry().getValue()), 0,
                        new MessageHeaderDecoder());
                System.out.println("GREEN order journal replication: 3 members byte equal 1.." + head + " sha256="
                        + sha256(first) + "; follower " + f.id + " restarted, overlap " + overlap + " records byte equal;"
                        + " epoch " + d.epoch() + "; record bytes " + (first.firstEntry().getValue().length - H + H));
            } finally {
                w.stopAll();
            }
        }
    }

    @Test
    public void threeMembersJournalByteEqualAndARestartedFollowerOverlapsByteForByte() throws Exception {
        story(31300, "order-journal-replication", null);
    }

    /**
     * Two independent three-member clusters fed one log: cluster A takes the flow; its members are stopped and their
     * directories copied to cluster B (other directories, other ports, its own drivers, archives and writers), which
     * boots with no snapshot and replays A's log from the start. The journals B writes (its new recordings only, never
     * the copied ones) and A's are byte equal on all six members per journalSeq, 1 to the head, SHA-256 equal.
     */
    @Test
    public void twoClustersFedOneLogJournalByteEqualOnAllSixMembers() throws Exception {
        twoClusters(32700, "order-journal-two", null);
    }

    /** Control: a member-local value (the member id) in the unused high bits of liquidity. */
    @Test
    public void controlAMemberLocalValueMakesTheJournalsDiffer() throws Exception {
        try {
            twoClusters(33500, "order-journal-two-member", (rigPort, member, boot) -> l -> l | ((member + 1) << 2));
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(), expected.getMessage().startsWith("order journal differs between members"));
            System.out.println("[order-journal two clusters] control: " + expected.getMessage());
            return;
        }
        fail("the member-local value passed the comparison");
    }

    /**
     * Control: a cluster-local value (from the rig's port base) in the unused high bits of liquidity. The three members
     * of each cluster agree with each other; only the comparison across the two clusters can see the difference.
     */
    @Test
    public void controlAClusterLocalValueMakesTheTwoClustersDiffer() throws Exception {
        try {
            twoClusters(35400, "order-journal-two-cluster", (rigPort, member, boot) -> l -> l | ((rigPort / 100 % 16) << 2));
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(),
                    expected.getMessage().startsWith("order journal differs between members: member B0"));
            System.out.println("[order-journal two clusters] cluster-local control: " + expected.getMessage());
            return;
        }
        fail("the cluster-local value passed the comparison");
    }

    /** A test-build liquidity rewrite per cluster (its rig's port base), member and boot. */
    @FunctionalInterface
    interface ClusterRewrite {
        IntUnaryOperator apply(int rigPort, int member, int boot);
    }

    private static void twoClusters(final int port, final String name, final ClusterRewrite rewrite)
            throws Exception {
        final java.nio.file.Path saved = java.nio.file.Files.createTempDirectory(name + "-log-");
        final Map<String, TreeMap<Long, byte[]>> journals = new TreeMap<>();
        final Map<Integer, java.util.Set<Long>> copiedRecordings = new HashMap<>();
        long head = -1;
        try {
            try (ShadowClusterRig a = new ShadowClusterRig(port, name + "-a")) {
                final Wiring w = new Wiring(a);
                w.rewrite = rewrite == null ? null : (member, boot) -> rewrite.apply(a.portBase, member, boot);
                try {
                    a.bootAll();
                    final ShadowClusterRig.Member leader = a.awaitLeader(null);
                    final ShadowClusterRig.Member g = a.followerOtherThan(leader, null);
                    final ShadowClusterRig.Client admin = a.connect(SessionClass.ADMIN, g);
                    a.adopt(admin);
                    a.connect(SessionClass.TOOL, g).deposits(100);
                    final CoreRestoreRig.Encoders enc = new CoreRestoreRig.Encoders();
                    admin.offer(enc.buf, enc.limits(0x77L, 1, 600_000L, 100_000, 1_024));
                    admin.offer(enc.buf, enc.gateOpen(0x73L));
                    a.await(() -> admin.in.count(x -> x.startsWith("ADMINACK ref=73 result=ACCEPTED")) > 0, 30_000,
                            () -> "the ADMIN OPEN must be accepted; frames " + admin.in.frames);
                    final ShadowClusterRig.Client gateway = a.connect(SessionClass.GATEWAY, g);
                    CoreRestoreRig.register(gateway.session, SessionClass.GATEWAY, false, false, a.sessions());
                    for (long request = 1; request <= 40; request++) {
                        gateway.offer(enc.buf, enc.limit(0x6000L + request, 1 + (request % 5), request, 1, true,
                                ONE / 10 + request, ONE, TimeInForce.GTC, 0L));
                    }
                    a.await(() -> gateway.in.acks >= 40, 60_000, () -> "acks " + gateway.in.acks);
                    head = leader.service.engine().getEventSeq();
                    final long h = head;
                    a.await(() -> a.members.stream().allMatch(m -> w.journal.get(m.id).recordedSeq() == h), 60_000,
                            () -> "every member of cluster A records eventSeq " + h);
                    for (final ShadowClusterRig.Member m : a.members) {
                        final java.util.Set<Long> ids = new java.util.HashSet<>();
                        a.archive(m).listRecordingsForUri(0, 1_000, "aeron:ipc",
                                InfrastructureConstants.ORDER_JOURNAL_STREAM_ID,
                                (c, k, recordingId, st, sp, s0, s1, i, t, se, mm, sid, str, ch, oc, src) -> ids.add(recordingId));
                        copiedRecordings.put(m.id, ids);
                        final TreeMap<Long, byte[]> u = new TreeMap<>();
                        union(recordings(a, m, java.util.Set.of()), u, "A" + m.id);
                        journals.put("A" + m.id, u);
                    }
                    a.forgetAll();
                } finally {
                    w.stopAll();
                }
                for (final ShadowClusterRig.Member m : a.members) {
                    a.kill(m);
                }
                for (final ShadowClusterRig.Member m : a.members) {
                    copyTree(m.baseDir.toPath(), saved.resolve(m.baseDir.getName()));
                }
            }
            try (ShadowClusterRig b = new ShadowClusterRig(port + 200, name + "-b")) {
                for (final ShadowClusterRig.Member m : b.members) {
                    copyTree(saved.resolve(m.baseDir.getName()), m.baseDir.toPath());
                }
                final Wiring w = new Wiring(b);
                w.rewrite = rewrite == null ? null : (member, boot) -> rewrite.apply(b.portBase, member, boot);
                try {
                    b.bootAll();
                    b.awaitLeader(null);
                    final long h = head;
                    b.await(() -> b.members.stream().allMatch(m -> w.journal.get(m.id).recordedSeq() == h
                            && m.service.engine().getEventSeq() == h), 120_000,
                            () -> "every member of cluster B replays A's log to eventSeq " + h);
                    for (final ShadowClusterRig.Member m : b.members) {
                        final TreeMap<Long, byte[]> u = new TreeMap<>();
                        final List<TreeMap<Long, byte[]>> own = recordings(b, m, copiedRecordings.get(m.id));
                        assertTrue("cluster B member " + m.id + " wrote its own recording", !own.isEmpty());
                        union(own, u, "B" + m.id);
                        journals.put("B" + m.id, u);
                    }
                } finally {
                    w.stopAll();
                }
            }
        } finally {
            deleteTree(saved);
        }
        final TreeMap<Long, byte[]> first = journals.get("A0");
        for (final Map.Entry<String, TreeMap<Long, byte[]>> e : journals.entrySet()) {
            assertEquals("member " + e.getKey() + " journals from 1", 1L, (long) e.getValue().firstKey());
            assertEquals("member " + e.getKey() + " journals to the head", head, (long) e.getValue().lastKey());
            for (final Map.Entry<Long, byte[]> r : first.entrySet()) {
                if (!Arrays.equals(r.getValue(), e.getValue().get(r.getKey()))) {
                    throw new AssertionError("order journal differs between members: member " + e.getKey()
                            + " journalSeq " + r.getKey() + " differs from member A0");
                }
            }
            assertEquals("SHA-256 over the ordered records of member " + e.getKey(), sha256(first), sha256(e.getValue()));
        }
        System.out.println("GREEN order journal two clusters fed one log: members " + journals.keySet() + " byte equal 1.."
                + head + " sha256=" + sha256(first));
    }

    private static void copyTree(final java.nio.file.Path from, final java.nio.file.Path to) throws java.io.IOException {
        try (java.util.stream.Stream<java.nio.file.Path> walk = java.nio.file.Files.walk(from)) {
            for (final java.nio.file.Path p : (Iterable<java.nio.file.Path>) walk::iterator) {
                final java.nio.file.Path target = to.resolve(from.relativize(p).toString());
                if (java.nio.file.Files.isDirectory(p)) {
                    java.nio.file.Files.createDirectories(target);
                } else {
                    java.nio.file.Files.copy(p, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }

    private static void deleteTree(final java.nio.file.Path root) throws java.io.IOException {
        if (!java.nio.file.Files.exists(root)) {
            return;
        }
        try (java.util.stream.Stream<java.nio.file.Path> walk = java.nio.file.Files.walk(root)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    /** Control: a process-start nonce (the boot count) in the unused high bits of liquidity. */
    @Test
    public void controlAProcessStartNonceMakesTheReplayDiffer() throws Exception {
        try {
            story(31700, "order-journal-replication-nonce", (member, boot) -> l -> l | (boot << 2));
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(), expected.getMessage().startsWith("replayed record differs from the first write"));
            return;
        }
        fail("the nonce passed the comparison");
    }
}
