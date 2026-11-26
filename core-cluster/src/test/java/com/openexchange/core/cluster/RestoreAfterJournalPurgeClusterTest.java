// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.cluster.journal.JournalReaderTool;
import com.openexchange.cluster.journal.JournalRetention;
import com.openexchange.cluster.journal.JournalRetentionAgent;
import com.openexchange.cluster.testkit.RecordedLog;
import com.openexchange.core.infrastructure.InfrastructureConstants;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.generated.TimeInForce;
import com.openexchange.core.infrastructure.journal.OrderJournal;
import com.openexchange.core.infrastructure.journal.OrderJournalSettings;
import com.openexchange.core.infrastructure.journal.OrderJournalTail;
import com.openexchange.core.infrastructure.journal.OrderJournalWriterAgent;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import com.openexchange.core.infrastructure.persistence.SnapshotModeSettings;
import io.aeron.archive.client.AeronArchive;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * T10. A member's own restore rules after its order journal was purged. A single-member core journals 1700 order
 * events in 64 KiB segments, its retention cuts the recording behind a reader at the head, the journal writer stops
 * (the recording is now a stopped recording whose every record is applied), and the member is killed and restarted
 * through each restore path: a SKIP, a TAIL, and its own full image. The tail the restore rules read
 * ({@link OrderJournalTail}) is the same after the retention's passes as before them, so a SKIP that was legal is
 * still taken and no restore declares an order journal hole.
 *
 * <p>Control: with tail cover off the retention deletes the stopped recording that carries the tail and the real
 * tail reader's answer falls ({@code journal tail fell from <a> to <b>}).
 */
public class RestoreAfterJournalPurgeClusterTest {

    private static final int SEG = 64 * 1024;
    private static final long GENESIS = ShadowClusterRig.GENESIS;
    private static final int USERS = 300;

    private enum Path { SKIP, TAIL, FULL_IMAGE }

    @Before
    public void settings() {
        System.setProperty("journal.retention.order", "readers");
        System.setProperty("journal.retention.poll.ms", "1000");
    }

    @After
    public void clear() {
        System.clearProperty("journal.retention.order");
        System.clearProperty("journal.retention.poll.ms");
        JournalRetention.Seams.reset();
    }

    /** The production ring and writer agent with a 64 KiB term, rebuilt on every boot of the member. */
    private static final class Wiring {
        final OrderJournalSettings settings = OrderJournalSettings.with(
                Map.of(OrderJournalSettings.Key.CORE_ORDER_JOURNAL_TERM_LENGTH, "64k"));
        final List<OrderJournal> boots = new ArrayList<>();
        AgentRunner writer;

        Wiring(final StubStory s) {
            s.m.beforeLaunch = svc -> {
                final OrderJournal j = new OrderJournal(settings);
                boots.add(j);
                svc.setOrderJournal(j, settings);
            };
            s.m.beforeContainer = m -> {
                writer = new AgentRunner(new BackoffIdleStrategy(), Throwable::printStackTrace, null,
                        new OrderJournalWriterAgent(journal(), m.cfg.aeronArchiveContext().clone()::clone,
                                settings.channel(), InfrastructureConstants.ORDER_JOURNAL_STREAM_ID, 1_000_000L));
                AgentRunner.startOnThread(writer);
            };
        }

        OrderJournal journal() {
            return boots.get(boots.size() - 1);
        }

        void stopWriter() {
            CloseHelper.quietClose(writer);
        }
    }

    private static long eventSeq(final StubStory s) {
        return s.m.service.engine().getEventSeq();
    }

    private static void awaitDurable(final StubStory s, final Wiring w, final String when) {
        s.rig.await(() -> w.journal().recordedSeq() == eventSeq(s), 60_000, () -> when + ": the order journal never"
                + " recorded eventSeq " + eventSeq(s) + " (recorded " + w.journal().recordedSeq() + ")");
    }

    /** {id, start, end, active} of every order journal recording of the member. */
    private static List<long[]> recordings(final StubStory s) {
        final AeronArchive archive = s.rig.archive(s.m);
        final List<long[]> out = new ArrayList<>();
        archive.listRecordingsForUri(0, 1_000, "aeron:ipc", InfrastructureConstants.ORDER_JOURNAL_STREAM_ID,
                (c, k, recordingId, st, sp, startPosition, stopPosition, i, t, se, mm, sid, str, ch, oc, src) ->
                        out.add(new long[] {recordingId, startPosition, stopPosition, 0}));
        for (final long[] r : out) {
            if (r[2] == AeronArchive.NULL_POSITION) {
                r[2] = archive.getRecordingPosition(r[0]);
                r[3] = 1;
            }
        }
        return out;
    }

    /** The tail the member's restore rules read, by the reader they read it with; -1 when there is no journal. */
    private static long restoreTail(final StubStory s) {
        final OrderJournalTail.Result r = new OrderJournalTail(s.rig.aeron(s.m), s.rig.archive(s.m),
                SystemNanoClock.INSTANCE, GENESIS).read(20_000_000_000L);
        if (r instanceof OrderJournalTail.Known known) {
            return known.seq();
        }
        if (r instanceof OrderJournalTail.NoJournal) {
            return -1;
        }
        throw new AssertionError("the order journal tail is unreadable: " + r);
    }

    private static void setReader(final StubStory s, final long applied) {
        final int exit = JournalReaderTool.run(new String[] {"set", "--cluster-dir", s.m.clusterDir().getAbsolutePath(),
                "--engine", "core", "--cluster-id", String.valueOf(s.m.cfg.consensusModuleContext().clusterId()),
                "--genesis-id", String.valueOf(GENESIS), "--journal", "order", "--reader", "oe-projector",
                "--applied-journal-seq", String.valueOf(applied), "--source", "test"}, System.out, System.err,
                System::currentTimeMillis);
        assertEquals("JournalReaderTool set", 0, exit);
    }

    private static void awaitPasses(final StubStory s, final int passes) {
        final JournalRetentionAgent agent = ServiceProbe.journalRetention(s.m.service);
        final long[] last = {agent.status("order").lastPassMs};
        final int[] seen = {0};
        s.rig.await(() -> {
            final long now = agent.status("order").lastPassMs;
            if (now != last[0]) {
                last[0] = now;
                seen[0]++;
            }
            return seen[0] >= passes;
        }, 60_000, () -> passes + " retention passes");
    }

    /** {@code n} more resting orders on a gateway session, ten per user as {@link OrderLoad} places them. */
    private static void place(final StubStory s, final ShadowClusterRig.Client gw, final int first, final int n) {
        final CoreRestoreRig.Encoders enc = new CoreRestoreRig.Encoders();
        final int before = gw.in.acks + gw.in.rejects;
        for (int k = 0; k < n; k++) {
            final int i = first - 1 + k;
            final long user = 1 + i / OrderLoad.ORDERS_PER_USER;
            final int slot = i % OrderLoad.ORDERS_PER_USER;
            final boolean buy = slot % 2 == 0;
            final int market = CoreRestoreRig.marketOf(user);
            CoreRestoreRig.offer(gw.session, enc.buf, enc.limit(i + 1, user, i + 1, market, buy,
                    CoreRestoreRig.priceOf(market, buy, slot / 2), ShadowClusterRig.ONE / 10, TimeInForce.GTC, 0L),
                    s.rig.sessions());
        }
        s.rig.await(() -> gw.in.acks + gw.in.rejects - before >= n, 120_000, "every create must be answered");
        assertEquals("no create refused: " + gw.in.firstReject, 0, gw.in.rejects);
    }

    private static void story(final int port, final Path path) throws Exception {
        final SnapshotModeSettings mode = path == Path.FULL_IMAGE ? StubStory.off() : StubStory.shadow();
        try (StubStory s = new StubStory(port, "cjr-restore-" + path, mode)) {
            final Wiring w = new Wiring(s);
            try {
                s.start(false);
                final OrderLoad load = OrderLoad.prepare(s.rig, s.m, USERS);
                load.orders(s.rig, 1, 1_600, TimeInForce.GTC, 0L);
                awaitDurable(s, w, "the first load");
                // the retention cuts the active recording behind a reader at the head
                setReader(s, eventSeq(s));
                s.rig.await(() -> recordings(s).get(0)[1] == (recordings(s).get(0)[2] & -(long) SEG) - 2L * SEG, 60_000,
                        () -> "the order journal start never rose: " + java.util.Arrays.toString(recordings(s).get(0)));
                final long purgedTo = recordings(s).get(0)[1];
                assertTrue("the journal was purged before the restore: start " + purgedTo, purgedTo > 0);

                // what each restore path needs
                long q = -1;
                long pS = -1;
                if (path != Path.FULL_IMAGE) {
                    final RecordedLog early = s.log();
                    q = early.positions()[early.positions().length - 1];
                    s.produce(early, q); // a TAIL base below P_cm
                }
                load.orders(s.rig, 1_601, 50, TimeInForce.GTC, 0L);
                final long pCm = s.snapshot();
                load.orders(s.rig, 1_651, 50, TimeInForce.GTC, 0L);
                if (path == Path.SKIP) {
                    final RecordedLog log = s.log();
                    pS = log.positions()[log.positions().length - 1];
                    assertTrue("P_s " + pS + " > P_cm " + pCm, pS > pCm);
                    s.produce(log, pS); // a SKIP base above P_cm
                }
                awaitDurable(s, w, "before the writer stops");
                final long head = eventSeq(s);
                assertEquals(1_700, head);
                // the writer stops: the recording is a stopped recording now, every record of it applied
                w.stopWriter();
                s.rig.await(() -> recordings(s).get(0)[3] == 0, 30_000, "the recording stops");
                setReader(s, head);
                final long tailBefore = restoreTail(s);
                assertEquals("the journal is durable to the head", head, tailBefore);
                awaitPasses(s, 3);
                final long tailAfter = restoreTail(s);
                if (tailAfter < tailBefore) {
                    throw new AssertionError("journal tail fell from " + tailBefore + " to " + tailAfter);
                }
                final List<long[]> afterPurge = recordings(s);
                assertEquals("the recording that carries the tail stays", 1, afterPurge.size());
                assertEquals("it is cut to its last segment", (afterPurge.get(0)[2] - 1) & -(long) SEG,
                        afterPurge.get(0)[1]);

                // kill and restart through the path
                s.rig.forgetAll();
                s.kill();
                s.restart();
                s.awaitReady();
                assertTrue(s.m.halts.toString(), s.m.halts.isEmpty());
                final String restored = String.valueOf(s.pathLine()).trim();
                if (path == Path.SKIP) {
                    final String abandoned = StubStory.line(s.rig.tee.lines, "SKIP abandoned: order journal behind");
                    if (abandoned != null || !restored.contains("restored path=SKIP P_s=" + pS + " ")) {
                        throw new AssertionError("a SKIP that was legal before the purge was not taken: " + restored
                                + " (" + abandoned + ")");
                    }
                } else if (path == Path.TAIL) {
                    assertTrue("TAIL from the base: " + restored, restored.contains("restored path=TAIL P_s=" + q + " "));
                }
                final String hole = StubStory.line(s.rig.errTee.lines, "CRITICAL order journal hole");
                if (hole != null || w.journal().holesTotal() != 0) {
                    throw new AssertionError("CRITICAL order journal hole after a restore over a purged journal: " + hole
                            + " (holes " + w.journal().holeRanges() + ")");
                }
                assertTrue("no hole on /health: " + ServiceProbe.probeDetail(s.m.service),
                        ServiceProbe.probeDetail(s.m.service).contains("holes=none"));

                // the journal goes on: more orders, the reader follows, the old recording goes whole
                final ShadowClusterRig.Client gw = s.rig.connect(SessionClass.GATEWAY, s.m);
                CoreRestoreRig.register(gw.session, SessionClass.GATEWAY, false, false, s.rig.sessions());
                place(s, gw, 1_701, 100);
                awaitDurable(s, w, "after the restart");
                final long newHead = eventSeq(s);
                assertEquals(1_800, newHead);
                setReader(s, newHead);
                s.rig.await(() -> recordings(s).stream().filter(r -> r[2] > r[1]).count() == 1
                        && recordings(s).stream().anyMatch(r -> r[3] == 1 && r[2] > r[1]), 60_000,
                        () -> "the stopped recording goes once the new one covers it: " + recordings(s).stream()
                                .map(java.util.Arrays::toString).toList());
                assertEquals(newHead, restoreTail(s));
                final TreeMap<Long, Long> seqs = OrderJournalRestoreClusterTest.journal(s);
                long expect = seqs.firstKey();
                for (final long seq : seqs.keySet()) {
                    assertEquals("the journal is dense from its start", expect, seq);
                    expect = seq + 1;
                }
                assertEquals(newHead, (long) seqs.lastKey());
                System.out.println("GREEN T10 " + path + ": purged to " + purgedTo + " before the restore; " + restored
                        + "; tail " + tailBefore + " kept; journal " + seqs.firstKey() + ".." + seqs.lastKey()
                        + " dense after it");
            } finally {
                w.stopWriter();
            }
        }
    }

    @Test
    public void aSkipThatWasLegalBeforeThePurgeIsStillTaken() throws Exception {
        story(37300, Path.SKIP);
    }

    @Test
    public void aTailRestoreAfterThePurgeDeclaresNoHole() throws Exception {
        story(37400, Path.TAIL);
    }

    @Test
    public void aFullImageRestoreAfterThePurgeDeclaresNoHole() throws Exception {
        story(37500, Path.FULL_IMAGE);
    }

    /** Control: without tail cover the retention deletes the recording the restore rules read their tail from. */
    @Test
    public void controlTailCoverOffMakesTheRestoreTailFall() throws Exception {
        JournalRetention.Seams.tailCoverOff = true;
        try {
            story(37600, Path.SKIP);
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(), expected.getMessage().startsWith("journal tail fell from 1700 to "));
            System.out.println("NC T10 tail cover off -> " + expected.getMessage());
            return;
        } finally {
            JournalRetention.Seams.reset();
        }
        fail("negative control did not fail: the restore tail did not fall with tail cover off");
    }
}
