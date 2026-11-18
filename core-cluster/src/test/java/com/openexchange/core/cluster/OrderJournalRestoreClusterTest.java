// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.cluster.testkit.RecordedLog;
import com.openexchange.core.infrastructure.InfrastructureConstants;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.generated.TimeInForce;
import com.openexchange.core.infrastructure.journal.OrderJournal;
import com.openexchange.core.infrastructure.journal.OrderJournalSettings;
import com.openexchange.core.infrastructure.journal.OrderJournalWriterAgent;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The order journal against the state a member restores, in a real single-member core (its own driver, archive and
 * consensus module), with the production ring and writer agent. A member's journal is put behind its state by
 * stopping the writer agent and placing orders: their records stay in the ring and die with the kill.
 *
 * <ul>
 *   <li>SKIP: a SKIP candidate whose image eventSeq the local journal does not hold is abandoned
 *   ({@code SKIP abandoned: order journal behind}); the TAIL below replays the log and regenerates the records, so the
 *   member's journal has no hole. Twice in one run. Control {@code skipSafeOff}: SKIP is taken and the hole stays.</li>
 *   <li>Every other restore (the member's own full image, an EQUAL shadow): the hole (D, S] is declared once,
 *   counted and shown in the probe detail; the member is ready and its journal continues at S+1. A snapshot taken while
 *   the writer is stopped completes and the member is not latched. Control: detection off leaves the hole undeclared.</li>
 * </ul>
 */
public class OrderJournalRestoreClusterTest {

    private static final long ONE = 100_000_000L;

    /** The production ring and writer agent, rebuilt on every boot of the member. */
    private static final class Wiring {
        final OrderJournalSettings settings = OrderJournalSettings.defaults();
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
                        new OrderJournalWriterAgent(journal(), m.cfg.aeronArchiveContext().clone()::clone, settings.channel(),
                                InfrastructureConstants.ORDER_JOURNAL_STREAM_ID, 1_000_000L));
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

    /** A GATEWAY session placing resting limit orders (no ME: each stays pending; each create is order events). */
    private static final class Orders {
        final StubStory s;
        final CoreRestoreRig.Encoders enc = new CoreRestoreRig.Encoders();
        ShadowClusterRig.Client gateway;
        long request = 1;

        Orders(final StubStory s) {
            this.s = s;
        }

        void open() throws InterruptedException {
            s.admin.offer(enc.buf, enc.limits(0x77L, 1, 600_000L, 100_000, 1_024));
            s.admin.offer(enc.buf, enc.gateOpen(0x73L));
            s.rig.await(() -> s.admin.in.count(f -> f.startsWith("ADMINACK ref=73 result=ACCEPTED")) > 0, 30_000,
                    () -> "the ADMIN OPEN must be accepted; frames " + s.admin.in.frames);
            s.deposits(100);
            connect();
        }

        void connect() throws InterruptedException {
            gateway = s.rig.connect(SessionClass.GATEWAY, s.m);
            CoreRestoreRig.register(gateway.session, SessionClass.GATEWAY, false, false, s.rig.sessions());
        }

        void place(final int n) {
            final int before = gateway.in.acks;
            for (int i = 0; i < n; i++) {
                final long r = request++;
                gateway.offer(enc.buf, enc.limit(0x6000L + r, 1 + (r % 5), r, 1, true, ONE / 10, ONE, TimeInForce.GTC, 0L));
            }
            s.rig.await(() -> gateway.in.acks >= before + n, 30_000,
                    () -> "every create must be acked: acks " + gateway.in.acks + " first reject " + gateway.in.firstReject);
        }
    }

    private static long eventSeq(final StubStory s) {
        return s.m.service.engine().getEventSeq();
    }

    private static void awaitDurable(final StubStory s, final Wiring w, final String when) {
        s.rig.await(() -> w.journal().recordedSeq() == eventSeq(s), 60_000,
                () -> when + ": the order journal never recorded eventSeq " + eventSeq(s) + " (recorded "
                        + w.journal().recordedSeq() + ")");
    }

    /** Every record in this member's archive on the order journal stream: journalSeq -> epoch. */
    static TreeMap<Long, Long> journal(final StubStory s) {
        return journal(s, 0L);
    }

    /** As above, only the records whose epoch is {@code epoch} (0: every record). */
    static TreeMap<Long, Long> journal(final StubStory s, final long epoch) {
        final io.aeron.archive.client.AeronArchive archive = s.rig.archive(s.m);
        final List<long[]> recs = new ArrayList<>();
        archive.listRecordingsForUri(0, 1_000, "aeron:ipc", InfrastructureConstants.ORDER_JOURNAL_STREAM_ID,
                (c, k, recordingId, st, sp, startPosition, stopPosition, i, t, se, m, sid, str, ch, oc, src) ->
                        recs.add(new long[] {recordingId, startPosition, stopPosition}));
        final TreeMap<Long, Long> seqs = new TreeMap<>();
        final com.openexchange.cluster.shadow.ShadowReplay replay = new com.openexchange.cluster.shadow.ShadowReplay(
                s.rig.aeron(s.m), archive, org.agrona.concurrent.SystemNanoClock.INSTANCE);
        for (final long[] r : recs) {
            final long stop = r[2] == io.aeron.archive.client.AeronArchive.NULL_POSITION
                    ? archive.getRecordingPosition(r[0]) : r[2];
            if (stop <= r[1]) {
                continue;
            }
            replay.readAll(r[0], r[1], stop, (buffer, offset, length, h) -> {
                final long e = buffer.getLong(offset + 16, java.nio.ByteOrder.LITTLE_ENDIAN);
                if (epoch == 0 || e == epoch) {
                    seqs.put(buffer.getLong(offset + 8, java.nio.ByteOrder.LITTLE_ENDIAN), e);
                }
            }, System.nanoTime() + 30_000_000_000L);
        }
        return seqs;
    }

    static void assertNoHole(final TreeMap<Long, Long> seqs, final long from, final long to, final String text) {
        for (long q = from; q <= to; q++) {
            if (!seqs.containsKey(q)) {
                throw new AssertionError(text + ": missing journalSeq " + q + " (recorded " + seqs.firstKey() + ".."
                        + seqs.lastKey() + ", " + seqs.size() + " records)");
            }
        }
    }

    private static void restartAndReconnect(final StubStory s, final Orders o, final String... faults)
            throws InterruptedException {
        s.rig.forgetAll();
        s.kill();
        s.restart(faults);
        s.awaitReady();
        assertTrue(s.m.halts.toString(), s.m.halts.isEmpty());
        s.connect(false);
        o.connect();
    }

    // ------------------------------------------------------------------------------------------ SKIP

    private static void skipRow(final int port, final boolean skipSafeOff) throws Exception {
        try (StubStory s = new StubStory(port, "order-journal-skip-" + skipSafeOff, StubStory.shadow())) {
            final Wiring w = new Wiring(s);
            try {
                s.start(true);
                final Orders o = new Orders(s);
                o.open();
                for (int round = 1; round <= 2; round++) {
                    o.place(5);
                    final RecordedLog early = s.log();
                    final long q = early.positions()[early.positions().length - 1];
                    s.produce(early, q); // the TAIL base below P_cm
                    o.place(5);
                    final long pCm = s.snapshot();
                    awaitDurable(s, w, "round " + round + " before the writer stops");
                    final long durable = w.journal().recordedSeq();
                    w.stopWriter();
                    o.place(5);
                    final RecordedLog log = s.log();
                    final long pS = log.positions()[log.positions().length - 1];
                    assertTrue("P_s " + pS + " > P_cm " + pCm, pS > pCm);
                    s.produce(log, pS);
                    final long seqAtPs = eventSeq(s);
                    assertTrue("events past the durable journal: " + seqAtPs + " > " + durable, seqAtPs > durable);
                    System.out.println("[order-journal skip] round " + round + " P_cm=" + pCm + " durable=" + durable + " P_s="
                            + pS + " eventSeq=" + seqAtPs);
                    if (skipSafeOff) {
                        restartAndReconnect(s, o, "skipSafeOff");
                    } else {
                        restartAndReconnect(s, o);
                    }
                    o.place(3);
                    awaitDurable(s, w, "round " + round + " after the restart");
                    final TreeMap<Long, Long> seqs = journal(s);
                    if (skipSafeOff) {
                        assertTrue("the control took SKIP: " + s.pathLine(), s.pathLine() != null
                                && s.pathLine().contains("restored path=SKIP P_s=" + pS + " "));
                        try {
                            assertNoHole(seqs, 1, eventSeq(s), "order journal hole after SKIP");
                        } catch (final AssertionError expected) {
                            assertTrue(expected.getMessage(), expected.getMessage().startsWith("order journal hole after SKIP"));
                            System.out.println("[order-journal skip] control failed as expected: " + expected.getMessage());
                            return;
                        }
                        fail("the control left no hole after SKIP");
                    }
                    final String abandoned = StubStory.line(s.rig.tee.lines, "SKIP abandoned: order journal behind");
                    if (abandoned == null || s.pathLine() == null || !s.pathLine().contains("restored path=TAIL P_s=" + q + " ")) {
                        throw new AssertionError("order journal hole after SKIP: SKIP was not abandoned for the order journal ("
                                + abandoned + "); path " + s.pathLine());
                    }
                    assertNoHole(seqs, 1, eventSeq(s), "order journal hole after SKIP");
                    seqs.values().forEach(e -> assertEquals("every record of one genesis", seqs.firstEntry().getValue(), e));
                    System.out.println("GREEN order journal SKIP round " + round + ": " + abandoned.trim() + "; "
                            + s.pathLine().trim() + "; journal 1.." + seqs.lastKey() + " dense");
                }
            } finally {
                w.stopWriter();
            }
        }
    }

    @Test
    public void aSkipOverAnOrderJournalBehindTheImageIsAbandonedAndTheJournalHasNoHoleTwice() throws Exception {
        skipRow(29300, false);
    }

    @Test
    public void controlSkipSafeOffLeavesAnOrderJournalHoleAfterSkip() throws Exception {
        skipRow(29400, true);
    }

    /**
     * A member whose only restore is the SKIP the order journal refuses: no TAIL base below P_cm, a log purged past 0
     * (no virtual genesis), no pull source. Nothing can replay the log, so the SKIP is taken and the hole (D, S] is
     * declared; the member boots and its journal continues at S+1. Without that it halts on every boot and its journal
     * never heals.
     */
    @Test
    public void aSkipOnlyMemberOnAPurgedLogTakesTheSkipAndDeclaresTheHole() throws Exception {
        try (StubStory s = new StubStory(34800, "order-journal-skip-only", StubStory.shadow())) {
            final Wiring w = new Wiring(s);
            try {
                s.start(true);
                final Orders o = new Orders(s);
                o.open();
                o.place(5);
                s.deposits(900); // past the first log segment, so the purge below leaves a log that starts above 0
                final long pCm = s.snapshot();
                final long boundary = pCm / StubStory.SEGMENT * StubStory.SEGMENT;
                assertTrue("P_cm " + pCm + " lies past the first segment", boundary > 0);
                awaitDurable(s, w, "before the writer stops");
                final long d = w.journal().recordedSeq();
                w.stopWriter();
                o.place(5);
                final RecordedLog log = s.log();
                final long pS = log.positions()[log.positions().length - 1];
                assertTrue("P_s " + pS + " > P_cm " + pCm, pS > pCm);
                s.produce(log, pS); // the only shadow: a SKIP base above P_cm
                final long sSeq = eventSeq(s);
                assertTrue("events past the durable journal: " + sSeq + " > " + d, sSeq > d);
                final long start = s.rig.purgeLogTo(s.m, boundary);
                assertTrue("log starts above 0: " + start, start > 0);
                s.rig.forgetAll();
                s.kill();
                s.restart();
                s.rig.await(() -> !s.m.halts.isEmpty() || s.pathLine() != null, 120_000,
                        () -> "neither a restore nor a halt; stderr " + s.rig.errTee.lines);
                if (!s.m.halts.isEmpty()) {
                    throw new AssertionError("a member whose only restore is a SKIP over an order journal behind it never"
                            + " boots: " + StubStory.line(s.rig.errTee.lines, "FATAL"));
                }
                s.awaitReady();
                assertTrue("SKIP taken: " + s.pathLine(), s.pathLine().contains("restored path=SKIP P_s=" + pS + " "));
                final OrderJournal j = w.journal();
                if (j.holesTotal() != 1 || !j.holeRanges().equals("(" + d + "," + sSeq + "]")) {
                    throw new AssertionError("order journal hole not declared: holes=" + j.holesTotal() + " ranges "
                            + j.holeRanges() + ", expected (" + d + "," + sSeq + "]; path " + s.pathLine());
                }
                assertTrue("ready with a hole", ServiceProbe.probe(s.m.service).ready());
                s.connect(false);
                o.connect();
                o.place(2);
                awaitDurable(s, w, "after the restart");
                assertNoHole(journal(s), sSeq + 1, eventSeq(s), "the journal continues at S+1");
                System.out.println("GREEN order journal SKIP-only: " + StubStory.line(s.rig.tee.lines,
                        "no path replays the log") + "; " + s.pathLine().trim() + "; holes " + j.holeRanges());
            } finally {
                w.stopWriter();
            }
        }
    }

    // ------------------------------------------------------------------------------------------ re-genesis

    private static final long REGENESIS = 0x5EEEL;

    /**
     * Same-archive re-genesis: the previous genesis journals more records than the new one will, the member's cluster
     * directory is wiped and its archive kept (the previous genesis's recordings stay on the order journal stream), a
     * new genesis is ADOPTed, and a SKIP candidate lies above the new genesis's journal. The tail reader ignores the
     * previous genesis (counted as foreign), so the SKIP is abandoned although the old recordings hold a higher
     * sequence; the TAIL replays the log and the new genesis's journal is dense. Control: the epoch filter off.
     */
    private static void regenesisRow(final int port, final boolean epochFilterOff) throws Exception {
        ServiceProbe.orderJournalEpochFilterOff(epochFilterOff);
        try (StubStory s = new StubStory(port, "order-journal-regenesis-" + epochFilterOff, StubStory.shadow())) {
            final Wiring w = new Wiring(s);
            try {
                s.start(true);
                Orders o = new Orders(s);
                o.open();
                o.place(30);
                awaitDurable(s, w, "the previous genesis");
                final long previousHead = eventSeq(s);
                s.rig.forgetAll();
                w.stopWriter();
                s.kill();
                org.agrona.IoUtil.delete(s.m.clusterDir(), false); // the archive and its recordings stay
                s.rig.adoptGenesis = REGENESIS;
                s.restart();
                s.rig.awaitLeader(null);
                s.connect(true);
                o = new Orders(s);
                o.open();
                o.place(5);
                final RecordedLog early = s.log();
                final long q = early.positions()[early.positions().length - 1];
                s.produce(early, q); // the TAIL base below P_cm
                o.place(5);
                final long pCm = s.snapshot();
                awaitDurable(s, w, "the new genesis before the writer stops");
                final long durable = w.journal().recordedSeq();
                w.stopWriter();
                o.place(5);
                final RecordedLog log = s.log();
                final long pS = log.positions()[log.positions().length - 1];
                assertTrue("P_s " + pS + " > P_cm " + pCm, pS > pCm);
                s.produce(log, pS);
                final long seqAtPs = eventSeq(s);
                assertTrue("events past the durable journal: " + seqAtPs + " > " + durable, seqAtPs > durable);
                assertTrue("INCONCLUSIVE: the previous genesis's recordings must hold a higher sequence than the image ("
                        + previousHead + " > " + seqAtPs + ")", previousHead > seqAtPs);
                System.out.println("[order-journal regenesis] previous genesis journaled 1.." + previousHead + "; new genesis "
                        + REGENESIS + " durable " + durable + ", image eventSeq " + seqAtPs + " at P_s=" + pS);
                restartAndReconnect(s, o);
                if (s.pathLine() != null && s.pathLine().contains("restored path=SKIP P_s=" + pS + " ")) {
                    throw new AssertionError("skip-safe passed on a previous genesis: image eventSeq " + seqAtPs
                            + ", new genesis journal at " + durable + ", previous genesis at " + previousHead + "; "
                            + s.pathLine().trim());
                }
                final String abandoned = StubStory.line(s.rig.tee.lines, "SKIP abandoned: order journal behind");
                assertTrue("the SKIP was abandoned for the order journal: " + abandoned + "; path " + s.pathLine(),
                        abandoned != null && s.pathLine() != null
                                && s.pathLine().contains("restored path=TAIL P_s=" + q + " "));
                final java.util.regex.Matcher foreign = java.util.regex.Pattern.compile("foreign=(\\d+)").matcher(abandoned);
                assertTrue("the tail counted the previous genesis as foreign: " + abandoned,
                        foreign.find() && Long.parseLong(foreign.group(1)) >= 1);
                o.place(3);
                awaitDurable(s, w, "after the restart");
                final TreeMap<Long, Long> seqs = journal(s, REGENESIS);
                assertNoHole(seqs, 1, eventSeq(s), "order journal hole after SKIP");
                assertTrue("the previous genesis's records are still in the archive",
                        journal(s, ShadowClusterRig.GENESIS).size() >= previousHead);
                System.out.println("GREEN order journal re-genesis: " + abandoned.trim() + "; " + s.pathLine().trim()
                        + "; new genesis journal 1.." + seqs.lastKey() + " dense");
            } finally {
                w.stopWriter();
            }
        } finally {
            ServiceProbe.orderJournalEpochFilterOff(false);
        }
    }

    @Test
    public void aReGenesisOnTheSameArchiveNeverMakesASkipLookSafe() throws Exception {
        regenesisRow(34200, false);
    }

    @Test
    public void controlEpochFilterOffPassesTheSkipOnAPreviousGenesis() throws Exception {
        try {
            regenesisRow(34400, true);
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(), expected.getMessage().startsWith("skip-safe passed on a previous genesis"));
            System.out.println("[order-journal regenesis] control: " + expected.getMessage());
            return;
        }
        fail("the control abandoned the SKIP: the unfiltered tail did not see the previous genesis");
    }

    // ------------------------------------------------------------------------------------------ other restores

    /** A snapshot that takes longer than this waited on something; a single-member snapshot here takes ~150 ms. */
    private static final long SNAPSHOT_AT_ONCE_MS = 2_000;

    private static void holeRow(final int port, final boolean shadowEqual, final boolean detectionOff,
                                final boolean snapshotWaits) throws Exception {
        final String name = "order-journal-hole-" + (shadowEqual ? "equal" : "full") + (detectionOff ? "-off" : "")
                + (snapshotWaits ? "-wait" : "");
        ServiceProbe.orderJournalDetectionOff(detectionOff);
        ServiceProbe.snapshotWaitsOnOrderJournal(snapshotWaits ? 2 * SNAPSHOT_AT_ONCE_MS : 0);
        try (StubStory s = new StubStory(port, name, shadowEqual ? StubStory.shadow() : StubStory.off())) {
            final Wiring w = new Wiring(s);
            try {
                s.start(true);
                final Orders o = new Orders(s);
                o.open();
                final List<String> ranges = new ArrayList<>();
                for (int round = 1; round <= 2; round++) {
                    o.place(5);
                    awaitDurable(s, w, "round " + round + " before the writer stops");
                    final long d = w.journal().recordedSeq();
                    w.stopWriter();
                    o.place(5);
                    final long sSeq = eventSeq(s);
                    final long t0 = System.nanoTime();
                    final long pCm = s.snapshot();
                    final long snapshotMs = (System.nanoTime() - t0) / 1_000_000;
                    if (snapshotMs >= SNAPSHOT_AT_ONCE_MS
                            || ServiceProbe.readiness(s.m.service).contains("application-callback-failed")) {
                        throw new AssertionError("snapshot waited on the order journal: snapshot_ms=" + snapshotMs
                                + " (writer stopped), readiness " + ServiceProbe.readiness(s.m.service));
                    }
                    if (shadowEqual) {
                        s.produce(s.log(), pCm); // an EQUAL shadow at P_cm
                    }
                    final long aboveImage = 2;
                    o.place((int) aboveImage); // the leader keeps committing with the writer stopped
                    System.out.println("[order-journal hole] round " + round + " D=" + d + " S=" + sSeq + " P_cm=" + pCm
                            + " snapshot_ms=" + snapshotMs);
                    restartAndReconnect(s, o);
                    final OrderJournal j = w.journal();
                    if (j.holesTotal() != 1 || !j.holeRanges().equals("(" + d + "," + sSeq + "]")) {
                        throw new AssertionError("order journal hole not declared: holes=" + j.holesTotal() + " ranges "
                                + j.holeRanges() + ", expected (" + d + "," + sSeq + "]; path " + s.pathLine());
                    }
                    final String detail = ServiceProbe.probe(s.m.service).detail();
                    assertTrue("the range in the probe detail: " + detail, detail.contains("holes=(" + d + "," + sSeq + "]"));
                    assertTrue("ready with a hole: " + detail, ServiceProbe.probe(s.m.service).ready());
                    if (shadowEqual) {
                        assertTrue("EQUAL path: " + s.pathLine(), s.pathLine() != null
                                && s.pathLine().contains("restored path=EQUAL"));
                    }
                    ranges.add(j.holeRanges());
                    o.place(2);
                    awaitDurable(s, w, "round " + round + " after the restart");
                    final TreeMap<Long, Long> seqs = journal(s);
                    assertNoHole(seqs, sSeq + 1, eventSeq(s), "the journal continues at S+1");
                }
                assertEquals("two holes, two ranges: " + ranges, 2, ranges.size());
                assertTrue("two different ranges: " + ranges, !ranges.get(0).equals(ranges.get(1)));
                System.out.println("GREEN order journal hole (" + (shadowEqual ? "EQUAL shadow" : "full image") + "): " + ranges);
            } finally {
                w.stopWriter();
                ServiceProbe.orderJournalDetectionOff(false);
                ServiceProbe.snapshotWaitsOnOrderJournal(0);
            }
        }
    }

    @Test
    public void aFullImageRestoreAboveTheJournalDeclaresTheHoleTwice() throws Exception {
        holeRow(29500, false, false, false);
    }

    @Test
    public void anEqualShadowRestoreAboveTheJournalDeclaresTheHoleTwice() throws Exception {
        holeRow(29600, true, false, false);
    }

    @Test
    public void controlDetectionOffLeavesTheHoleUndeclared() throws Exception {
        try {
            holeRow(29700, false, true, false);
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(), expected.getMessage().startsWith("order journal hole not declared"));
            return;
        }
        fail("the control declared the hole");
    }

    /** Control: a test build whose snapshot waits for the order journal to be recorded (the writer is stopped). */
    @Test
    public void controlASnapshotThatWaitsOnTheJournalIsCaught() throws Exception {
        try {
            holeRow(34700, false, false, true);
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(), expected.getMessage().startsWith("snapshot waited on the order journal"));
            System.out.println("[order-journal hole] control: " + expected.getMessage());
            return;
        }
        fail("the control's snapshot did not wait");
    }
}
