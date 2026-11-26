// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.cluster.journal.JournalReaders;
import com.openexchange.cluster.journal.JournalRetention;
import com.openexchange.core.cluster.RetentionStory.Pusher;
import com.openexchange.core.cluster.RetentionStory.Reader;
import com.openexchange.core.cluster.RetentionStory.Rec;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import org.junit.Test;

import java.util.List;
import java.util.TreeMap;

import static com.openexchange.core.cluster.RetentionStory.MONEY_SEG;
import static com.openexchange.core.cluster.RetentionStory.ORDER_FRAME;
import static com.openexchange.core.cluster.RetentionStory.SEG;
import static com.openexchange.core.cluster.RetentionStory.money;
import static com.openexchange.core.cluster.RetentionStory.order;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Journal retention on a real three-member core (embedded drivers): each member purges its own journal recordings
 * behind a reader that really reads them, from cursor files a pusher delivers one step late. Nothing here calls a
 * pass: the tests write reader files and wait for the archives' start positions.
 */
public class JournalRetentionClusterTest {

    private static long expectedStart(final long end, final int segment) {
        return Math.max(0, (end & -(long) segment) - 2L * segment);
    }

    /**
     * T1. Order flow in rounds. After each round, once the reader has applied the head and its cursor is delivered,
     * every member's order journal recording starts two whole segments below its head segment, so the start rises
     * round over round and what a member retains is at most three segments.
     */
    @Test
    public void theOrderJournalStartRisesOnEveryMemberBehindARealReader() throws Exception {
        try (RetentionStory s = new RetentionStory(36300, "cjr-order", "journal.retention.order", "readers",
                "journal.retention.poll.ms", "1000").boot(600)) {
            final Reader reader = s.new Reader(order(), "oe-projector");
            // registered on every member before the reader first starts
            s.setAll(order(), "oe-projector", 0);
            // member g's file is updated one step later than the others'
            final Pusher pusher = s.new Pusher(reader, s.g);
            final long[] lastStart = new long[3];
            for (int round = 0; round < 6; round++) {
                s.orders(1_000);
                final long head = s.orderHead();
                final int r = round;
                s.pump(List.of(reader), List.of(pusher), 1_000, () -> reader.applied == head
                        && s.rig.members.stream().allMatch(m -> {
                            final Rec rec = s.active(m, order());
                            return rec.end() == head * ORDER_FRAME && rec.start() == expectedStart(rec.end(), SEG);
                        }), 120_000, () -> "round " + r + ": the order journal start position did not reach two"
                        + " segments below the head " + head + " on every member (reader at " + reader.applied
                        + "): " + s.describe(order()));
                for (final ShadowClusterRig.Member m : s.rig.members) {
                    final Rec rec = s.active(m, order());
                    assertTrue("round " + round + " member " + m.id + ": the start position rises (" + lastStart[m.id]
                            + " -> " + rec.start() + ")", rec.start() > lastStart[m.id]);
                    lastStart[m.id] = rec.start();
                    final long retained = s.retained(m, order());
                    assertTrue("round " + round + " member " + m.id + ": retained " + retained + " bytes with the"
                            + " reader at the head: at most three segments", retained <= 3L * SEG);
                }
            }
            final long head = s.orderHead();
            assertEquals("the reader applied every record, one after the other", head, reader.applied);
            for (final ShadowClusterRig.Member m : s.rig.members) {
                // what the member says about itself agrees with its archive
                s.pump(List.of(), List.of(pusher), 1_000, () -> s.metric(m, order(), "safe_seq") == head, 30_000,
                        () -> "member " + m.id + " safe_seq " + s.metric(m, order(), "safe_seq"));
                final Rec rec = s.active(m, order());
                assertEquals(JournalRetention.State.VALID.code(), s.metric(m, order(), "state"));
                assertEquals(JournalRetention.Mode.READERS.code(), s.metric(m, order(), "mode"));
                assertEquals(head, s.metric(m, order(), "tail_seq"));
                assertEquals(rec.bytes(), s.metric(m, order(), "retained_bytes"));
                assertEquals(1, s.metric(m, order(), "recordings"));
                assertEquals(0, s.metric(m, order(), "recordings_foreign"));
                assertEquals(rec.start(), s.metric(m, order(), "purged_bytes_total"));
                assertTrue(ServiceProbe.metrics(m.service).contains(
                        "core_journal_retention_reader_applied_seq{journal=\"order\",reader=\"oe-projector\"} " + head));
                assertEquals("the money journal's mode is unset: off", JournalRetention.State.OFF.code(),
                        s.metric(m, money(), "state"));
                assertFalse("a member that purges says nothing on /health: " + ServiceProbe.probeDetail(m.service),
                        ServiceProbe.probeDetail(m.service).contains("journal-retention"));
                // every record above the purged range is still there, and nothing below the start is
                TreeMap<Long, Integer> held;
                do {
                    held = s.held(m, order());
                } while (held == null);
                assertEquals(rec.start() / ORDER_FRAME + 1, (long) held.firstKey());
                assertEquals(head, (long) held.lastKey());
                assertEquals(head - rec.start() / ORDER_FRAME, held.size());
            }
            System.out.println("GREEN T1 order journal retention: head " + head + ", starts " + s.describe(order()));
        }
    }

    /**
     * T2. The money journal's reader is registered while the journal is dark, and the journal is armed seconds
     * later: from then on it is purged like any journal. Its term is 4 MiB, so its segments are 4 MiB.
     */
    @Test
    public void theMoneyJournalArmedAfterItsReaderWasRegisteredIsPurgedToo() throws Exception {
        try (RetentionStory s = new RetentionStory(36500, "cjr-money", "journal.retention.money", "readers",
                "journal.retention.poll.ms", "1000").boot(50)) {
            final Reader reader = s.new Reader(money(), "money-projector");
            s.setAll(money(), "money-projector", 0);
            final Pusher pusher = s.new Pusher(reader, s.f);
            // the journal is armed seconds after its reader was registered
            final long armAt = System.currentTimeMillis() + 2_500;
            s.pump(List.of(reader), List.of(pusher), 1_000, () -> System.currentTimeMillis() >= armAt, 10_000, () -> "");
            s.armMoneyJournal();
            // 132k deposits of 96 bytes cross the third 4 MiB segment: one segment can go
            final int deposits = 3 * MONEY_SEG / 96 + 1_000;
            int done = 0;
            while (done < deposits) {
                s.deposits(12_000);
                done += 12_000;
                s.pump(List.of(reader), List.of(pusher), 1_000, () -> true, 1_000, () -> "");
            }
            final long head = s.moneyHead();
            assertTrue("the money journal journaled the deposits: " + head, head >= deposits);
            s.pump(List.of(reader), List.of(pusher), 1_000, () -> reader.applied == head
                    && s.rig.members.stream().allMatch(m -> {
                        final Rec rec = s.active(m, money());
                        return rec.start() > 0 && rec.start() == expectedStart(rec.end(), MONEY_SEG);
                    }), 180_000, () -> "the money journal start position did not rise on every member (head " + head
                    + ", reader at " + reader.applied + "): " + s.describe(money()));
            for (final ShadowClusterRig.Member m : s.rig.members) {
                final Rec rec = s.active(m, money());
                assertTrue("member " + m.id + " retains at most three segments: " + rec, rec.bytes() <= 3L * MONEY_SEG);
                s.pump(List.of(), List.of(pusher), 1_000, () -> s.metric(m, money(), "safe_seq") == head, 30_000,
                        () -> "member " + m.id + " safe_seq " + s.metric(m, money(), "safe_seq"));
                assertEquals(JournalRetention.State.VALID.code(), s.metric(m, money(), "state"));
                assertEquals(head, s.metric(m, money(), "tail_seq"));
                assertEquals(rec.start(), s.metric(m, money(), "purged_bytes_total"));
            }
            assertEquals(head, reader.applied);
            System.out.println("GREEN T2 money journal retention: head " + head + ", " + s.describe(money()));
        }
    }

    /** A journal that is not armed is no journal: nothing to do and no alarm, with its reader registered or not. */
    @Test
    public void aDarkMoneyJournalIsNoJournalAndNoAlarm() throws Exception {
        try (RetentionStory s = new RetentionStory(36600, "cjr-dark", "journal.retention.money", "readers",
                "journal.retention.poll.ms", "1000").boot(10)) {
            s.rig.await(() -> s.rig.members.stream().allMatch(m -> s.status(m, money()).report != null), 30_000,
                    "a first pass on every member");
            s.rig.pumpMs(2_500);
            s.setAll(money(), "money-projector", 0);
            s.rig.pumpMs(2_500);
            for (final ShadowClusterRig.Member m : s.rig.members) {
                assertEquals("member " + m.id + " dark", JournalRetention.State.NO_JOURNAL.code(),
                        s.metric(m, money(), "state"));
                assertEquals(0, s.metric(m, money(), "retained_bytes"));
                assertFalse(ServiceProbe.probeDetail(m.service).contains("journal-retention"));
            }
            assertFalse("no alarm for a journal that is not armed: " + s.lines, s.said("CRITICAL"));
        }
    }

    /**
     * T13. A reader that died holds the journal on every member: its cursor stays, the retained bytes pass the
     * alarm and both alarms are printed. An operator's release moves the bound and is said on every pass; a set
     * that arrives later, at or above the release, clears it. Before any reader is registered the journal is held
     * too, and the member says so on /health without leaving /ready.
     */
    @Test
    public void aDeadReaderHoldsWithAlarmsUntilAnOperatorReleasesIt() throws Exception {
        try (RetentionStory s = new RetentionStory(36700, "cjr-dead", "journal.retention.order", "readers",
                "journal.retention.poll.ms", "1000", "journal.retention.alarm.bytes", "1048576",
                "journal.retention.stale.ms", "2000").boot(600)) {
            // no reader at all: held, CRITICAL, on /health, still ready
            s.orders(300);
            s.pump(List.of(), List.of(), 1_000, () -> s.rig.members.stream().allMatch(m ->
                    ServiceProbe.probeDetail(m.service).contains("journal-retention: journal retention order NO_READERS")),
                    30_000, () -> "NO_READERS on /health: " + s.describe(order()));
            assertTrue(s.lines.toString(), s.said("CRITICAL: no reader is registered for the order journal"));
            for (final ShadowClusterRig.Member m : s.rig.members) {
                assertTrue("a hold never fails /ready on member " + m.id, ServiceProbe.ready(m.service));
            }

            final Reader live = s.new Reader(order(), "oe-projector");
            final Reader dead = s.new Reader(order(), "audit");
            s.setAll(order(), "oe-projector", 0);
            s.setAll(order(), "audit", 0);
            dead.frozen = true; // registered, then it never applies anything
            final Pusher pusher = s.new Pusher(live, null);
            s.orders(4_500);
            final long head = s.orderHead();
            s.pump(List.of(live), List.of(pusher), 1_000, () -> live.applied == head
                    && s.said("bytes of the order journal (above JOURNAL_RETENTION_ALARM_BYTES 1048576)")
                    && s.said("a reader file of the order journal was written")
                    && s.rig.members.stream().allMatch(m -> s.status(m, order()).report != null
                            && s.status(m, order()).report.evaluation.safeSeq == 0), 120_000,
                    () -> "the alarms of a held journal: " + s.lines);
            for (final ShadowClusterRig.Member m : s.rig.members) {
                assertEquals("member " + m.id + " holds everything for the dead reader", 0, s.active(m, order()).start());
                assertTrue(s.retained(m, order()) > 1_048_576);
                assertEquals("held at the dead reader's cursor is VALID with safeSeq 0",
                        JournalRetention.State.VALID.code(), s.metric(m, order(), "state"));
                assertTrue(ServiceProbe.metrics(m.service).contains(
                        "core_journal_retention_reader_applied_seq{journal=\"order\",reader=\"audit\"} 0"));
            }

            // the operator releases the dead reader's hold up to journalSeq 3000, member by member
            for (final ShadowClusterRig.Member m : s.rig.members) {
                assertEquals(0, s.tool(m, "release", "--genesis-id", String.valueOf(RetentionStory.GENESIS), "--journal",
                        "order", "--reader", "audit", "--up-to-journal-seq", "3000", "--reason", "audit retired"));
            }
            // the greatest base whose first record is at most 3001: base 11 (journalSeq 2817)
            final long released = 11L * SEG;
            s.pump(List.of(live), List.of(pusher), 1_000, () -> s.rig.members.stream().allMatch(
                    m -> s.active(m, order()).start() == released), 60_000,
                    () -> "the start after the release: " + s.describe(order()));
            final String releaseLine = "CRITICAL: hold of reader audit of the order journal released by operator up to"
                    + " journalSeq 3000 (applied 0)";
            final long saidBefore = s.lines.stream().filter(l -> l.startsWith("[JOURNAL-RETENTION]")
                    && l.contains(releaseLine)).count();
            s.pump(List.of(live), List.of(pusher), 1_000, () -> s.lines.stream().filter(
                    l -> l.startsWith("[JOURNAL-RETENTION]") && l.contains(releaseLine)).count() >= saidBefore + 6,
                    30_000, () -> "the release is said on every pass: " + s.lines);
            for (final ShadowClusterRig.Member m : s.rig.members) {
                assertEquals(3_000, s.metric(m, order(), "safe_seq"));
                assertTrue(ServiceProbe.metrics(m.service).contains(
                        "core_journal_retention_reader_released_seq{journal=\"order\",reader=\"audit\"} 3000"));
            }

            // the reader comes back seconds later and its cursor passes the release: the release is cleared
            s.setAll(order(), "audit", 3_500);
            for (final ShadowClusterRig.Member m : s.rig.members) {
                final JournalReaders.Record file = JournalReaders.read(m.clusterDir(), "order", "audit").record;
                assertEquals(JournalReaders.NO_RELEASE, file.releasedUpToJournalSeq);
                assertEquals("", file.releaseReason);
            }
            // base 13 starts at journalSeq 3329 <= 3501
            s.pump(List.of(live), List.of(pusher), 1_000, () -> s.rig.members.stream().allMatch(
                    m -> s.active(m, order()).start() == 13L * SEG && s.metric(m, order(), "safe_seq") == 3_500),
                    60_000, () -> "the start after the late set: " + s.describe(order()));
            System.out.println("GREEN T13 dead reader: held at 0, released to 3000, cleared by a set at 3500: "
                    + s.describe(order()));
        }
    }

    /**
     * T14. A purge under a reader's slow replay: the archive refuses it, the member counts it and changes nothing;
     * once the replay has moved on the next pass purges.
     */
    @Test
    public void aPurgeUnderASlowReplayIsRefusedCountedAndDoneOnALaterPass() throws Exception {
        try (RetentionStory s = new RetentionStory(36900, "cjr-slow", "journal.retention.order", "readers",
                "journal.retention.poll.ms", "1000").boot(300)) {
            s.orders(2_600);
            final long head = s.orderHead();
            s.rig.await(() -> s.rig.members.stream().allMatch(m -> s.active(m, order()).end() == head * ORDER_FRAME),
                    60_000, () -> "every member records " + head);
            // a reader of member f that started at the recording's start and does not poll
            final String channel = "aeron:ipc?term-length=64k";
            final Subscription slow = s.rig.aeron(s.f).addSubscription(channel, 4_911);
            final long session = s.rig.archive(s.f).startReplay(s.active(s.f, order()).id(), 0, AeronArchive.NULL_LENGTH,
                    channel, 4_911);
            s.rig.await(() -> slow.imageCount() > 0, 10_000, "the slow replay connects");
            s.setAll(order(), "oe-projector", head);
            final long expected = expectedStart(head * ORDER_FRAME, SEG);
            s.rig.await(() -> s.active(s.leader, order()).start() == expected
                    && s.active(s.g, order()).start() == expected
                    && s.status(s.f, order()).purgeFailuresTotal >= 2, 60_000,
                    () -> "the other members purge, member f counts refusals: " + s.describe(order()));
            assertEquals("nothing changed under the replay", 0, s.active(s.f, order()).start());
            assertEquals(0, s.metric(s.f, order(), "purged_bytes_total"));
            assertTrue(s.metric(s.f, order(), "purge_failures_total") >= 2);
            assertEquals("a refused purge is the archive's hold, not a reader's or an input's",
                    JournalRetention.State.VALID.code(), s.metric(s.f, order(), "state"));
            assertTrue(s.lines.toString(), s.said("the archive refused to purge order journal recording"));
            assertTrue(s.lines.toString(), s.said("replay in progress"));
            assertFalse(ServiceProbe.probeDetail(s.f.service).contains("journal-retention"));
            // the replay ends: a later pass purges
            s.rig.archive(s.f).stopReplay(session);
            slow.close();
            s.rig.await(() -> s.active(s.f, order()).start() == expected
                    && s.metric(s.f, order(), "purged_bytes_total") == expected, 60_000,
                    () -> "member f purges after the replay ended: " + s.describe(order()));
            System.out.println("GREEN T14 slow replay: refused " + s.metric(s.f, order(), "purge_failures_total")
                    + " times on member " + s.f.id + ", then purged to " + expected);
        }
    }

    /** The log position (frame end of the applying entry) the order journal record {@code seq} carries on {@code m}. */
    private static long logPositionOf(final RetentionStory s, final ShadowClusterRig.Member m, final long seq) {
        final long[] position = {-1};
        new com.openexchange.cluster.shadow.ShadowReplay(s.rig.aeron(m), s.rig.archive(m),
                org.agrona.concurrent.SystemNanoClock.INSTANCE).readRange(s.active(m, order()).id(),
                (seq - 1) * ORDER_FRAME, ORDER_FRAME, (buffer, offset, length, h) -> {
                    assertEquals(seq, buffer.getLong(offset + 8, java.nio.ByteOrder.LITTLE_ENDIAN));
                    position[0] = buffer.getLong(offset + 24, java.nio.ByteOrder.LITTLE_ENDIAN);
                }, System.nanoTime() + 10_000_000_000L);
        return position[0];
    }

    /**
     * T9 in the core. With {@code RETENTION_OFFBOX=required} a member holds its journal while it has no off-box
     * watermark, and with one it purges no record whose log position is above it, although the reader has applied
     * everything.
     */
    @Test
    public void underOffBoxRequiredNoRecordAboveTheWatermarkIsPurged() throws Exception {
        try (RetentionStory s = new RetentionStory(37700, "cjr-offbox", "journal.retention.order", "readers",
                "journal.retention.poll.ms", "1000", "retention.offbox", "required").boot(300)) {
            s.orders(2_600);
            final long head = s.orderHead();
            s.rig.await(() -> s.rig.members.stream().allMatch(m -> s.active(m, order()).end() == head * ORDER_FRAME),
                    60_000, () -> "every member records " + head);
            s.setAll(order(), "oe-projector", head);
            // no watermark: a hold, named on /health, although the reader is at the head
            s.rig.await(() -> s.rig.members.stream().allMatch(m -> s.status(m, order()).report != null
                    && s.status(m, order()).report.evaluation.state == JournalRetention.State.OFFBOX_HOLD), 30_000,
                    () -> "OFFBOX_HOLD on every member: " + s.describe(order()));
            s.rig.pumpMs(2_500);
            for (final ShadowClusterRig.Member m : s.rig.members) {
                assertEquals("member " + m.id + " holds without a watermark", 0, s.active(m, order()).start());
                assertTrue(ServiceProbe.probeDetail(m.service),
                        ServiceProbe.probeDetail(m.service).contains("journal retention order OFFBOX_HOLD"));
            }
            // the off-box copy ends with the entry that produced journalSeq 1300: base 5 (journalSeq 1281) is the
            // greatest base whose first record is off-box
            final long x = logPositionOf(s, s.f, 1_300);
            assertTrue("the record carries a log position: " + x, x > 0);
            for (final ShadowClusterRig.Member m : s.rig.members) {
                OffBoxTool.set(m.clusterDir(), RetentionStory.GENESIS, x);
            }
            s.rig.await(() -> s.rig.members.stream().allMatch(m -> s.active(m, order()).start() == 5L * SEG), 60_000,
                    () -> "the start at the off-box bound: " + s.describe(order()));
            s.rig.pumpMs(2_500); // two more passes: nothing above the watermark goes
            for (final ShadowClusterRig.Member m : s.rig.members) {
                final Rec rec = s.active(m, order());
                if (rec.start() > 5L * SEG) {
                    throw new AssertionError("purged past the off-box position: member " + m.id + " start "
                            + rec.start() + ", the watermark allows " + 5L * SEG);
                }
                assertEquals(JournalRetention.State.VALID.code(), s.metric(m, order(), "state"));
            }
            // the off-box copy reaches the head: only the reader and the two-segment clamp bound the purge
            final long y = logPositionOf(s, s.f, head);
            for (final ShadowClusterRig.Member m : s.rig.members) {
                OffBoxTool.set(m.clusterDir(), RetentionStory.GENESIS, y);
            }
            final long expected = expectedStart(head * ORDER_FRAME, SEG);
            s.rig.await(() -> s.rig.members.stream().allMatch(m -> s.active(m, order()).start() == expected), 60_000,
                    () -> "the start after the watermark reached the head: " + s.describe(order()));
            System.out.println("GREEN T9 off-box required: held at 0, then " + 5L * SEG + " at watermark " + x
                    + ", then " + expected + " at " + y);
        }
    }
}
