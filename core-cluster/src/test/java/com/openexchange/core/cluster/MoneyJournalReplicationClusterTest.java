// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.domain.MoneyJournalSink;
import com.openexchange.core.infrastructure.InfrastructureConstants;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.journal.MoneyJournal;
import com.openexchange.core.infrastructure.journal.MoneyJournalWriterAgent;
import com.openexchange.core.infrastructure.journal.generated.JournalDepositDecoder;
import com.openexchange.core.infrastructure.journal.generated.MessageHeaderDecoder;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The money journal (version 2) on every member of a real three-member core, with the production ring and writer
 * agent on each: every record carries the log position of the entry that produced it (a frame end of the committed
 * log), and the members' journals are byte equal per journalSeq.
 *
 * <p>Control: {@code -Dmoney.journal.control=member-local} wires one member's sink to write a member-local value in
 * the log position (the position plus the member id); the test then fails with
 * {@code money journal differs between members}.
 */
public class MoneyJournalReplicationClusterTest {

    private static final int DEPOSITS = 30;

    @Test
    public void everyMemberJournalsTheSameBytesWithTheLogPositionOfEachEntry() throws Exception {
        final boolean memberLocal = "member-local".equals(System.getProperty("money.journal.control"));
        try (ShadowClusterRig rig = new ShadowClusterRig(28100, "money-journal-v2")) {
            final Map<Integer, AgentRunner> writers = new HashMap<>();
            for (final ShadowClusterRig.Member m : rig.members) {
                final MoneyJournal[] ring = {null};
                m.beforeLaunch = svc -> {
                    ring[0] = new MoneyJournal(16 * 1024 * 1024);
                    svc.setMoneyJournalSink(memberLocal && m.id == 2 ? memberLocal(ring[0], m.id) : ring[0]);
                };
                m.beforeContainer = mm -> {
                    final AgentRunner w = new AgentRunner(new BackoffIdleStrategy(), Throwable::printStackTrace, null,
                            new MoneyJournalWriterAgent(ring[0], mm.cfg.aeronArchiveContext().clone()::clone,
                                    InfrastructureConstants.MONEY_JOURNAL_CHANNEL,
                                    InfrastructureConstants.MONEY_JOURNAL_STREAM_ID));
                    writers.put(mm.id, w);
                    AgentRunner.startOnThread(w);
                };
            }
            try {
                rig.bootAll();
                final ShadowClusterRig.Member leader = rig.awaitLeader(null);
                final ShadowClusterRig.Member f = rig.followerOtherThan(leader, null);
                final ShadowClusterRig.Client admin = rig.connect(SessionClass.ADMIN, f);
                rig.adopt(admin);
                rig.armMoneyJournal(admin);
                rig.connect(SessionClass.TOOL, f).deposits(DEPOSITS);
                rig.await(() -> rig.members.stream().allMatch(m -> journal(rig, m).size() == DEPOSITS), 60_000,
                        () -> "every member journals " + DEPOSITS + " records");

                final Set<Long> committed = new HashSet<>();
                Arrays.stream(rig.captureCommitted(leader).positions()).forEach(committed::add);
                final Map<Integer, TreeMap<Long, byte[]>> journals = new HashMap<>();
                for (final ShadowClusterRig.Member m : rig.members) {
                    journals.put(m.id, journal(rig, m));
                }
                final ShadowClusterRig.Member first = rig.members.get(0);
                for (final ShadowClusterRig.Member m : rig.members) {
                    for (final Map.Entry<Long, byte[]> e : journals.get(first.id).entrySet()) {
                        if (!Arrays.equals(e.getValue(), journals.get(m.id).get(e.getKey()))) {
                            throw new AssertionError("money journal differs between members: journalSeq " + e.getKey()
                                    + " member " + first.id + " " + Arrays.toString(e.getValue()) + " member " + m.id
                                    + " " + Arrays.toString(journals.get(m.id).get(e.getKey())));
                        }
                    }
                }
                long previous = 0;
                final List<Long> positions = new ArrayList<>();
                for (final byte[] r : journals.get(first.id).values()) {
                    final UnsafeBuffer b = new UnsafeBuffer(r);
                    final MessageHeaderDecoder h = new MessageHeaderDecoder().wrap(b, 0);
                    assertEquals("version 2 on every record", 2, h.version());
                    assertEquals(JournalDepositDecoder.TEMPLATE_ID, h.templateId());
                    final long position = new JournalDepositDecoder().wrapAndApplyHeader(b, 0, h).logPosition();
                    assertTrue("journalSeq order is log order: " + position + " after " + previous, position > previous);
                    assertTrue("the log position " + position + " is a frame end of the committed log",
                            committed.contains(position));
                    positions.add(position);
                    previous = position;
                }
                System.out.println("money journal v2: " + DEPOSITS + " records byte equal on " + rig.members.size()
                        + " members; log positions " + positions.get(0) + ".." + positions.get(positions.size() - 1));
            } finally {
                writers.values().forEach(CloseHelper::quietClose);
            }
        }
    }

    /** One member's money journal: the union of its recordings, journalSeq -> record bytes. */
    private static TreeMap<Long, byte[]> journal(final ShadowClusterRig rig, final ShadowClusterRig.Member m) {
        final io.aeron.archive.client.AeronArchive archive = rig.archive(m);
        final List<long[]> recs = new ArrayList<>();
        archive.listRecordingsForUri(0, 1_000, "aeron:ipc", InfrastructureConstants.MONEY_JOURNAL_STREAM_ID,
                (c, k, recordingId, st, sp, startPosition, stopPosition, i, t, se, mm, sid, str, ch, oc, src) ->
                        recs.add(new long[] {recordingId, startPosition, stopPosition}));
        final TreeMap<Long, byte[]> out = new TreeMap<>();
        final com.openexchange.cluster.shadow.ShadowReplay replay = new com.openexchange.cluster.shadow.ShadowReplay(
                rig.aeron(m), archive, org.agrona.concurrent.SystemNanoClock.INSTANCE);
        for (final long[] r : recs) {
            final long stop = r[2] == io.aeron.archive.client.AeronArchive.NULL_POSITION
                    ? archive.getRecordingPosition(r[0]) : r[2];
            if (stop > r[1]) {
                replay.readAll(r[0], r[1], stop, (buffer, offset, length, h) -> {
                    final byte[] b = new byte[length];
                    buffer.getBytes(offset, b);
                    out.put(buffer.getLong(offset + MessageHeaderDecoder.ENCODED_LENGTH, ByteOrder.LITTLE_ENDIAN), b);
                }, System.nanoTime() + 30_000_000_000L);
            }
        }
        return out;
    }

    /** Test build: a sink that writes a member-local value in the log position. */
    private static MoneyJournalSink memberLocal(final MoneyJournalSink d, final int memberId) {
        return new MoneyJournalSink() {
            @Override
            public void onOpeningBalance(final long seq, final long userId, final int assetId, final long amount,
                                         final long logPosition) {
                d.onOpeningBalance(seq, userId, assetId, amount, logPosition + memberId);
            }

            @Override
            public void onDeposit(final long seq, final long userId, final int assetId, final long amount,
                                  final long balanceAfter, final long clusterTimeMs, final long logPosition) {
                d.onDeposit(seq, userId, assetId, amount, balanceAfter, clusterTimeMs, logPosition + memberId);
            }

            @Override
            public void onWithdraw(final long seq, final long userId, final int assetId, final long amount,
                                   final long balanceAfter, final long clusterTimeMs, final long logPosition) {
                d.onWithdraw(seq, userId, assetId, amount, balanceAfter, clusterTimeMs, logPosition + memberId);
            }

            @Override
            public void onSettle(final long seq, final long tradeId, final int marketId, final long price,
                                 final long quantity, final long buyerUserId, final long sellerUserId,
                                 final boolean takerIsBuy, final long buyerBaseAfter, final long buyerQuoteAfter,
                                 final long sellerBaseAfter, final long sellerQuoteAfter, final long clusterTimeMs,
                                 final long logPosition) {
                d.onSettle(seq, tradeId, marketId, price, quantity, buyerUserId, sellerUserId, takerIsBuy,
                        buyerBaseAfter, buyerQuoteAfter, sellerBaseAfter, sellerQuoteAfter, clusterTimeMs,
                        logPosition + memberId);
            }
        };
    }
}
