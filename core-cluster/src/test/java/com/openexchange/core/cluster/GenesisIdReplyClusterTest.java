// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder;
import com.openexchange.core.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.core.infrastructure.generated.OrderEventsEndDecoder;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import io.aeron.cluster.client.EgressListener;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongBinaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The genesis id on the query replies (OrderEventsEnd 116, CoreConfigReport 118) in a real three-member cluster. Each
 * question goes through the log, so every member answers it in its own apply (the egress tap sees a follower's frames
 * too) and the client gets the leader's answer. Before the ADOPT every member answers 0; after it, the ADOPTed id; after
 * a member restarts from a snapshot, the same; after the leader dies, the follower that took over answers the same; and
 * every member's value is equal at every step. Fails with {@code genesisId differs between members} or
 * {@code genesisId on the wire is <got>, expected <id>}.
 */
public class GenesisIdReplyClusterTest {

    /** The client's view: correlation id to the genesisId of the reply it received. */
    private static final class Replies implements EgressListener {
        final Map<Long, Long> genesisByCorrelation = new ConcurrentHashMap<>();
        private final MessageHeaderDecoder header = new MessageHeaderDecoder();

        @Override
        public void onMessage(final long clusterSessionId, final long timestamp, final org.agrona.DirectBuffer buffer,
                              final int offset, final int length, final io.aeron.logbuffer.Header h) {
            header.wrap(buffer, offset);
            if (header.templateId() == OrderEventsEndDecoder.TEMPLATE_ID) {
                final OrderEventsEndDecoder d = new OrderEventsEndDecoder().wrapAndApplyHeader(buffer, offset, header);
                genesisByCorrelation.put(d.correlationId(), d.genesisId());
            } else if (header.templateId() == CoreConfigReportDecoder.TEMPLATE_ID) {
                final CoreConfigReportDecoder d = new CoreConfigReportDecoder().wrapAndApplyHeader(buffer, offset, header);
                genesisByCorrelation.put(d.correlationId(), d.genesisId());
            }
        }
    }

    private long nextCorrelation = 0x9000L;
    private final Map<Integer, List<String>> taps = new TreeMap<>();

    private void tap(final Member m) {
        taps.put(m.id, ServiceProbe.tapEgress(m.service));
    }

    /** The genesisId member {@code id} wrote on the reply with {@code correlationId}, or null when it wrote none yet. */
    private Long tapped(final int id, final long correlationId) {
        final List<String> frames = taps.get(id);
        synchronized (frames) {
            for (final String f : frames) {
                final UnsafeBuffer b = new UnsafeBuffer(HexFormat.of().parseHex(f.substring(f.lastIndexOf('/') + 1)));
                final MessageHeaderDecoder h = new MessageHeaderDecoder().wrap(b, 0);
                if (h.templateId() == OrderEventsEndDecoder.TEMPLATE_ID) {
                    final OrderEventsEndDecoder d = new OrderEventsEndDecoder().wrapAndApplyHeader(b, 0, h);
                    if (d.correlationId() == correlationId) {
                        return d.genesisId();
                    }
                } else if (h.templateId() == CoreConfigReportDecoder.TEMPLATE_ID) {
                    final CoreConfigReportDecoder d = new CoreConfigReportDecoder().wrapAndApplyHeader(b, 0, h);
                    if (d.correlationId() == correlationId) {
                        return d.genesisId();
                    }
                }
            }
        }
        return null;
    }

    /**
     * Asks a head probe and the config report through the log; returns {@code {member id -> {116 value, 118 value}}} of
     * every live member, once each answered both, and checks the client's answer.
     */
    private Map<Integer, long[]> ask(final ShadowClusterRig rig, final Client admin, final Replies replies, final String stage) {
        final long end = nextCorrelation++;
        final long report = nextCorrelation++;
        final CoreRestoreRig.Encoders enc = new CoreRestoreRig.Encoders();
        admin.offer(enc.buf, enc.orderEvents(end, 0, 0));
        admin.offer(enc.buf, enc.coreConfigRequest(report));
        rig.await(() -> {
            if (!replies.genesisByCorrelation.containsKey(end) || !replies.genesisByCorrelation.containsKey(report)) {
                return false;
            }
            for (final Member m : rig.live()) {
                if (tapped(m.id, end) == null || tapped(m.id, report) == null) {
                    return false;
                }
            }
            return true;
        }, 60_000, () -> stage + ": every live member and the client must see both replies; roles " + rig.roles());
        final Map<Integer, long[]> out = new TreeMap<>();
        for (final Member m : rig.live()) {
            out.put(m.id, new long[] {tapped(m.id, end), tapped(m.id, report)});
        }
        out.put(-1, new long[] {replies.genesisByCorrelation.get(end), replies.genesisByCorrelation.get(report)});
        return out;
    }

    /**
     * Every member's value on both replies is equal, and equal to {@code expected}; the client (key -1) received it too.
     * {@code tamper} rewrites a member's value before the comparison (the negative control; identity otherwise).
     */
    static void assertGenesisEverywhere(final Map<Integer, long[]> values, final long expected, final String stage,
                                        final LongBinaryOperator tamper) {
        Long first = null;
        final StringBuilder all = new StringBuilder();
        for (final Map.Entry<Integer, long[]> e : values.entrySet()) {
            for (final long raw : e.getValue()) {
                final long v = tamper.applyAsLong(e.getKey(), raw);
                all.append(e.getKey() < 0 ? "client" : "member " + e.getKey()).append('=').append(v).append(' ');
                if (first == null) {
                    first = v;
                } else if (first != v) {
                    throw new AssertionError("genesisId differs between members (" + stage + "): " + all.toString().trim());
                }
            }
        }
        if (first == null || first != expected) {
            throw new AssertionError("genesisId on the wire is " + first + ", expected " + expected + " (" + stage + ")");
        }
        System.out.println("[genesis] " + stage + ": " + all.toString().trim());
    }

    private static void assertGenesisEverywhere(final Map<Integer, long[]> values, final long expected, final String stage) {
        assertGenesisEverywhere(values, expected, stage, (member, v) -> v);
    }

    @Test
    public void everyMemberAnswersZeroBeforeAdoptAndTheRecordedIdAfterThroughRestoreAndFailover() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(26100, "genesis-id-replies")) {
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            final Member host = rig.followerOtherThan(leader, null);    // never killed: the client's driver lives here
            final Member restored = rig.followerOtherThan(leader, host);
            final Replies replies = new Replies();
            final Client admin = rig.connect(SessionClass.ADMIN, host, replies);
            for (final Member m : rig.live()) {
                tap(m);
            }

            assertGenesisEverywhere(ask(rig, admin, replies, "before ADOPT"), 0L, "before ADOPT");

            rig.adopt(admin);
            assertGenesisEverywhere(ask(rig, admin, replies, "after ADOPT"), ShadowClusterRig.GENESIS, "after ADOPT");

            // a member restarts from a snapshot taken after the ADOPT
            rig.snapshotAll();
            assertFalse("precondition: the restarting member holds a snapshot", rig.snapshots(restored).isEmpty());
            rig.kill(restored);
            rig.restart(restored);
            rig.awaitReady(restored, 120_000);
            tap(restored);
            assertGenesisEverywhere(ask(rig, admin, replies, "after a restore from the snapshot"), ShadowClusterRig.GENESIS,
                    "after a restore from the snapshot");

            // the leader dies; a follower takes over and answers the client
            rig.kill(leader);
            taps.remove(leader.id);
            final Member next = rig.awaitLeader(leader);
            assertTrue("a follower took over", next.id != leader.id);
            assertGenesisEverywhere(ask(rig, admin, replies, "on the follower turned leader " + next.id),
                    ShadowClusterRig.GENESIS, "on the follower turned leader " + next.id);

            // the old leader comes back and answers the same
            rig.restart(leader);
            rig.awaitReady(leader, 120_000);
            tap(leader);
            final Map<Integer, long[]> last = ask(rig, admin, replies, "all three after the old leader returned");
            assertEquals("three members and the client", 4, last.size());
            assertGenesisEverywhere(last, ShadowClusterRig.GENESIS, "all three after the old leader returned");

            // negative control: a member-local value in the field (the member id added on one member) must be caught
            try {
                assertGenesisEverywhere(last, ShadowClusterRig.GENESIS, "control",
                        (member, v) -> member == next.id ? v + member + 1 : v);
                fail("a member-local genesisId passed the equality check");
            } catch (final AssertionError expected) {
                assertTrue(expected.getMessage(), expected.getMessage().startsWith("genesisId differs between members (control)"));
                System.out.println("[control] member-local value caught: " + expected.getMessage());
            }
            for (final Member m : rig.live()) {
                assertTrue("member " + m.id + " halts " + m.halts, m.halts.isEmpty());
            }
        }
    }
}
