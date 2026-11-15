// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.core.infrastructure.generated.OrderEventsEndDecoder;
import com.openexchange.core.infrastructure.generated.OrderEventsPageDecoder;
import com.openexchange.core.infrastructure.generated.OrderStatus;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.generated.TimeInForce;
import io.aeron.cluster.client.EgressListener;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Order events across a core leader change, in a real three-member cluster. A gateway that lost its session with the
 * old leader asks the new one for everything above the last event it saw and gets every event, in order, from the
 * replicated ring: the new leader was a follower when those events were applied and emitted none of them. The limits
 * record decides the same on the new leader, and a member that restarts afterwards ends byte-equal to it.
 */
public class OrderEventReplayClusterTest {

    private static final long ONE = 100_000_000L;
    private static final int ORDERS = 30;

    /** The replay reply as one client receives it. */
    private static final class Replay implements EgressListener {
        final List<Long> seqs = new ArrayList<>();
        final List<String> statuses = new ArrayList<>();
        final List<long[]> ends = new ArrayList<>(); // {correlationId, count, firstAvailable, lastSeq, nextFromSeq}
        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final OrderEventsPageDecoder page = new OrderEventsPageDecoder();
        private final OrderEventsEndDecoder end = new OrderEventsEndDecoder();

        @Override
        public void onMessage(final long clusterSessionId, final long timestamp, final org.agrona.DirectBuffer buffer,
                              final int offset, final int length, final io.aeron.logbuffer.Header h) {
            header.wrap(buffer, offset);
            if (header.templateId() == OrderEventsPageDecoder.TEMPLATE_ID) {
                page.wrapAndApplyHeader(buffer, offset, header);
                for (final OrderEventsPageDecoder.EventsDecoder e : page.events()) {
                    seqs.add(e.eventSeq());
                    statuses.add(String.valueOf(e.status()));
                }
            } else if (header.templateId() == OrderEventsEndDecoder.TEMPLATE_ID) {
                end.wrapAndApplyHeader(buffer, offset, header);
                ends.add(new long[] {end.correlationId(), end.count(), end.firstAvailableSeq(), end.lastSeq(), end.nextFromSeq()});
            }
        }
    }

    @Test
    public void aGatewayReplaysEveryEventFromTheNewLeaderAndTheLimitsRecordDecidesTheSame() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(23700, "order-event-replay-leader-change")) {
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            final Member g = rig.followerOtherThan(leader, null);
            final Member other = rig.followerOtherThan(leader, g);
            final Client admin = rig.connect(SessionClass.ADMIN, g);
            rig.adopt(admin);
            final Client tool = rig.connect(SessionClass.TOOL, g);
            tool.deposits(100); // users 1..5 hold 20 USD each
            final CoreRestoreRig.Encoders enc = new CoreRestoreRig.Encoders();
            admin.offer(enc.buf, enc.limits(0x77L, 1, 600_000L, 8, 1_024)); // 8 remembered requests per user
            admin.offer(enc.buf, enc.gateOpen(0x73L));
            rig.await(() -> admin.in.count(f -> f.startsWith("ADMINACK ref=73 result=ACCEPTED")) > 0
                            && admin.in.count(f -> f.startsWith("ADMINACK ref=77 result=ACCEPTED")) > 0, 30_000,
                    () -> "the limits record and the ADMIN OPEN must be accepted; frames " + admin.in.frames);

            final Replay replay = new Replay();
            final Client gateway = rig.connect(SessionClass.GATEWAY, g, replay);
            CoreRestoreRig.register(gateway.session, SessionClass.GATEWAY, false, false, rig.sessions());
            for (int i = 0; i < ORDERS; i++) {
                gateway.offer(enc.buf, enc.limit(0x6000L + i, 1 + (i % 5), 0x6000L + i, 1, true, ONE / 10, ONE, TimeInForce.GTC, 0L));
            }
            rig.await(() -> gateway.in.acks >= ORDERS, 30_000,
                    () -> "every create must be acked: acks " + gateway.in.acks + " first reject " + gateway.in.firstReject);
            assertEquals("no create was refused", 0, gateway.in.rejects);
            rig.awaitImagesEqual(g, leader, 60_000);

            // the leader dies; the gateway's broadcast stream of these events died with its session's egress
            rig.kill(leader);
            final Member next = rig.awaitLeader(leader);
            assertTrue("a follower took over", next.id != leader.id);
            System.out.println("[replay] leader " + leader.id + " killed, new leader " + next.id);

            // ask the new leader for everything above event 10, in pages of 7, following the cursor
            long cursor = 10;
            int requests = 0;
            do {
                final int before = replay.ends.size();
                gateway.offer(enc.buf, enc.orderEvents(0x540L + requests, cursor, 7));
                rig.await(() -> replay.ends.size() > before, 60_000, () -> "the replay request must be answered by the new leader");
                final long[] end = replay.ends.get(replay.ends.size() - 1);
                assertEquals("the terminator echoes the correlation", 0x540L + requests, end[0]);
                assertEquals("the ring still starts at the first event", 1L, end[2]);
                assertEquals("the head is the last event of the old leader", (long) ORDERS, end[3]);
                assertEquals(cursor + end[1], end[4]);
                cursor = end[4];
                requests++;
            } while (cursor != ORDERS && requests < 20);
            assertEquals("the replay reached the head", (long) ORDERS, cursor);
            assertEquals("pages of seven from 10 to 30", 3, requests);
            final List<Long> expected = new ArrayList<>();
            for (long s = 11; s <= ORDERS; s++) {
                expected.add(s);
            }
            assertEquals("every event above the cursor, in order, none twice", expected, replay.seqs);
            assertTrue("the replayed events are the accepted creates: " + replay.statuses,
                    replay.statuses.stream().allMatch(s -> s.equals(String.valueOf(OrderStatus.PENDING_NEW))));

            // the limits record on the new leader: users 1..5 hold six requests each; two more are admitted, the ninth is not
            final int acksBefore = gateway.in.acks;
            for (int i = 0; i < 3; i++) {
                gateway.offer(enc.buf, enc.limit(0x7000L + i, 1, 0x7000L + i, 1, true, ONE / 10, ONE, TimeInForce.GTC, 0L));
            }
            rig.await(() -> gateway.in.acks + gateway.in.rejects >= acksBefore + 3, 30_000,
                    () -> "three creates must be answered: acks " + gateway.in.acks + " rejects " + gateway.in.rejects);
            assertEquals("the seventh and the eighth request of user 1 are admitted", acksBefore + 2, gateway.in.acks);
            assertEquals("the ninth is refused by the recorded cap of 8", 1, gateway.in.rejects);
            assertTrue("TOO_MANY_PENDING_REQUESTS: " + gateway.in.firstReject,
                    String.valueOf(gateway.in.firstReject).contains("TOO_MANY_PENDING_REQUESTS"));

            // the member that was never the leader agrees with the new leader, and so does the old leader once it is back
            rig.awaitImagesEqual(other.id == next.id ? g : other, next, 60_000);
            rig.restart(leader);
            rig.awaitReady(leader, 120_000);
            rig.awaitImagesEqual(leader, next, 60_000);
            assertTrue(leader.halts.toString(), leader.halts.isEmpty());
        }
    }
}
