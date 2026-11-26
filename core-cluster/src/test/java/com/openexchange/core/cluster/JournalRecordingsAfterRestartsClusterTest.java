// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.RetentionStory.Pusher;
import com.openexchange.core.cluster.RetentionStory.Reader;
import org.junit.Test;

import java.util.List;

import static com.openexchange.core.cluster.RetentionStory.ORDER_FRAME;
import static com.openexchange.core.cluster.RetentionStory.order;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * T3. Every process run of a member opens a new order journal recording and leaves the old one stopped. Five
 * restarts of one member under order flow: the stopped recordings go once the new one has passed them and the reader
 * has applied them, so the member never keeps more than two recordings with bytes, and its start position advances
 * again after every restart. The member replays its log on each restart; while it is behind its reader it holds.
 */
public class JournalRecordingsAfterRestartsClusterTest {

    @Test
    public void fiveRestartsLeaveAtMostTwoOrderJournalRecordingsWithBytes() throws Exception {
        try (RetentionStory s = new RetentionStory(37100, "cjr-restarts", "journal.retention.order", "readers",
                "journal.retention.poll.ms", "1000").boot(700)) {
            final Reader reader = s.new Reader(order(), "oe-projector");
            s.setAll(order(), "oe-projector", 0);
            final Pusher pusher = s.new Pusher(reader, null);
            final ShadowClusterRig.Member f = s.f;
            s.orders(1_000);
            for (int restart = 1; restart <= 5; restart++) {
                final int n = restart;
                s.stopWriters(f);
                s.rig.kill(f);
                s.orders(500); // the flow goes on while the member is down
                s.rig.restart(f);
                s.rig.awaitReady(f, 120_000);
                // the member's reader file survived the restart; the pusher goes on delivering to it (a cursor written
                // here, ahead of the pusher's one-step-old value, would make the pusher's next write a lower one,
                // which the command refuses)
                s.orders(500);
                final long head = s.orderHead();
                // the restarted member records again from the start of its log, passes its old recordings, and its
                // retention deletes them once the reader has applied them
                s.pump(List.of(reader), List.of(pusher), 1_000, () -> reader.applied == head
                        && s.recordingsWithBytes(f, order()) <= 2, 120_000,
                        () -> "restart " + n + ": member " + f.id + " keeps " + s.recordingsWithBytes(f, order())
                                + " order journal recordings with bytes (head " + head + ", reader at "
                                + reader.applied + "): " + s.describe(order()));
            }
            final long head = s.orderHead();
            final int n = 5;
                // the old recording went or is going; the new one is cut again after the recovery
                s.pump(List.of(reader), List.of(pusher), 1_000, () -> s.recordingsWithBytes(f, order()) <= 2
                        && s.recordings(f, order()).stream().anyMatch(r -> r.active() && r.start() > 0
                                && r.end() == head * ORDER_FRAME)
                        && s.status(f, order()).report != null && s.status(f, order()).report.tailSeq == head
                        && s.status(f, order()).report.recordings == 1, 120_000,
                        () -> "restart " + n + ": the start position of member " + f.id + " does not advance after"
                                + " the restart: " + s.describe(order()));
            assertEquals("six process runs", 6, f.starts);
            assertTrue("the old recordings went whole: " + s.lines, s.said(": stopped, last journalSeq "));
            System.out.println("GREEN T3 five restarts: " + s.describe(order()));
        }
    }
}
