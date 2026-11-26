// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * S4 T6 (core, embedded, in-process rig): R-B4 step 1 is the rule. The cluster runs g1.1 and snapshots there, then
 * rolls to g1.2 without a snapshot. A member restarted on g1.3 meets its newest snapshot, stamped g1.1, two levels
 * back: its validator refuses (the node's refusal is the exit 78 of {@code fatalOnRefusal}; in-process it is recorded)
 * and it never becomes ready. Put back on g1.2 it starts; the wrong build again is refused again; after a snapshot at
 * g1.2 the roll to g1.3 succeeds and the member rejoins (C4, C5).
 */
public class SnapshotTwoLevelsBackTest {

    @Test(timeout = 900_000)
    public void aSnapshotTwoLevelsBackRefusesUntilASnapshotAtNMinus1IsTaken() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(29700, "core-snapshot-two-levels-back")) {
            final MemberLevels levels = new MemberLevels(rig, 1);
            rig.bootAll();
            for (final ShadowClusterRig.Member m : rig.members) {
                rig.awaitReady(m, 120_000);
            }
            ShadowClusterRig.Member leader = rig.awaitLeader(null);
            rig.adopt(rig.connect(SessionClass.ADMIN, leader));
            rig.connect(SessionClass.TOOL, leader).deposits(30);
            rig.snapshotAll(); // stamped g1.1
            rig.forgetAll();

            // roll everything to g1.2, followers first, leader last, no snapshot
            final List<ShadowClusterRig.Member> order = new ArrayList<>();
            for (final ShadowClusterRig.Member m : rig.members) {
                if (m != leader) {
                    order.add(m);
                }
            }
            order.add(leader);
            for (final ShadowClusterRig.Member m : order) {
                levels.reboot(m, 2);
                levels.awaitCompat(m, 2);
            }
            leader = rig.awaitLeader(null);
            final ShadowClusterRig.Member x = rig.followerOtherThan(leader, null);
            final ShadowClusterRig.Member host = rig.followerOtherThan(leader, x);
            rig.connect(SessionClass.TOOL, host).withIdentity().deposits(20);
            rig.forgetAll();

            // wrong build, right build, wrong build again
            for (int round = 1; round <= 2; round++) {
                refusedOnG13(rig, levels, x, round);
                levels.reboot(x, 2);
                levels.awaitCompat(x, 2);
            }
            assertEquals("only the member on the wrong build refused: " + levels.refusals, 2,
                    levels.refusals.size());

            // the rule: a snapshot at N-1 (g1.2), then the roll to N succeeds
            rig.snapshotAll();
            levels.reboot(x, 3);
            levels.awaitCompat(x, 3);
            final ShadowClusterRig.Client tool = rig.connect(SessionClass.TOOL, host).withIdentity();
            tool.deposits(20);
            final long head = rig.awaitLeader(null).service.engine().getEventSeq();
            rig.await(() -> x.service.engine().getEventSeq() == head, 60_000, () -> "member " + x.id + " rejoins");
            assertEquals("no refusal after the snapshot at N-1: " + levels.refusals, 2, levels.refusals.size());
            assertEquals(0, tool.in.rejects);
            System.out.println("GREEN snapshot two levels back: refused twice on g1.3, recovered on g1.2, rolled to"
                    + " g1.3 after a g1.2 snapshot");
        }
    }

    private static void refusedOnG13(final ShadowClusterRig rig, final MemberLevels levels,
                                     final ShadowClusterRig.Member x, final int round) {
        rig.kill(x);
        levels.level.put(x.id, 3);
        final int before = levels.refusals.size();
        try {
            rig.restart(x);
        } catch (final RuntimeException launchFailed) {
            System.out.println("[two-levels-back] round " + round + ": the launch failed: " + launchFailed);
        }
        rig.await(() -> levels.refusals.size() > before, 60_000,
                () -> "member " + x.id + " on g1.3 must refuse its g1.1 snapshot (round " + round + ")");
        assertEquals("member " + x.id + ": g1.3 refused g1.1", levels.refusals.get(before));
        rig.pumpMs(2_000);
        assertFalse("a refused member never reads ready", x.service != null && ServiceProbe.ready(x.service));
        assertTrue(x.halts.toString(), x.halts.isEmpty());
        rig.kill(x);
    }
}
