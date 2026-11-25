// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.domain.Asset;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.CoreSbeDemuxerAccess;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import com.openexchange.core.infrastructure.persistence.WireVersionFrames;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.security.MessageDigest;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A wire bump rolled through a real three-member core on the embedded driver, the jar seam standing in for the next jar
 * (money-schema 11). A v10 producer keeps depositing throughout and every deposit is acked. The members are rolled to
 * the next jar one at a time (one of them rolled back and forward again before the raise). A v11 probe sent before the
 * raise is dropped by all three from its header (one above-ceiling drop on each, no ack); the raise, encoded at v10, is
 * ACCEPTED and recorded on every member; a v11 probe after it is applied. At the end every member writes the same
 * snapshot bytes.
 */
public class WireBumpRollingClusterTest {

    private final Map<Integer, Integer> jar = new HashMap<>();
    private long probeCorr = 0x7_0000L;

    private void onJar(final ShadowClusterRig rig, final Member m, final int version) {
        jar.put(m.id, version);
        rig.kill(m);
        rig.restart(m);
        rig.awaitReady(m, 120_000);
        assertEquals("member " + m.id + " runs the jar it was rolled to", version,
                m.service.demuxer().maxSupportedVersion());
    }

    private static String sha256(final byte[] b) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(b));
    }

    private int offerProbe(final Client tool, final int version) {
        final byte[] frame = new WireVersionFrames().deposit(version, ++probeCorr, 9, Asset.USD.id(), ShadowClusterRig.ONE);
        tool.offer(new UnsafeBuffer(frame), frame.length);
        return frame.length;
    }

    @Test(timeout = 900_000)
    public void aWireBumpRollsMemberByMemberWithNoRejectAndOneDecisionPerFrame() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(26700, "wire-bump-rolling")) {
            for (final Member m : rig.members) {
                jar.put(m.id, 10);
                m.beforeLaunch = svc -> CoreSbeDemuxerAccess.standInForJar(svc, jar.get(m.id));
            }
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            Member host = rig.followerOtherThan(leader, null);
            Client admin = rig.connect(SessionClass.ADMIN, host);
            Client tool = rig.connect(SessionClass.TOOL, host);
            rig.adopt(admin);
            int deposits = 0;
            tool.deposits(20);
            deposits += 20;
            for (final Member m : rig.live()) {
                assertEquals(10L, m.service.engine().coreConfig().recordedMaxWireVersion());
            }

            // roll every member to the next jar, one at a time, followers first and the leader last, a v10 producer
            // depositing in between; the clients leave before each roll and reconnect through a member that stays up
            final Member firstLeader = rig.awaitLeader(null);
            final List<Member> order = new java.util.ArrayList<>(rig.members);
            order.remove(firstLeader);
            order.add(firstLeader);
            for (final Member m : order) {
                rig.forgetAll();
                onJar(rig, m, 11);
                final Member now = rig.awaitLeader(null);
                host = rig.followerOtherThan(now, null);
                admin = rig.connect(SessionClass.ADMIN, host);
                tool = rig.connect(SessionClass.TOOL, host);
                tool.deposits(10);
                deposits += 10;
            }
            // one member back to the earlier jar and forward again: allowed before the raise
            final Member back = rig.followerOtherThan(rig.awaitLeader(null), host);
            onJar(rig, back, 10);
            tool.deposits(10);
            deposits += 10;
            onJar(rig, back, 11);
            tool.deposits(10);
            deposits += 10;

            // the rollout gate's precondition: every member supports 11 (core_supported_max_wire_version)
            for (final Member m : rig.live()) {
                assertTrue("member " + m.id + " supports 11", m.service.demuxer().maxSupportedVersion() >= 11);
            }
            // the probes travel on their own session, so their acks never stand in for the producer's
            final Client probe = rig.connect(SessionClass.TOOL, host);
            final Map<Integer, Long> dropsBefore = new HashMap<>();
            for (final Member m : rig.live()) {
                dropsBefore.put(m.id, m.service.demuxer().aboveCeilingDropCount());
            }

            // a v11 probe before the raise: dropped from the header by all three, never acked
            final long earlyProbe = probeCorr + 1;
            offerProbe(probe, 11);
            final Map<Integer, Long> before = dropsBefore;
            rig.await(() -> rig.live().stream().allMatch(m ->
                            m.service.demuxer().aboveCeilingDropCount() == before.get(m.id) + 1), 60_000,
                    () -> "the v11 probe must be an above-ceiling drop on every member");
            tool.deposits(5); // the log moved on past the probe
            deposits += 5;
            assertTrue("no ack for the dropped probe", probe.in.maxDepositAckCorr < earlyProbe);
            for (final Member m : rig.live()) {
                assertEquals("member " + m.id + ": nothing of the probe applied", null, m.service.engine().account(9));
            }

            // the raise, encoded at v10 by the previous admin tool
            final byte[] raise = new WireVersionFrames().coreConfig(10, 0x72L, 2, 1, 11);
            admin.offer(new UnsafeBuffer(raise), raise.length);
            final Client a = admin;
            rig.await(() -> a.in.count(f -> f.startsWith("ADMINACK ref=70 result=ACCEPTED")) > 0, 60_000,
                    () -> "the raise must be ACCEPTED; frames " + a.in.frames);
            rig.await(() -> rig.live().stream().allMatch(m ->
                            m.service.engine().coreConfig().recordedMaxWireVersion() == 11L), 60_000,
                    () -> "core_max_wire_version must read 11 on every member");

            // a v11 probe after the raise is applied on every member
            final long lateProbe = probeCorr + 1;
            offerProbe(probe, 11);
            rig.await(() -> probe.in.maxDepositAckCorr >= lateProbe, 60_000, () -> "the v11 deposit after the raise is acked");
            rig.await(() -> rig.live().stream().allMatch(m -> m.service.engine().account(9) != null
                            && m.service.engine().account(9).available(Asset.USD.id()) == ShadowClusterRig.ONE), 60_000,
                    () -> "the v11 deposit is applied on every member");
            tool.deposits(5);
            deposits += 5;

            for (final Member m : rig.live()) {
                assertEquals("member " + m.id + ": one above-ceiling drop, the early probe",
                        dropsBefore.get(m.id) + 1, m.service.demuxer().aboveCeilingDropCount());
                assertEquals("member " + m.id + " halts " + m.halts, List.of(), m.halts);
            }
            assertEquals("every v10 deposit acked, none rejected", 0, tool.in.rejects + admin.in.rejects);

            // every member at one position writes the same bytes
            final String[] why = {""};
            rig.await(() -> {
                try {
                    String first = null;
                    final StringBuilder all = new StringBuilder();
                    for (final Member m : rig.live()) {
                        final String h = sha256(ServiceProbe.image(m.service));
                        all.append("member ").append(m.id).append('=').append(h, 0, 12).append(' ');
                        if (first == null) {
                            first = h;
                        } else if (!first.equals(h)) {
                            why[0] = all.toString();
                            return false;
                        }
                    }
                    return true;
                } catch (final Exception e) {
                    why[0] = e.toString();
                    return false;
                }
            }, 60_000, () -> "the state hash must be equal on every member: " + why[0]);
            System.out.println("GREEN wire bump rolled: " + deposits + " v10 deposits acked, probe dropped on 3 before"
                    + " the raise and applied after it");
        }
    }
}
