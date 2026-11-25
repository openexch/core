// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.cluster.ShadowClusterRig.Client;
import com.openexchange.core.cluster.ShadowClusterRig.Member;
import com.openexchange.core.infrastructure.generated.SessionClass;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.concurrent.status.CountersReader;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Three members, embedded drivers: every member's consensus publications (stream 108, one to each peer) and the images
 * of its peers' consensus publications are on {@code TRANSPORT_CONSENSUS_TERM_LENGTH}, read from the member driver's
 * counters (the channel in the publication's label) and from the log buffer files in its directory (the length is
 * three terms plus the metadata page). The leader is then killed while deposits are in flight: an election closes on
 * the small term, every deposit is answered by the new leader, and the new leader's consensus publications are still
 * on the configured term.
 *
 * <p>Control: the same rig with the consensus channel set back to Aeron's bare {@code aeron:udp} (the shape before the
 * setting) fails the same check, with its publications on the driver's 16m term.</p>
 */
public class ConsensusTermClusterTest {

    private static final int PORT_BASE = 24600;
    private static final int CONSENSUS_STREAM_ID = 108;
    /** A term no other channel of the rig uses (ingress and log 1m, archive control 16m ipc), so a file length is ours. */
    private static final String TERM = "128k";
    private static final long TERM_BYTES = 128 * 1024;
    private static final long DRIVER_TERM_BYTES = 16L * 1024 * 1024;

    @After
    public void tearDown() {
        System.clearProperty("transport.consensus.term.length");
    }

    @Test(timeout = 300_000)
    public void consensusRunsOnTheConfiguredTermAndAnElectionClosesUnderLoad() throws Exception {
        System.setProperty("transport.consensus.term.length", TERM);
        try (ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE, "consensus-term")) {
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            // the client's Aeron runs on a follower's driver: the leader's driver dies with the leader
            final Client tool = rig.connect(SessionClass.TOOL, rig.followerOtherThan(leader, null)).withIdentity();
            tool.deposits(200);
            for (final Member m : rig.members) {
                assertEquals("", consensusTermProblems(m, TERM_BYTES, rig.members.size() - 1, rig.members.size() - 1));
            }

            // the leader dies with deposits in flight; the deposits are re-offered to whoever wins
            tool.deposits(2_000, () -> rig.kill(leader));
            final Member next = rig.awaitLeader(leader);
            tool.deposits(200);
            assertEquals("the new leader's consensus publications", "",
                    consensusTermProblems(next, TERM_BYTES, rig.members.size() - 1, 1));
            System.out.println("GREEN consensus term " + TERM + ": 3 members x 2 publications + 2 images at "
                    + fileLength(TERM_BYTES) + " B; leader " + leader.id + " killed under load, member " + next.id
                    + " leads, 2400 deposits answered");
        }
    }

    /** Control: a bare consensus channel (no term) fails the same check, on the driver's 16m term. */
    @Test(timeout = 300_000)
    public void aConsensusChannelWithoutATermTakesTheDriverTerm() throws Exception {
        System.setProperty("transport.consensus.term.length", TERM);
        try (ShadowClusterRig rig = new ShadowClusterRig(PORT_BASE + 400, "consensus-term-control")) {
            for (final Member m : rig.members) {
                m.afterConfig = mm -> mm.cfg.consensusModuleContext().consensusChannel("aeron:udp");
            }
            rig.bootAll();
            final Member leader = rig.awaitLeader(null);
            rig.connect(SessionClass.TOOL, leader).deposits(50);
            final String problems = consensusTermProblems(leader, TERM_BYTES, rig.members.size() - 1,
                    rig.members.size() - 1);
            assertTrue("the control must fail the check: " + problems,
                    problems.contains("names no term-length=" + TERM));
            assertEquals("the control's publications are on the driver's term", "",
                    consensusTermProblems(leader, DRIVER_TERM_BYTES, rig.members.size() - 1, rig.members.size() - 1)
                            .replaceAll("[^\\n]*names no term-length=16m[^\\n]*\\n?", ""));
            System.out.println("NC CONSENSUS_CHANNEL_WITHOUT_TERM -> " + problems.lines().findFirst().orElse(""));
        }
    }

    private static long fileLength(final long term) {
        return 3 * term + LogBufferDescriptor.LOG_META_DATA_LENGTH;
    }

    /**
     * Empty when member {@code m} has exactly {@code peers} consensus publications (one per other member of the static
     * membership, a dead one included), each naming the term and backed by a log buffer of three such terms, and at
     * least {@code images} images of that length (one per live peer); otherwise one line per problem.
     */
    static String consensusTermProblems(final Member m, final long term, final int peers, final int images) {
        return consensusTermProblems(m.id, m.cfg.consensusModuleContext().aeron().countersReader(),
                new File(m.driverDir), term, peers, images);
    }

    /** The same check over any driver: its counters and its directory (an external aeronmd's included). */
    static String consensusTermProblems(final int memberId, final CountersReader counters, final File driverDir,
                                        final long term, final int peers, final int images) {
        final String termText = term % (1024 * 1024) == 0 ? term / (1024 * 1024) + "m" : term / 1024 + "k";
        final StringBuilder problems = new StringBuilder();
        final List<String[]> pubs = new ArrayList<>();
        counters.forEach((counterId, typeId, keyBuffer, label) -> {
            if (typeId != 1 || !label.startsWith("pub-lmt: ")) {
                return;
            }
            final String[] f = label.substring("pub-lmt: ".length()).split(" ", 4);
            if (f.length == 4 && Integer.parseInt(f[2]) == CONSENSUS_STREAM_ID && f[3].startsWith("aeron:udp")) {
                pubs.add(f);
            }
        });
        if (pubs.size() != peers) {
            problems.append("member ").append(memberId).append(": ").append(pubs.size())
                    .append(" consensus publications, expected ").append(peers).append('\n');
        }
        final long expected = fileLength(term);
        for (final String[] p : pubs) {
            if (!p[3].contains("term-length=" + termText)) {
                problems.append("member ").append(memberId).append(": consensus publication ").append(p[0])
                        .append(" names no term-length=").append(termText).append(": ").append(p[3]).append('\n');
            }
            final File log = new File(driverDir, "publications/" + p[0] + ".logbuffer");
            if (log.length() != expected) {
                problems.append("member ").append(memberId).append(": ").append(log).append(" is ").append(log.length())
                        .append(" B, expected ").append(expected).append('\n');
            }
        }
        final File[] imageFiles = new File(driverDir, "images").listFiles();
        int sized = 0;
        if (imageFiles != null) {
            for (final File f : imageFiles) {
                if (f.length() == expected) {
                    sized++;
                }
            }
        }
        if (sized < images) {
            problems.append("member ").append(memberId).append(": ").append(sized).append(" images of ").append(expected)
                    .append(" B, expected at least ").append(images).append('\n');
        }
        return problems.toString();
    }
}
