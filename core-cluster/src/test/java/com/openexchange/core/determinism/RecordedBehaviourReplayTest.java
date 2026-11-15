// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.determinism;

import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A log recorded under behaviour 3 replays to the bytes it produced when it was recorded. The digests below are the
 * SHA-256 of each scenario's output as the core gave it BEFORE the create path's requestId lookup moved (they equal the
 * committed goldens of that build); the two create-retry scenarios are the ones whose answers the move changes under
 * behaviour 4. The control runs the same log with the lookup first and no other difference, and must fail with
 * {@code v3 replay digest differs}: it shows that this check would catch a move made without the version test.
 */
public class RecordedBehaviourReplayTest {

    private static final Path DIR = Path.of("src", "test", "resources", "determinism");

    /** scenario name to the SHA-256 of its recorded output. */
    private static final Map<String, String> RECORDED = new LinkedHashMap<>();

    static {
        RECORDED.put("create_retry_after_gate_closed_v3", "5900bacb46101b8b631667cec2fd12105a607e3d7b922d12104914a53527ef44");
        RECORDED.put("create_retry_after_gtd_deadline_v3", "27bd480a8da40d1db6c7e8c0f6654d0c7699f42cbfc2b6367e48c0ab0dac39e1");
        // the behaviour 3 scenario of the corpus as it stood before this change
        RECORDED.put("feed_lane_halts_auto_reopen", "7eb634ed1a7b3fb420064f1a8d316389a9672509f45c59c1c8c51c4c53e3b8e3");
    }

    static String sha256(final String text) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8)));
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void assertRecorded(final String name, final String output) {
        final String digest = sha256(output);
        if (!RECORDED.get(name).equals(digest)) {
            fail("v3 replay digest differs: " + name + " recorded " + RECORDED.get(name) + " replayed " + digest);
        }
    }

    @Test
    public void everyRecordedBehaviour3LogReplaysToItsRecordedDigest() throws IOException {
        for (final String name : RECORDED.keySet()) {
            final List<String> lines = Files.readAllLines(DIR.resolve(name + ".scenario"), StandardCharsets.UTF_8);
            assertTrue(name + " is a behaviour 3 log (boots through the genesis verb with no later behaviour)",
                    lines.stream().anyMatch(l -> l.startsWith("V3GENESIS"))
                            && lines.stream().noneMatch(l -> l.contains("behavior=4")));
            assertRecorded(name, CoreScenarioRunner.runLines(lines));
        }
        assertTrue("non-vacuous", RECORDED.size() >= 2);
    }

    /**
     * The same log with the lookup first: boot one behaviour step higher and drop that step's own two output lines, so
     * the only difference left is what the moved lookup answers.
     */
    private static String replayWithTheLookupFirst(final String name) throws IOException {
        final List<String> lines = new ArrayList<>();
        for (final String l : Files.readAllLines(DIR.resolve(name + ".scenario"), StandardCharsets.UTF_8)) {
            lines.add(l.startsWith("V3GENESIS") ? "V3GENESIS behavior=4" : l);
        }
        final StringBuilder out = new StringBuilder();
        int dropped = 0;
        for (final String l : CoreScenarioRunner.runLines(lines).split("\n", -1)) {
            if (l.equals("ADMINACK corr=7004 ref=70 result=ACCEPTED reason=0 cfg=4")
                    || l.equals("CONFIGAPPLIED kind=CORE market=0 version=4")) {
                dropped++;
                continue;
            }
            out.append(l).append('\n');
        }
        assertEquals("the step to behaviour 4 emitted its ack and its ConfigApplied", 2, dropped);
        out.setLength(out.length() - 1);
        return out.toString();
    }

    @Test
    public void controlTheLookupMovedWithoutTheVersionTestChangesTheDigest() throws IOException {
        for (final String name : List.of("create_retry_after_gate_closed_v3", "create_retry_after_gtd_deadline_v3")) {
            final String moved = replayWithTheLookupFirst(name);
            try {
                assertRecorded(name, moved);
            } catch (final AssertionError expected) {
                assertTrue(expected.getMessage(), expected.getMessage().startsWith("v3 replay digest differs: " + name));
                continue;
            }
            fail("the control did not fail: " + name + " replays to its recorded digest with the lookup first, so the "
                    + "digest does not see the move");
        }
    }
}
