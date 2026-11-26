// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.cluster.MemberCompat;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * S4 T2 (core): the capability manifest of this build against the committed files. The build's manifest equals the
 * committed file of its level ({@code compat/manifest-g<g>.<level>.txt}); against the previous level's file the build
 * runs N-1's highest behaviour, reads the layout N-1 writes at it, keeps the wire floor and does not lower the top.
 * The genesis level (g1.1) has no previous file: those checks are skipped by name.
 */
public class MemberCompatManifestTest {

    static String resource(final MemberCompat level) throws IOException {
        if (level == null) {
            return null;
        }
        try (InputStream in = MemberCompatManifestTest.class.getResourceAsStream(
                "/compat/" + MemberCompat.manifestFileName(level))) {
            return in == null ? null : new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    public void theBuildsManifestEqualsTheCommittedFileOfItsLevel() throws IOException {
        final MemberCompat own = CoreMemberCompat.MEMBER_COMPAT;
        final List<String> failures = MemberCompat.manifestFailures(own, CoreMemberCompat.manifest(), resource(own),
                null);
        assertEquals(String.join("\n", failures), List.of(), failures);
    }

    @Test
    public void againstThePreviousLevelTheBuildRunsItsBehaviourReadsItsLayoutAndKeepsTheFloor() throws IOException {
        final MemberCompat own = CoreMemberCompat.MEMBER_COMPAT;
        final String previous = resource(own.previousLevel());
        Assume.assumeTrue("previous-level checks skipped: " + own + " is the genesis level (no manifest-"
                + own.previousLevel() + ".txt)", previous != null);
        final List<String> failures = MemberCompat.manifestFailures(own, CoreMemberCompat.manifest(), resource(own),
                previous);
        assertEquals(String.join("\n", failures), List.of(), failures);
    }

    @Test
    public void theTokensAndTheManifestNameTheSameBuild() {
        final MemberCompat.Manifest m = CoreMemberCompat.manifest();
        final String tokens = CoreMemberCompat.capabilities(CoreMemberCompat.MEMBER_COMPAT, true).get();
        assertTrue(tokens, tokens.startsWith(" compat=" + CoreMemberCompat.MEMBER_COMPAT + " wire=" + m.wireFloor()
                + "-" + m.wireTop() + " behaviors=" + String.join(",",
                Arrays.stream(m.behaviors()).mapToObj(Integer::toString).toList())));
    }

    /** Negative control: one extra behaviour at the old level fails the level rule by its text. */
    @Test
    public void negativeControlAnExtraBehaviourWithoutALevelBumpFails() throws IOException {
        final MemberCompat own = CoreMemberCompat.MEMBER_COMPAT;
        final MemberCompat.Manifest current = CoreMemberCompat.manifest();
        final int[] more = Arrays.copyOf(current.behaviors(), current.behaviors().length + 1);
        more[more.length - 1] = current.maxBehavior() + 1;
        final MemberCompat.Manifest extra = rebuild(current, current.wireFloor(), current.wireTop(), more);
        final List<String> failures = MemberCompat.manifestFailures(own, extra, resource(own), null);
        assertEquals(failures.toString(), 1, failures.size());
        assertTrue(failures.get(0), failures.get(0).startsWith("capability manifest changed without a level bump"));
    }

    /** Negative control: a next level whose wire floor moved fails the floor rule against this level's file. */
    @Test
    public void negativeControlAMovedFloorFailsTheFloorRule() throws IOException {
        final MemberCompat own = CoreMemberCompat.MEMBER_COMPAT;
        final MemberCompat.Manifest current = CoreMemberCompat.manifest();
        final MemberCompat.Manifest moved = rebuild(current, current.wireFloor() + 1, current.wireTop() + 1,
                current.behaviors());
        final MemberCompat next = MemberCompat.of(own.generation(), own.level() + 1);
        final List<String> failures = MemberCompat.manifestFailures(next, moved, moved.text(), resource(own));
        assertEquals(List.of("level rule: the wire floor moved from " + current.wireFloor() + " to "
                + (current.wireFloor() + 1) + " (the floor never moves within a generation)"), failures);
    }

    private static MemberCompat.Manifest rebuild(final MemberCompat.Manifest m, final int floor, final int top,
                                                 final int[] behaviors) {
        final MemberCompat.Manifest.Builder b = MemberCompat.Manifest.builder().wire(floor, top).behaviors(behaviors);
        m.layoutSince().forEach(b::layout);
        m.journals().forEach((name, value) -> {
            final String[] parts = value.split(" ");
            b.journal(name, Integer.parseInt(parts[0].substring("schema=".length())),
                    Integer.parseInt(parts[1].substring("version=".length())));
        });
        return b.build();
    }
}
