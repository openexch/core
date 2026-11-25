// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.determinism;

import com.openexchange.cluster.SnapshotEnvelope;
import com.openexchange.cluster.SnapshotLayouts;
import com.openexchange.core.application.engine.CoreEngine;
import com.openexchange.core.application.engine.config.CoreConfigStateMachine;
import com.openexchange.core.infrastructure.persistence.BalanceSnapshotCodec;
import com.openexchange.core.infrastructure.persistence.CoreShadowIdentity;
import com.openexchange.core.infrastructure.persistence.SnapshotLayoutTestBuilds;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

import static com.openexchange.core.infrastructure.persistence.SnapshotLayoutTestBuilds.BUILD_AFTER_NEXT;
import static com.openexchange.core.infrastructure.persistence.SnapshotLayoutTestBuilds.LAYOUT_V7_TAG;
import static com.openexchange.core.infrastructure.persistence.SnapshotLayoutTestBuilds.NEXT_BEHAVIORS;
import static com.openexchange.core.infrastructure.persistence.SnapshotLayoutTestBuilds.NEXT_BUILD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Snapshot layouts across builds. A committed image of the current layout (v6, behaviour 4) is the fixture: it
 * equals a replay of its log, it restores under this build and under a build that already knows a next layout, and
 * the restored state is the state a replay reaches. The layout a build writes follows the recorded behaviour
 * version, so two builds on one cluster write the same bytes until the switch that adopts the next layout is in the
 * log. A layout older than the previous one and an unknown tag are refused by name.
 *
 * <p>The fixture is in the machine's native byte order, as every snapshot is (little-endian on every supported
 * platform). Rewrite it only for a deliberate layout change: {@code -Dsnapshot.layout.golden.write=true}.</p>
 */
public class SnapshotLayoutRegistryTest {

    private static final Path GOLDEN = Path.of("src", "test", "resources", "snapshot-layouts", "core-v6-behaviour4.image");
    private static final int[] THIS_BEHAVIORS = CoreConfigStateMachine.SUPPORTED_BEHAVIOR_VERSIONS;

    /** The log the fixture is the snapshot of: behaviour 4, two users, live orders, a cancelled stop, a wrapped ring. */
    private static final List<String> LOG = List.of(
            "CLOCK 1700000000000",
            "V3GENESIS behavior=4",
            "GATE market=0 open=1",
            "LIMITS ver=1 ttlMs=900000 cap=256 ring=4 corr=1",
            "DEPOSIT u=1 asset=0 amt=100000.0",
            "DEPOSIT u=2 asset=1 amt=10.0",
            "DEPOSIT u=7 asset=0 amt=2500.0",
            "CREATE u=1 market=1 side=0 type=0 px=100.0 qty=1.0 rid=1",
            "CREATE u=1 market=1 side=0 type=0 px=101.0 qty=1.0 rid=2",
            "CREATE u=2 market=1 side=1 type=4 px=90.0 qty=1.0 stop=95.0 rid=3",
            "CREATE u=1 market=1 side=0 type=0 px=102.0 qty=1.0 rid=4",
            "CANCEL u=2 order=3 rid=5",
            "CREATE u=7 market=1 side=0 type=0 px=99.0 qty=2.0 rid=6",
            "CREATE u=1 market=1 side=0 type=0 px=104.0 qty=1.0 rid=7");

    /** What the log goes on to say after the snapshot. */
    private static final List<String> TAIL = List.of(
            "CLOCK 1700000001000",
            "CREATE u=1 market=1 side=0 type=0 px=105.0 qty=1.0 rid=8",
            "CREATE u=2 market=1 side=1 type=4 px=91.0 qty=1.0 stop=96.0 rid=9",
            "DEPOSIT u=7 asset=0 amt=10.0",
            "ORDEREVENTS from=4 max=100 corr=22",
            "USERBALANCES u=1 corr=23");

    /** The recorded switch to the behaviour that adopts the next layout, then more log. */
    private static final List<String> SWITCH = List.of(
            "CORECONFIG ver=5 behavior=5 corr=75",
            "CONFIGREPORT corr=76");
    private static final List<String> AFTER_SWITCH = List.of(
            "CLOCK 1700000002000",
            "CREATE u=7 market=1 side=0 type=0 px=98.0 qty=1.0 rid=10",
            "ORDEREVENTS from=6 max=100 corr=32",
            "USERBALANCES u=7 corr=33");

    // ---------------------------------------------------------------------------------------------- the fixture

    @Test
    public void theCommittedImageIsTheSnapshotOfItsLog() throws IOException {
        final byte[] replayed = replay(BalanceSnapshotCodec.LAYOUTS, THIS_BEHAVIORS, LOG).snapshotBytes();
        if (Boolean.getBoolean("snapshot.layout.golden.write")) {
            Files.createDirectories(GOLDEN.getParent());
            Files.write(GOLDEN, replayed);
        }
        final byte[] golden = golden();
        assertSameImage("the v6 layout moved: a replay of the fixture's log no longer writes the committed image", golden,
                replayed);
        assertEquals(BalanceSnapshotCodec.LAYOUT_V6_TAG, tag(golden));
        // the comparison can fail: one changed byte in the expected image is named
        final byte[] drifted = golden.clone();
        drifted[SnapshotEnvelope.payloadOffset() + 40] ^= 0x01;
        final AssertionError drift = assertThrows(AssertionError.class,
                () -> assertSameImage("the v6 layout moved", drifted, replayed));
        assertTrue(drift.getMessage(), drift.getMessage().contains("the v6 layout moved"));
        assertTrue(drift.getMessage(), drift.getMessage().contains("first difference at byte " + (SnapshotEnvelope.payloadOffset() + 40)));
    }

    @Test
    public void theCommittedImageRestoresUnderThisBuild() throws IOException {
        assertRestoresLikeAReplay(BalanceSnapshotCodec.LAYOUTS, THIS_BEHAVIORS);
    }

    // ------------------------------------------------------------------- a build that knows the next layout

    @Test
    public void theCommittedImageRestoresUnderABuildThatKnowsTheNextLayout() throws IOException {
        assertRestoresLikeAReplay(NEXT_BUILD, NEXT_BEHAVIORS);
    }

    @Test
    public void theNextLayoutIsWrittenOnlyAfterTheRecordedSwitch() throws IOException {
        // one engine restored from the v6 image, one that replayed the whole log; both on the next build
        final CoreScenarioRunner restored = restoredFromGolden(NEXT_BUILD, NEXT_BEHAVIORS);
        final CoreScenarioRunner replayed = replay(NEXT_BUILD, NEXT_BEHAVIORS, LOG);
        restored.execAll(TAIL);
        replayed.execAll(TAIL);
        assertEquals("behaviour 4 still", 4L, restored.engine().coreConfig().behaviorVersion());
        assertEquals("before the switch the next build writes v6", BalanceSnapshotCodec.LAYOUT_V6_TAG,
                tag(restored.snapshotBytes()));

        restored.execAll(SWITCH);
        replayed.execAll(SWITCH);
        assertEquals("the switch was recorded", 5L, restored.engine().coreConfig().behaviorVersion());
        final byte[] v7 = restored.snapshotBytes();
        assertEquals("from the switch on the image is v7", LAYOUT_V7_TAG, tag(v7));
        assertSameImage("restored and replayed engines write the same v7 image", replayed.snapshotBytes(), v7);
        assertEquals(sha256(replayed.snapshotBytes()), sha256(v7));

        // the v7 image restores under the next build and the log continues as on the engine that never restarted
        final CoreScenarioRunner fromV7 = replay(NEXT_BUILD, NEXT_BEHAVIORS, List.of());
        fromV7.restoreFrom(v7);
        assertSameImage("a v7 round trip keeps the bytes", v7, fromV7.snapshotBytes());
        final int mark = replayed.renderedEvents().size();
        fromV7.execAll(AFTER_SWITCH);
        replayed.execAll(AFTER_SWITCH);
        final List<String> continued = replayed.renderedEvents().subList(mark, replayed.renderedEvents().size());
        assertTrue("the continuation produced output", continued.size() >= 3);
        assertEquals(continued, fromV7.renderedEvents());
        assertSameImage("and the same image afterwards", replayed.snapshotBytes(), fromV7.snapshotBytes());
    }

    // ----------------------------------------------------------------------------------- two builds, one cluster

    @Test
    public void twoBuildsOnOneClusterWriteEqualBytes() {
        final CoreScenarioRunner older = replay(BalanceSnapshotCodec.LAYOUTS, THIS_BEHAVIORS, LOG);
        final CoreScenarioRunner newer = replay(NEXT_BUILD, NEXT_BEHAVIORS, LOG);
        assertMixedBuildsWriteEqualBytes(older.snapshotBytes(), newer.snapshotBytes());
        older.execAll(TAIL);
        newer.execAll(TAIL);
        assertMixedBuildsWriteEqualBytes(older.snapshotBytes(), newer.snapshotBytes());
        assertNotEquals("the two positions are different states", sha256(golden0(older)), sha256(older.snapshotBytes()));
        // what a shadow header says about the layout follows the behaviour too
        assertEquals(6, new CoreShadowIdentity(older.engine(), BalanceSnapshotCodec.LAYOUTS).payloadLayoutHint());
        assertEquals(6, new CoreShadowIdentity(newer.engine(), NEXT_BUILD).payloadLayoutHint());
        newer.execAll(SWITCH);
        assertEquals(7, new CoreShadowIdentity(newer.engine(), NEXT_BUILD).payloadLayoutHint());
    }

    /** The control: a build that writes its newest layout whatever the log says breaks the mixed cluster. */
    @Test
    public void aBuildThatPicksTheLayoutByItsOwnVersionFailsTheEqualBytesCheck() {
        final CoreScenarioRunner older = replay(BalanceSnapshotCodec.LAYOUTS, THIS_BEHAVIORS, LOG);
        final CoreScenarioRunner newer = replay(NEXT_BUILD, NEXT_BEHAVIORS, LOG);
        final byte[] byJarVersion = sealed(NEXT_BUILD.current().codec(), newer.engine());
        final AssertionError broken = assertThrows(AssertionError.class,
                () -> assertMixedBuildsWriteEqualBytes(older.snapshotBytes(), byJarVersion));
        assertTrue(broken.getMessage(), broken.getMessage().contains(
                "two builds wrote different snapshot bytes at one log position: layout tag -6 against -7"));
        // the same engine through the rule writes the older build's bytes
        assertMixedBuildsWriteEqualBytes(older.snapshotBytes(), newer.snapshotBytes());
    }

    // ------------------------------------------------------------------------------------------------- refusals

    @Test
    public void aLayoutOlderThanThePreviousOneIsRefused() throws IOException {
        final byte[] golden = golden();
        // two builds on, v6 is two layouts back
        final String twoBack = refusal(BUILD_AFTER_NEXT, golden);
        assertTrue(twoBack, twoBack.contains("balance snapshot layout v6 (tag -6) refused: older than the previous layout;"
                + " this build reads only v8 and v7"));
        // this build: v5 and v4 are the retired ones
        for (final long old : new long[] {BalanceSnapshotCodec.LAYOUT_V5_TAG, BalanceSnapshotCodec.LAYOUT_V4_TAG}) {
            final String text = refusal(BalanceSnapshotCodec.LAYOUTS, retagged(golden, old));
            assertTrue(text, text.contains("layout v" + -old + " (tag " + old + ") refused: older than the previous layout;"
                    + " this build reads only v6"));
            assertTrue(peekRefusal(retagged(golden, old)).contains("older than the previous layout"));
            final String shadow = assertThrows(IllegalStateException.class,
                    () -> SnapshotLayoutTestBuilds.shadowPeekRequireFullImage(old)).getMessage();
            assertTrue(shadow, shadow.contains("older than the previous layout"));
        }
        // the image itself is fine: the build that carries v6 restores it
        assertSameImage("the image is intact", golden, restoredFromGolden(NEXT_BUILD, NEXT_BEHAVIORS).snapshotBytes());
    }

    @Test
    public void anUnknownLayoutTagIsRefused() throws IOException {
        // a real v7 image, written by the next build after the switch, reaches a member still on this build
        final CoreScenarioRunner next = replay(NEXT_BUILD, NEXT_BEHAVIORS, LOG);
        next.execAll(SWITCH);
        final byte[] v7 = next.snapshotBytes();
        final String newer = refusal(BalanceSnapshotCodec.LAYOUTS, v7);
        assertTrue(newer, newer.contains("balance snapshot layout tag -7 refused: unknown to this build, which reads only v6"));
        assertTrue(peekRefusal(v7).contains("layout tag -7 refused: unknown to this build"));
        final String shadow = assertThrows(IllegalStateException.class,
                () -> SnapshotLayoutTestBuilds.shadowPeekRequireFullImage(LAYOUT_V7_TAG)).getMessage();
        assertTrue(shadow, shadow.contains("layout tag -7 refused: unknown to this build"));
        for (final long tag : new long[] {0L, 6L, -3L, 0x4145534EL, Long.MIN_VALUE}) {
            final String text = refusal(BalanceSnapshotCodec.LAYOUTS, retagged(golden(), tag));
            assertTrue(text, text.contains("layout tag " + tag + " refused: unknown to this build"));
            final String onNext = refusal(NEXT_BUILD, retagged(golden(), tag));
            assertTrue(onNext, onNext.contains("unknown to this build, which reads only v7 and v6"));
        }
        // and the next build reads that v7 image
        final CoreScenarioRunner reader = replay(NEXT_BUILD, NEXT_BEHAVIORS, List.of());
        reader.restoreFrom(v7);
        assertSameImage("v7 round trip", v7, reader.snapshotBytes());
    }

    @Test
    public void anImageWhoseLayoutIsNotTheOneItsBehaviourWritesIsRefused() {
        final CoreScenarioRunner atFour = replay(NEXT_BUILD, NEXT_BEHAVIORS, LOG);
        final CoreScenarioRunner atFive = replay(NEXT_BUILD, NEXT_BEHAVIORS, LOG);
        atFive.execAll(SWITCH);
        // a readable v6 image that records behaviour 5, and a readable v7 image that records behaviour 4
        final String oldLayout = refusal(NEXT_BUILD, sealed(SnapshotLayoutTestBuilds.V6, atFive.engine()));
        assertTrue(oldLayout, oldLayout.contains("balance snapshot layout v6 does not belong to behaviour 5, which writes v7"));
        final String newLayout = refusal(NEXT_BUILD, sealed(SnapshotLayoutTestBuilds.V7, atFour.engine()));
        assertTrue(newLayout, newLayout.contains("balance snapshot layout v7 does not belong to behaviour 4, which writes v6"));
        // the rule is what refuses them: each engine's own image restores
        replay(NEXT_BUILD, NEXT_BEHAVIORS, List.of()).restoreFrom(atFour.snapshotBytes());
        replay(NEXT_BUILD, NEXT_BEHAVIORS, List.of()).restoreFrom(atFive.snapshotBytes());
    }

    @Test
    public void everyBehaviourThisBuildRunsHasALayoutThisBuildWrites() {
        for (final int behavior : CoreConfigStateMachine.SUPPORTED_BEHAVIOR_VERSIONS) {
            assertTrue("behaviour " + behavior, BalanceSnapshotCodec.LAYOUTS.writes(behavior));
            assertEquals(BalanceSnapshotCodec.LAYOUT_V6_TAG, BalanceSnapshotCodec.LAYOUTS.writtenBy(behavior).tag());
        }
        // an engine with no recorded config is behaviour 1
        assertEquals(1L, new CoreEngine().coreConfig().behaviorVersion());
        final String below = assertThrows(IllegalStateException.class,
                () -> BalanceSnapshotCodec.LAYOUTS.writtenBy(0L)).getMessage();
        assertTrue(below, below.contains("behaviour 0 writes a layout this build no longer carries"));
    }

    // -------------------------------------------------------------------------------------------------- helpers

    /**
     * The committed image restored on {@code build} is the state a replay of the log reaches on that build: equal
     * image bytes, equal state hash, and the same output and image after more log.
     */
    private static void assertRestoresLikeAReplay(final SnapshotLayouts<BalanceSnapshotCodec.Layout> build,
                                                  final int[] behaviors) throws IOException {
        final byte[] golden = golden();
        final CoreScenarioRunner restored = restoredFromGolden(build, behaviors);
        final CoreScenarioRunner replayed = replay(build, behaviors, LOG);
        final byte[] fromRestore = restored.snapshotBytes();
        final byte[] fromReplay = replayed.snapshotBytes();
        assertSameImage("the restored engine writes the committed image again", golden, fromRestore);
        assertSameImage("the replayed engine writes the committed image", golden, fromReplay);
        assertEquals("state hash (the production gauge: the sealed payload CRC32C)", stateHash(fromReplay), stateHash(fromRestore));
        assertEquals("SHA-256 of the image", sha256(fromReplay), sha256(fromRestore));

        final int mark = replayed.renderedEvents().size();
        restored.execAll(TAIL);
        replayed.execAll(TAIL);
        final List<String> continued = replayed.renderedEvents().subList(mark, replayed.renderedEvents().size());
        assertTrue("the continuation produced output", continued.size() >= 4);
        assertEquals("the log continues identically after the restore", continued, restored.renderedEvents());
        assertSameImage("and reaches the same image", replayed.snapshotBytes(), restored.snapshotBytes());
        assertNotEquals("the continuation changed the state", sha256(golden), sha256(restored.snapshotBytes()));
    }

    /** A runner standing for {@code build} that has applied {@code log} from genesis. */
    private static CoreScenarioRunner replay(final SnapshotLayouts<BalanceSnapshotCodec.Layout> build, final int[] behaviors,
                                             final List<String> log) {
        final CoreScenarioRunner runner = CoreScenarioRunner.newRunner();
        runner.useBuild(build, behaviors);
        runner.execAll(log);
        runner.useBuild(build, behaviors); // the genesis line resets the test seam to this build's set
        return runner;
    }

    private static CoreScenarioRunner restoredFromGolden(final SnapshotLayouts<BalanceSnapshotCodec.Layout> build,
                                                        final int[] behaviors) throws IOException {
        final CoreScenarioRunner runner = replay(build, behaviors, List.of());
        runner.restoreFrom(golden());
        return runner;
    }

    private static byte[] golden() throws IOException {
        return Files.readAllBytes(GOLDEN);
    }

    private static byte[] golden0(final CoreScenarioRunner unused) {
        try {
            return golden();
        } catch (final IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /** An image written by one layout's codec directly, sealed as the service seals it. */
    private static byte[] sealed(final BalanceSnapshotCodec.Layout codec, final CoreEngine engine) {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final int payloadLength = codec.write(engine, buffer, SnapshotEnvelope.payloadOffset());
        final int length = new SnapshotEnvelope().seal(buffer, payloadLength);
        final byte[] bytes = new byte[length];
        buffer.getBytes(0, bytes);
        return bytes;
    }

    /** {@code image} with another layout tag, sealed again so only the tag is wrong. */
    private static byte[] retagged(final byte[] image, final long tag) {
        final byte[] copy = image.clone();
        final UnsafeBuffer buffer = new UnsafeBuffer(copy);
        buffer.putLong(SnapshotEnvelope.payloadOffset(), tag);
        new SnapshotEnvelope().seal(buffer, copy.length - SnapshotEnvelope.HEADER_LENGTH - SnapshotEnvelope.TRAILER_LENGTH);
        return copy;
    }

    private static long tag(final byte[] image) {
        return new UnsafeBuffer(image).getLong(SnapshotEnvelope.payloadOffset());
    }

    /** The refusal text of a restore of {@code image} on {@code build}; fails when the image restores. */
    private static String refusal(final SnapshotLayouts<BalanceSnapshotCodec.Layout> build, final byte[] image) {
        final UnsafeBuffer buffer = new UnsafeBuffer(image);
        final int payloadLength = new SnapshotEnvelope().open(buffer, image.length); // the envelope is intact
        final CoreEngine fresh = new CoreEngine();
        fresh.coreConfig().setSupportedBehaviorVersions(NEXT_BEHAVIORS);
        return assertThrows(IllegalStateException.class, () -> BalanceSnapshotCodec.deserialize(build, buffer,
                SnapshotEnvelope.payloadOffset(), payloadLength, fresh)).getMessage();
    }

    private static String peekRefusal(final byte[] image) {
        final UnsafeBuffer buffer = new UnsafeBuffer(image);
        final int payloadLength = new SnapshotEnvelope().open(buffer, image.length);
        return assertThrows(IllegalStateException.class, () -> BalanceSnapshotCodec.peekEventSeq(buffer,
                SnapshotEnvelope.payloadOffset(), payloadLength)).getMessage();
    }

    /** The rule a cluster on two builds lives by: at one log position both write the same image. */
    private static void assertMixedBuildsWriteEqualBytes(final byte[] older, final byte[] newer) {
        if (!Arrays.equals(older, newer)) {
            throw new AssertionError("two builds wrote different snapshot bytes at one log position: layout tag "
                    + tag(older) + " against " + tag(newer) + ", " + older.length + " against " + newer.length + " bytes");
        }
    }

    private static void assertSameImage(final String what, final byte[] expected, final byte[] actual) {
        final int at = Arrays.mismatch(expected, actual);
        if (at >= 0) {
            throw new AssertionError(what + ": first difference at byte " + at + " (" + expected.length + " against "
                    + actual.length + " bytes)");
        }
    }

    private static long stateHash(final byte[] image) {
        return SnapshotEnvelope.sealedCrc(new UnsafeBuffer(image), image.length);
    }

    private static String sha256(final byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
