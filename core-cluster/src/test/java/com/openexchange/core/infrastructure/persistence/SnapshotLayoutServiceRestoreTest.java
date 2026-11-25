// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.cluster.SnapshotEnvelope;
import com.openexchange.core.application.engine.CoreEngine;
import com.openexchange.core.determinism.V3Genesis;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static com.openexchange.core.infrastructure.persistence.SnapshotLayoutTestBuilds.NEXT_BEHAVIORS;
import static com.openexchange.core.infrastructure.persistence.SnapshotLayoutTestBuilds.NEXT_BUILD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * The layout refusals through the service's restore path: what the operator reads when a member is handed an image
 * whose layout is not the one its recorded behaviour writes is the layout reason, and the node stops once.
 */
public class SnapshotLayoutServiceRestoreTest {

    private static final long NOW = 1_700_000_000_000L;

    /** An engine on the next build at behaviour 5, the behaviour that adopts v7. */
    private static CoreEngine atBehaviourFive() {
        final CoreEngine engine = new CoreEngine();
        engine.setEventSink(NoopCoreEventSink.INSTANCE);
        V3Genesis.boot(engine, NOW, 4L);
        engine.coreConfig().setSupportedBehaviorVersions(NEXT_BEHAVIORS);
        engine.applyCoreConfig(V3Genesis.config(5L, 5L), NOW);
        assertEquals(5L, engine.coreConfig().behaviorVersion());
        return engine;
    }

    private static int sealed(final BalanceSnapshotCodec.Layout codec, final CoreEngine engine, final ExpandableArrayBuffer image) {
        return new SnapshotEnvelope().seal(image, codec.write(engine, image, SnapshotEnvelope.payloadOffset()));
    }

    /** A service standing for the next build. */
    private static CoreClusteredService nextBuildService(final int[] stops) {
        final CoreClusteredService service = new CoreClusteredService(SnapshotModeSettings.defaults());
        service.failFast = () -> stops[0]++;
        service.applier().useLayouts(NEXT_BUILD);
        service.engine().coreConfig().setSupportedBehaviorVersions(NEXT_BEHAVIORS);
        return service;
    }

    @Test
    public void anImageInTheWrongLayoutForItsBehaviourStopsTheNodeWithTheLayoutReason() {
        // a well-formed v6 image that records behaviour 5: every section decodes, every byte is consumed
        final ExpandableArrayBuffer image = new ExpandableArrayBuffer();
        final int length = sealed(SnapshotLayoutTestBuilds.V6, atBehaviourFive(), image);
        final int[] stops = new int[1];
        final CoreClusteredService service = nextBuildService(stops);

        final PrintStream err = System.err;
        final ByteArrayOutputStream captured = new ByteArrayOutputStream();
        final IllegalStateException refused;
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            refused = assertThrows(IllegalStateException.class, () -> service.restoreFromImage(image, length));
        } finally {
            System.setErr(err);
        }
        final String reason = "balance snapshot layout v6 does not belong to behaviour 5, which writes v7";
        assertEquals(reason, refused.getMessage());
        final String log = captured.toString(StandardCharsets.UTF_8);
        assertTrue(log, log.contains("FATAL snapshot refused: " + reason));
        assertEquals("the node stops once", 1, stops[0]);
    }

    @Test
    public void theSameStateInTheLayoutItsBehaviourWritesRestores() {
        // the control: the refusal above is the layout rule, not the image or the service set-up
        final ExpandableArrayBuffer image = new ExpandableArrayBuffer();
        final CoreEngine source = atBehaviourFive();
        final int length = sealed(NEXT_BUILD.writtenBy(5L).codec(), source, image);
        final int[] stops = new int[1];
        final CoreClusteredService service = nextBuildService(stops);
        // the peek ahead of the applier knows this build's layouts only, so the v7 image goes in through the applier
        service.applier().restore(image, length);
        assertEquals(0, stops[0]);
        assertEquals(5L, service.engine().coreConfig().behaviorVersion());
        assertEquals(source.accountCount(), service.engine().accountCount());
    }
}
