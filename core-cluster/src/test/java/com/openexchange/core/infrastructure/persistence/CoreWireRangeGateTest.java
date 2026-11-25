// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.application.engine.CoreEngine;
import com.openexchange.core.application.projection.SettlementProjector;
import com.openexchange.core.determinism.RecordingCoreSink;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * The ingress wire gate: the schema id, then a frame above the recorded ceiling is dropped from its header alone
 * whatever the jar, then the jar range. A jar that could decode the frame (the seam stands in for the next jar,
 * money-schema 11) drops it exactly like the jar at the ceiling until the raise is applied.
 */
public class CoreWireRangeGateTest {

    private static final long ONE = 100_000_000L;

    /** One member: an engine and the demuxer of a jar whose top supported version is {@code jarMax}. */
    private static final class Member {
        final CoreEngine engine = new CoreEngine();
        final CoreSbeDemuxer demuxer;
        final List<String> stops = new ArrayList<>();

        Member(final int jarMax) {
            engine.setEventSink(new RecordingCoreSink());
            engine.coreConfig().setSupportedMaxWireVersion(jarMax);
            engine.setConfigFailFast(() -> stops.add(engine.coreConfig().crossCheck()));
            demuxer = new CoreSbeDemuxer(engine, new SettlementProjector(engine), jarMax);
        }

        void dispatch(final byte[] frame) {
            demuxer.dispatchTrusted(new UnsafeBuffer(frame), 0, frame.length, 1L);
        }

        long available(final long user) {
            return engine.account(user) == null ? 0L : engine.account(user).available(0);
        }
    }

    private final WireVersionFrames f = new WireVersionFrames();

    @Test
    public void v10IsAcceptedAndV11IsDroppedAboveTheCeilingByBothJarsUntilTheRaise() {
        final Member atTen = new Member(10);
        final Member atEleven = new Member(11);
        for (final Member m : List.of(atTen, atEleven)) {
            m.dispatch(f.coreConfig(10, 1, 1, 1, 10));
            assertEquals(10L, m.engine.coreConfig().recordedMaxWireVersion());
            m.dispatch(f.deposit(10, 2, 1, 0, 5 * ONE));
            assertEquals("v10 accepted", 5 * ONE, m.available(1));
            m.dispatch(f.deposit(11, 3, 1, 0, 7 * ONE));
            assertEquals("v11 above the recorded ceiling: no state change", 5 * ONE, m.available(1));
            assertEquals(1L, m.demuxer.aboveCeilingDropCount());
            assertEquals("never the jar's schema drop", 0L, m.demuxer.schemaRejectCount());
            // a raise encoded at v11 is itself above the ceiling: dropped, nothing recorded, no ack
            m.dispatch(f.coreConfig(11, 4, 2, 1, 11));
            assertEquals(2L, m.demuxer.aboveCeilingDropCount());
            assertEquals(10L, m.engine.coreConfig().recordedMaxWireVersion());
            assertEquals(1L, m.engine.coreConfig().recorded().configVersion);
        }
        assertEquals(List.of(), atTen.stops);
        assertEquals(List.of(), atEleven.stops);

        // the raise, encoded at v10 (by the previous admin tool)
        atTen.dispatch(f.coreConfig(10, 5, 2, 1, 11));
        atEleven.dispatch(f.coreConfig(10, 5, 2, 1, 11));
        assertEquals(11L, atTen.engine.coreConfig().recordedMaxWireVersion());
        assertEquals(11L, atEleven.engine.coreConfig().recordedMaxWireVersion());
        assertEquals("the jar at 10 stops itself at the raise",
                List.of("recorded maxWireVersion=11 is above this jar's money-schema version 10"), atTen.stops);
        assertEquals(List.of(), atEleven.stops);

        // after the raise the next jar applies v11, and v12 is above the new ceiling
        atEleven.dispatch(f.deposit(11, 6, 1, 0, 7 * ONE));
        assertEquals(12 * ONE, atEleven.available(1));
        atEleven.dispatch(f.deposit(12, 7, 1, 0, ONE));
        assertEquals(12 * ONE, atEleven.available(1));
        assertEquals(3L, atEleven.demuxer.aboveCeilingDropCount());
        assertEquals(0L, atEleven.demuxer.schemaRejectCount());
    }

    @Test
    public void theCeilingDecidesBeforeTheJarRangeOnEitherJar() {
        // a ceiling of 10 recorded; the next jar (seam 11) and this jar (seam 10) agree on every header version
        final Member atTen = new Member(10);
        final Member atEleven = new Member(11);
        for (final Member m : List.of(atTen, atEleven)) {
            m.dispatch(f.coreConfig(10, 1, 1, 1, 10));
            for (final int v : new int[] {11, 12, 65_535}) {
                m.dispatch(f.deposit(v, 10 + v, 1, 0, ONE));
            }
            assertEquals(3L, m.demuxer.aboveCeilingDropCount());
            assertEquals(0L, m.demuxer.schemaRejectCount());
            assertEquals(0L, m.available(1));
        }
    }

    @Test
    public void withNothingRecordedTheJarRangeDecides() {
        // the virgin-cluster hole, documented: no ceiling yet, so the next jar takes a v11 frame this jar drops
        final Member atTen = new Member(10);
        final Member atEleven = new Member(11);
        atTen.dispatch(f.deposit(11, 1, 1, 0, ONE));
        atEleven.dispatch(f.deposit(11, 1, 1, 0, ONE));
        assertEquals(1L, atTen.demuxer.schemaRejectCount());
        assertEquals(0L, atTen.available(1));
        assertEquals(0L, atEleven.demuxer.schemaRejectCount());
        assertEquals(ONE, atEleven.available(1));
        assertEquals(0L, atTen.demuxer.aboveCeilingDropCount() + atEleven.demuxer.aboveCeilingDropCount());
    }

    @Test
    public void outOfRangeWrongSchemaAndShortBlockAreOneRuleEach() {
        final Member m = new Member(10);
        m.dispatch(f.coreConfig(10, 1, 1, 1, 10));
        // below the floor: the jar range, not the ceiling
        m.dispatch(f.deposit(9, 2, 1, 0, ONE));
        assertEquals(1L, m.demuxer.schemaRejectCount());
        assertEquals(0L, m.demuxer.aboveCeilingDropCount());
        // another schema id above the ceiling version is a schema drop: the ceiling applies to schema 2 only
        final byte[] foreign = f.deposit(11, 3, 1, 0, ONE);
        new UnsafeBuffer(foreign).putShort(4, (short) 1, ByteOrder.LITTLE_ENDIAN);
        m.dispatch(foreign);
        assertEquals(2L, m.demuxer.schemaRejectCount());
        assertEquals(0L, m.demuxer.aboveCeilingDropCount());
        // a v10 frame whose declared block is shorter than the v10 block: the frame-bounds rule
        final byte[] shortBlock = f.deposit(10, 4, 1, 0, ONE);
        new UnsafeBuffer(shortBlock).putShort(0, (short) 8, ByteOrder.LITTLE_ENDIAN);
        m.dispatch(shortBlock);
        assertEquals(1L, m.demuxer.decodeRejectCount());
        assertEquals(0L, m.available(1));
        assertNull(m.engine.account(1));
    }

    /**
     * The floor is a jar constant, so two jars with different floors would apply and drop the same frame. It moves only
     * together with a member compatibility generation change (a clean genesis), never with a routine bump.
     */
    @Test
    public void theFloorIsPinned() {
        assertEquals("the jar floor moves only with a clean genesis", 10, CoreSbeDemuxer.MIN_SUPPORTED_SCHEMA_VERSION);
        assertEquals(10, CoreSbeDemuxer.EXPECTED_SCHEMA_VERSION);
    }

    @Test
    public void theAboveCeilingDropIsCountedAndLoggedFirstThenOneInAThousand() {
        final Member m = new Member(11);
        m.dispatch(f.coreConfig(10, 1, 1, 1, 10));
        final PrintStream err = System.err;
        final ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true));
        try {
            for (int i = 0; i < 1_001; i++) {
                m.dispatch(f.deposit(11, 100 + i, 1, 0, ONE));
            }
        } finally {
            System.setErr(err);
        }
        final String log = captured.toString();
        assertTrue(log, log.contains("INGRESS DROP (above ceiling): templateId=1 version=11 is above the recorded"
                + " maxWireVersion=10; aboveCeilingDrops=1"));
        assertEquals(1_001L, m.demuxer.aboveCeilingDropCount());
        assertEquals("first, then one in every thousand", 2, log.split("INGRESS DROP \\(above ceiling\\)", -1).length - 1);
    }
}
