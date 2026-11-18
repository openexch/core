// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.publisher;

import com.openexchange.core.application.engine.CoreEngine;
import com.openexchange.core.application.engine.feed.V3Rig;
import com.openexchange.core.determinism.V3Genesis;
import com.openexchange.core.domain.CoreConfigReportView;
import com.openexchange.core.domain.CoreEventSink;
import com.openexchange.core.domain.commands.CoreConfigCommand;
import com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder;
import com.openexchange.core.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.core.infrastructure.generated.OrderEventsEndDecoder;
import com.openexchange.core.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The genesis id on the two query replies (OrderEventsEnd 116, CoreConfigReport 118, money-schema v9), as the production
 * publisher writes them: 0 while no CoreConfig is recorded, the recorded CoreConfig's genesisId once one is ADOPTed, the
 * same after a refused re-genesis, and the same on an engine restored from the snapshot image, whose replies are byte
 * equal to the original's. A reply is not state: the snapshot image does not change.
 */
public class GenesisIdReplyTest {

    private static final long NOW = 1_700_000_000_000L;

    private record Frame(int templateId, byte[] bytes) { }

    private static final class Capture implements CoreEventPublisher.Egress {
        final List<Frame> frames = new ArrayList<>();

        @Override
        public void emit(final org.agrona.MutableDirectBuffer buffer, final int offset, final int length, final int channel,
                         final CoreEventPublisher.Routing routing) {
            final byte[] b = new byte[length];
            buffer.getBytes(offset, b);
            frames.add(new Frame(new MessageHeaderDecoder().wrap(buffer, offset).templateId(), b));
        }
    }

    /** Asks both queries of {@code engine} (a head probe and the config report) and returns the reply frames. */
    private static List<Frame> ask(final CoreEngine engine, final Capture capture) {
        capture.frames.clear();
        engine.orders().applyRequestOrderEvents(0x116L, 0, 0);
        engine.requestCoreConfig(0x118L);
        return new ArrayList<>(capture.frames);
    }

    /**
     * The check every row of this class runs: one OrderEventsEnd and one CoreConfigReport, each carrying
     * {@code expected}. Fails with {@code genesisId on the wire is <got> ..., the recorded genesis is <expected>}.
     */
    static void assertGenesisOnReplies(final List<Frame> frames, final long expected, final String stage) {
        int ends = 0;
        int reports = 0;
        for (final Frame f : frames) {
            final long got;
            if (f.templateId == OrderEventsEndDecoder.TEMPLATE_ID) {
                ends++;
                got = new OrderEventsEndDecoder().wrapAndApplyHeader(new UnsafeBuffer(f.bytes), 0, new MessageHeaderDecoder())
                        .genesisId();
            } else if (f.templateId == CoreConfigReportDecoder.TEMPLATE_ID) {
                reports++;
                got = new CoreConfigReportDecoder().wrapAndApplyHeader(new UnsafeBuffer(f.bytes), 0, new MessageHeaderDecoder())
                        .genesisId();
            } else {
                continue;
            }
            if (got != expected) {
                throw new AssertionError("genesisId on the wire is " + got + " in template " + f.templateId
                        + ", the recorded genesis is " + expected + " (" + stage + ")");
            }
        }
        if (ends != 1 || reports != 1) {
            throw new AssertionError(stage + ": expected one OrderEventsEnd and one CoreConfigReport, got " + ends + " and "
                    + reports);
        }
    }

    private static CoreEngine publishingEngine(final Capture capture) {
        final CoreEngine engine = new CoreEngine();
        V3Genesis.widen(engine);
        engine.setEventSink(new CoreEventPublisher(capture, () -> false));
        return engine;
    }

    @Test
    public void zeroBeforeAdoptTheRecordedIdAfterAndUnchangedByARefusedReGenesis() {
        final Capture capture = new Capture();
        final CoreEngine engine = publishingEngine(capture);
        assertEquals("precondition: nothing recorded", null, engine.coreConfig().recorded());
        assertGenesisOnReplies(ask(engine, capture), 0L, "before any CoreConfig");

        engine.applyCoreConfig(V3Genesis.config(1, 1), NOW);
        assertEquals("precondition: ADOPTed", V3Genesis.GENESIS_ID, engine.coreConfig().recorded().genesisId);
        assertGenesisOnReplies(ask(engine, capture), V3Genesis.GENESIS_ID, "after ADOPT");

        // a CoreConfig of another genesis is a re-genesis attempt: refused, the recorded id stays
        final CoreConfigCommand other = V3Genesis.config(2, 2);
        other.genesisId = V3Genesis.GENESIS_ID + 1;
        engine.applyCoreConfig(other, NOW);
        assertEquals(V3Genesis.GENESIS_ID, engine.coreConfig().recorded().genesisId);
        assertGenesisOnReplies(ask(engine, capture), V3Genesis.GENESIS_ID, "after a refused re-genesis");

        engine.applyCoreConfig(V3Genesis.config(2, 2), NOW);
        engine.applyCoreConfig(V3Genesis.config(3, 3), NOW);
        assertEquals(3L, engine.coreConfig().behaviorVersion());
        assertGenesisOnReplies(ask(engine, capture), V3Genesis.GENESIS_ID, "after the switches to behaviour 3");
    }

    @Test
    public void aPageTerminatorCarriesItTooAndARestoredEngineAnswersByteForByteTheSame() {
        final Capture capture = new Capture();
        final V3Rig r = V3Rig.openAt(4);
        r.engine.setEventSink(new CoreEventPublisher(capture, () -> false));
        r.deposit(1, V3Rig.USD, 10_000_000 * V3Rig.ONE);
        for (int i = 0; i < 5; i++) {
            r.engine.orders().applyCreate(r.limit(1, 0, V3Rig.ONE, V3Rig.ONE), r.now);
            r.now += 25;
        }
        assertEquals(5L, r.engine.getEventSeq());
        r.engine.setLogPosition(8_192L);

        capture.frames.clear();
        r.engine.orders().applyRequestOrderEvents(0x115L, 0, 100);
        final List<Frame> page = new ArrayList<>(capture.frames);
        assertEquals("a page then the terminator", 2, page.size());
        final OrderEventsEndDecoder end = new OrderEventsEndDecoder().wrapAndApplyHeader(
                new UnsafeBuffer(page.get(1).bytes), 0, new MessageHeaderDecoder());
        assertEquals(5, end.count());
        assertEquals(V3Genesis.GENESIS_ID, end.genesisId());
        assertGenesisOnReplies(ask(r.engine, capture), V3Genesis.GENESIS_ID, "live");
        final List<Frame> live = ask(r.engine, capture);

        // restore: a new engine from the snapshot image, as a member restarting from its snapshot does
        final ExpandableArrayBuffer image = new ExpandableArrayBuffer();
        final int length = BalanceSnapshotCodec.serialize(r.engine, image, 0);
        final CoreEngine restored = new CoreEngine();
        restored.coreConfig().setSupportedBehaviorVersions(1, 2, 3, 4);
        BalanceSnapshotCodec.deserialize(image, 0, length, restored);
        restored.verifyRestoredInvariants();
        restored.setLogPosition(8_192L);
        final Capture restoredCapture = new Capture();
        restored.setEventSink(new CoreEventPublisher(restoredCapture, () -> false));
        final List<Frame> after = ask(restored, restoredCapture);
        assertGenesisOnReplies(after, V3Genesis.GENESIS_ID, "after a restore from the snapshot");
        assertEquals(live.size(), after.size());
        for (int i = 0; i < live.size(); i++) {
            assertArrayEquals("reply " + i + " byte equal after the restore", live.get(i).bytes, after.get(i).bytes);
        }

        // the replies are not state: the restored engine's image is the original's, byte for byte
        final ExpandableArrayBuffer again = new ExpandableArrayBuffer();
        final int againLength = BalanceSnapshotCodec.serialize(restored, again, 0);
        assertEquals(length, againLength);
        for (int i = 0; i < length; i++) {
            if (image.getByte(i) != again.getByte(i)) {
                fail("the restored image differs at byte " + i);
            }
        }
    }

    /**
     * Negative control: an encoder that writes 0 always (the publisher fed through a sink that drops the id) must fail
     * the check after ADOPT with its text, and pass it before ADOPT, where 0 is the right answer.
     */
    @Test
    public void controlAnEncoderThatWritesZeroAlwaysIsCaught() {
        final Capture capture = new Capture();
        final CoreEngine engine = publishingEngine(capture);
        final CoreEventSink publisher = new CoreEventPublisher(capture, () -> false);
        engine.setEventSink(zeroGenesis(publisher, true, true));
        assertGenesisOnReplies(ask(engine, capture), 0L, "control, before ADOPT");
        engine.applyCoreConfig(V3Genesis.config(1, 1), NOW);
        try {
            assertGenesisOnReplies(ask(engine, capture), V3Genesis.GENESIS_ID, "control, after ADOPT");
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(), expected.getMessage().startsWith("genesisId on the wire is 0 in template 116, "
                    + "the recorded genesis is " + V3Genesis.GENESIS_ID));
            System.out.println("[control] zero encoder caught: " + expected.getMessage());
            return;
        }
        fail("an encoder that writes 0 always passed the genesis check");
    }

    /**
     * Negative control for the config report alone: an encoder that writes 0 only into CoreConfigReport must fail the
     * check with the text naming template 118, so that branch of the check is shown to catch it too.
     */
    @Test
    public void controlAConfigReportThatWritesZeroIsCaught() {
        final Capture capture = new Capture();
        final CoreEngine engine = publishingEngine(capture);
        engine.setEventSink(zeroGenesis(new CoreEventPublisher(capture, () -> false), false, true));
        engine.applyCoreConfig(V3Genesis.config(1, 1), NOW);
        try {
            assertGenesisOnReplies(ask(engine, capture), V3Genesis.GENESIS_ID, "control, report only");
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(), expected.getMessage().startsWith("genesisId on the wire is 0 in template 118, "
                    + "the recorded genesis is " + V3Genesis.GENESIS_ID));
            System.out.println("[control] zero config report caught: " + expected.getMessage());
            return;
        }
        fail("a config report that writes 0 passed the genesis check");
    }

    /** {@code real} with the genesis id of the chosen query replies replaced by 0; every other call passes through. */
    private static CoreEventSink zeroGenesis(final CoreEventSink real, final boolean zeroEnd, final boolean zeroReport) {
        return (CoreEventSink) java.lang.reflect.Proxy.newProxyInstance(CoreEventSink.class.getClassLoader(),
                new Class<?>[] {CoreEventSink.class}, (proxy, method, args) -> {
                    if (zeroEnd && "onOrderEventsEnd".equals(method.getName())) {
                        args[5] = 0L;
                    } else if (zeroReport && "onCoreConfigReport".equals(method.getName())) {
                        final CoreConfigReportView report = (CoreConfigReportView) args[1];
                        args[1] = java.lang.reflect.Proxy.newProxyInstance(CoreConfigReportView.class.getClassLoader(),
                                new Class<?>[] {CoreConfigReportView.class}, (p, m, a) -> "genesisId".equals(m.getName())
                                        ? (Object) 0L : m.invoke(report, a));
                    }
                    return method.invoke(real, args);
                });
    }
}
