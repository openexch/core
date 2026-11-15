// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.application.engine.config;

import com.openexchange.core.application.engine.CoreEngine;
import com.openexchange.core.application.engine.feed.V3Rig;
import com.openexchange.core.application.engine.gate.OrderEntryGate;
import com.openexchange.core.determinism.CoreEngineEvent;
import com.openexchange.core.determinism.RecordingCoreSink;
import com.openexchange.core.determinism.V3Genesis;
import com.openexchange.core.domain.AdminResult;
import com.openexchange.core.domain.ConfigKind;
import com.openexchange.core.domain.CoreRejectReason;
import com.openexchange.core.domain.GateReason;
import com.openexchange.core.domain.GateState;
import com.openexchange.core.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The behaviour 4 transition rules. 3 to 4 is a plain CoreConfig step with no precondition: it may be taken with the
 * gate open and orders live, converts nothing, emits its ack and its ConfigApplied and nothing else. A genesis reaches
 * 4 by the ladder (ADOPT at 1, one step at a time); no config is adopted at 4 directly and no step is skipped or taken
 * backwards. Every rule that holds from behaviour 3 on holds under 4: the same feed story gives the same events and
 * the same state.
 */
public class BehaviorFourTransitionTest {

    @Test
    public void theProductionJarRunsBehaviourFour() {
        assertArrayEquals(new int[] {1, 2, 3, 4}, CoreConfigStateMachine.SUPPORTED_BEHAVIOR_VERSIONS);
        assertArrayEquals(new int[] {1, 2, 3, 4}, new CoreEngine().coreConfig().supportedBehaviorVersions());
    }

    @Test
    public void threeToFourWithTheGateOpenAndOrdersLive() {
        final V3Rig r = V3Rig.open();
        r.twoParty();
        assertTrue(r.sellHits(1, V3Rig.ONE)); // both orders partially filled, legs resting
        final int reasonsBefore = r.globalReasons();
        final long eventSeq = r.engine.getEventSeq();
        final long dispatchSeq = r.engine.getDispatchSeq();
        final long switches = r.engine.coreConfig().switchCount();
        assertEquals("precondition: GLOBAL is open", GateState.OPEN, r.engine.gate().state(OrderEntryGate.GLOBAL));
        r.mark();
        r.engine.applyCoreConfig(V3Genesis.config(4, 4), r.now);
        assertEquals("the ack and the ConfigApplied, nothing else (no order event, no admission, no dispatch)", List.of(
                        new CoreEngineEvent.AdminAck(7_004L, 70, AdminResult.ACCEPTED, 0, 4L),
                        new CoreEngineEvent.ConfigApplied(ConfigKind.CORE, 0, 4L)), r.events());
        assertEquals(4L, r.engine.coreConfig().behaviorVersion());
        assertEquals(switches + 1, r.engine.coreConfig().switchCount());
        assertEquals("the gate is untouched", GateState.OPEN, r.engine.gate().state(OrderEntryGate.GLOBAL));
        assertEquals(reasonsBefore, r.globalReasons());
        assertEquals(eventSeq, r.engine.getEventSeq());
        assertEquals(dispatchSeq, r.engine.getDispatchSeq());
        assertEquals("the live orders are untouched", 2, r.engine.orders().orders().size());
        // the feed continues on the same cursor under 4
        assertTrue(r.sellHits(2, V3Rig.ONE));
        assertTrue(r.terminal(1, 101, 1, V3Rig.ME_FILLED, 0));
        r.assertNotLatched("after the step to 4");
        // a resend is a DUPLICATE, a step back is refused, a skipped step is refused
        r.mark();
        r.engine.applyCoreConfig(V3Genesis.config(4, 4), r.now);
        assertEquals(AdminResult.DUPLICATE, r.onlyAck().result());
        r.mark();
        r.engine.applyCoreConfig(V3Genesis.config(5, 3), r.now);
        assertEquals(AdminResult.REJECTED, r.onlyAck().result());
        r.mark();
        r.engine.applyCoreConfig(V3Genesis.config(5, 6), r.now);
        assertEquals(AdminResult.REJECTED, r.onlyAck().result());
        assertEquals(4L, r.engine.coreConfig().behaviorVersion());
    }

    @Test
    public void aGenesisReachesFourByTheLadderOnly() {
        // no config is adopted at 4 (or at any behaviour but 1)
        final CoreEngine direct = new CoreEngine();
        final RecordingCoreSink sink = new RecordingCoreSink();
        direct.setEventSink(sink);
        direct.applyCoreConfig(V3Genesis.config(1, 4), 1_000L);
        assertEquals(List.of(new CoreEngineEvent.AdminAck(7_001L, 70, AdminResult.REJECTED, CoreRejectReason.INVALID.code(), 0L)),
                sink.events());
        assertEquals("nothing recorded", null, direct.coreConfig().recorded());
        // 2 to 4 skips a step
        direct.applyCoreConfig(V3Genesis.config(1, 1), 1_000L);
        direct.applyCoreConfig(V3Genesis.config(2, 2), 1_000L);
        sink.events();
        final int mark = sink.events().size();
        direct.applyCoreConfig(V3Genesis.config(3, 4), 1_000L);
        assertEquals(AdminResult.REJECTED, ((CoreEngineEvent.AdminAck) sink.events().get(mark)).result());
        assertEquals(2L, direct.coreConfig().behaviorVersion());

        // the ladder, taken while GLOBAL is still SUSPENDED at genesis and before the feed binds
        final V3Rig r = V3Rig.unbound();
        r.engine.applyCoreConfig(V3Genesis.config(4, 4), r.now);
        assertEquals(4L, r.engine.coreConfig().behaviorVersion());
        assertEquals("GLOBAL stays suspended for GENESIS", GateState.SUSPENDED, r.engine.gate().state(OrderEntryGate.GLOBAL));
        assertTrue(r.globalHas(GateReason.GENESIS));
        // the behaviour 3 open rule still holds under 4: no ADMIN OPEN before the feed is bound
        r.mark();
        r.adminGate(OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN);
        assertEquals(CoreRejectReason.ME_LANE_INACTIVE.code(), r.onlyAck().reason());
        assertTrue("the feed binds under 4", r.bindFresh());
        r.mark();
        r.adminGate(OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN);
        assertEquals(AdminResult.ACCEPTED, r.onlyAck().result());
        r.twoParty();
        assertTrue(r.sellHits(1, 2 * V3Rig.ONE));
    }

    /** The same feed story on a rig; returns the rendered events after the genesis. */
    private static List<String> story(final V3Rig r) {
        r.mark();
        r.twoParty();
        r.sellHits(1, V3Rig.ONE);
        r.amend(1, 1, 99 * V3Rig.ONE, 0);
        r.adminGate(OrderEntryGate.GLOBAL, GateState.SUSPENDED, GateReason.ADMIN);
        r.engine.orders().applyCreate(r.limit(1, 0, 95 * V3Rig.ONE, V3Rig.ONE), r.now); // GATE_CLOSED, first time
        r.adminGate(OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN);
        final long third = r.create(r.limit(1, 0, 95 * V3Rig.ONE, V3Rig.ONE));
        r.ack(third, 0, 303);
        r.sellHitsAt(r.nextJseq() + 5, V3Rig.EPOCH, ++r.egress, 9, V3Rig.ONE); // a gap: the feed latches, BRIDGE_HALT
        r.terminal(2, 202, 2, V3Rig.ME_CANCELLED, 0);
        return new ArrayList<>(r.renders());
    }

    @Test
    public void theBehaviourThreeRulesHoldUnderFour() {
        final V3Rig three = V3Rig.open();
        final V3Rig four = V3Rig.openAt(4);
        final List<String> a = story(three);
        final List<String> b = story(four);
        assertTrue("non-vacuous: the story dispatches, fills and latches, got " + a,
                a.stream().anyMatch(l -> l.startsWith("MEDISPATCH")) && a.stream().anyMatch(l -> l.contains("kind=FILL"))
                        && a.stream().anyMatch(l -> l.startsWith("FEEDLATCH")));
        assertEquals("a story with no retried create is the same under 3 and under 4", a, b);
        assertEquals(three.globalReasons(), four.globalReasons());
        assertEquals(three.engine.getEventSeq(), four.engine.getEventSeq());
        assertEquals(three.engine.getDispatchSeq(), four.engine.getDispatchSeq());
        assertEquals(three.cursor(), four.cursor());
        assertFalse("and the two cores do differ in their recorded behaviour",
                three.engine.coreConfig().behaviorVersion() == four.engine.coreConfig().behaviorVersion());
    }

    @Test
    public void aBehaviourFourImageRestoresAndAJarWithoutFourStops() {
        final V3Rig r = V3Rig.openAt(4);
        r.twoParty();
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final int length = BalanceSnapshotCodec.serialize(r.engine, buf, 0);
        final V3Rig fork = r.fork();
        assertEquals(4L, fork.engine.coreConfig().behaviorVersion());
        final ExpandableArrayBuffer again = new ExpandableArrayBuffer();
        assertEquals(length, BalanceSnapshotCodec.serialize(fork.engine, again, 0));
        // a jar that runs only 1 to 3 must not serve a behaviour 4 image: the node-local cross-check stops it
        final CoreEngine old = new CoreEngine();
        old.coreConfig().setSupportedBehaviorVersions(1, 2, 3);
        final boolean[] stopped = {false};
        old.setConfigFailFast(() -> stopped[0] = true);
        BalanceSnapshotCodec.deserialize(buf, 0, length, old);
        assertTrue("the cross-check ran the fail-fast hook", stopped[0]);
    }
}
