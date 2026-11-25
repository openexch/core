// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.application.engine.config;

import com.openexchange.core.application.engine.CoreEngine;
import com.openexchange.core.application.engine.config.CoreConfigStateMachine.Decision;
import com.openexchange.core.determinism.CoreScenarioRunner;
import com.openexchange.core.determinism.RecordingCoreSink;
import com.openexchange.core.domain.commands.CoreConfigCommand;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * The wire ceiling rows of the CoreConfig decision table: the ceiling is valid in [10, 65534], recorded as sent at
 * ADOPT, part of the DUPLICATE equality, never lowered, and installed by ACCEPT and SWITCH. The node-local check stops a
 * member whose jar decodes less than the recorded ceiling.
 */
public final class CoreConfigCeilingTableTest {

    private CoreEngine engine;
    private CoreConfigStateMachine sm;
    private final List<String> stops = new ArrayList<>();
    private long corr = 9_000L;

    @Before
    public void setUp() {
        engine = new CoreEngine();
        engine.setEventSink(new RecordingCoreSink());
        sm = engine.coreConfig();
        engine.setConfigFailFast(() -> stops.add(sm.crossCheck()));
    }

    private CoreConfigCommand config(final long version, final long behavior, final long ceiling) {
        final CoreConfigCommand c = CoreScenarioRunner.coreConfigFor(version, behavior, 1L, 1L, 1_000_000_000_000_000L, 0L,
                corr++);
        c.maxWireVersion = ceiling;
        return c;
    }

    private Decision decideAndApply(final CoreConfigCommand c) {
        final Decision d = sm.decide(c, false, true);
        engine.applyCoreConfig(c, 1L);
        return d;
    }

    // ------------------------------------------------------------------ row 1: the range

    @Test
    public void row1_aCeilingOutsideTheRangeIsInvalid() {
        for (final long bad : new long[] {0L, 9L, 65_535L, -1L, 1L << 20}) {
            final CoreConfigCommand c = config(1, 1, bad);
            assertEquals("maxWireVersion=" + bad + " must be in [10, 65534]", CoreConfigStateMachine.invalidReason(c));
            assertEquals("ceiling " + bad, Decision.INVALID, decideAndApply(c));
            assertNull("nothing recorded", sm.recorded());
        }
        assertEquals(0L, sm.recordedMaxWireVersion());
    }

    @Test
    public void row1_theEdgesOfTheRangeAreValid() {
        assertNull(CoreConfigStateMachine.invalidReason(config(1, 1, 10)));
        assertNull(CoreConfigStateMachine.invalidReason(config(1, 1, 65_534)));
    }

    // ------------------------------------------------------------------ row 2: ADOPT records it as sent

    @Test
    public void row2_adoptRecordsTheCeilingAsSent() {
        sm.setSupportedMaxWireVersion(12);
        assertEquals(Decision.ADOPT, decideAndApply(config(1, 1, 12)));
        assertEquals(12L, sm.recordedMaxWireVersion());
        assertEquals(List.of(), stops);
    }

    // ------------------------------------------------------------------ row 4: DUPLICATE includes the ceiling

    @Test
    public void row4_anEqualResendIsDuplicateAndAnotherCeilingAtTheSameVersionIsNot() {
        assertEquals(Decision.ADOPT, decideAndApply(config(1, 1, 10)));
        assertEquals(Decision.DUPLICATE, decideAndApply(config(1, 1, 10)));
        assertEquals("same configVersion, another ceiling: STALE, not DUPLICATE", Decision.STALE,
                decideAndApply(config(1, 1, 11)));
        assertEquals(10L, sm.recordedMaxWireVersion());
    }

    // ------------------------------------------------------------------ row 7: the ceiling only rises

    @Test
    public void row7_aLoweredCeilingIsInvalidAndNothingChanges() {
        sm.setSupportedMaxWireVersion(11);
        assertEquals(Decision.ADOPT, decideAndApply(config(1, 1, 11)));
        final CoreConfigCommand lower = config(2, 1, 10);
        assertEquals("maxWireVersion 10 below the recorded 11 (the ceiling only rises)", sm.ceilingLoweredReason(lower));
        assertEquals("the ceiling only rises", Decision.INVALID, decideAndApply(lower));
        assertEquals(11L, sm.recordedMaxWireVersion());
        assertEquals("configVersion unchanged", 1L, sm.recorded().configVersion);
        assertEquals(1L, sm.rejectCount());
        // the text names the ceiling only when the ceiling is what refused the command
        final CoreConfigCommand otherGenesis = config(3, 1, 10);
        otherGenesis.genesisId = 0x5EEEL;
        assertNull(sm.ceilingLoweredReason(otherGenesis));
        assertNull(sm.ceilingLoweredReason(config(1, 1, 10)));
    }

    // ------------------------------------------------------------------ row 8: ACCEPT and SWITCH install it

    @Test
    public void row8_anEqualCeilingIsAcceptedAndARaiseIsInstalled() {
        sm.setSupportedMaxWireVersion(12);
        assertEquals(Decision.ADOPT, decideAndApply(config(1, 1, 10)));
        assertEquals("equal ceiling, next version", Decision.ACCEPT, decideAndApply(config(2, 1, 10)));
        assertEquals(10L, sm.recordedMaxWireVersion());
        assertEquals("the raise", Decision.ACCEPT, decideAndApply(config(3, 1, 12)));
        assertEquals(12L, sm.recordedMaxWireVersion());
        assertEquals(3L, sm.recorded().configVersion);
        assertEquals(List.of(), stops);
    }

    @Test
    public void row8_aRaiseRidesTheBehaviourSwitch() {
        sm.setSupportedMaxWireVersion(11);
        assertEquals(Decision.ADOPT, decideAndApply(config(1, 1, 10)));
        assertEquals(Decision.SWITCH, decideAndApply(config(2, 2, 11)));
        assertEquals(2L, sm.behaviorVersion());
        assertEquals(11L, sm.recordedMaxWireVersion());
        // and a lowered ceiling refuses a switch as well
        assertEquals(Decision.INVALID, decideAndApply(config(3, 3, 10)));
        assertEquals(2L, sm.behaviorVersion());
    }

    // ------------------------------------------------------------------ the node-local check

    @Test
    public void aJarBelowTheRecordedCeilingStopsTheNodeAndNeverRejects() {
        // this jar decodes money-schema 10 only (the production constant)
        assertEquals(10, sm.supportedMaxWireVersion());
        assertEquals(Decision.ADOPT, decideAndApply(config(1, 1, 10)));
        assertEquals(Decision.ACCEPT, decideAndApply(config(2, 1, 11)));
        assertEquals("the raise is applied on every member: replicated state", 11L, sm.recordedMaxWireVersion());
        assertEquals("and this member stops itself, once",
                List.of("recorded maxWireVersion=11 is above this jar's money-schema version 10"), stops);
        assertEquals(0L, sm.rejectCount());
    }

    @Test
    public void anAdoptAboveThisJarStopsTheNode() {
        assertEquals(Decision.ADOPT, decideAndApply(config(1, 1, 11)));
        assertEquals(List.of("recorded maxWireVersion=11 is above this jar's money-schema version 10"), stops);
    }
}
