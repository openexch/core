// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.CoreMemberCompat;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * S4 T3 (core, through a real service): {@code /health} and {@code /ready} carry the capability tokens after
 * {@code build=}; the recorded values read 0 until the CoreConfig ADOPT is applied and then the replicated values;
 * the gauges name the same build.
 */
public class CoreReadyCapabilitiesClusterTest {

    @Test(timeout = 300_000)
    public void theReadyBodyCarriesTheCapabilitiesAndTheRecordedValuesAfterTheApply() throws Exception {
        try (ShadowClusterRig rig = new ShadowClusterRig(29100, "core-ready-capabilities", 1, "1m", 0)) {
            final MemberLevels levels = new MemberLevels(rig, CoreMemberCompat.MEMBER_COMPAT.level());
            rig.bootAll();
            final ShadowClusterRig.Member m = rig.members.get(0);
            rig.awaitReady(m, 120_000);
            final String before = levels.ready(m);
            final String constant = " compat=g1.1 wire=10-10 behaviors=1,2,3,4 layouts=7";
            assertTrue(before, before.contains(" build=") && before.indexOf(" build=") < before.indexOf(" compat="));
            assertTrue(before, before.contains(constant + " recorded_behavior=0 recorded_wire_max=0 recorded_genesis=0"
                    + " driver=embedded\n"));
            // the body before S4 is a prefix: it ended with the build token, which the capabilities follow
            assertTrue(before, before.substring(0, before.indexOf(" compat=")).matches("(?s).* version=\\S+ build=\\S+"));

            final ShadowClusterRig.Client admin = rig.connect(SessionClass.ADMIN, m);
            rig.adopt(admin);
            final String[] after = {""};
            rig.await(() -> {
                try {
                    after[0] = levels.ready(m);
                } catch (final java.io.IOException e) {
                    return false;
                }
                return after[0].contains(" recorded_behavior=1 recorded_wire_max=10 recorded_genesis=1");
            }, 30_000, () -> "the recorded values after the ADOPT: " + after[0]);
            assertTrue(after[0], after[0].contains(constant + " recorded_behavior=1 recorded_wire_max=10"
                    + " recorded_genesis=1 driver=embedded\n"));
            assertTrue("the genesis id itself is never printed: " + after[0],
                    !after[0].contains(Long.toString(ShadowClusterRig.GENESIS)));

            final String metrics = ServiceProbe.metricsText(m.service);
            assertEquals("1", ServiceProbe.seriesValue(metrics, "core_member_compat_generation"));
            assertEquals("1", ServiceProbe.seriesValue(metrics, "core_member_compat_level"));
            assertEquals("15", ServiceProbe.seriesValue(metrics, "core_supported_behavior_versions"));
            assertEquals("10", ServiceProbe.seriesValue(metrics, "core_supported_min_wire_version"));
            assertEquals("10", ServiceProbe.seriesValue(metrics, "core_supported_max_wire_version"));
            assertEquals(levels.refusals.toString(), 0, levels.refusals.size());
        }
    }
}
