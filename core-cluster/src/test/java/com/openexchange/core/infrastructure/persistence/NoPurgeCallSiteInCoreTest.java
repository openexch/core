// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.cluster.testkit.NeverSchedulesScan;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The core half of the one-purge-owner rule: every main class of
 * core-cluster ({@code target/classes}, never the test tree) is scanned for calls to
 * {@code AeronArchive.purgeRecording}, {@code purgeSegments}, {@code truncateRecording}, {@code detachSegments},
 * {@code deleteDetachedSegments} and {@code ShadowCatalog.purge}, and the count must be ZERO. The kit's
 * {@code ShadowRetention} is the only production path that deletes a shadow ({@code ShadowPurgeCallSiteTest} holds the
 * kit side), and the kit's {@code JournalRetention} the only one that deletes journal segments or a journal recording
 * ({@code JournalPurgeCallSiteTest}). Controls: a probe class calling {@code purgeRecording} and one calling
 * {@code purgeSegments} are each flagged with the 11.15 text.
 */
public class NoPurgeCallSiteInCoreTest {

    static final String PREFIX = "com/openexchange/core/";

    static NeverSchedulesScan.Rules purgeRules() {
        return new NeverSchedulesScan.Rules(Set.of(), List.of(), Set.of(),
                Map.of("io/aeron/archive/client/AeronArchive", Set.of("purgeRecording", "purgeSegments",
                                "truncateRecording", "detachSegments", "deleteDetachedSegments"),
                        "com/openexchange/cluster/shadow/ShadowCatalog", Set.of("purge")),
                Map.of(), Map.of());
    }

    static Path mainRoot() {
        final Path root = CoreShadowEngineConformanceTest.rootOf(CoreLogApplier.class);
        final Path testRoot = CoreShadowEngineConformanceTest.rootOf(NoPurgeCallSiteInCoreTest.class);
        assertTrue("the main class root is not the test root: " + root, !root.equals(testRoot));
        return root;
    }

    /** Fails naming the first purge call site; zero is the only passing count. */
    static void check(final List<NeverSchedulesScan.Finding> findings) {
        for (final NeverSchedulesScan.Finding f : findings) {
            fail("purge call site in core: " + f.className() + " (" + f.reference() + ")");
        }
    }

    static List<NeverSchedulesScan.Finding> mainFindings() {
        final NeverSchedulesScan.Report report = NeverSchedulesScan.scanDirectory(mainRoot(),
                n -> n.startsWith(PREFIX), purgeRules());
        assertTrue("the scan saw only " + report.classesScanned() + " core-cluster main classes (expected >= 130)",
                report.classesScanned() >= 130); // 138 measured (core-cluster main)
        assertTrue("the scan reached the shadow housekeeping",
                report.classes().contains(PREFIX + "infrastructure/persistence/MemberShadowHousekeeping"));
        assertTrue("the scan reached the restore", report.classes().contains(PREFIX
                + "infrastructure/persistence/ShadowRestore"));
        System.out.println("GREEN scan: " + report.classesScanned() + " core-cluster main classes, purge findings "
                + report.findings().size());
        return report.findings();
    }

    static byte[] bytes(final Class<?> c) {
        try (InputStream in = c.getResourceAsStream(c.getSimpleName() + ".class")) {
            return in.readAllBytes();
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void coreMainHasZeroPurgeCallSites() {
        check(mainFindings());
    }

    @Test
    public void aProbeCallingPurgeRecordingIsFlagged() {
        final List<NeverSchedulesScan.Finding> withProbe = new ArrayList<>(mainFindings());
        withProbe.addAll(NeverSchedulesScan.scanClass(PREFIX + "infrastructure/persistence/ForbiddenCorePurgeProbe",
                bytes(ForbiddenCorePurgeProbe.class), purgeRules()));
        RestoreClosureNeverSchedulesTest.nc("core purge probe", "purge call site in core: " + PREFIX
                + "infrastructure/persistence/ForbiddenCorePurgeProbe", () -> check(withProbe));
    }

    @Test
    public void aProbeCallingPurgeSegmentsIsFlagged() {
        final List<NeverSchedulesScan.Finding> withProbe = new ArrayList<>(mainFindings());
        withProbe.addAll(NeverSchedulesScan.scanClass(PREFIX + "infrastructure/persistence/ForbiddenCoreSegmentPurgeProbe",
                bytes(ForbiddenCoreSegmentPurgeProbe.class), purgeRules()));
        RestoreClosureNeverSchedulesTest.nc("core segment purge probe", "purge call site in core: " + PREFIX
                + "infrastructure/persistence/ForbiddenCoreSegmentPurgeProbe", () -> check(withProbe));
    }

    private static final String RETENTION_SEAMS = "com/openexchange/cluster/journal/JournalRetention$Seams";

    private static NeverSchedulesScan.Rules seamRules() {
        return new NeverSchedulesScan.Rules(Set.of(RETENTION_SEAMS), List.of(), Set.of(), Map.of(), Map.of(), Map.of());
    }

    /**
     * The kit's journal retention keeps its negative-control seams as public statics so this module's tests can
     * switch one. No main class of core-cluster may name them: a rule of the retention is never switched off in a
     * member. Control: a test class that does switch one is flagged.
     */
    @Test
    public void coreMainNeverNamesTheJournalRetentionSeams() {
        final NeverSchedulesScan.Report report = NeverSchedulesScan.scanDirectory(mainRoot(),
                n -> n.startsWith(PREFIX), seamRules());
        assertTrue("scanned " + report.classesScanned(), report.classesScanned() >= 130);
        for (final NeverSchedulesScan.Finding f : report.findings()) {
            fail("journal retention seam named in core main: " + f.className() + " (" + f.reference() + ")");
        }
        final Class<?> user = com.openexchange.core.cluster.RetentionSeamUser.class;
        final List<NeverSchedulesScan.Finding> control;
        try (InputStream in = user.getResourceAsStream(user.getSimpleName() + ".class")) {
            control = NeverSchedulesScan.scanClass("com/openexchange/core/cluster/RetentionSeamUser", in.readAllBytes(),
                    seamRules());
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
        assertTrue("the scan sees a class that names a seam", !control.isEmpty());
    }
}
