// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Test;

import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * A gap on one member's journal is that member's fault while another member holds the range: the projector reads the
 * next endpoint before it parks. Real archives (one per member), a real Postgres.
 *
 * <p>Control: {@code -Dprojector.control=switch-off} parks on the first gap of any source (the behaviour before the
 * switch); {@link #aGapOnTheFirstEndpointIsFilledFromTheSecondTwice} then fails with
 * {@code parked on a gap another member could fill}. {@code -Dprojector.control=gap-wait-forever} sets an unfilled
 * gap's waiting time to the largest value; the two unfilled-gap tests then fail with
 * {@code a gap no other source fills never parked}.
 */
public class ProjectorSourceSwitchTest {

    private final ProjectorProcesses procs = new ProjectorProcesses();

    @After
    public void cleanUp() throws Exception {
        procs.close();
    }

    @Test
    public void aGapOnTheFirstEndpointIsFilledFromTheSecondTwice() throws Exception {
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", procs.pgBin() != null);
        procs.startPostgres();
        try (InProcessJournalSource a = new InProcessJournalSource(procs);
             InProcessJournalSource b = new InProcessJournalSource(procs)) {
            a.newRecording().publish(without(JournalRecords.range(1, 6), 3)); // a lost 3
            b.newRecording().publish(JournalRecords.range(1, 6));
            try (InProcessProjector p = projector(a, b).start()) {
                awaitDense(p, 6);
            }
            // again, on a later range: the first endpoint is tried first again and loses 9
            a.publish(without(JournalRecords.range(7, 12), 9));
            b.publish(JournalRecords.range(7, 12));
            try (InProcessProjector p = projector(a, b).start()) {
                awaitDense(p, 12);
            }
        }
    }

    @Test
    public void aGapOnEveryEndpointParks() throws Exception {
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", procs.pgBin() != null);
        procs.startPostgres();
        try (InProcessJournalSource a = new InProcessJournalSource(procs);
             InProcessJournalSource b = new InProcessJournalSource(procs)) {
            a.newRecording().publish(without(JournalRecords.range(1, 6), 4));
            b.newRecording().publish(without(JournalRecords.range(1, 6), 4));
            try (InProcessProjector p = projector(a, b).start()) {
                InProcessProjector.await(() -> p.state.halted, 90_000,
                        () -> "a gap no member can fill must park: " + p.state.render());
                Thread.sleep(500);
                assertEquals("everything below the gap is projected, nothing past it", 3, p.rows());
                assertEquals("GAP", p.state.haltReason);
            }
        }
    }

    @Test
    public void aGapTheOtherEndpointCannotFillBecauseItIsEmptyIsShownThenParks() throws Exception {
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", procs.pgBin() != null);
        procs.startPostgres();
        try (InProcessJournalSource a = new InProcessJournalSource(procs);
             InProcessJournalSource b = new InProcessJournalSource(procs)) {
            a.newRecording().publish(without(JournalRecords.range(1, 6), 4));
            // b records nothing: an empty journal shows nothing above the gap
            unfilledGapParks(List.of(a.endpoint(), b.endpoint()));
        }
    }

    @Test
    public void aGapTheOtherEndpointCannotFillBecauseItIsUnreachableIsShownThenParks() throws Exception {
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", procs.pgBin() != null);
        procs.startPostgres();
        try (InProcessJournalSource a = new InProcessJournalSource(procs)) {
            a.newRecording().publish(without(JournalRecords.range(1, 6), 4));
            unfilledGapParks(List.of(a.endpoint(), "localhost:" + ProjectorProcesses.freePort()));
        }
    }

    /** The pending gap is visible on the metrics while it waits, then parks once the setting's time has passed. */
    private void unfilledGapParks(final List<String> endpoints) throws Exception {
        final long parkMs = "gap-wait-forever".equals(System.getProperty("projector.control"))
                ? Long.MAX_VALUE : 3_000;
        try (InProcessProjector p = projector(endpoints, parkMs).start()) {
            final ProjectorMetricsServer metrics = new ProjectorMetricsServer(p.state);
            InProcessProjector.await(() -> p.state.gapPending || p.state.halted, 60_000,
                    () -> "a gap one source showed is invisible: " + p.state.render());
            if (!p.state.halted) {
                assertTrue("the gauge shows the pending gap", metrics.render().contains("projector_gap_pending 1"));
                assertTrue("the gauge shows the high-water below it",
                        metrics.render().contains("projector_gap_high_water 3"));
            }
            InProcessProjector.await(() -> p.state.halted, 90_000,
                    () -> "a gap no other source fills never parked: " + p.state.render());
            Thread.sleep(300);
            assertEquals("GAP", p.state.haltReason);
            assertEquals("everything below the gap is projected, nothing past it", 3, p.rows());
        }
    }

    private InProcessProjector projector(final InProcessJournalSource a, final InProcessJournalSource b) {
        return projector(List.of(a.endpoint(), b.endpoint()), ProjectorConfig.DEFAULT_GAP_UNFILLED_PARK_MS);
    }

    private InProcessProjector projector(final List<String> endpoints, final long gapUnfilledParkMs) {
        final boolean switchOnGap = !"switch-off".equals(System.getProperty("projector.control"));
        return new InProcessProjector(procs, "core", endpoints, true, switchOnGap, gapUnfilledParkMs);
    }

    private static void awaitDense(final InProcessProjector p, final long n) throws Exception {
        InProcessProjector.await(() -> p.state.halted || p.hasRows(n), 90_000,
                () -> "projected neither " + n + " rows nor parked: " + p.state.render());
        if (p.state.halted) {
            throw new AssertionError("parked on a gap another member could fill: " + p.state.render());
        }
        assertEquals("one switch per gap", 1, p.state.sourceSwitches);
        assertEquals("dense 1.." + n, n + "|1|" + n, p.scalar("SELECT COUNT(DISTINCT journal_seq) FROM money_journal")
                + "|" + p.scalar("SELECT MIN(journal_seq) FROM money_journal")
                + "|" + p.scalar("SELECT MAX(journal_seq) FROM money_journal"));
    }

    private static byte[][] without(final byte[][] records, final long seq) {
        return Arrays.stream(records).filter(r -> new UnsafeBuffer(r).getLong(8, ByteOrder.LITTLE_ENDIAN) != seq)
                .toArray(byte[][]::new);
    }
}
