// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import com.openexchange.cluster.journal.JournalReaderTool;
import com.openexchange.core.projector.ProjectorProcesses.Projector;
import com.openexchange.core.projector.ProjectorProcesses.Source;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * T11. The production money projector ({@link ProjectorMain}, a child JVM, a real PostgreSQL) against a journal
 * source that purges its own recording as a core member does: cluster-kit's journal retention thread on the
 * source's archive, mode {@code readers}, fed by a pusher that reads the projector's committed cursor from
 * PostgreSQL and writes the reader file with the kit's command one step later.
 *
 * <p>The story: the projector applies 21000 records and is stopped; the journal grows to 48000 and the source purges
 * behind the cursor; the projector comes back and goes on where it stopped; then it follows live to 70000 while the
 * source keeps purging under it. Its rows equal the journal and it never parks. Last, a projector with an empty
 * database is started against the purged source: it parks on the gap. That is the stated limit: after retention
 * the projection database is the only copy of history below the bound.
 *
 * <p>Control: a pusher that sends {@code applied + 1000}. The source purges records the projector has not applied,
 * the projector parks on the gap when it comes back, and the story's "no park" assertion fails
 * ({@code money projector parked}).
 */
public class MoneyProjectorAcrossPurgeProcessTest {

    private static final long GENESIS = 17;
    private static final int SEGMENT = 1024 * 1024;
    private static final String READER = "money-projector";

    private final ProjectorProcesses procs = new ProjectorProcesses();

    @After
    public void cleanUp() throws Exception {
        procs.close();
    }

    private static void set(final Path clusterDir, final long applied) {
        final ByteArrayOutputStream err = new ByteArrayOutputStream();
        final int exit = JournalReaderTool.run(new String[] {"set", "--cluster-dir", clusterDir.toString(), "--engine",
            "core", "--cluster-id", "0", "--genesis-id", String.valueOf(GENESIS), "--journal", "money", "--reader",
            READER, "--applied-journal-seq", String.valueOf(applied), "--source", "test-pusher"},
            new PrintStream(new ByteArrayOutputStream()), new PrintStream(err), System::currentTimeMillis);
        assertEquals("JournalReaderTool set: " + err, 0, exit);
    }

    /** The projector's committed cursor: the highest journalSeq in its table. */
    private long cursor() throws Exception {
        final String max = procs.query("select coalesce(max(journal_seq), 0) from money_journal");
        return Long.parseLong(max);
    }

    private static void assertNotParked(final Projector p) throws Exception {
        for (final String line : p.out().split("\n")) {
            if (line.contains("HALTED")) {
                throw new AssertionError("money projector parked: " + line.trim());
            }
        }
    }

    private void awaitRowsNotParked(final Projector p, final int expected, final long timeoutMs) throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        String n;
        while (!(n = procs.rows()).equals(String.valueOf(expected))) {
            assertNotParked(p);
            assertTrue("projected " + n + " of " + expected + " within " + timeoutMs + " ms: " + tail(p.out()),
                    System.nanoTime() < deadline && p.process().isAlive());
            Thread.sleep(200);
        }
        assertNotParked(p);
    }

    private static String tail(final String out) {
        return out.length() > 4_000 ? out.substring(out.length() - 4_000) : out;
    }

    private void dense(final int n) throws Exception {
        assertEquals("dense 1.." + n + ", nothing lost, nothing twice", n + "|1|" + n,
                procs.query("select count(distinct journal_seq) || '|' || min(journal_seq) || '|' || max(journal_seq) "
                        + "from money_journal"));
    }

    private static void awaitStart(final Source source, final long atLeast, final String what) throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        long start;
        while ((start = source.start()) < atLeast) {
            assertTrue(what + ": the source's start position is " + start + ", expected at least " + atLeast + ": "
                    + tail(java.nio.file.Files.readString(source.log())), System.nanoTime() < deadline);
            Thread.sleep(200);
        }
    }

    private void story(final long skew) throws Exception {
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", procs.pgBin() != null);
        procs.startPostgres();
        final Path clusterDir = procs.dir("projector-purge-cluster-");
        set(clusterDir, 0); // registered before the reader first starts
        final Source source = procs.startRetainingSource(21_000, clusterDir, GENESIS);
        final Projector first = procs.projector(procs.dir("projector-purge-1-"), source, Map.of());
        awaitRowsNotParked(first, 21_000, 180_000);
        assertEquals("held at the registered cursor 0", 0, source.start());

        // the projector stops; the journal grows; the pusher delivers the cursor the projector committed
        assertEquals("SIGTERM: " + tail(first.out()), 143, ProjectorProcesses.sigterm(first));
        final long committed = cursor();
        assertEquals(21_000, committed);
        source.publish(27_000); // 48000 records: the head is in the fifth 1 MiB segment
        set(clusterDir, committed + skew);
        // 10912 records fill a segment: base 1 starts at 10913 (applied), base 2 at 21825 (not applied)
        awaitStart(source, SEGMENT, "the purge behind the stopped projector");
        Thread.sleep(2_500); // two more passes: an honest cursor moves the start no further
        final long startBehindCursor = source.start();
        System.out.println("skew " + skew + ": cursor " + (committed + skew) + " delivered, source start "
                + startBehindCursor);

        // it comes back and goes on where it stopped
        final Projector second = procs.projector(procs.dir("projector-purge-2-"), source, Map.of());
        awaitRowsNotParked(second, 48_000, 180_000);
        dense(48_000);
        assertEquals("an honest cursor purges one segment behind 21000", SEGMENT, startBehindCursor);

        // live: the journal grows in steps, the pusher follows the committed cursor, the source purges under it
        long delivered = cursor();
        for (int step = 0; step < 4; step++) {
            source.publish(5_500);
            set(clusterDir, delivered); // the value read one step ago
            delivered = cursor();
            Thread.sleep(1_200);
            assertNotParked(second);
        }
        awaitRowsNotParked(second, 70_000, 180_000);
        dense(70_000);
        set(clusterDir, cursor());
        // 70000 records: the head is in the seventh segment, two whole segments stay below it
        awaitStart(source, 4L * SEGMENT, "the purge under the live projector");
        assertNotParked(second);
        assertTrue("the projector is alive", second.process().isAlive());
        dense(70_000);

        // the stated limit: an empty database cannot be filled from a purged source
        procs.createDatabase("core_empty");
        final Projector empty = procs.projector(procs.dir("projector-purge-empty-"), source, Map.of(), "core_empty");
        ProjectorProcesses.awaitLine(empty.process(), empty.log(), "HALTED - LOST MONEY MOVEMENT SUSPECTED", 120_000);
        assertTrue(tail(empty.out()), empty.out().contains("dense journalSeq gap"));
        System.out.println("GREEN T11 money projector across purges: rows 1..70000 dense, source start "
                + source.start() + "; an empty database parks on the gap");
    }

    @Test
    public void theProjectorFollowsWhilePurgesRunUnderItAndAnEmptyDatabaseParksOnTheGap() throws Exception {
        story(0);
    }

    /** Control: the pusher sends a cursor 1000 above what the projector committed. */
    @Test
    public void controlACursorAboveTheProjectorsMakesItParkOnAGap() throws Exception {
        try {
            story(1_000);
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(), expected.getMessage().startsWith("money projector parked: "));
            assertTrue(expected.getMessage(), expected.getMessage().contains("dense journalSeq gap"));
            System.out.println("NC T11 cursor + 1000 -> " + expected.getMessage());
            return;
        }
        fail("negative control did not fail: the projector did not park although the pusher sent applied + 1000");
    }
}
