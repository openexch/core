// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import com.openexchange.core.projector.ProjectorProcesses.Driver;
import com.openexchange.core.projector.ProjectorProcesses.Projector;
import com.openexchange.core.projector.ProjectorProcesses.Source;
import org.junit.After;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * ((a), row 44): the production {@link ProjectorMain} in a child JVM in both driver modes, projecting
 * a real archive recording ({@link JournalSourceMain}, its own archiving driver, as the core is) into a throwaway
 * Postgres. The exit statuses asserted are the ones a process manager sees.
 *
 * <p>External mode runs on a separate driver process: the real C aeronmd when {@code -Dprojector.aeronmd=<path>} (or
 * {@code PROJECTOR_AERONMD}) names one (its directory is its {@code LD_LIBRARY_PATH}), else a Java
 * {@code io.aeron.driver.MediaDriver} child (the engines' {@code EngineDriverLifecycleProcessTest} shape).
 * Needs the PostgreSQL server binaries ({@link ProjectorProcesses}); the projecting cases are skipped without them.
 *
 * <p>RED evidence: {@code -Dprojector.driver.classpath=<pre-W10 projector.jar>} runs the child on that jar: it ignores
 * the mode, starts its own driver in every case, never exits on an absent driver.
 */
public class ProjectorDriverModeProcessTest {

    private static final int INITIAL = 5;

    private final ProjectorProcesses procs = new ProjectorProcesses();

    @After
    public void cleanUp() throws Exception {
        procs.close();
    }

    @Test
    public void embeddedModeProjectsTheJournalOnItsOwnDriver() throws Exception {
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", procs.pgBin() != null);
        procs.startPostgres();
        final Source source = procs.startSource(INITIAL);
        final Path base = procs.dir("projector-w10-emb-");
        final Projector p = procs.projector(base, source, Map.of()); // no driver keys at all: the default
        ProjectorProcesses.awaitLine(p.process(), p.log(), "[PROJECTOR] driver mode EMBEDDED", 30_000);
        procs.awaitRows(p, INITIAL, 60_000);
        assertTrue("the embedded driver dir exists while running", Files.exists(p.embeddedDir()));

        final int exit = ProjectorProcesses.sigterm(p);
        final String out = p.out();
        assertEquals("SIGTERM exit status: " + out, 143, exit);
        assertFalse("the embedded driver removed its dir", Files.exists(p.embeddedDir()));
        ProjectorProcesses.assertNoCrash(base, out);
    }

    @Test
    public void externalModeProjectsTheJournalThroughTheExternalDriverAndOwnsNone() throws Exception {
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", procs.pgBin() != null);
        procs.startPostgres();
        final Source source = procs.startSource(INITIAL);
        final Path base = procs.dir("projector-w10-ext-");
        final Driver driver = procs.startDriver(base);
        final Projector p = procs.projector(base, source, Map.of(ProjectorDriver.MODE_ENV, "external",
                ProjectorDriver.AERON_DIR_ENV, driver.dir().toString()));
        ProjectorProcesses.awaitLine(p.process(), p.log(), "[PROJECTOR] external media driver active at " + driver.dir(),
                30_000);
        procs.awaitRows(p, INITIAL, 60_000);
        assertFalse("external mode launches no embedded driver", Files.exists(p.embeddedDir()));
        source.publish(3);
        procs.awaitRows(p, INITIAL + 3, 60_000); // live-follow through the external driver

        final int exit = ProjectorProcesses.sigterm(p);
        final String out = p.out();
        assertEquals("SIGTERM exit status: " + out, 143, exit);
        assertTrue("the agent stopped cleanly: " + out, out.contains("[PROJECTOR] stopped."));
        assertTrue("the projector does not own (or stop) the external driver: " + driver.out(), driver.process().isAlive());
        assertTrue("and leaves its CnC in place", Files.exists(driver.dir().resolve("cnc.dat")));
        ProjectorProcesses.assertNoCrash(base, out);
    }

    @Test
    public void externalDriverLossExitsOneAndTheRestartResumesFromPostgres() throws Exception {
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", procs.pgBin() != null);
        procs.startPostgres();
        final Source source = procs.startSource(INITIAL);
        final Path base = procs.dir("projector-w10-loss-");
        final Driver first = procs.startDriver(base);
        final Map<String, String> env = Map.of(ProjectorDriver.MODE_ENV, "external",
                ProjectorDriver.AERON_DIR_ENV, first.dir().toString());
        final Projector p = procs.projector(base, source, env);
        procs.awaitRows(p, INITIAL, 60_000);

        first.process().destroyForcibly(); // kill -9 of the external driver
        first.process().waitFor(10, TimeUnit.SECONDS);
        final long t0 = System.nanoTime();
        final int exit = ProjectorProcesses.awaitExit(p.process(), 30_000);
        final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        final String out = p.out();
        System.out.println("W10 projector, external driver kill -9: exit=" + exit + " after " + ms + " ms");
        // The lifecycle halts at once on the driver loss (C13: 1 for Java), on the archive client's
        // conductor thread before that client unmaps the CnC; never Aeron's default handler (prints "exiting
        // client", Runtime.exit(-1) = 255 through the shutdown path).
        assertEquals("driver loss must exit 1: " + out, 1, exit);
        assertTrue("within the 10 s client driver timeout plus the halt: " + ms, ms < 18_000);
        assertTrue("the lifecycle halted on the driver loss: " + out,
                out.contains("media driver lost in EXTERNAL driver mode")
                        && out.contains("halting now, no component close"));
        assertFalse("no Aeron default error handler exit: " + out, out.contains("exiting client"));
        ProjectorProcesses.assertNoCrash(base, out);

        source.publish(4); // the core side keeps recording while the projector is down
        final Driver second = procs.startDriver(base, first.dir()); // the process manager restarts aeronmd on the same dir
        final Path base2 = procs.dir("projector-w10-loss2-");
        final Projector restarted = procs.projector(base2, source, env);
        procs.awaitRows(restarted, INITIAL + 4, 60_000);
        assertEquals("dense, nothing lost, nothing twice", INITIAL + 4 + "|1|" + (INITIAL + 4),
                procs.query("select count(distinct journal_seq) || '|' || min(journal_seq) || '|' || max(journal_seq) "
                        + "from money_journal"));
        assertEquals(143, ProjectorProcesses.sigterm(restarted));
        assertTrue(second.process().isAlive());
        ProjectorProcesses.assertNoCrash(base2, restarted.out());
    }

    @Test
    public void externalModeWithNoDriverExitsOneWithinTheDriverWait() throws Exception {
        final Path base = procs.dir("projector-w10-nodriver-");
        final Path aeronDir = Files.createDirectories(base.resolve("aeron"));
        final long t0 = System.nanoTime();
        final Projector p = procs.projectorNoSource(base, Map.of(ProjectorDriver.MODE_ENV, "external",
                ProjectorDriver.AERON_DIR_ENV, aeronDir.toString(), "TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS", "2000"));
        final int exit = ProjectorProcesses.awaitExit(p.process(), 20_000);
        final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        System.out.println("W10 projector, no driver, wait 2000 ms: exit=" + exit + " after " + ms + " ms");
        final String out = p.out();
        assertEquals("absent driver must exit 1: " + out, 1, exit);
        assertTrue("within the 2000 ms wait plus the JVM start: " + ms, ms >= 2_000 && ms < 12_000);
        assertTrue(out, out.contains("No external media driver found at " + aeronDir));
        assertFalse("no embedded fallback", Files.exists(p.embeddedDir()));
    }

    @Test
    public void sigtermDuringTheExternalDriverWaitExitsPromptlyWith143() throws Exception {
        final Path base = procs.dir("projector-w10-waitterm-");
        final Path aeronDir = Files.createDirectories(base.resolve("aeron"));
        final Projector p = procs.projectorNoSource(base, Map.of(ProjectorDriver.MODE_ENV, "external",
                ProjectorDriver.AERON_DIR_ENV, aeronDir.toString()));
        ProjectorProcesses.awaitLine(p.process(), p.log(), "[PROJECTOR] driver mode EXTERNAL", 30_000);
        Thread.sleep(300);
        final long t0 = System.nanoTime();
        final int exit = ProjectorProcesses.sigterm(p);
        final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        assertEquals("SIGTERM in the 30 s driver wait: " + p.out(), 143, exit);
        assertTrue("the wait ends on the stop, not at its bound: " + ms, ms < 3_000);
    }

    @Test
    public void externalModeOnTheStaleCncOfAKilledDriverExitsOne() throws Exception {
        final Path base = procs.dir("projector-w10-stale-");
        final Driver dead = procs.startDriver(base);
        dead.process().destroyForcibly(); // kill -9: no shutdown, so its cnc.dat stays behind
        dead.process().waitFor(10, TimeUnit.SECONDS);
        assertTrue("the killed driver left its cnc.dat", Files.exists(dead.dir().resolve("cnc.dat")));
        Thread.sleep(1_500); // the CnC heartbeat is now older than the projector's driver timeout below
        final Projector p = procs.projectorNoSource(base, Map.of(ProjectorDriver.MODE_ENV, "external",
                ProjectorDriver.AERON_DIR_ENV, dead.dir().toString(), "TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS", "2000",
                // a heartbeat older than this is a dead driver (Aeron's own check); short so the case is quick
                "JAVA_TOOL_OPTIONS", "-Daeron.driver.timeout=1000"));
        final int exit = ProjectorProcesses.awaitExit(p.process(), 20_000);
        final String out = p.out();
        assertEquals("a dead driver's CnC must exit 1: " + out, 1, exit);
        assertTrue(out, out.contains("a cnc.dat exists but its driver is not active"));
    }

    @Test
    public void anUnknownModeOrAStrayAeronDirRefusesTheStart() throws Exception {
        final Path base = procs.dir("projector-w10-strict-");
        for (final Map<String, String> env : List.of(
                Map.of(ProjectorDriver.MODE_ENV, "extern"),
                Map.of(ProjectorDriver.MODE_ENV, "external"),
                Map.of(ProjectorDriver.AERON_DIR_ENV, "/dev/shm/some-aeronmd"),
                Map.of("TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS", "0"))) {
            final Projector p = procs.projectorNoSource(Files.createTempDirectory(base, "case-"), env);
            final int exit = ProjectorProcesses.awaitExit(p.process(), 20_000);
            final String out = p.out();
            assertEquals(env + " must refuse the start: " + out, 1, exit);
            assertTrue(env + ": " + out, out.contains("FATAL (exit 1)"));
            assertFalse(env + ": no driver started", Files.exists(p.embeddedDir()));
        }
    }

}
