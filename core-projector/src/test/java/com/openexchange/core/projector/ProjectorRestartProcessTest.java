// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import com.openexchange.core.projector.ProjectorProcesses.Driver;
import com.openexchange.core.projector.ProjectorProcesses.Projector;
import com.openexchange.core.projector.ProjectorProcesses.Source;
import com.openexchange.core.infrastructure.archive.ArchiveJournalSource;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.junit.After;
import org.junit.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * The production {@link ProjectorMain} in a child JVM, in both driver modes, across: a restart of its source member
 * (kill -9 of the member, which comes back on its own archive and re-records its journal), its own kill -9 and
 * restart, and the replay its source member holds for it afterwards. Each step records what it measured.
 *
 * <p>The replay count is the source archive's replay session count counter (the resource its replay cap counts); its
 * baseline is the count before the projector first attaches (zero). A replay left behind by a dead projector must be gone within
 * {@link #ORPHAN_BOUND_MS} of its death, and so must the replay of one stopped by SIGTERM.
 *
 * <p>Control: {@code -Dprojector.control=hold-replay} keeps a replay open on the member after the projector's kill -9
 * (a replay of this test JVM that it keeps polling, standing for one nobody ended); the kill leg then fails with
 * {@code money projector replay left open}.
 */
public class ProjectorRestartProcessTest {

    private static final int INITIAL = 5;
    /** How long a replay of a killed projector may outlive it on the member. */
    private static final long ORPHAN_BOUND_MS = 30_000;

    private final ProjectorProcesses procs = new ProjectorProcesses();

    @After
    public void cleanUp() throws Exception {
        procs.close();
    }

    @Test
    public void embeddedModeContinuesAcrossTheMemberRestartAndItsOwnKill() throws Exception {
        story("embedded", Map.of());
    }

    @Test
    public void externalModeContinuesAcrossTheMemberRestartAndItsOwnKill() throws Exception {
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", procs.pgBin() != null);
        final Driver driver = procs.startDriver(procs.dir("projector-restart-drv-"));
        story("external", Map.of(ProjectorDriver.MODE_ENV, "external", ProjectorDriver.AERON_DIR_ENV,
                driver.dir().toString()));
    }

    private void story(final String mode, final Map<String, String> env) throws Exception {
        assumeTrue("no PostgreSQL server binaries (set PROJECTOR_PG_BIN)", procs.pgBin() != null);
        procs.startPostgres();
        Source source = procs.startSource(INITIAL);
        final int baseline = source.replays();
        System.out.println(mode + ": replay baseline on the member " + baseline);

        final Path base = procs.dir("projector-restart-" + mode + "-");
        final Projector first = procs.projector(base, source, env);
        procs.awaitRows(first, INITIAL, 60_000);
        System.out.println(mode + ": attached, member replays " + source.replays());

        // 1. the source member restarts under the projector: no manual step
        long t0 = System.nanoTime();
        source = procs.restartSource(source, 3);
        procs.awaitRows(first, INITIAL + 3, 90_000);
        System.out.println(mode + ": continued after the member restart in "
                + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) + " ms");
        dense(INITIAL + 3);

        // 2. its own kill -9: the member's replay must go, within the orphan bound
        first.process().destroyForcibly();
        assertTrue(first.process().waitFor(10, TimeUnit.SECONDS));
        try (HeldReplay held = "hold-replay".equals(System.getProperty("projector.control")) ? new HeldReplay(source)
                : null) {
            awaitReplaysAt(source, baseline, mode, "the projector's kill -9");
        }

        // 3. restart: resumes at its high-water, nothing lost, nothing twice
        source.publish(2);
        final Projector second = procs.projector(procs.dir("projector-restart2-" + mode + "-"), source, env);
        procs.awaitRows(second, INITIAL + 5, 60_000);
        dense(INITIAL + 5);
        assertEquals("SIGTERM: " + second.out(), 143, ProjectorProcesses.sigterm(second));
        awaitReplaysAt(source, baseline, mode, "the projector's SIGTERM exit");
    }

    private static void awaitReplaysAt(final Source source, final int baseline, final String mode, final String after)
            throws Exception {
        final long t0 = System.nanoTime();
        int open;
        while ((open = source.replays()) != baseline
                && System.nanoTime() - t0 < TimeUnit.MILLISECONDS.toNanos(ORPHAN_BOUND_MS)) {
            Thread.sleep(20);
        }
        System.out.println(mode + ": member replay sessions " + open + " (baseline " + baseline + ") "
                + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) + " ms after " + after);
        if (open != baseline) {
            throw new AssertionError("money projector replay left open: " + open + " replay sessions on the member "
                    + ORPHAN_BOUND_MS + " ms after " + after + " (baseline " + baseline + ")");
        }
    }

    /** The control: a replay on the member that this JVM keeps polling, as one nobody ended would stay. */
    private final class HeldReplay implements AutoCloseable {
        private final MediaDriver driver;
        private final ArchiveJournalSource archive;
        private final Subscription replay;
        private final Thread poller;
        private volatile boolean running = true;

        HeldReplay(final Source source) {
            driver = MediaDriver.launch(new MediaDriver.Context()
                    .aeronDirectoryName(procs.shmName("projector-held-").toString()).threadingMode(ThreadingMode.SHARED)
                    .termBufferSparseFile(true).dirDeleteOnStart(true).dirDeleteOnShutdown(true));
            archive = ArchiveJournalSource.connectFirstHealthy(List.of("localhost:" + source.controlPort()), 10,
                    ProjectorConfig.MONEY_JOURNAL_STREAM_ID, ProjectorConfig.MONEY_JOURNAL_CHANNEL,
                    ProjectorConfig.REPLAY_STREAM_ID, driver.aeronDirectoryName(), "localhost");
            final List<ArchiveJournalSource.Recording> chain = archive.recordings();
            replay = archive.openReplay(chain.get(chain.size() - 1));
            poller = new Thread(() -> {
                while (running) {
                    replay.poll((b, o, l, h) -> { }, 16);
                    Thread.onSpinWait();
                }
            }, "held-replay");
            poller.start();
            System.out.println("control: holding a replay open on the member");
        }

        @Override
        public void close() throws Exception {
            running = false;
            poller.join(5_000);
            CloseHelper.quietClose(replay);
            CloseHelper.quietClose(archive);
            CloseHelper.quietClose(driver);
        }
    }

    private void dense(final int n) throws Exception {
        assertEquals("dense 1.." + n + ", nothing lost, nothing twice", n + "|1|" + n,
                procs.query("select count(distinct journal_seq) || '|' || min(journal_seq) || '|' || max(journal_seq) "
                        + "from money_journal"));
    }
}
