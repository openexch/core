// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.junit.Assert.fail;

/**
 * The production {@link ProjectorAgent} on a thread of the test JVM, on its own embedded driver, writing one database
 * of the throwaway Postgres. Build (optionally with a money control off), then {@link #start()}.
 */
final class InProcessProjector implements AutoCloseable {

    final ProjectorState state = new ProjectorState();
    final HikariDataSource dataSource;
    final PostgresMoneyJournalStore store;
    final ProjectorAgent agent;
    private final MediaDriver driver;
    private Thread thread;

    InProcessProjector(final ProjectorProcesses procs, final String database, final List<String> endpoints) {
        this(procs, database, endpoints, true, true, ProjectorConfig.DEFAULT_GAP_UNFILLED_PARK_MS);
    }

    InProcessProjector(final ProjectorProcesses procs, final String database, final List<String> endpoints,
                       final boolean compareDuplicates, final boolean switchSourceOnGap,
                       final long gapUnfilledParkMs) {
        this(procs, database, endpoints, compareDuplicates, switchSourceOnGap, gapUnfilledParkMs, "core");
    }

    /** As above, with the projector logging in to Postgres as {@code user} (trust auth). */
    InProcessProjector(final ProjectorProcesses procs, final String database, final List<String> endpoints,
                       final boolean compareDuplicates, final boolean switchSourceOnGap,
                       final long gapUnfilledParkMs, final String user) {
        final HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(procs.jdbcUrl(database));
        cfg.setUsername(user);
        cfg.setMaximumPoolSize(2);
        cfg.setConnectionTimeout(5000);
        dataSource = new HikariDataSource(cfg);
        store = new PostgresMoneyJournalStore(dataSource);
        driver = MediaDriver.launch(new MediaDriver.Context()
                .aeronDirectoryName(procs.shmName("projector-inproc-").toString())
                .threadingMode(ThreadingMode.SHARED).termBufferSparseFile(true)
                .dirDeleteOnStart(true).dirDeleteOnShutdown(true));
        final ProjectorConfig config = new ProjectorConfig(endpoints, 10, procs.jdbcUrl(database), user, "", true, 0,
                "localhost", gapUnfilledParkMs);
        agent = new ProjectorAgent(config, driver.aeronDirectoryName(), state, store, null, compareDuplicates,
                switchSourceOnGap);
    }

    InProcessProjector start() {
        thread = new Thread(agent, "test-money-projector");
        thread.setDaemon(true);
        thread.start();
        return this;
    }

    long rows() throws Exception {
        return scalar("SELECT COUNT(*) FROM money_journal");
    }

    long scalar(final String sql) throws Exception {
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    /** Wait for {@code done}; on the deadline fail with {@code why}. */
    static void await(final BooleanSupplier done, final long timeoutMs, final Supplier<String> why)
            throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!done.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail(why.get());
            }
            Thread.sleep(50);
        }
    }

    /** Rows as a boolean probe for {@link #await}: a database error reads as "not yet". */
    boolean hasRows(final long n) {
        try {
            return rows() == n;
        } catch (final Exception e) {
            return false;
        }
    }

    @Override
    public void close() throws Exception {
        agent.stop();
        if (thread != null) {
            thread.interrupt();
            thread.join(5_000);
        }
        CloseHelper.quietClose(driver);
        dataSource.close();
    }
}
