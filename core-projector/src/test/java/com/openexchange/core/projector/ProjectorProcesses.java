// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Child processes for the projector's process-level tests (SIGTERM, driver modes, restarts): a throwaway local
 * Postgres with V001 applied, child JVMs on this build's classpath (the journal source stand-in, an external driver,
 * the production projector), and the bookkeeping that removes them. One per test; {@link #close()} in {@code @After}.
 *
 * <p>Postgres needs the server binaries ({@code initdb}, {@code postgres}, {@code psql}): {@code PROJECTOR_PG_BIN}, or
 * the newest {@code /usr/lib/postgresql/<n>/bin}; {@link #pgBin()} is null when there are none (tests assume it).
 */
final class ProjectorProcesses implements AutoCloseable {

    final List<Process> processes = new ArrayList<>();
    private final List<Path> dirs = new ArrayList<>();
    private Path pgData;
    private Path pgBin;
    private int pgPort = -1;

    @Override
    public void close() throws Exception {
        for (final Process p : processes) {
            p.destroyForcibly();
            p.waitFor(10, TimeUnit.SECONDS);
        }
        if (pgData != null && Files.exists(pgData.resolve("postmaster.pid"))) {
            new ProcessBuilder(pgBin.resolve("pg_ctl").toString(), "-D", pgData.toString(), "-m", "immediate", "stop")
                    .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start()
                    .waitFor(20, TimeUnit.SECONDS);
        }
        for (final Path d : dirs) {
            deleteTree(d);
        }
    }

    /** Delete {@code d} in {@link #close()} (temp dirs, /dev/shm driver dirs a killed child left behind). */
    Path track(final Path d) {
        dirs.add(d);
        return d;
    }

    Path dir(final String prefix) throws IOException {
        return track(Files.createTempDirectory(prefix));
    }

    /** A fresh name under /dev/shm (not created), removed in {@link #close()}. */
    Path shmName(final String prefix) {
        return track(Path.of("/dev/shm", prefix + ProcessHandle.current().pid() + "-" + System.nanoTime()));
    }

    Process start(final ProcessBuilder pb, final Path log) throws IOException {
        final Process p = pb.redirectErrorStream(true).redirectOutput(log.toFile()).start();
        processes.add(p);
        return p;
    }

    static List<String> java(final String classpath, final Path errorDir, final String... jvmArgs) {
        final List<String> cmd = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "-Xmx256m", "-XX:ErrorFile=" + errorDir.resolve("hs_err_pid%p.log")));
        cmd.addAll(List.of(jvmArgs));
        cmd.addAll(List.of("-cp", classpath));
        return cmd;
    }

    static String testClasspath() {
        return System.getProperty("java.class.path");
    }

    // ---- Postgres --------------------------------------------------------------------------------------------------

    Path pgBin() {
        if (pgBin == null) {
            pgBin = findPgBin();
        }
        return pgBin;
    }

    int pgPort() {
        return pgPort;
    }

    /** A throwaway cluster: trust auth, TCP on 127.0.0.1 only, socket in its own dir, V001 applied to "core". */
    int startPostgres() throws Exception {
        final Path bin = pgBin();
        assertTrue("no PostgreSQL server binaries", bin != null);
        final Path root = dir("projector-pg-");
        pgData = root.resolve("data");
        final Path socketDir = Files.createDirectories(root.resolve("sock"));
        run(root.resolve("initdb.log"), bin.resolve("initdb").toString(), "-D", pgData.toString(), "-U", "core",
                "--auth=trust", "--no-sync", "-E", "UTF8");
        final int port = freePort();
        final Process pg = start(new ProcessBuilder(bin.resolve("postgres").toString(), "-D", pgData.toString(),
                "-p", String.valueOf(port), "-k", socketDir.toString(), "-c", "listen_addresses=127.0.0.1",
                "-c", "fsync=off"), root.resolve("postgres.log"));
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (true) {
            assertTrue("postgres exited: " + Files.readString(root.resolve("postgres.log")), pg.isAlive());
            final Process probe = new ProcessBuilder(psql(port, "postgres", "-Atc", "select 1"))
                    .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
            if (probe.waitFor(10, TimeUnit.SECONDS) && probe.exitValue() == 0) {
                break;
            }
            assertTrue("postgres not ready: " + Files.readString(root.resolve("postgres.log")),
                    System.nanoTime() < deadline);
            Thread.sleep(200);
        }
        run(root.resolve("createdb.log"), psql(port, "postgres", "-v", "ON_ERROR_STOP=1", "-c", "CREATE DATABASE core"));
        final Path migration = root.resolve("V001__money_journal.sql");
        try (var in = ProjectorProcesses.class.getResourceAsStream("/db/migration/V001__money_journal.sql")) {
            assertTrue("V001 on the classpath", in != null);
            Files.copy(in, migration);
        }
        run(root.resolve("migrate.log"), psql(port, "core", "-v", "ON_ERROR_STOP=1", "-f", migration.toString()));
        pgPort = port;
        return port;
    }

    /** A further database {@code name} on the running throwaway cluster, with V001 applied. */
    void createDatabase(final String name) throws Exception {
        final Path root = dir("projector-pg-db-");
        run(root.resolve("createdb.log"), psql(pgPort, "postgres", "-v", "ON_ERROR_STOP=1", "-c",
                "CREATE DATABASE " + name));
        final Path migration = root.resolve("V001__money_journal.sql");
        try (var in = ProjectorProcesses.class.getResourceAsStream("/db/migration/V001__money_journal.sql")) {
            assertTrue("V001 on the classpath", in != null);
            Files.copy(in, migration);
        }
        run(root.resolve("migrate.log"), psql(pgPort, name, "-v", "ON_ERROR_STOP=1", "-f", migration.toString()));
    }

    /** Applies the packaged migration {@code file} (under /db/migration) to the "core" database as user core. */
    void migrate(final String file) throws Exception {
        final Path root = dir("projector-pg-migrate-");
        final Path migration = root.resolve(file);
        try (var in = ProjectorProcesses.class.getResourceAsStream("/db/migration/" + file)) {
            assertTrue(file + " on the classpath", in != null);
            Files.copy(in, migration);
        }
        run(root.resolve("migrate.log"), psql(pgPort, "core", "-v", "ON_ERROR_STOP=1", "-f", migration.toString()));
    }

    /** The JDBC URL of database {@code name} on the throwaway cluster (user core, trust auth). */
    String jdbcUrl(final String name) {
        return "jdbc:postgresql://127.0.0.1:" + pgPort + "/" + name;
    }

    /** One scalar from the "core" database. */
    String query(final String sql) throws Exception {
        final Path out = Files.createTempFile("projector-psql-", ".out");
        try {
            run(out, psql(pgPort, "core", "-v", "ON_ERROR_STOP=1", "-Atc", sql));
            return Files.readString(out).trim();
        } finally {
            Files.deleteIfExists(out);
        }
    }

    private String[] psql(final int port, final String db, final String... args) {
        final List<String> cmd = new ArrayList<>(List.of(pgBin.resolve("psql").toString(), "-h", "127.0.0.1",
                "-p", String.valueOf(port), "-U", "core", "-d", db));
        cmd.addAll(List.of(args));
        return cmd.toArray(String[]::new);
    }

    private static Path findPgBin() {
        final String env = System.getenv("PROJECTOR_PG_BIN");
        if (env != null && !env.isBlank()) {
            return Files.isExecutable(Path.of(env, "initdb")) ? Path.of(env) : null;
        }
        final File[] versions = new File("/usr/lib/postgresql").listFiles();
        if (versions == null) {
            return null;
        }
        Path best = null;
        int bestVersion = -1;
        for (final File v : versions) {
            final Path bin = v.toPath().resolve("bin");
            if (!Files.isExecutable(bin.resolve("initdb")) || !Files.isExecutable(bin.resolve("postgres"))
                    || !Files.isExecutable(bin.resolve("psql"))) {
                continue;
            }
            try {
                final int n = Integer.parseInt(v.getName());
                if (n > bestVersion) {
                    bestVersion = n;
                    best = bin;
                }
            } catch (final NumberFormatException ignored) {
                // not a version directory
            }
        }
        return best;
    }

    // ---- the journal source stand-in, an external driver, the production projector ----------------------------------

    record Projector(Process process, Path log) {
        String out() throws IOException {
            return Files.readString(log);
        }

        Path embeddedDir() {
            return Path.of("/dev/shm/aeron-projector-" + process.pid());
        }
    }

    record Driver(Process process, Path dir, Path log) {
        String out() throws IOException {
            return Files.readString(log);
        }
    }

    /** A {@link JournalSourceMain} child; {@code last[0]} is the last journalSeq it recorded. */
    record Source(Process process, Path log, int controlPort, Path archiveDir, int[] last) {
        void publish(final int n) throws Exception {
            command("publish " + n);
            last[0] += n;
            awaitLine(process, log, "[SOURCE] recorded " + last[0], 30_000);
        }

        /** The replay sessions the source's archive runs now (its replay session count counter). */
        int replays() throws Exception {
            final int before = count(Files.readString(log), "[SOURCE] replays ");
            command("replays");
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (true) {
                final String out = Files.readString(log);
                if (count(out, "[SOURCE] replays ") > before) {
                    final int at = out.lastIndexOf("[SOURCE] replays ") + "[SOURCE] replays ".length();
                    return Integer.parseInt(out.substring(at, out.indexOf('\n', at)).trim());
                }
                assertTrue("no replay count from the source: " + out, System.nanoTime() < deadline && process.isAlive());
                Thread.sleep(20);
            }
        }

        /** The start position of the source's journal recording as its archive has it. */
        long start() throws Exception {
            final int before = count(Files.readString(log), "[SOURCE] start ");
            command("start");
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (true) {
                final String out = Files.readString(log);
                if (count(out, "[SOURCE] start ") > before) {
                    final int at = out.lastIndexOf("[SOURCE] start ") + "[SOURCE] start ".length();
                    final int end = out.indexOf('\n', at);
                    if (end > at) {
                        return Long.parseLong(out.substring(at, end).trim());
                    }
                }
                assertTrue("no start position from the source: " + out, System.nanoTime() < deadline && process.isAlive());
                Thread.sleep(20);
            }
        }

        private void command(final String line) throws IOException {
            final OutputStream in = process.getOutputStream();
            in.write((line + "\n").getBytes(StandardCharsets.UTF_8));
            in.flush();
        }

        private static int count(final String text, final String token) {
            int n = 0;
            for (int i = text.indexOf(token); i >= 0; i = text.indexOf(token, i + 1)) {
                n++;
            }
            return n;
        }
    }

    /** A fresh source with its own archive, {@code initial} records recorded. */
    Source startSource(final int initial) throws Exception {
        final Path base = dir("projector-src-");
        return startSource(base, base.resolve("archive"), ProjectorProcesses.freePort(), initial, false);
    }

    /**
     * The source's member restarts: kill -9, then a new process on the same archive and control port re-records
     * journalSeq 1..last (a new recording whose overlap is byte equal, as a core member's log replay re-journals) and
     * {@code more} new records.
     */
    Source restartSource(final Source old, final int more) throws Exception {
        old.process.destroyForcibly();
        assertTrue("the source did not die", old.process.waitFor(10, TimeUnit.SECONDS));
        // a killed archive leaves its mark file looking active for the driver timeout; its process is gone
        Files.deleteIfExists(old.archiveDir.resolve("archive-mark.dat"));
        return startSource(dir("projector-src-restart-"), old.archiveDir, old.controlPort, old.last[0] + more, true);
    }

    /**
     * A fresh source that purges its own journal recording as a core member does: cluster-kit's journal retention
     * thread in mode {@code readers}, a pass every second, the reader files under {@code clusterDir}.
     */
    Source startRetainingSource(final int initial, final Path clusterDir, final long genesisId) throws Exception {
        final Path base = dir("projector-src-");
        return startSource(base, base.resolve("archive"), ProjectorProcesses.freePort(), initial, false,
                List.of("-Djournal.retention.money=readers", "-Djournal.retention.poll.ms=1000"),
                List.of(clusterDir.toString(), String.valueOf(genesisId)));
    }

    private Source startSource(final Path base, final Path archiveDir, final int port, final int initial,
                               final boolean keepArchive) throws Exception {
        return startSource(base, archiveDir, port, initial, keepArchive, List.of(), List.of());
    }

    private Source startSource(final Path base, final Path archiveDir, final int port, final int initial,
                               final boolean keepArchive, final List<String> jvmArgs, final List<String> moreArgs)
            throws Exception {
        final Path aeronDir = shmName("projector-src-");
        final List<String> cmd = java(testClasspath(), base,
                "-Daeron.term.buffer.sparse.file=true", "-Daeron.term.buffer.length=1m");
        cmd.addAll(jvmArgs);
        cmd.addAll(List.of(JournalSourceMain.class.getName(), aeronDir.toString(), archiveDir.toString(),
                String.valueOf(port), String.valueOf(initial), keepArchive ? "keep" : "fresh"));
        cmd.addAll(moreArgs);
        final Path log = base.resolve("source.log");
        final Process p = start(new ProcessBuilder(cmd), log);
        awaitLine(p, log, "[SOURCE] recorded " + initial, 60_000);
        return new Source(p, log, port, archiveDir, new int[] {initial});
    }

    Driver startDriver(final Path base) throws Exception {
        return startDriver(base, shmName("projector-aeronmd-"));
    }

    /** An external driver: the real C aeronmd when {@code -Dprojector.aeronmd} or PROJECTOR_AERONMD names one, else Java. */
    Driver startDriver(final Path base, final Path dir) throws Exception {
        final Path log = Files.createTempFile(base, "driver-", ".log");
        final String aeronmd = aeronmd();
        final ProcessBuilder pb;
        if (aeronmd != null) {
            pb = new ProcessBuilder(aeronmd);
            final Map<String, String> env = pb.environment();
            env.put("LD_LIBRARY_PATH", Path.of(aeronmd).getParent().toString());
            env.put("AERON_DIR", dir.toString());
            env.put("AERON_DIR_DELETE_ON_START", "1");
            env.put("AERON_DIR_DELETE_ON_SHUTDOWN", "1");
            env.put("AERON_TERM_BUFFER_SPARSE_FILE", "1");
            env.put("AERON_TERM_BUFFER_LENGTH", String.valueOf(1024 * 1024));
            env.put("AERON_THREADING_MODE", "SHARED");
        } else {
            final List<String> cmd = java(testClasspath(), base,
                    "-Daeron.dir=" + dir, "-Daeron.dir.delete.on.start=true", "-Daeron.dir.delete.on.shutdown=true",
                    "-Daeron.term.buffer.sparse.file=true", "-Daeron.term.buffer.length=1m",
                    "-Daeron.threading.mode=SHARED");
            cmd.add("io.aeron.driver.MediaDriver");
            pb = new ProcessBuilder(cmd);
        }
        final Process p = start(pb, log);
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!Files.exists(dir.resolve("cnc.dat"))) {
            assertTrue("driver did not start: " + Files.readString(log), System.nanoTime() < deadline && p.isAlive());
            Thread.sleep(50);
        }
        System.out.println("external driver: " + (aeronmd != null ? aeronmd : "Java MediaDriver") + " at " + dir);
        return new Driver(p, dir, log);
    }

    static String aeronmd() {
        String v = System.getProperty("projector.aeronmd");
        if (v == null || v.isEmpty()) {
            v = System.getenv("PROJECTOR_AERONMD");
        }
        if (v == null || v.isEmpty()) {
            return null;
        }
        assertTrue("not an executable aeronmd: " + v, Files.isExecutable(Path.of(v)));
        return v;
    }

    /** The production projector on the "core" database, reading {@code source}. */
    Projector projector(final Path base, final Source source, final Map<String, String> extraEnv) throws Exception {
        return projector(base, source, extraEnv, "core");
    }

    /** The production projector on database {@code database} of the throwaway cluster. */
    Projector projector(final Path base, final Source source, final Map<String, String> extraEnv,
                        final String database) throws Exception {
        final ProcessBuilder pb = projectorBuilder(base, extraEnv);
        final Map<String, String> env = pb.environment();
        env.put("PROJECTOR_POSTGRES_URL", jdbcUrl(database));
        env.put("PROJECTOR_CORE_JOURNAL_ARCHIVES", "localhost:" + source.controlPort);
        final Path log = base.resolve("projector-" + System.nanoTime() + ".log");
        final Process p = start(pb, log);
        track(Path.of("/dev/shm/aeron-projector-" + p.pid()));
        return new Projector(p, log);
    }

    /** A projector with no Postgres and no source: for the cases that must refuse before either is touched. */
    Projector projectorNoSource(final Path base, final Map<String, String> extraEnv) throws Exception {
        final ProcessBuilder pb = projectorBuilder(base, extraEnv);
        pb.environment().put("PROJECTOR_POSTGRES_URL", "jdbc:postgresql://127.0.0.1:" + freePort() + "/core");
        pb.environment().put("PROJECTOR_CORE_JOURNAL_ARCHIVES", "localhost:" + freePort());
        final Path log = base.resolve("projector.log");
        final Process p = start(pb, log);
        track(Path.of("/dev/shm/aeron-projector-" + p.pid()));
        return new Projector(p, log);
    }

    private static ProcessBuilder projectorBuilder(final Path base, final Map<String, String> extraEnv)
            throws IOException {
        final List<String> cmd = java(childClasspath(), base);
        cmd.add(ProjectorMain.class.getName());
        final ProcessBuilder pb = new ProcessBuilder(cmd);
        final Map<String, String> env = pb.environment();
        env.remove(ProjectorDriver.MODE_ENV);
        env.remove(ProjectorDriver.AERON_DIR_ENV);
        env.remove("TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS");
        env.put("PROJECTOR_POSTGRES_USER", "core");
        env.put("PROJECTOR_METRICS_PORT", String.valueOf(freePort()));
        env.put("PROJECTOR_LOCAL_HOST", "localhost");
        env.putAll(extraEnv);
        return pb;
    }

    private static String childClasspath() {
        final String override = System.getProperty("projector.driver.classpath");
        return override == null || override.isEmpty() ? testClasspath() : override;
    }

    String rows() throws Exception {
        return query("select count(*) from money_journal");
    }

    void awaitRows(final Projector p, final int expected, final long timeoutMs) throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        String n;
        while (!(n = rows()).equals(String.valueOf(expected))) {
            assertTrue("projected " + n + " of " + expected + " within " + timeoutMs + " ms: " + p.out(),
                    System.nanoTime() < deadline && p.process.isAlive());
            Thread.sleep(200);
        }
    }

    static int sigterm(final Projector p) throws Exception {
        p.process.destroy();
        return awaitExit(p.process, 8_000);
    }

    static int awaitExit(final Process p, final long timeoutMs) throws Exception {
        assertTrue("did not exit within " + timeoutMs + " ms", p.waitFor(timeoutMs, TimeUnit.MILLISECONDS));
        return p.exitValue();
    }

    static void assertNoCrash(final Path base, final String out) throws IOException {
        assertFalse("no JVM crash: " + out, out.contains("A fatal error has been detected by the Java Runtime"));
        try (Stream<Path> files = Files.list(base)) {
            assertEquals("no hs_err file", List.of(), files.filter(f -> f.getFileName().toString().startsWith("hs_err"))
                    .toList());
        }
    }

    // ---- small helpers ---------------------------------------------------------------------------------------------

    static void run(final Path log, final String... cmd) throws Exception {
        final Process p = new ProcessBuilder(cmd).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        assertTrue(String.join(" ", cmd) + " timed out", p.waitFor(60, TimeUnit.SECONDS));
        assertEquals(String.join(" ", cmd) + ": " + Files.readString(log), 0, p.exitValue());
    }

    static void awaitLine(final Process p, final Path log, final String line, final long timeoutMs)
            throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!(Files.exists(log) && Files.readString(log).contains(line))) {
            assertTrue("no '" + line + "' within " + timeoutMs + " ms: " + Files.readString(log),
                    System.nanoTime() < deadline && p.isAlive());
            Thread.sleep(100);
        }
    }

    static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    static void deleteTree(final Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }
}
