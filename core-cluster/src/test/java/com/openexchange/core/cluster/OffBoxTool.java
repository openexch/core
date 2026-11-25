// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Runs the kit's off-box watermark command the way an archiver does: a process of its own on the member's host,
 * against that member's cluster directory. Shared by the off-box retention rows of both driver modes.
 */
final class OffBoxTool {

    private static final String MAIN = "com.openexchange.cluster.OffBoxWatermarkTool";

    private OffBoxTool() {
    }

    /** The command's exit code and its combined output. */
    record Run(int exit, String output) { }

    static Run run(final String... args) {
        try {
            final List<String> cmd = new ArrayList<>(List.of(
                    Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xshare:auto", "-Xmx64m",
                    "-cp", System.getProperty("java.class.path"), MAIN));
            cmd.addAll(List.of(args));
            final Path out = Files.createTempFile("offbox-tool-", ".log");
            try {
                final Process p = new ProcessBuilder(cmd).redirectErrorStream(true).redirectOutput(out.toFile())
                        .start();
                if (!p.waitFor(60, TimeUnit.SECONDS)) {
                    p.destroyForcibly();
                    throw new AssertionError("the watermark command did not finish: " + String.join(" ", args));
                }
                return new Run(p.exitValue(), Files.readString(out));
            } finally {
                Files.deleteIfExists(out);
            }
        } catch (final IOException e) {
            throw new java.io.UncheckedIOException(e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static Run ok(final String... args) {
        final Run r = run(args);
        if (r.exit() != 0) {
            throw new AssertionError("OffBoxWatermarkTool " + String.join(" ", args) + " exited " + r.exit() + ": "
                    + r.output());
        }
        return r;
    }

    static Run set(final File clusterDir, final long genesis, final long position, final String... more) {
        final List<String> args = new ArrayList<>(List.of("set", "--cluster-dir", clusterDir.getAbsolutePath(),
                "--engine", "core", "--cluster-id", "0", "--genesis-id", Long.toString(genesis),
                "--log-position-bytes", Long.toString(position), "--source", "test-archiver"));
        args.addAll(List.of(more));
        return ok(args.toArray(new String[0]));
    }

    static Run release(final File clusterDir, final long genesis, final long upTo, final String reason) {
        return ok("release", "--cluster-dir", clusterDir.getAbsolutePath(), "--genesis-id", Long.toString(genesis),
                "--up-to-log-position-bytes", Long.toString(upTo), "--reason", reason);
    }

    static Run clear(final File clusterDir, final long genesis) {
        return ok("clear", "--cluster-dir", clusterDir.getAbsolutePath(), "--genesis-id", Long.toString(genesis));
    }
}
