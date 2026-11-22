// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.bridge;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/** The CoreDump driver choice without a cluster: strict parsing and the refused starts. */
public class CoreDumpDriverTest {

    @Test
    public void parsesBothModesCaseInsensitively() {
        final CoreDumpDriver embedded = CoreDumpDriver.parse("Embedded", null);
        assertEquals(CoreDumpDriver.Mode.EMBEDDED, embedded.mode);
        assertNull(embedded.aeronDir);

        final CoreDumpDriver external = CoreDumpDriver.parse(" EXTERNAL ", " /dev/shm/aeron-x ");
        assertEquals(CoreDumpDriver.Mode.EXTERNAL, external.mode);
        assertEquals("/dev/shm/aeron-x", external.aeronDir);
    }

    @Test
    public void missingModeRefuses() {
        for (final String missing : new String[] {null, "", "  "}) {
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> CoreDumpDriver.parse(missing, null));
            assertTrue(e.getMessage(), e.getMessage().startsWith("COREDUMP_DRIVER_MODE is not set"));
        }
    }

    @Test
    public void unknownModeRefuses() {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> CoreDumpDriver.parse("shared", null));
        assertEquals("Invalid COREDUMP_DRIVER_MODE 'shared' (expected external|embedded)", e.getMessage());
    }

    @Test
    public void externalWithoutAeronDirRefuses() {
        for (final String dir : new String[] {null, "", "  "}) {
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> CoreDumpDriver.parse("external", dir));
            assertTrue(e.getMessage(), e.getMessage().startsWith("COREDUMP_DRIVER_MODE=external needs AERON_DIR"));
        }
    }

    @Test
    public void embeddedWithAeronDirRefuses() {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> CoreDumpDriver.parse("embedded", "/dev/shm/aeron"));
        assertTrue(e.getMessage(), e.getMessage().startsWith("AERON_DIR='/dev/shm/aeron' is set but"));
    }

    /** The real entry point: with no mode in the environment the tool exits 1 and starts no driver. */
    @Test
    public void entryPointWithoutModeExitsOneAndStartsNoDriver() {
        assumeTrue("needs COREDUMP_DRIVER_MODE unset in the test environment",
                System.getenv(CoreDumpDriver.MODE_ENV) == null);
        final File embeddedDir = new File("/dev/shm/aeron-aedump-" + ProcessHandle.current().pid());

        assertEquals(CoreDump.EXIT_REFUSED, CoreDump.run(new String[0]));
        assertFalse("no embedded driver directory after a refused start", embeddedDir.exists());
    }

    /** External mode with no live driver at AERON_DIR refuses at once (exit 1), never launching its own. */
    @Test
    public void externalWithNoActiveDriverExitsOne() {
        final File dir = new File(System.getProperty("java.io.tmpdir"), "coredump-no-driver-" + ProcessHandle.current().pid());
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BridgeConfig config = new BridgeConfig(List.of(), List.of("localhost"), 24600, "localhost:0",
                "localhost", true, 1_000, 0);

        final int exit = CoreDump.run(CoreDumpDriver.parse("external", dir.getAbsolutePath()), config,
                ctx -> {
                    throw new AssertionError(CoreDumpDriverModeTest.EMBEDDED_IN_EXTERNAL);
                },
                new PrintStream(out, true, StandardCharsets.UTF_8));

        assertEquals(CoreDump.EXIT_REFUSED, exit);
        assertEquals("nothing on stdout", "", out.toString(StandardCharsets.UTF_8));
    }
}
