// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.bridge;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** The jar runs its two tools only: a start without a known subcommand is a usage error, never a bridge. */
public class BridgeMainTest {

    @Test
    public void noSubcommandIsAUsageError() {
        assertEquals(BridgeMain.EXIT_USAGE, BridgeMain.run(new String[0]));
    }

    @Test
    public void anUnknownSubcommandIsAUsageError() {
        assertEquals(BridgeMain.EXIT_USAGE, BridgeMain.run(new String[] {"bridge"}));
    }
}
