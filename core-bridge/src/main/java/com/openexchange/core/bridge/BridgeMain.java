// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.bridge;

import java.util.Arrays;

/**
 * Entry point of the core client tools jar ({@code core-bridge.jar}). Two subcommands:
 * <ul>
 *   <li>{@code dump}: read-only ledger snapshot for the money check ({@link CoreDump}: 0 = JSON
 *       printed, 3 = snapshot timeout);</li>
 *   <li>{@code set-money-journal on|off}: arm or disarm the money journal through the cluster log
 *       ({@link CoreSetMoneyJournal}).</li>
 * </ul>
 * The settlement bridge itself is the C++ {@code ct-bridge} (cluster-tools); this jar no longer
 * runs one. A start without a known subcommand prints the usage and exits 64.
 */
public final class BridgeMain {

    static final int EXIT_USAGE = 64;
    static final String USAGE = "usage: java -jar core-bridge.jar dump [...] | set-money-journal on|off";

    public static void main(final String[] args) {
        // First, for every subcommand: a deployment written for the retired names refuses before any setting is read.
        com.openexchange.core.config.RetiredSettings.refuseRetired("core-bridge.jar");
        System.exit(run(args));
    }

    static int run(final String[] args) {
        if (args.length > 0 && "dump".equals(args[0])) {
            return CoreDump.run(Arrays.copyOfRange(args, 1, args.length));
        }
        // The one WRITE tool here, and the only way to change that setting: it is replicated state,
        // so it travels through the log rather than through a node's environment.
        if (args.length > 0 && "set-money-journal".equals(args[0])) {
            return CoreSetMoneyJournal.run(Arrays.copyOfRange(args, 1, args.length));
        }
        System.err.println(USAGE);
        return EXIT_USAGE;
    }

    private BridgeMain() {
    }
}
