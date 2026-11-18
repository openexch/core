// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import java.security.Permission;

/**
 * Test build of the core's entry point that ignores driver loss, for a control's child JVM only: it runs the production
 * {@code com.openexchange.core.Main} under a security manager whose exit check parks the calling thread for good. The
 * halt the lifecycle takes on a lost media driver (and every later halt or exit) never happens, so the member stays alive
 * with a dead driver. The child needs {@code -Djava.security.manager=allow} (Java 21).
 */
public final class DriverLossIgnoredMain {

    static final String SWALLOWED = "[test build] exit swallowed";

    private DriverLossIgnoredMain() {
    }

    @SuppressWarnings("removal")
    public static void main(final String[] args) {
        System.setSecurityManager(new SecurityManager() {
            @Override
            public void checkPermission(final Permission perm) {
            }

            @Override
            public void checkPermission(final Permission perm, final Object context) {
            }

            @Override
            public void checkExit(final int status) {
                System.out.println(SWALLOWED + ": status " + status + " on thread " + Thread.currentThread().getName());
                System.out.flush();
                parkForever();
            }
        });
        com.openexchange.core.Main.main(args);
        parkForever();
    }

    private static void parkForever() {
        while (true) {
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (final InterruptedException ignored) {
                // a close interrupting this thread does not let the exit through
            }
        }
    }
}
