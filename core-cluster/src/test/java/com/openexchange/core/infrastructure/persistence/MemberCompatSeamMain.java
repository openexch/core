// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

/**
 * Test build of the core's entry point (process tests only): the production {@code com.openexchange.core.Main} with
 * the kit's member-version test seam set from {@code -Dtest.member.compat=<generation>.<level>} and, for the negative
 * controls, {@code -Dtest.member.compat.prelaunch=off} or {@code -Dtest.member.compat.validator=aeron}
 * ({@code MemberCompat.TestSeam}, the seam the matching engine's tests use too).
 */
public final class MemberCompatSeamMain {

    private MemberCompatSeamMain() {
    }

    public static void main(final String[] args) {
        com.openexchange.cluster.MemberCompat.TestSeam.fromSystemProperties();
        com.openexchange.core.Main.main(args);
    }
}
