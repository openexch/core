// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.cluster.journal.JournalRetention;

/**
 * Positive control for {@code NoPurgeCallSiteInCoreTest.coreMainNeverNamesTheJournalRetentionSeams} (test scope
 * only): a class that switches a journal retention seam, as a negative control does.
 */
public final class RetentionSeamUser {
    private RetentionSeamUser() {
    }

    public static void tailCoverOff(final boolean off) {
        JournalRetention.Seams.tailCoverOff = off;
    }
}
