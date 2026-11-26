// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import io.aeron.archive.client.AeronArchive;

/**
 * Positive control for {@code NoPurgeCallSiteInCoreTest} (test scope only): a journal segment purge in the core
 * tree, outside the kit's one purge owner for journal recordings ({@code JournalRetention}).
 */
public final class ForbiddenCoreSegmentPurgeProbe {
    private ForbiddenCoreSegmentPurgeProbe() {
    }

    public static void purge(final AeronArchive archive, final long recordingId, final long newStartPosition) {
        archive.purgeSegments(recordingId, newStartPosition);
    }
}
