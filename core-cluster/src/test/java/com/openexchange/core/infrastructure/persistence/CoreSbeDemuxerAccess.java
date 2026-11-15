// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

/** Test access to the demuxer's package-private template table from other test packages. */
public final class CoreSbeDemuxerAccess {

    private CoreSbeDemuxerAccess() {
    }

    /** The minimum root block length of a known template, -1 for a template this build does not define. */
    public static int minimumBlockLength(final int templateId) {
        return CoreSbeDemuxer.minimumBlockLength(templateId);
    }
}
