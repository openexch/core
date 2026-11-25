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

    /** The floor of the jar range. */
    public static int minSupportedVersion() {
        return CoreSbeDemuxer.MIN_SUPPORTED_SCHEMA_VERSION;
    }

    /** A demuxer standing in for a jar whose highest supported money-schema version is {@code maxSupportedVersion}. */
    public static CoreSbeDemuxer demuxer(final com.openexchange.core.application.engine.CoreEngine engine,
                                         final com.openexchange.core.application.projection.SettlementProjector projector,
                                         final int maxSupportedVersion) {
        return new CoreSbeDemuxer(engine, projector, maxSupportedVersion);
    }

    /**
     * Stand a member in for a jar whose highest supported money-schema version is {@code version}: its demuxer's range
     * and its config machine's node-local check both move, as they would with a different jar.
     */
    public static void standInForJar(final CoreClusteredService service, final int version) {
        service.demuxer().setMaxSupportedVersionForTest(version);
        service.engine().coreConfig().setSupportedMaxWireVersion(version);
    }
}
