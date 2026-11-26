// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.cluster.MemberCompat;
import com.openexchange.core.application.engine.config.CoreConfigStateMachine;

/**
 * The core's member version (the kit's {@link MemberCompat}) and the capabilities it stands for. The level rises by
 * exactly one in every release that changes {@link #manifest()}; {@code MemberCompatManifestTest} refuses a changed
 * manifest at an unchanged level. Core and match levels are independent numbers.
 *
 * <p>Every consensus module and service container context carries {@link #own()}'s app version and the kit validator
 * ({@link ClusterConfig#memberCompat}), the replica binding returns the same pair, and {@code /ready} reports it
 * ({@code compat=}): the value the validator uses, never what a launcher asked for.</p>
 */
public final class CoreMemberCompat {

    /** g1.1: the first build after the S4 genesis. */
    public static final MemberCompat MEMBER_COMPAT = MemberCompat.of(1, 1);

    private CoreMemberCompat() {
    }

    /** The version this build runs: {@link #MEMBER_COMPAT} outside the process tests (the kit's test seam). */
    public static MemberCompat own() {
        return MemberCompat.effective(MEMBER_COMPAT);
    }

    /** The lowest money-schema version this jar accepts. */
    public static int wireFloor() {
        return CoreSbeDemuxer.MIN_SUPPORTED_SCHEMA_VERSION;
    }

    /** The highest money-schema version this jar accepts. */
    public static int wireTop() {
        return CoreConfigStateMachine.SUPPORTED_MAX_WIRE_VERSION;
    }

    public static int[] behaviors() {
        return CoreConfigStateMachine.SUPPORTED_BEHAVIOR_VERSIONS.clone();
    }

    /** The balance snapshot layouts this build reads (current, and previous when it carries one). */
    public static int[] layouts() {
        final var l = BalanceSnapshotCodec.LAYOUTS;
        return l.previous() == null ? new int[] {l.current().version()}
                : new int[] {l.previous().version(), l.current().version()};
    }

    /** The capability manifest of this build (the committed {@code compat/manifest-g<g>.<level>.txt}). */
    public static MemberCompat.Manifest manifest() {
        return MemberCompat.Manifest.builder()
                .wire(wireFloor(), wireTop())
                .behaviors(CoreConfigStateMachine.SUPPORTED_BEHAVIOR_VERSIONS)
                .layouts(BalanceSnapshotCodec.LAYOUTS)
                .journal("money-schema", com.openexchange.core.infrastructure.generated.MessageHeaderDecoder.SCHEMA_ID,
                        com.openexchange.core.infrastructure.generated.MessageHeaderDecoder.SCHEMA_VERSION)
                .journal("money-journal",
                        com.openexchange.core.infrastructure.journal.generated.MessageHeaderDecoder.SCHEMA_ID,
                        com.openexchange.core.infrastructure.journal.generated.MessageHeaderDecoder.SCHEMA_VERSION)
                .journal("order-journal",
                        com.openexchange.core.infrastructure.orderjournal.generated.MessageHeaderDecoder.SCHEMA_ID,
                        com.openexchange.core.infrastructure.orderjournal.generated.MessageHeaderDecoder.SCHEMA_VERSION)
                .build();
    }

    /** The {@code /health} and {@code /ready} tokens for {@code own} on this driver mode. */
    static MemberCompat.Capabilities capabilities(final MemberCompat own, final boolean externalDriver) {
        return new MemberCompat.Capabilities(own, wireFloor(), wireTop(), behaviors(), layouts(),
                externalDriver ? "external" : "embedded");
    }
}
