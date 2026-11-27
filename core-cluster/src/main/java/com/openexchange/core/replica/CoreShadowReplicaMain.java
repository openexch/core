// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.replica;

import com.openexchange.core.infrastructure.Logger;
import com.openexchange.core.infrastructure.persistence.CoreLogApplier;
import com.openexchange.core.infrastructure.persistence.CoreShadowIdentity;
import com.openexchange.cluster.EngineLifecycle;
import com.openexchange.cluster.log.ShadowEngine;
import com.openexchange.cluster.replica.ReplicaEngine;
import com.openexchange.cluster.replica.ReplicaProcess;
import com.openexchange.cluster.shadow.ShadowHeader;
import com.openexchange.cluster.shadow.ShadowIdentitySource;
import com.openexchange.cluster.shadow.ShadowSelfCheck;
import com.openexchange.cluster.BuildInfo;
import com.openexchange.cluster.MemberCompat;
import com.openexchange.core.infrastructure.persistence.CoreMemberCompat;
import io.aeron.cluster.VersionValidator;

import java.util.Objects;

/**
 * {@code java -jar core-cluster.jar replica}: the core shadow replica, a fourth process that is not a
 * cluster member. It follows the core cluster log with Aeron's {@code ClusterBackup} into its own archive, applies the
 * committed recording through the members' {@link CoreLogApplier} and writes shadows at ALIGNED, STANDBY
 * and LOCAL positions. The skeleton is the kit's {@code cluster.replica}; this class binds the core.
 *
 * <p>Order (the {@code AeronCluster} discipline, AC:58-67): install the lifecycle, parse {@link ReplicaSettings} under
 * the {@value ReplicaSettings#PREFIX} guard BEFORE any Aeron component (the mismatch dump dir included), build the
 * binding, then {@code ReplicaProcess.launch} with the lifecycle's host and wait for the stop. A refused setting, a
 * failed launch or a fatal fault exits 1 through the lifecycle; a signal exits 143; a decided re-bootstrap (members
 * pruned past this replica's log, a newer ALIGNED pulled and verified) exits 3 for the unit to restart; there is no
 * cluster termination for a replica. An oracle mismatch (the members' snapshot at an ALIGNED position differs from the replica's image,
 * same genesis) holds instead of exiting: the process stays up with {@code /ready} 503 and its metrics served until
 * it is stopped (143), the external driver is lost (1) or {@code REPLICA_MISMATCH_HOLD_MS} elapses (1).</p>
 *
 * <p>Rules: the replica's engine is touched only by the kit follower thread through
 * {@code ReplayContainer}; it publishes nothing (no event sink, journal sink or transport hooks: the applier's
 * defaults are {@code NoopCoreEventSink} and no journal), schedules nothing and sends no ingress. Positions,
 * times, terms and principals reaching the applier come from log frames only; nothing in this class reads a clock.</p>
 */
public final class CoreShadowReplicaMain {

    private static final Logger log = Logger.getLogger(CoreShadowReplicaMain.class);

    /** The text a CoreConfig cross-check failure carries on the replica. */
    static final String CORE_CONFIG_FAILED = "CoreConfig cross-check failed on the replica";

    private CoreShadowReplicaMain() {
    }

    /** Runs the replica until it stops, then exits through the kit's {@link EngineLifecycle}. Never returns normally. */
    public static void run() {
        // A fault goes to the log at ERROR; that a watchdog was armed, and with which bounds, is information.
        final EngineLifecycle lifecycle = EngineLifecycle.install("Core replica", msg -> log.error("%s", msg),
                msg -> log.info("%s", msg));
        try {
            run(lifecycle);
        } catch (final Throwable t) {
            lifecycle.startupFailed(t);
            return;
        }
        lifecycle.finish();
    }

    private static void run(final EngineLifecycle lifecycle) {
        // Settings first: a bad value is refused with the boot guard prefix and exit 1 before anything launches.
        final ReplicaSettings settings = ReplicaSettings.enforce();
        for (final String line : settings.storageWarnings()) {
            log.warn("%s", line);
        }
        for (final String line : settings.pullDisabledWarnings()) {
            log.warn("%s", line);
        }
        for (final String line : settings.pullWarnings()) {
            log.error("%s", line);
        }
        // an incompatible log (another generation, or two levels away) stops the replica with exit 78 at the first
        // term event, through the same refusal the members use (S4 R-A3)
        final Binding binding = new Binding(() -> lifecycle.fatal(EngineLifecycle.EXIT_FATAL, CORE_CONFIG_FAILED),
                CoreMemberCompat.MEMBER_COMPAT, MemberCompat.fatalOnRefusal(lifecycle, BuildInfo.of(Binding.class)));
        log.info("Core replica R%d starting: members=%s driverMode=%s source=%s",
                settings.slot(), settings.memberHosts(), settings.driverMode(), settings.sourceType());
        final ReplicaProcess.Host host = host(ReplicaProcess.of(lifecycle));
        try (ReplicaProcess ignored = ReplicaProcess.launch(settings.config(), binding, host, null)) {
            lifecycle.awaitStop();
        }
    }

    /**
     * The lifecycle's host with the oracle mismatch hold logged as CRITICAL through the core logger. A hold changes
     * nothing in the lifecycle: the process keeps running (endpoint, metrics, housekeeping) until the operator's stop
     * (143), driver loss (1, the error handler stays armed) or the kit's {@code REPLICA_MISMATCH_HOLD_MS} bound (1).
     */
    static ReplicaProcess.Host host(final ReplicaProcess.Host lifecycle) {
        return new ReplicaProcess.Host() {
            @Override
            public void fatal(final String reason) {
                lifecycle.fatal(reason);
            }

            @Override
            public void onError(final Throwable t, final boolean externalDriver) {
                lifecycle.onError(t, externalDriver);
            }

            @Override
            public void awaitExternalDriver(final String aeronDir, final long timeoutMs) {
                lifecycle.awaitExternalDriver(aeronDir, timeoutMs);
            }

            @Override
            public void restart(final int code, final String reason) {
                // a decided re-bootstrap: the lifecycle closes the process (cluster/ moves aside after the backup and
                // the archive closed) and exits with code (3); the unit restarts it
                log.error("CRITICAL replica restart (exit %d): %s", code, reason);
                lifecycle.restart(code, reason);
            }

            @Override
            public void hold(final String reason) {
                log.error("CRITICAL replica HOLD: %s (nothing is applied or written; /ready reads fenced, /metrics and"
                        + " the mismatch dump stay served until the process is stopped)", reason);
            }
        };
    }

    /**
     * The core for the kit replica: one live {@link CoreLogApplier}, its identity, the
     * scratch-applier self-check and the members' container app version and validator (S4: the core's member version
     * and the kit validator).
     */
    static final class Binding implements ReplicaEngine {

        private final CoreLogApplier applier;
        private final CoreShadowIdentity identity;
        private final ApplierSelfCheck selfCheck = new ApplierSelfCheck();
        private final int appVersion;
        private final VersionValidator validator;

        /**
         * @param onCoreConfigFailure the node-local stop for a CoreConfig cross-check failure; after it the
         *                            applier's fail-fast THROWS, so the frame's apply fails and the follower fences
         *                            before any further shadow is produced
         */
        Binding(final Runnable onCoreConfigFailure) {
            this(onCoreConfigFailure, CoreMemberCompat.MEMBER_COMPAT, MemberCompat.testRefusal("Core replica"));
        }

        /**
         * @param own       the members' version ({@link CoreMemberCompat#MEMBER_COMPAT}; tests pass another level)
         * @param onRefusal what an incompatible log does ({@code MemberCompat.fatalOnRefusal}: exit 78)
         */
        Binding(final Runnable onCoreConfigFailure, final MemberCompat own, final MemberCompat.Refusal onRefusal) {
            Objects.requireNonNull(onCoreConfigFailure, "onCoreConfigFailure");
            this.applier = new CoreLogApplier(() -> {
                onCoreConfigFailure.run();
                throw new IllegalStateException(CORE_CONFIG_FAILED);
            });
            this.identity = new CoreShadowIdentity(applier.engine());
            // the members' pair (ClusterConfig.memberCompat): the engine's version and the kit validator
            this.appVersion = own.appVersion();
            this.validator = MemberCompat.validator(own, Objects.requireNonNull(onRefusal, "onRefusal"));
        }

        @Override
        public int engineId() {
            return CoreLogApplier.ENGINE_ID_CORE;
        }

        @Override
        public ShadowEngine engine() {
            return applier;
        }

        @Override
        public ShadowIdentitySource identity() {
            return identity;
        }

        @Override
        public ShadowSelfCheck selfCheck() {
            return selfCheck;
        }

        @Override
        public int appVersion() {
            return appVersion;
        }

        @Override
        public VersionValidator validator() {
            return validator;
        }

        /** The ShadowRestore post-restore witness: the restored engine never ran ahead of the image's cluster time. */
        @Override
        public String postRestoreWitness(final ShadowHeader header) {
            final long lastLogTime = applier.engine().getLastLogTime();
            if (lastLogTime > header.clusterTime()) {
                return "shadow P_s witness failed: lastLogTime=" + lastLogTime + " > header.clusterTime="
                        + header.clusterTime();
            }
            return null;
        }

        /**
         * The genesis a member's full snapshot image ADOPTed, through a SCRATCH applier (the live one is
         * never touched; the image is the members' {@code restoreFromImage} input). A CoreConfig cross-check on the
         * scratch throws, so an undecodable image is a loud refusal on the follower thread, never a guess.
         */
        @Override
        public long imageGenesisId(final org.agrona.DirectBuffer image, final int length) {
            final CoreLogApplier scratch = new CoreLogApplier(() -> {
                throw new IllegalStateException("CoreConfig cross-check failed on the genesis-check scratch applier");
            });
            scratch.restore(image, length);
            return new CoreShadowIdentity(scratch.engine()).genesisId();
        }

        /** The live applier (tests and the scans read it; only the follower thread applies to it). */
        CoreLogApplier applier() {
            return applier;
        }
    }
}
