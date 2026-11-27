// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import com.openexchange.core.infrastructure.archive.ArchiveJournalSource;
import com.openexchange.core.infrastructure.journal.generated.BoolFlag;
import com.openexchange.core.infrastructure.journal.generated.JournalDepositDecoder;
import com.openexchange.core.infrastructure.journal.generated.JournalOpeningBalanceDecoder;
import com.openexchange.core.infrastructure.journal.generated.JournalSettleDecoder;
import com.openexchange.core.infrastructure.journal.generated.JournalWithdrawDecoder;
import com.openexchange.core.infrastructure.journal.generated.MessageHeaderDecoder;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.IdleStrategy;

import java.nio.ByteOrder;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The money projector's engine: core money journal -> filter -> Postgres.
 *
 * Stateless with respect to journal position. Every epoch reads the Postgres high-water
 * ({@code MAX(journal_seq)}, 0 on an empty ledger), builds a fresh {@link ProjectorFilter}, then
 * replays every money-journal recording from the earliest retained byte forward. Postgres is
 * authoritative; a PG failure never skips, it rolls back the batch, tears down the epoch and retries
 * from the unchanged high-water.
 *
 * What it does with a record:
 *  - above the high-water and dense: projected (inserted in journalSeq order, committed in batches);
 *  - at or below the high-water (replay overlap, a restarted member's re-recorded journal): never
 *    inserted, always compared with its stored rows field for field; a difference (or no stored row)
 *    is a determinism fault and PARKS the projector with PAYLOAD_CONFLICT, never a silent skip. The one
 *    missing row that is accepted is one age-based retention deleted: at or below the mark its delete
 *    wrote in the same transaction ({@link PostgresMoneyJournalStore.PrunedMark}); every other missing
 *    row still parks;
 *  - past a dense gap: that source's fault while another configured source has not shown the same gap:
 *    everything below the gap is committed and the next epoch reads the next source; only a gap that
 *    every configured source shows at the same high-water PARKS the projector (a lost money movement);
 *  - of a journal version other than {@link #JOURNAL_VERSION}: PARKS the projector (a version 1 record
 *    has no log position; a later version is unknown to this build).
 * A retention mark the login may not read PARKS it too (RETENTION_MARK).
 * A parked projector keeps serving /health and /metrics; a human decides, then restarts it.
 *
 * Source death is detected the same three ways as the settlement bridge, because a dead source
 * otherwise looks exactly like a quiet market: (1) a replay image that existed and vanished; (2) a
 * replay that never connects within {@link #CONNECT_TIMEOUT_MS}; (3) a periodic archive probe (the
 * recording stopped, or grew while the replay sat still, or the probe throws because the archive
 * process is gone). All three end the epoch; the next epoch re-lists the chain and re-attaches.
 */
public final class ProjectorAgent implements Runnable {

    private static final int POLL_LIMIT = 32;
    private static final long ERROR_BACKOFF_MS = 1_000;
    private static final long STATUS_LOG_INTERVAL_MS = 10_000;
    /** Cadence of the live-follow source-progress probe (also an archive liveness check). */
    private static final long SOURCE_CHECK_INTERVAL_MS = 10_000;
    /** A replay whose image never arrives is a dead source, not a slow start, past this. */
    private static final long CONNECT_TIMEOUT_MS = 15_000;
    /** Sentinel: no previous source-progress check this replay (never stall on the first probe). */
    private static final long NO_PRIOR_CHECK = Long.MIN_VALUE;
    /** Flush the pending batch once it reaches this many records mid-burst (bounds memory + latency). */
    private static final int FLUSH_MAX_BATCH = 256;
    private static final int SCHEMA_ID = JournalDepositDecoder.SCHEMA_ID;
    /** The one money journal version this build reads: version 2 carries the log position on every record. */
    static final int JOURNAL_VERSION = JournalDepositDecoder.SCHEMA_VERSION;
    /** Byte offset of journalSeq in every money journal message, in every version (header 8, first field). */
    private static final int JOURNAL_SEQ_OFFSET = MessageHeaderDecoder.ENCODED_LENGTH;

    private final ProjectorConfig config;
    private final String aeronDirectoryName;
    private final ProjectorState state;
    private final PostgresMoneyJournalStore store;
    /** The archive clients' Aeron error handler (D1: the lifecycle's); null = Aeron's default. */
    private final ErrorHandler aeronErrorHandler;

    private final MessageHeaderDecoder journalHeader = new MessageHeaderDecoder();
    private final JournalOpeningBalanceDecoder openingDecoder = new JournalOpeningBalanceDecoder();
    private final JournalDepositDecoder depositDecoder = new JournalDepositDecoder();
    private final JournalWithdrawDecoder withdrawDecoder = new JournalWithdrawDecoder();
    private final JournalSettleDecoder settleDecoder = new JournalSettleDecoder();
    private final ProjectorIdle.Mode idleMode;
    private final IdleStrategy idle;

    /** Records projected this epoch but not yet committed (journalSeq order preserved). */
    private final List<MoneyJournalRecord> pending = new ArrayList<>();
    /** Records at or below the high-water read this epoch and not yet compared with their stored rows. */
    private final List<MoneyJournalRecord> duplicates = new ArrayList<>();
    /** Sources that showed a dense gap right above {@link #gapHighWater}; reset when the high-water moves. */
    private final Set<String> gappedSources = new LinkedHashSet<>();
    private long gapHighWater = -1;
    /**
     * When the first source showed the current gap, moved forward while a source that has not shown it delivers
     * records below it; the gap parks once no such source delivered anything within the setting.
     */
    private long gapPendingSinceMs;

    /** True in production; a test control turns it off to show the duplicate comparison is what parks. */
    private final boolean compareDuplicates;
    /** True in production; a test control turns it off to show the source switch is what fills a one-member gap. */
    private final boolean switchSourceOnGap;
    /** True in production; a test control turns it off to show the retention mark is what accepts a deleted row. */
    private boolean acceptPruned = true;

    private volatile boolean running = true;
    private ProjectorFilter filter;
    private String currentSource = "";
    /** Set by a gap on the current source: the epoch ends and the next one reads another source. */
    private boolean epochOver;
    private long lastStatusLogMs;

    public ProjectorAgent(final ProjectorConfig config, final String aeronDirectoryName,
                          final ProjectorState state, final PostgresMoneyJournalStore store) {
        this(config, aeronDirectoryName, state, store, null);
    }

    public ProjectorAgent(final ProjectorConfig config, final String aeronDirectoryName,
                          final ProjectorState state, final PostgresMoneyJournalStore store,
                          final ErrorHandler aeronErrorHandler) {
        this(config, aeronDirectoryName, state, store, aeronErrorHandler, ProjectorIdle.Mode.BACKOFF);
    }

    /** As above, idling on {@code idleMode} (the agent's and the archive clients' idle strategies). */
    public ProjectorAgent(final ProjectorConfig config, final String aeronDirectoryName,
                          final ProjectorState state, final PostgresMoneyJournalStore store,
                          final ErrorHandler aeronErrorHandler, final ProjectorIdle.Mode idleMode) {
        this(config, aeronDirectoryName, state, store, aeronErrorHandler, true, true, idleMode);
    }

    /** Tests only: {@code false} turns a money control off, to show that the control is what a test observes. */
    ProjectorAgent(final ProjectorConfig config, final String aeronDirectoryName,
                   final ProjectorState state, final PostgresMoneyJournalStore store,
                   final ErrorHandler aeronErrorHandler, final boolean compareDuplicates,
                   final boolean switchSourceOnGap) {
        this(config, aeronDirectoryName, state, store, aeronErrorHandler, compareDuplicates, switchSourceOnGap,
                ProjectorIdle.Mode.BACKOFF);
    }

    private ProjectorAgent(final ProjectorConfig config, final String aeronDirectoryName,
                           final ProjectorState state, final PostgresMoneyJournalStore store,
                           final ErrorHandler aeronErrorHandler, final boolean compareDuplicates,
                           final boolean switchSourceOnGap, final ProjectorIdle.Mode idleMode) {
        this.idleMode = idleMode;
        this.idle = ProjectorIdle.newStrategy(idleMode);
        this.config = config;
        this.aeronDirectoryName = aeronDirectoryName;
        this.state = state;
        this.store = store;
        this.aeronErrorHandler = aeronErrorHandler;
        this.compareDuplicates = compareDuplicates;
        this.switchSourceOnGap = switchSourceOnGap;
    }

    /** Tests only: a duplicate whose rows retention deleted is a missing row like any other. Call before the start. */
    ProjectorAgent withoutPrunedAccept() {
        acceptPruned = false;
        return this;
    }

    /** The idle strategy of the agent's follow loop (tests). */
    IdleStrategy idleStrategy() {
        return idle;
    }

    public void stop() {
        running = false;
    }

    @Override
    public void run() {
        System.out.println("[PROJECTOR] starting: aeJournalArchives=" + config.journalArchiveEndpoints
                + " pg=" + config.postgresUrl + " haltOnGap=" + config.haltOnGap
                + " idleMode=" + idleMode.name().toLowerCase(java.util.Locale.ROOT));
        while (running) {
            if (state.halted) {
                // Parked (a gap on every source, a payload conflict, a refused version): keep the process
                // (and its metrics) alive; a human decides.
                sleep(ERROR_BACKOFF_MS);
                continue;
            }
            try {
                runEpoch();
            } catch (Exception e) {
                state.errors++;
                state.connectedToPg = false;
                System.err.println("[PROJECTOR] epoch failed (" + e.getClass().getSimpleName() + ": "
                        + e.getMessage() + ") - resyncing" + (running ? "" : " (stopping)"));
                sleep(ERROR_BACKOFF_MS);
            }
        }
        System.out.println("[PROJECTOR] stopped. " + state.render());
    }

    private void runEpoch() throws SQLException {
        final long hwm = store.highWater();
        state.connectedToPg = true;
        if (hwm != gapHighWater) {
            clearGap(); // the high-water moved: an earlier gap was filled
        }
        if (parkIfGapUnfilled()) {
            return;
        }
        filter = new ProjectorFilter(hwm, config.haltOnGap);
        pending.clear();
        duplicates.clear();
        epochOver = false;
        state.epochHighWater = hwm;
        state.sourceBacklogBytes = 0;
        state.epochs++;
        System.out.println("[PROJECTOR] epoch " + state.epochs + ": Postgres high-water journalSeq=" + hwm);

        try (ArchiveJournalSource source = ArchiveJournalSource.connectFirstHealthy(
                sourceOrder(),
                config.archiveControlStreamId,
                ProjectorConfig.MONEY_JOURNAL_STREAM_ID,
                ProjectorConfig.MONEY_JOURNAL_CHANNEL,
                ProjectorConfig.REPLAY_STREAM_ID,
                aeronDirectoryName,
                config.localHost,
                aeronErrorHandler,
                ProjectorIdle.newStrategy(idleMode))) {
            currentSource = source.endpoint();
            state.journalSource = currentSource;
            followChain(source);
        }
    }

    /** The configured endpoints, those that showed the current gap last (still tried: they may be the only ones up). */
    private List<String> sourceOrder() {
        final List<String> first = new ArrayList<>();
        final List<String> last = new ArrayList<>();
        for (final String e : config.journalArchiveEndpoints) {
            (gappedSources.contains(e.trim()) ? last : first).add(e.trim());
        }
        first.addAll(last);
        return first;
    }

    private void followChain(final ArchiveJournalSource source) {
        final List<ArchiveJournalSource.Recording> chain = source.recordings();
        if (chain.isEmpty()) {
            // Journal enabled but nothing recorded yet (or dark): wait and re-list next epoch.
            System.out.println("[PROJECTOR] no journal recordings at " + source.endpoint() + " yet - waiting");
            sleep(ERROR_BACKOFF_MS);
            return;
        }
        final FragmentHandler handler = (buffer, offset, length, header) -> onJournalEntry(buffer, offset);

        for (final ArchiveJournalSource.Recording recording : chain) {
            System.out.println("[PROJECTOR] following recording " + recording.recordingId()
                    + (recording.isActive() ? " (ACTIVE, live-follow)" : " (stopped)"));
            try (Subscription replay = source.openReplay(recording)) {
                final boolean liveFollow = recording.isActive();
                final long connectDeadlineMs = System.currentTimeMillis() + CONNECT_TIMEOUT_MS;
                long nextSourceCheckMs = System.currentTimeMillis() + SOURCE_CHECK_INTERVAL_MS;
                long consumedAtLastCheck = NO_PRIOR_CHECK;
                boolean imageWasLive = false;
                boolean followedRecordingEnded = false;
                while (running && !state.halted && !epochOver) {
                    final int fragments = source.poll(replay, handler, POLL_LIMIT);
                    maybeLogStatus();
                    imageWasLive |= replay.imageCount() > 0;
                    if (epochOver || state.halted) {
                        break;
                    }
                    if (fragments == 0) {
                        // Caught up for now: compare the duplicates read, durably commit what this batch projected.
                        flush();
                        if (state.halted || parkIfGapUnfilled()) {
                            break;
                        }
                        if (!liveFollow && replayDrained(replay, recording)) {
                            break; // stopped recording fully consumed -> next in chain
                        }
                        if (replay.isClosed() || (imageWasLive && replay.imageCount() == 0)) {
                            // The image we HAD is gone (source node died / purge race):
                            // epoch restart re-lists the chain and re-attaches.
                            sourceStall("journal replay image lost on recording "
                                    + recording.recordingId());
                        }
                        if (!imageWasLive && System.currentTimeMillis() > connectDeadlineMs) {
                            // Replay accepted but no image ever arrived: dead source, not slow start.
                            sourceStall("journal replay never connected on recording "
                                    + recording.recordingId() + " within " + CONNECT_TIMEOUT_MS + "ms");
                        }
                        if (liveFollow && System.currentTimeMillis() >= nextSourceCheckMs) {
                            nextSourceCheckMs = System.currentTimeMillis() + SOURCE_CHECK_INTERVAL_MS;
                            final long consumed = replay.imageCount() > 0
                                    ? replay.imageAtIndex(0).position() : -1;
                            // Throws when the archive itself is gone -> epoch restart. This is
                            // what un-wedges a live-follow whose source process was killed.
                            final long recPos = source.recordingPosition(recording.recordingId());
                            if (recPos == AeronArchive.NULL_POSITION) {
                                // Recording stopped under our live-follow (source restarted; its
                                // successor recording isn't in this epoch's chain listing).
                                final long stop = source.stopPosition(recording.recordingId());
                                if (consumed >= stop) {
                                    System.out.println("[PROJECTOR] followed recording "
                                            + recording.recordingId() + " stopped and is fully "
                                            + "consumed - ending epoch to re-list the chain");
                                    followedRecordingEnded = true;
                                    break;
                                }
                                if (consumed == consumedAtLastCheck && consumedAtLastCheck != NO_PRIOR_CHECK) {
                                    sourceStall("recording " + recording.recordingId()
                                            + " stopped but replay is stuck at " + consumed
                                            + " short of stop position " + stop);
                                }
                            } else {
                                state.sourceBacklogBytes = Math.max(0, recPos - Math.max(consumed, 0));
                                if (recPos > consumed && consumed == consumedAtLastCheck
                                        && consumedAtLastCheck != NO_PRIOR_CHECK) {
                                    sourceStall("replay stalled on recording " + recording.recordingId()
                                            + ": recorded position " + recPos
                                            + " but replay stuck at " + consumed);
                                }
                            }
                            consumedAtLastCheck = consumed;
                        }
                        idle.idle();
                    } else {
                        if (pending.size() >= FLUSH_MAX_BATCH || duplicates.size() >= FLUSH_MAX_BATCH) {
                            flush();
                        }
                        idle.reset();
                    }
                }
                if (followedRecordingEnded) {
                    break; // end the epoch: the next one re-lists and follows the successor
                }
            }
            if (!running || state.halted || epochOver) {
                return;
            }
        }
        // Chain exhausted without a live-follow in progress (source down or mid-restart):
        // pause before the next epoch so we don't hot-loop re-reading the whole chain.
        sleep(ERROR_BACKOFF_MS);
    }

    private void sourceStall(final String reason) {
        state.sourceStalls++;
        throw new IllegalStateException(reason);
    }

    private boolean replayDrained(final Subscription replay, final ArchiveJournalSource.Recording recording) {
        if (replay.imageCount() == 0) {
            return replay.isClosed(); // bounded replay closes its image at the bound
        }
        return replay.imageAtIndex(0).position()
                >= recording.stopPosition() || replay.imageAtIndex(0).isEndOfStream();
    }

    /**
     * Decode one journal fragment into a {@link MoneyJournalRecord} and act on the filter's answer. After a gap ended
     * the epoch or the projector parked, the rest of the poll batch is ignored.
     */
    private void onJournalEntry(final DirectBuffer buffer, final int offset) {
        if (epochOver || state.halted) {
            return;
        }
        journalHeader.wrap(buffer, offset);
        if (journalHeader.schemaId() != SCHEMA_ID) {
            return; // foreign schema on the journal stream - ignore
        }
        final int templateId = journalHeader.templateId();
        if (templateId < JournalOpeningBalanceDecoder.TEMPLATE_ID || templateId > JournalSettleDecoder.TEMPLATE_ID) {
            return; // unknown template on schema 3 - ignore (its journalSeq shows as a gap on the next record)
        }
        if (journalHeader.version() != JOURNAL_VERSION) {
            refuseVersion(journalHeader.version(), templateId,
                    buffer.getLong(offset + JOURNAL_SEQ_OFFSET, ByteOrder.LITTLE_ENDIAN));
            return;
        }
        final MoneyJournalRecord r;
        final String kind;
        switch (templateId) {
            case JournalOpeningBalanceDecoder.TEMPLATE_ID -> {
                openingDecoder.wrapAndApplyHeader(buffer, offset, journalHeader);
                kind = "opening";
                r = MoneyJournalRecord.opening(openingDecoder.journalSeq(), openingDecoder.userId(),
                        openingDecoder.assetId(), openingDecoder.amount(), openingDecoder.logPosition());
            }
            case JournalDepositDecoder.TEMPLATE_ID -> {
                depositDecoder.wrapAndApplyHeader(buffer, offset, journalHeader);
                kind = "deposit";
                r = MoneyJournalRecord.deposit(depositDecoder.journalSeq(), depositDecoder.userId(),
                        depositDecoder.assetId(), depositDecoder.amount(), depositDecoder.balanceAfter(),
                        depositDecoder.clusterTimeMs(), depositDecoder.logPosition());
            }
            case JournalWithdrawDecoder.TEMPLATE_ID -> {
                withdrawDecoder.wrapAndApplyHeader(buffer, offset, journalHeader);
                kind = "withdraw";
                r = MoneyJournalRecord.withdraw(withdrawDecoder.journalSeq(), withdrawDecoder.userId(),
                        withdrawDecoder.assetId(), withdrawDecoder.amount(), withdrawDecoder.balanceAfter(),
                        withdrawDecoder.clusterTimeMs(), withdrawDecoder.logPosition());
            }
            default -> {
                settleDecoder.wrapAndApplyHeader(buffer, offset, journalHeader);
                kind = "settle";
                r = MoneyJournalRecord.settle(settleDecoder.journalSeq(), settleDecoder.tradeId(),
                        settleDecoder.marketId(), settleDecoder.price(), settleDecoder.quantity(),
                        settleDecoder.buyerUserId(), settleDecoder.sellerUserId(),
                        settleDecoder.takerIsBuy() == BoolFlag.TRUE,
                        settleDecoder.buyerBaseAfter(), settleDecoder.buyerQuoteAfter(),
                        settleDecoder.sellerBaseAfter(), settleDecoder.sellerQuoteAfter(),
                        settleDecoder.clusterTimeMs(), settleDecoder.logPosition());
            }
        }
        switch (filter.onRecord(r.journalSeq())) {
            case PROJECT -> pending.add(r);
            case DUPLICATE -> onDuplicate(r);
            case GAP -> onGap(kind, r.journalSeq());
        }
        // Surface the gap counter live in both halt and haltOnGap=false override modes.
        state.gapsDetected = filter.gapsDetected();
    }

    /**
     * A record at or below the high-water: queued for comparison with its stored rows. Anything pending is committed
     * first, so the stored rows of every queued duplicate exist (a duplicate can repeat a record of this epoch).
     */
    private void onDuplicate(final MoneyJournalRecord r) {
        state.duplicateRecords++;
        if (state.gapPending && !gappedSources.contains(currentSource)) {
            // a source that has not shown the gap is still reading up to it: its catch-up is not silence
            gapPendingSinceMs = System.currentTimeMillis();
        }
        if (!compareDuplicates) {
            return;
        }
        if (!pending.isEmpty()) {
            flush();
            if (state.halted) {
                return;
            }
        }
        duplicates.add(r);
    }

    /**
     * A dense gap on the current source. Everything below it is committed first. While another configured source has
     * not shown a gap at this high-water, the epoch ends and the next one reads that source; a gap every source shows
     * parks the projector.
     */
    private void onGap(final String kind, final long seq) {
        // Persist everything strictly below the gap (throws on PG failure -> the epoch retries and re-detects the
        // gap; nothing below it is lost).
        flush();
        if (state.halted) {
            return;
        }
        state.gapsDetected = filter.gapsDetected();
        final long below = filter.highWater();
        if (below != gapHighWater || gappedSources.isEmpty()) {
            gappedSources.clear();
            gapHighWater = below;
            gapPendingSinceMs = System.currentTimeMillis();
            state.gapPendingHighWater = below;
            state.gapPending = true;
        }
        final boolean seenBefore = !gappedSources.add(currentSource);
        if (switchSourceOnGap && !allSourcesGapped()) {
            state.sourceSwitches++;
            epochOver = true;
            System.err.println("[PROJECTOR] dense journalSeq gap on source " + currentSource + ": " + kind
                    + " journalSeq=" + seq + " after high-water=" + below + " | a source fault while another member "
                    + "may hold the range: reading the next source before parking (sources with this gap: "
                    + gappedSources + ")");
            if (seenBefore) {
                sleep(ERROR_BACKOFF_MS); // this source again: the others were unreachable; do not spin on it
            }
            return;
        }
        park("GAP", "*** HALTED - LOST MONEY MOVEMENT SUSPECTED *** dense journalSeq gap: "
                + kind + " journalSeq=" + seq + " after high-water=" + below + " on "
                + (switchSourceOnGap ? "every configured source " + gappedSources : "source " + currentSource)
                + " | a journal gap means a money movement Postgres will never see; NOT projecting past it. "
                + "Investigate the core journal archives and the money_journal table, then restart the projector.");
    }

    private void clearGap() {
        gappedSources.clear();
        state.gapPending = false;
    }

    /**
     * A gap one source showed that no other source filled, or read towards, within
     * {@link ProjectorConfig#gapUnfilledParkMs} (they are empty, quiet at the high-water, or unreachable) parks the
     * projector: it is not left waiting unseen.
     */
    private boolean parkIfGapUnfilled() {
        if (gappedSources.isEmpty()
                || System.currentTimeMillis() - gapPendingSinceMs < config.gapUnfilledParkMs) {
            return false;
        }
        final List<String> silent = new ArrayList<>();
        for (final String e : config.journalArchiveEndpoints) {
            if (!gappedSources.contains(e.trim())) {
                silent.add(e.trim());
            }
        }
        park("GAP", "*** HALTED - LOST MONEY MOVEMENT SUSPECTED *** dense journalSeq gap after high-water="
                + gapHighWater + " shown by " + gappedSources + " and not filled within " + config.gapUnfilledParkMs
                + " ms by " + silent + " (empty, quiet at the high-water, or unreachable) | NOT projecting past it. "
                + "Investigate the core journal archives and the money_journal table, then restart the projector.");
        return true;
    }

    private boolean allSourcesGapped() {
        for (final String e : config.journalArchiveEndpoints) {
            if (!gappedSources.contains(e.trim())) {
                return false;
            }
        }
        return true;
    }

    private void refuseVersion(final int version, final int templateId, final long seq) {
        flush();
        if (state.halted) {
            return;
        }
        state.versionRefusals++;
        park("VERSION", "*** HALTED - MONEY JOURNAL VERSION REFUSED *** record version " + version + " (template "
                + templateId + ", journalSeq " + seq + ") from source " + currentSource + "; this projector reads "
                + "version " + JOURNAL_VERSION + " only: a version 1 record carries no log position, a later version "
                + "is unknown to this build. Nothing past it is projected.");
    }

    /**
     * Compare the queued duplicates with their stored rows; a difference or a missing row parks the projector. A
     * duplicate with no stored header row is accepted only when the retention mark, read after the rows, covers it.
     */
    private void compareQueuedDuplicates() {
        if (duplicates.isEmpty()) {
            return;
        }
        final List<Long> seqs = new ArrayList<>(duplicates.size());
        for (final MoneyJournalRecord r : duplicates) {
            seqs.add(r.journalSeq());
        }
        final Map<Long, MoneyJournalRecord> stored;
        PostgresMoneyJournalStore.PrunedMark mark = null;
        try {
            stored = store.load(seqs);
            for (final MoneyJournalRecord r : duplicates) {
                if (!stored.containsKey(r.journalSeq())) {
                    mark = readPrunedMark(r); // after the load: see PostgresMoneyJournalStore#prunedMark
                    if (mark == null) {
                        return; // parked
                    }
                    break;
                }
            }
        } catch (SQLException e) {
            state.pgFlushFailures++;
            state.connectedToPg = false;
            throw new IllegalStateException("postgres compare failed (the epoch re-reads and compares again): "
                    + e.getMessage(), e);
        }
        for (final MoneyJournalRecord r : duplicates) {
            final MoneyJournalRecord s = stored.get(r.journalSeq());
            if (s == null && acceptPruned && mark != null && mark.covers(r)) {
                state.prunedRecords++;
                continue;
            }
            if (!r.equals(s)) {
                duplicates.clear();
                state.payloadConflicts++;
                park("PAYLOAD_CONFLICT", "*** HALTED - PAYLOAD_CONFLICT *** money journal record journalSeq="
                        + r.journalSeq() + " from source " + currentSource + " differs from what Postgres holds: "
                        + "journal=" + r + " stored=" + (s == null ? "none (no row below the high-water, and not one "
                        + "retention deleted: retention mark " + mark + ")" : s)
                        + " | a changed duplicate is a determinism fault: nothing is projected past it. Compare the "
                        + "members' money journals at this journalSeq, then restart the projector.");
                return;
            }
        }
        duplicates.clear();
    }

    /**
     * The retention mark for a duplicate with no stored row. A mark this login may not read is not a failure that a
     * retry heals: it parks the projector (null), instead of an epoch that fails every second.
     */
    private PostgresMoneyJournalStore.PrunedMark readPrunedMark(final MoneyJournalRecord missing) throws SQLException {
        try {
            return store.prunedMark();
        } catch (SQLException e) {
            if (!PostgresMoneyJournalStore.INSUFFICIENT_PRIVILEGE.equals(e.getSQLState())) {
                throw e;
            }
            duplicates.clear();
            park("RETENTION_MARK", "*** HALTED - RETENTION MARK UNREADABLE *** money journal record journalSeq="
                    + missing.journalSeq() + " from source " + currentSource + " has no stored row, and this login may "
                    + "not read money_journal_pruned (" + e.getMessage() + "), so it cannot be told whether retention "
                    + "deleted it. The retention migration must be applied as the projector's own login (the owner of "
                    + "money_journal); fix the ownership or grant SELECT on money_journal_pruned, then restart the "
                    + "projector.");
            return null;
        }
    }

    private void park(final String reason, final String text) {
        state.haltReason = reason;
        state.halted = true;
        pending.clear(); // uncommitted records are re-read after the restart; nothing past the fault is committed
        System.err.println("[PROJECTOR] " + text + " " + state.render());
    }

    /**
     * Compare the queued duplicates, then commit the pending batch. On success advance the in-memory
     * progress. On failure count it, drop the PG-connected flag and THROW: the batch rolled back, the
     * epoch tears down and retries from the unchanged Postgres high-water (never skip). Preserves
     * journalSeq order: the batch was built in order and committed atomically, so the high-water only
     * ever advances over committed records. A conflict found by the comparison parks the projector and
     * commits nothing.
     */
    private void flush() {
        compareQueuedDuplicates();
        if (state.halted || pending.isEmpty()) {
            return;
        }
        try {
            store.project(pending);
            state.connectedToPg = true;
            state.rowsProjected += pending.size();
            state.lastJournalSeq = pending.get(pending.size() - 1).journalSeq();
            pending.clear();
            if (state.lastJournalSeq > gapHighWater) {
                clearGap(); // another source filled the gap
            }
        } catch (SQLException e) {
            state.pgFlushFailures++;
            state.connectedToPg = false;
            throw new IllegalStateException("postgres flush failed (batch kept for retry): "
                    + e.getMessage(), e);
        }
    }

    private void maybeLogStatus() {
        final long nowMs = System.currentTimeMillis();
        if (nowMs - lastStatusLogMs >= STATUS_LOG_INTERVAL_MS) {
            lastStatusLogMs = nowMs;
            System.out.println("[PROJECTOR] " + state.render());
        }
    }

    private static void sleep(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
