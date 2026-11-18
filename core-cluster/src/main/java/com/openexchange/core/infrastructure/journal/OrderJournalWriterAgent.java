// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.journal;

import com.openexchange.core.infrastructure.Logger;
import io.aeron.ChannelUri;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;
import org.agrona.SystemUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.ControlledMessageHandler;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

/**
 * Drains the {@link OrderJournal} ring into a RECORDED exclusive publication on the member's own archive (stream
 * {@code ORDER_JOURNAL_STREAM_ID}). The money journal writer's shape ({@link MoneyJournalWriterAgent}: own thread, lazy
 * connect, so a member that never emits an order event creates no recording) with three differences:
 *
 * <ul>
 *   <li>"recorded" is what the archive holds: the journal's recorded sequence advances when the archive's recording
 *   position of this publication passes the record, not when the offer returns.</li>
 *   <li>A record leaves the ring only once the publication took it ({@code controlledRead}): a back-pressured offer
 *   leaves it there for the next duty cycle, and a publication that is closed or at its maximum position is dropped
 *   and reconnected, the record still in the ring. Nothing is lost on the way.</li>
 *   <li>A graceful close drains the ring and waits for the archive, bounded by
 *   {@code CORE_ORDER_JOURNAL_CLOSE_DRAIN_MS}.</li>
 * </ul>
 */
public final class OrderJournalWriterAgent implements Agent {

    private static final Logger log = Logger.getLogger(OrderJournalWriterAgent.class);

    /** Byte offset of journalSeq inside every journal message: SBE header (8) + the first field. */
    private static final int JOURNAL_SEQ_OFFSET = 8;
    private static final int DRAIN_LIMIT = 64;
    /** One record's aligned frame: the in-flight bound of the pending table is the term length over this. */
    private static final int FRAME_BYTES = 256;
    private static final long CONNECT_RETRY_BACKOFF_MS = 1_000;
    private static final long STATS_INTERVAL_MS = 10_000;

    private final OrderJournal journal;
    private final OneToOneRingBuffer ring;
    private final Supplier<AeronArchive.Context> archiveClientContext;
    private final String channel;
    private final int streamId;
    private final long closeDrainNs;
    private final ControlledMessageHandler handler = this::onRingMessage;

    // offered, not yet recorded: (publication position after the record, its journalSeq), oldest first
    private final long[] pendingPosition;
    private final long[] pendingSeq;
    private final int pendingMask;
    private long pendingHead;
    private long pendingTail;

    private AeronArchive archive;
    private ExclusivePublication publication;
    private CountersReader counters;
    private long archiveId;
    private int recordingCounterId = CountersReader.NULL_COUNTER_ID;
    private String lostReason;
    private long lastConnectAttemptMs;
    private long lastStatsMs;
    private long lastStatsSeq = -1;
    private volatile long journalPosition;

    /** The production constructor: channel, stream and close drain from the settings. */
    public OrderJournalWriterAgent(final OrderJournal journal, final Supplier<AeronArchive.Context> archiveClientContext,
                                   final int streamId, final OrderJournalSettings settings) {
        this(journal, archiveClientContext, streamId, settings.channel(), settings.closeDrainMs());
    }

    /** A writer with the default close drain (tests that build the agent around their own ring). */
    public OrderJournalWriterAgent(final OrderJournal journal, final Supplier<AeronArchive.Context> archiveClientContext,
                                   final String channel, final int streamId, final long idleMaxNsUnused) {
        this(journal, archiveClientContext, streamId, channel, OrderJournalSettings.defaults().closeDrainMs());
    }

    private OrderJournalWriterAgent(final OrderJournal journal, final Supplier<AeronArchive.Context> archiveClientContext,
                                    final int streamId, final String channel, final long closeDrainMs) {
        this.journal = journal;
        this.ring = journal.ringBuffer();
        this.archiveClientContext = archiveClientContext;
        this.channel = channel;
        this.streamId = streamId;
        this.closeDrainNs = TimeUnit.MILLISECONDS.toNanos(closeDrainMs);
        final String term = ChannelUri.parse(channel).get("term-length");
        final long termLength = term == null ? 64 * 1024 * 1024 : SystemUtil.parseSize("term-length", term);
        // a full table only makes the writer wait for the archive, so it is capped (1 MiB of positions)
        final int capacity = BitUtil.findNextPositivePowerOfTwo((int) Math.min(1 << 16, termLength / FRAME_BYTES + DRAIN_LIMIT));
        this.pendingPosition = new long[capacity];
        this.pendingSeq = new long[capacity];
        this.pendingMask = capacity - 1;
    }

    @Override
    public int doWork() {
        journal.sampleAlarm(System.nanoTime());
        if (publication == null) {
            if (ring.size() == 0) {
                return 0;
            }
            return tryConnect() ? 1 : 0;
        }
        int work = advanceRecorded();
        work += ring.controlledRead(handler, DRAIN_LIMIT);
        if (lostReason != null) {
            dropPublication();
            return 1;
        }
        maybeLogStats();
        return work;
    }

    private boolean tryConnect() {
        final long nowMs = System.currentTimeMillis();
        if (nowMs - lastConnectAttemptMs < CONNECT_RETRY_BACKOFF_MS) {
            return false;
        }
        lastConnectAttemptMs = nowMs;
        try {
            archive = AeronArchive.connect(archiveClientContext.get());
            publication = archive.addRecordedExclusivePublication(channel, streamId);
            counters = archive.context().aeron().countersReader();
            archiveId = archive.archiveId();
            recordingCounterId = CountersReader.NULL_COUNTER_ID;
            log.info("Order journal writer connected: channel=%s stream=%d session=%d",
                    channel, streamId, publication.sessionId());
            return true;
        } catch (final Exception e) {
            journal.connectFailed();
            CloseHelper.quietClose(publication);
            CloseHelper.quietClose(archive);
            archive = null;
            publication = null;
            log.error("Order journal archive connect failed (attempt %d): %s", journal.connectFailures(), e.getMessage());
            return false;
        }
    }

    private ControlledMessageHandler.Action onRingMessage(final int msgTypeId, final MutableDirectBuffer buffer,
                                                          final int index, final int length) {
        if (pendingTail - pendingHead > pendingMask) {
            return ControlledMessageHandler.Action.ABORT; // the archive is that far behind: wait for it
        }
        final long result = publication.offer(buffer, index, length);
        if (result > 0) {
            final int at = (int) (pendingTail & pendingMask);
            pendingPosition[at] = result;
            pendingSeq[at] = buffer.getLong(index + JOURNAL_SEQ_OFFSET);
            pendingTail++;
            journalPosition = result;
            return ControlledMessageHandler.Action.COMMIT;
        }
        if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
            lostReason = result == Publication.CLOSED ? "closed" : "max position exceeded";
        } else {
            journal.sampleAlarm(System.nanoTime()); // back pressured or not connected: the record stays in the ring
        }
        return ControlledMessageHandler.Action.ABORT;
    }

    /** Moves the journal's recorded sequence to the newest record the archive's recording position has passed. */
    private int advanceRecorded() {
        if (pendingHead == pendingTail) {
            return 0;
        }
        if (recordingCounterId == CountersReader.NULL_COUNTER_ID) {
            recordingCounterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId(), archiveId);
            if (recordingCounterId == CountersReader.NULL_COUNTER_ID) {
                return 0;
            }
        }
        if (counters.getCounterState(recordingCounterId) != CountersReader.RECORD_ALLOCATED
                || counters.getCounterTypeId(recordingCounterId) != RecordingPos.RECORDING_POSITION_TYPE_ID) {
            recordingCounterId = CountersReader.NULL_COUNTER_ID;
            return 0;
        }
        final long recorded = counters.getCounterValue(recordingCounterId);
        long seq = Long.MIN_VALUE;
        while (pendingHead != pendingTail && pendingPosition[(int) (pendingHead & pendingMask)] <= recorded) {
            seq = pendingSeq[(int) (pendingHead & pendingMask)];
            pendingHead++;
        }
        if (seq == Long.MIN_VALUE) {
            return 0;
        }
        journal.recordedSeq(seq);
        return 1;
    }

    /**
     * The publication is unusable: close it and the archive client and connect again on a later duty cycle. The record
     * that found it closed is still in the ring; records offered but not yet recorded stay unconfirmed (the recorded
     * sequence does not pass them) and their fate is whatever the old recording holds.
     */
    private void dropPublication() {
        journal.publicationLost();
        log.error("ORDER JOURNAL PUBLICATION LOST (%s) at position %d, %d record(s) offered but not confirmed recorded;"
                        + " reconnecting", lostReason, journalPosition, pendingTail - pendingHead);
        lostReason = null;
        CloseHelper.quietClose(publication);
        CloseHelper.quietClose(archive);
        publication = null;
        archive = null;
        counters = null;
        recordingCounterId = CountersReader.NULL_COUNTER_ID;
        pendingHead = pendingTail;
    }

    private void maybeLogStats() {
        final long nowMs = System.currentTimeMillis();
        if (nowMs - lastStatsMs < STATS_INTERVAL_MS) {
            return;
        }
        lastStatsMs = nowMs;
        final long seq = journal.recordedSeq();
        if (seq != lastStatsSeq) {
            lastStatsSeq = seq;
            log.info("ORDER JOURNAL STATS: recordedSeq=%d bytes=%d backpressureEvents=%d",
                    seq, journalPosition, journal.backpressureEvents());
        }
    }

    /** Recorded publication position = journal bytes written by this process's recording. */
    public long journalPosition() {
        return journalPosition;
    }

    /**
     * A graceful stop: drain the ring and wait for the archive to record what was offered, bounded by
     * {@code CORE_ORDER_JOURNAL_CLOSE_DRAIN_MS}; what is left is named in one ERROR line. A kill skips this, which is
     * the exposure the restore-side rule reports.
     */
    @Override
    public void onClose() {
        final long deadline = System.nanoTime() + closeDrainNs;
        try {
            lastConnectAttemptMs = 0; // a close does not wait out the connect backoff
            while (System.nanoTime() - deadline < 0 && (ring.size() > 0 || pendingHead != pendingTail)) {
                if (publication == null) {
                    if (!tryConnect()) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                        lastConnectAttemptMs = 0;
                        continue;
                    }
                }
                final int work = advanceRecorded() + ring.controlledRead(handler, DRAIN_LIMIT);
                if (lostReason != null) {
                    dropPublication();
                    lastConnectAttemptMs = 0;
                } else if (work == 0) {
                    LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100));
                }
            }
            if (ring.size() > 0 || pendingHead != pendingTail) {
                log.error("ORDER JOURNAL CLOSED UNDRAINED after %d ms: %d ring bytes not offered, %d record(s) offered"
                                + " but not confirmed recorded (recorded=%d)", TimeUnit.NANOSECONDS.toMillis(closeDrainNs),
                        ring.size(), pendingTail - pendingHead, journal.recordedSeq());
            }
        } catch (final RuntimeException drainFailed) {
            log.error("Order journal close drain failed: %s", drainFailed.toString());
        } finally {
            CloseHelper.quietClose(publication);
            CloseHelper.quietClose(archive);
            publication = null;
            archive = null;
        }
    }

    @Override
    public String roleName() {
        return "order-journal-writer";
    }
}
