// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.journal;

import com.openexchange.core.infrastructure.InfrastructureConstants;
import com.openexchange.cluster.shadow.ShadowReplay;
import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import com.openexchange.cluster.journal.RecordingTail;
import org.agrona.concurrent.NanoClock;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The largest {@code journalSeq} THIS member's archive holds in its order-journal recordings of one genesis: the
 * {@code localOrderJournalDurableSeq} of the restore-side rule. The money journal tail reader's algorithm
 * ({@link MoneyJournalTail}: every recording on the stream, the last message of each through the kit's
 * {@link RecordingTail}, which never asks the archive for a replay that starts inside a frame, the maximum over them)
 * with two changes: a message counts when it is long enough to carry the epoch, and a recording whose last message
 * carries an epoch other than the expected genesisId is ignored (counted as foreign), so a previous genesis left in
 * the same archive can never make a restore look safe. Never writes, never purges. Every reader remembers nothing
 * (its callers read once, at a restore), so each read replays up to one term of every recording, each replay held to
 * the process's {@link TailReadBudget}.
 */
public final class OrderJournalTail {

    /** Byte offset of {@code journalSeq} inside every journal message (SBE header 8 + the first field). */
    public static final int JOURNAL_SEQ_OFFSET = 8;
    /** Byte offset of {@code epoch} (the second field). */
    public static final int EPOCH_OFFSET = 16;
    /**
     * One journal message: header 8 + block 208 = 216 bytes, plus the 32-byte data header, aligned to 32: 256. A format
     * constant tied to the codec (the retention profile reads it); the tail read does not depend on it.
     */
    public static final int MAX_FRAME_LENGTH = 256;

    /** What a read found. */
    public sealed interface Result permits Known, NoJournal, Unknown {
    }

    /** The largest journalSeq over {@code recordings} recordings ({@code active} of them still recording). */
    public record Known(long seq, int recordings, int active, int foreign) implements Result {
    }

    /** No journal recording of the expected genesis with any bytes. */
    public record NoJournal(int recordings, int foreign) implements Result {
    }

    /**
     * Not readable now; the caller treats every SKIP as not skip-safe. {@code deadline}: the read ran out of time (its
     * own deadline, or waiting for the tail read budget), so a later read may get further.
     */
    public record Unknown(String reason, boolean deadline) implements Result {
        public Unknown(final String reason) {
            this(reason, false);
        }
    }

    private record Rec(long id, long start, long stop, int termLength) {
    }

    private final Aeron aeron;
    private final AeronArchive archive;
    private final NanoClock clock;
    private final String channelFragment;
    private final int streamId;
    private final long expectedEpoch;
    private boolean epochFilterOff;
    /** {journalSeq, epoch} of every message long enough to carry both; a shorter message does not count. */
    private final RecordingTail<long[]> tail = new RecordingTail<>((buffer, offset, length, header) ->
            length >= EPOCH_OFFSET + Long.BYTES
                    ? new long[] {buffer.getLong(offset + JOURNAL_SEQ_OFFSET), buffer.getLong(offset + EPOCH_OFFSET)}
                    : null);

    private final TailReadBudget budget;
    /** Recordings the last {@link #read} answered (their last message read), whether the read ended Known or not. */
    private int answered;

    /** A reader not held to a replay budget: for tests and tools. A member's readers pass its {@link TailReadBudget}. */
    public OrderJournalTail(final Aeron aeron, final AeronArchive archive, final NanoClock clock, final long expectedEpoch) {
        this(aeron, archive, clock, expectedEpoch, TailReadBudget.unbounded());
    }

    /** @param budget the bytes this process's tail readers may replay per window ({@link MoneyJournalTail}'s rule) */
    public OrderJournalTail(final Aeron aeron, final AeronArchive archive, final NanoClock clock, final long expectedEpoch,
                            final TailReadBudget budget) {
        this(aeron, archive, clock, "aeron:ipc", InfrastructureConstants.ORDER_JOURNAL_STREAM_ID, expectedEpoch, budget);
    }

    OrderJournalTail(final Aeron aeron, final AeronArchive archive, final NanoClock clock,
                     final String channelFragment, final int streamId, final long expectedEpoch) {
        this(aeron, archive, clock, channelFragment, streamId, expectedEpoch, TailReadBudget.unbounded());
    }

    OrderJournalTail(final Aeron aeron, final AeronArchive archive, final NanoClock clock,
                     final String channelFragment, final int streamId, final long expectedEpoch,
                     final TailReadBudget budget) {
        this.budget = Objects.requireNonNull(budget, "budget");
        this.aeron = Objects.requireNonNull(aeron, "aeron");
        this.archive = Objects.requireNonNull(archive, "archive");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.channelFragment = Objects.requireNonNull(channelFragment, "channelFragment");
        this.streamId = streamId;
        this.expectedEpoch = expectedEpoch;
    }

    /** Test seam only (the epoch filter's negative control): every recording counts whatever its epoch. */
    public OrderJournalTail epochFilterOff() {
        epochFilterOff = true;
        return this;
    }

    /** @param timeoutNs the whole read's budget ({@code SHADOW_LOAD_TIMEOUT_MS}) */
    public Result read(final long timeoutNs) {
        final long deadlineNs = clock.nanoTime() + timeoutNs;
        answered = 0;
        final List<Rec> recs = new ArrayList<>();
        try {
            long from = 0;
            while (true) {
                final int before = recs.size();
                final int n = archive.listRecordingsForUri(from, 100, channelFragment, streamId,
                        (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition,
                         stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId,
                         stream, strippedChannel, originalChannel, sourceIdentity) -> recs.add(
                                 new Rec(recordingId, startPosition, stopPosition, termBufferLength)));
                if (n == 0 || recs.size() == before) {
                    break;
                }
                from = recs.get(recs.size() - 1).id() + 1;
            }
        } catch (final RuntimeException listFailed) {
            return new Unknown("listing journal recordings failed: " + listFailed);
        }
        long max = -1;
        int read = 0;
        int active = 0;
        int foreign = 0;
        for (final Rec r : recs) {
            long stop = r.stop();
            if (stop == AeronArchive.NULL_POSITION) {
                try {
                    stop = archive.getRecordingPosition(r.id());
                } catch (final RuntimeException positionFailed) {
                    return new Unknown("recording position of active journal recording " + r.id() + ": "
                            + positionFailed);
                }
                if (stop == AeronArchive.NULL_POSITION) {
                    // stopped between the listing and now: read its descriptor stop position again
                    try {
                        stop = archive.getStopPosition(r.id());
                    } catch (final RuntimeException stopFailed) {
                        return new Unknown("stop position of journal recording " + r.id() + ": " + stopFailed);
                    }
                } else {
                    active++;
                }
            }
            if (stop <= r.start()) {
                continue;
            }
            final long[] last;
            try {
                last = lastSeq(r, stop, deadlineNs);
            } catch (final RuntimeException readFailed) {
                return new Unknown("last message of journal recording " + r.id() + " [" + r.start() + "," + stop
                        + "): " + readFailed, TailReadBudget.isDeadline(readFailed));
            }
            answered++;
            final long seq = last[0];
            if (seq == Long.MIN_VALUE) {
                return new Unknown("no journal message in the last term of recording " + r.id() + " [" + r.start()
                        + "," + stop + ")");
            }
            if (!epochFilterOff && last[1] != expectedEpoch) {
                foreign++;
                continue;
            }
            read++;
            max = Math.max(max, seq);
        }
        if (read == 0) {
            return new NoJournal(recs.size(), foreign);
        }
        return new Known(max, read, active, foreign);
    }

    /**
     * {journalSeq, epoch} of the last message of {@code [max(r.start, base of the term holding stop - 1), stop)};
     * journalSeq {@code Long.MIN_VALUE} when that range holds none.
     */
    private long[] lastSeq(final Rec r, final long stop, final long deadlineNs) {
        final long want = TailReadBudget.estimate(r.start(), stop, r.termLength(), tail.resumePosition(r.id()));
        budget.acquireOrThrow(want, deadlineNs);
        if (want > 0 && clock.nanoTime() - deadlineNs >= 0) {
            // the room came at the deadline: no replay is started past it (an end that did not move replays nothing)
            budget.settle(want, 0);
            throw new TailReadBudget.DeadlineException("deadline reached as the tail read budget made room ("
                    + budget + ")");
        }
        final long before = tail.replayBytes();
        try {
            final long[] last = tail.last(new ShadowReplay(aeron, archive, clock), r.id(), r.start(), stop,
                    r.termLength(), deadlineNs);
            return last == null ? new long[] {Long.MIN_VALUE, 0L} : last;
        } finally {
            budget.settle(want, tail.replayBytes() - before);
        }
    }

    /** The bytes this reader asked the archive to replay, over all its reads. */
    public long replayBytes() {
        return tail.replayBytes();
    }

    /**
     * The recordings the last {@link #read} got an answer for before it ended: a read that ran out of time answered
     * the ones before the one it stopped in.
     */
    public int answered() {
        return answered;
    }
}
