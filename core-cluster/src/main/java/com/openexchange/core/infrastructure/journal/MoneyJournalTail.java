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
 * The largest {@code journalSeq} THIS node's archive holds in its money-journal recordings, the
 * {@code localJournalDurableSeq} of the skip-safe rule (8.1). Bounded and read only: it lists every recording on
 * {@link InfrastructureConstants#MONEY_JOURNAL_STREAM_ID} over IPC, reads the LAST message of each with a positive
 * length through a session-scoped replay (the replay discipline, join and end positions checked) and answers the
 * {@code max} over them (review N3: a process that crashed during an earlier replay may have left its newest recording
 * short of an older recording's high-water, so the newest alone would under-read and refuse a legal SKIP). A
 * recording still ACTIVE (no stop position yet: a writer not yet stopped, or this process's own writer) is read to
 * its current recorded position and counted. Never writes, never purges.
 *
 * <p>The last message of a recording is read by the kit's {@link RecordingTail}: a replay that starts at the
 * recording's start or at the base of the term that holds its end (both the start of a frame), or just after the last
 * message this reader's {@code RecordingTail} already delivered from that recording. The archive is never asked for a
 * start inside a frame, which it would refuse and raise into the node's error log. A journal message is one small SBE
 * message (header + one block, at most 128 bytes, {@code MoneyJournal}'s scratch buffer) in one frame of at most
 * {@value #MAX_FRAME_LENGTH} aligned bytes, with {@code journalSeq} at {@link #JOURNAL_SEQ_OFFSET}
 * ({@code MoneyJournalWriterAgent}); a message too short to carry one does not count, and the tail is then the last
 * one before it in that term. A last term with no message is {@code Unknown}: SKIP is then not taken (the safe
 * direction).</p>
 *
 * <p>Cost: a reader with a new {@code RecordingTail} (the default constructor: a restore, read once) replays up to
 * one term of every recording. A caller that reads periodically keeps one {@link #newTail()} and passes it to every
 * reader it builds: a later read then replays only what was recorded since, and nothing for a recording whose end
 * did not move. What the {@code RecordingTail} remembers changes how many bytes are replayed, never the answer. Each
 * replay first takes its bytes from the process's {@link TailReadBudget} and waits for room when the window is used
 * up, so many first looks cannot fill the media driver's directory at once; a read whose deadline passes while it
 * waits answers {@link Unknown}, and what it already read stays in the {@code RecordingTail} for the next read.</p>
 *
 * <p>Answers: {@link Known} (seq, recordings read, active ones), {@link NoJournal} (no recording, or none with bytes:
 * skip-safe only when the shadow says journaling is off), {@link Unknown} (an archive or replay failure, or a deadline:
 * SKIP is not taken, counted by the caller). One owner thread per call, and one owner thread for a shared
 * {@code RecordingTail}; the replays are serial.</p>
 */
public final class MoneyJournalTail {

    /** Byte offset of {@code journalSeq} inside every journal message (SBE header 8 + the first field). */
    public static final int JOURNAL_SEQ_OFFSET = 8;
    /**
     * One journal message: header + JournalSettle (101 in version 2) = 109 bytes, plus the 32-byte data header,
     * aligned. A format constant coupled to the largest journal message (the retention profile reads it); the tail
     * read does not depend on it. Raise it with the codec.
     */
    public static final int MAX_FRAME_LENGTH = 160;

    /** What a read found. */
    public sealed interface Result permits Known, NoJournal, Unknown {
    }

    /** The largest journalSeq over {@code recordings} recordings ({@code active} of them still recording). */
    public record Known(long seq, int recordings, int active) implements Result {
    }

    /** No journal recording with any bytes. */
    public record NoJournal(int recordings) implements Result {
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
    private final RecordingTail<Long> tail;
    private final TailReadBudget budget;
    /** Recordings the last {@link #read} answered (their last message read), whether the read ended Known or not. */
    private int answered;

    /**
     * A reader that remembers nothing (every read looks at each recording for the first time) and is not held to a
     * replay budget: for tests and tools. A member's readers pass the process's {@link TailReadBudget}.
     */
    public MoneyJournalTail(final Aeron aeron, final AeronArchive archive, final NanoClock clock) {
        this(aeron, archive, clock, newTail(), TailReadBudget.unbounded());
    }

    /**
     * @param tail   what earlier reads of this archive's journal recordings delivered ({@link #newTail()}), kept by a
     *               caller that reads more than once; owned by one thread
     * @param budget the bytes this process's tail readers may replay per window; a read waits for room before each
     *               replay, within its own deadline
     */
    public MoneyJournalTail(final Aeron aeron, final AeronArchive archive, final NanoClock clock,
                            final RecordingTail<Long> tail, final TailReadBudget budget) {
        this(aeron, archive, clock, "aeron:ipc", InfrastructureConstants.MONEY_JOURNAL_STREAM_ID, tail, budget);
    }

    /** The {@code journalSeq} of every message long enough to carry one; a shorter message does not count. */
    public static RecordingTail<Long> newTail() {
        return new RecordingTail<>((buffer, offset, length, header) ->
                length >= JOURNAL_SEQ_OFFSET + Long.BYTES ? buffer.getLong(offset + JOURNAL_SEQ_OFFSET) : null);
    }

    MoneyJournalTail(final Aeron aeron, final AeronArchive archive, final NanoClock clock,
                     final String channelFragment, final int streamId, final RecordingTail<Long> tail) {
        this(aeron, archive, clock, channelFragment, streamId, tail, TailReadBudget.unbounded());
    }

    MoneyJournalTail(final Aeron aeron, final AeronArchive archive, final NanoClock clock,
                     final String channelFragment, final int streamId, final RecordingTail<Long> tail,
                     final TailReadBudget budget) {
        this.tail = Objects.requireNonNull(tail, "tail");
        this.budget = Objects.requireNonNull(budget, "budget");
        this.aeron = Objects.requireNonNull(aeron, "aeron");
        this.archive = Objects.requireNonNull(archive, "archive");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.channelFragment = Objects.requireNonNull(channelFragment, "channelFragment");
        this.streamId = streamId;
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
        // recordings the archive no longer lists (purged whole) are forgotten; recording ids are never reused
        tail.retain(id -> {
            for (final Rec r : recs) {
                if (r.id() == id) {
                    return true;
                }
            }
            return false;
        });
        long max = -1;
        int read = 0;
        int active = 0;
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
            final long seq;
            try {
                seq = lastSeq(r, stop, deadlineNs);
            } catch (final RuntimeException readFailed) {
                return new Unknown("last message of journal recording " + r.id() + " [" + r.start() + "," + stop
                        + "): " + readFailed, TailReadBudget.isDeadline(readFailed));
            }
            answered++;
            if (seq == Long.MIN_VALUE) {
                return new Unknown("no journal message in the last term of recording " + r.id() + " [" + r.start()
                        + "," + stop + ")");
            }
            read++;
            max = Math.max(max, seq);
        }
        if (read == 0) {
            return new NoJournal(recs.size());
        }
        return new Known(max, read, active);
    }

    /**
     * The journalSeq of the last message of {@code [max(r.start, base of the term holding stop - 1), stop)}, or
     * {@code Long.MIN_VALUE} when that range holds none.
     */
    private long lastSeq(final Rec r, final long stop, final long deadlineNs) {
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
            final Long seq = tail.last(new ShadowReplay(aeron, archive, clock), r.id(), r.start(), stop,
                    r.termLength(), deadlineNs);
            return seq == null ? Long.MIN_VALUE : seq;
        } finally {
            budget.settle(want, tail.replayBytes() - before);
        }
    }

    /**
     * The recordings the last {@link #read} got an answer for before it ended: a read that ran out of time answered
     * the ones before the one it stopped in.
     */
    public int answered() {
        return answered;
    }
}
