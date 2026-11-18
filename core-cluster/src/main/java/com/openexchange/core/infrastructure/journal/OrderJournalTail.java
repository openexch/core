// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.journal;

import com.openexchange.core.infrastructure.InfrastructureConstants;
import com.openexchange.cluster.shadow.ShadowReplay;
import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.logbuffer.FrameDescriptor;
import org.agrona.BitUtil;
import org.agrona.concurrent.NanoClock;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The largest {@code journalSeq} THIS member's archive holds in its order-journal recordings of one genesis: the
 * {@code localOrderJournalDurableSeq} of the restore-side rule. The money journal tail reader's algorithm
 * ({@link MoneyJournalTail}: every recording on the stream, the last message of each through a bounded,
 * session-scoped replay, the maximum over them) with two changes: the frame bound is {@value #MAX_FRAME_LENGTH}, the
 * one aligned frame of this record, and a recording whose last message carries an epoch other than the expected
 * genesisId is ignored (counted as foreign), so a previous genesis left in the same archive can never make a restore
 * look safe. Never writes, never purges.
 */
public final class OrderJournalTail {

    /** Byte offset of {@code journalSeq} inside every journal message (SBE header 8 + the first field). */
    public static final int JOURNAL_SEQ_OFFSET = 8;
    /** Byte offset of {@code epoch} (the second field). */
    public static final int EPOCH_OFFSET = 16;
    /**
     * One journal message: header 8 + block 208 = 216 bytes, plus the 32-byte data header, aligned to 32: 256. A format
     * constant tied to the codec; a larger message falls through to the whole-last-term read (correct, slower).
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

    /** Not readable now; the caller treats every SKIP as not skip-safe. */
    public record Unknown(String reason) implements Result {
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

    public OrderJournalTail(final Aeron aeron, final AeronArchive archive, final NanoClock clock, final long expectedEpoch) {
        this(aeron, archive, clock, "aeron:ipc", InfrastructureConstants.ORDER_JOURNAL_STREAM_ID, expectedEpoch);
    }

    OrderJournalTail(final Aeron aeron, final AeronArchive archive, final NanoClock clock,
                     final String channelFragment, final int streamId, final long expectedEpoch) {
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
                        + "): " + readFailed);
            }
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
     * {journalSeq, epoch} of the last message of {@code [r.start, stop)}; journalSeq {@code Long.MIN_VALUE} when there
     * is none.
     */
    private long[] lastSeq(final Rec r, final long stop, final long deadlineNs) {
        final ShadowReplay replay = new ShadowReplay(aeron, archive, clock);
        // every record is one 256-byte frame, so the widest window is tried first: one replay per recording normally
        for (int w = MAX_FRAME_LENGTH; w >= FrameDescriptor.FRAME_ALIGNMENT; w -= FrameDescriptor.FRAME_ALIGNMENT) {
            final long from = stop - w;
            if (from < r.start()) {
                continue;
            }
            final long[] seq = {Long.MIN_VALUE, 0L};
            final int[] messages = {0};
            final int[] frameLength = {0};
            try {
                replay.readRange(r.id(), from, w, (buffer, offset, length, header) -> {
                    messages[0]++;
                    frameLength[0] = header.frameLength();
                    if (length >= EPOCH_OFFSET + Long.BYTES) {
                        seq[0] = buffer.getLong(offset + JOURNAL_SEQ_OFFSET);
                        seq[1] = buffer.getLong(offset + EPOCH_OFFSET);
                    }
                }, deadlineNs);
            } catch (final ArchiveException notAFrame) {
                if (notAFrame.errorCode() == ArchiveException.INVALID_POSITION) {
                    continue; // not a frame header: a longer window
                }
                throw notAFrame;
            }
            if (messages[0] == 1 && BitUtil.align(frameLength[0], FrameDescriptor.FRAME_ALIGNMENT) == w
                    && seq[0] != Long.MIN_VALUE) {
                return seq;
            }
        }
        // the end is not one message frame (padding at a term end): read the term holding the end, frame aligned
        final long termStart = Math.max(r.start(), (stop - 1) & ~((long)r.termLength() - 1));
        final long[] seq = {Long.MIN_VALUE, 0L};
        replay.readRange(r.id(), termStart, stop - termStart, (buffer, offset, length, header) -> {
            if (length >= EPOCH_OFFSET + Long.BYTES) {
                seq[0] = buffer.getLong(offset + JOURNAL_SEQ_OFFSET);
                seq[1] = buffer.getLong(offset + EPOCH_OFFSET);
            }
        }, deadlineNs);
        return seq;
    }
}
