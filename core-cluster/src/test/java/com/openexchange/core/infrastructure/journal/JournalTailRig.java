// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.journal;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * An archive in a temp directory for the journal tail tests, with what the archive raises into its error handler
 * captured in {@link #archiveErrors}: a replay the archive refuses (a start inside a frame) is raised there on the
 * archive's own thread, besides being answered to the caller. Recordings have 64 KiB terms, so a few hundred records
 * cross a term end. Segments are one term long, so a purge of whole segments moves a recording's start by terms.
 * Public for the restore's tests in the persistence package.
 */
public final class JournalTailRig implements AutoCloseable {

    public static final int TERM = 64 * 1024;
    public static final String CHANNEL = "aeron:ipc?term-length=64k";
    public static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(20);

    /** Everything the archive raised into its error handler since the last {@code clear()}. */
    public final List<String> archiveErrors = new CopyOnWriteArrayList<>();

    private final File dir;
    private final ArchivingMediaDriver driver;
    private final Aeron aeron;
    private volatile AeronArchive archive;

    public JournalTailRig() {
        try {
            dir = Files.createTempDirectory("core-journal-tail-").toFile();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        final String aeronDir = new File(dir, "driver").getAbsolutePath();
        driver = ArchivingMediaDriver.launch(
                new MediaDriver.Context().aeronDirectoryName(aeronDir).threadingMode(ThreadingMode.SHARED)
                        .dirDeleteOnStart(true).dirDeleteOnShutdown(true),
                new Archive.Context().aeronDirectoryName(aeronDir).archiveDir(new File(dir, "archive"))
                        .controlChannelEnabled(false).segmentFileLength(TERM)
                        .archiveClientContext(new AeronArchive.Context().controlResponseChannel("aeron:ipc"))
                        .replicationChannel("aeron:udp?endpoint=localhost:0").recordingEventsEnabled(false)
                        .threadingMode(ArchiveThreadingMode.SHARED).deleteArchiveOnStart(true)
                        .errorHandler(t -> archiveErrors.add(String.valueOf(t))));
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
        archive = connectArchive();
    }

    private AeronArchive connectArchive() {
        return AeronArchive.connect(new AeronArchive.Context().aeron(aeron).controlRequestChannel("aeron:ipc")
                .controlResponseChannel("aeron:ipc"));
    }

    public Aeron aeron() {
        return aeron;
    }

    public AeronArchive archive() {
        return archive;
    }

    /** Closes the archive client and connects a new one (a reader's reconnect); the writers use the new one. */
    public AeronArchive reconnectArchive() {
        final AeronArchive old = archive;
        archive = connectArchive();
        CloseHelper.quietClose(old);
        return archive;
    }

    /** A new recording on {@code streamId}: one process run's journal recording. */
    public Writer writer(final int streamId) {
        return new Writer(streamId, CHANNEL);
    }

    /** A new recording on {@code streamId} over {@code channel} (its own term length). */
    public Writer writer(final int streamId, final String channel) {
        return new Writer(streamId, channel);
    }

    static void await(final long deadline, final String what) {
        if (System.nanoTime() > deadline) {
            throw new IllegalStateException("timed out waiting for " + what);
        }
        Thread.onSpinWait();
        Thread.yield();
    }

    @Override
    public void close() {
        CloseHelper.quietCloseAll(archive, aeron, driver);
        IoUtil.delete(dir, true);
    }

    /** One recorded publication. Offers come from one thread; the archive calls from the test's thread. */
    public final class Writer {
        final ExclusivePublication publication;
        public final long recordingId;
        private boolean stopped;

        private Writer(final int streamId, final String channel) {
            publication = archive.addRecordedExclusivePublication(channel, streamId);
            final CountersReader counters = aeron.countersReader();
            final long deadline = System.nanoTime() + TIMEOUT_NS;
            int counterId;
            while ((counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId(),
                    archive.archiveId())) == CountersReader.NULL_COUNTER_ID) {
                await(deadline, "the recording counter of session " + publication.sessionId());
            }
            recordingId = RecordingPos.getRecordingId(counters, counterId);
            while (!publication.isConnected()) {
                await(deadline, "the recorded publication to connect");
            }
        }

        /** Offers one message until it is taken. */
        public Writer append(final DirectBuffer message) {
            final long deadline = System.nanoTime() + TIMEOUT_NS;
            while (publication.offer(message, 0, message.capacity()) < 0) {
                await(deadline, "an offer");
            }
            return this;
        }

        /** One offer, no retry: {@code ADMIN_ACTION} means the term was padded to its end and rotated. */
        long offerOnce(final DirectBuffer message) {
            return publication.offer(message, 0, message.capacity());
        }

        public long position() {
            return publication.position();
        }

        /** Waits until the archive recorded everything offered. */
        public Writer awaitRecorded() {
            final long target = publication.position();
            final long deadline = System.nanoTime() + TIMEOUT_NS;
            while (archive.getRecordingPosition(recordingId) < target) {
                await(deadline, "recording " + recordingId + " to reach " + target);
            }
            return this;
        }

        /** Waits until everything offered is recorded, stops the recording and returns its stop position. */
        public long stop() {
            if (stopped) {
                return archive.getStopPosition(recordingId);
            }
            awaitRecorded();
            archive.stopRecording(publication);
            final long deadline = System.nanoTime() + TIMEOUT_NS;
            long stop;
            while ((stop = archive.getStopPosition(recordingId)) == AeronArchive.NULL_POSITION) {
                await(deadline, "the stop position of recording " + recordingId);
            }
            publication.close();
            stopped = true;
            return stop;
        }
    }
}
