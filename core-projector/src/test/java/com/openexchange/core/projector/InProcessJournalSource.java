// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import io.aeron.ExclusivePublication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * In-process stand-in for one core member's money journal: an archiving driver of its own with its control endpoint
 * on UDP {@code localhost:<port>}, recording {@code aeron:ipc} stream 4101 as the core does. Each
 * {@link #newRecording()} starts a new recording (the previous one stops), as a member restart does; {@link #publish}
 * appends raw record bytes and returns once the archive has recorded them.
 */
final class InProcessJournalSource implements AutoCloseable {

    private static final String CHANNEL = "aeron:ipc?term-length=64k";

    final ProjectorProcesses procs;
    final int controlPort;
    private final ArchivingMediaDriver driver;
    private final AeronArchive archive;
    private ExclusivePublication pub;
    private int counterId;

    InProcessJournalSource(final ProjectorProcesses procs) throws Exception {
        this.procs = procs;
        controlPort = ProjectorProcesses.freePort();
        final String aeronDir = procs.shmName("projector-inproc-src-").toString();
        final Path archiveDir = procs.dir("projector-inproc-archive-");
        final String control = "aeron:udp?endpoint=localhost:" + controlPort;
        driver = ArchivingMediaDriver.launch(
                new MediaDriver.Context().aeronDirectoryName(aeronDir).threadingMode(ThreadingMode.SHARED)
                        .termBufferSparseFile(true).dirDeleteOnStart(true).dirDeleteOnShutdown(true),
                new Archive.Context().aeronDirectoryName(aeronDir).archiveDir(archiveDir.toFile())
                        .deleteArchiveOnStart(true).threadingMode(ArchiveThreadingMode.SHARED).controlChannel(control)
                        .replicationChannel("aeron:udp?endpoint=localhost:0").recordingEventsEnabled(false)
                        .segmentFileLength(1024 * 1024));
        archive = AeronArchive.connect(new AeronArchive.Context().aeronDirectoryName(aeronDir)
                .controlRequestChannel(control).controlResponseChannel("aeron:udp?endpoint=localhost:0"));
        archive.startRecording(CHANNEL, ProjectorConfig.MONEY_JOURNAL_STREAM_ID, SourceLocation.LOCAL);
    }

    String endpoint() {
        return "localhost:" + controlPort;
    }

    /** Stop the current recording (if any) and start the next one. */
    InProcessJournalSource newRecording() throws InterruptedException {
        CloseHelper.quietClose(pub);
        pub = archive.context().aeron().addExclusivePublication(CHANNEL, ProjectorConfig.MONEY_JOURNAL_STREAM_ID);
        final CountersReader counters = archive.context().aeron().countersReader();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while ((counterId = RecordingPos.findCounterIdBySession(counters, pub.sessionId(), archive.archiveId()))
                == CountersReader.NULL_COUNTER_ID) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("the archive did not start recording the new publication");
            }
            Thread.sleep(5);
        }
        return this;
    }

    /** Append each record as one message and wait until the archive has recorded them. */
    InProcessJournalSource publish(final byte[]... records) throws InterruptedException {
        for (final byte[] r : records) {
            final UnsafeBuffer b = new UnsafeBuffer(r);
            while (pub.offer(b, 0, r.length) < 0) {
                Thread.sleep(1);
            }
        }
        final CountersReader counters = archive.context().aeron().countersReader();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (counters.getCounterValue(counterId) < pub.position()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("the archive did not record the published records");
            }
            Thread.sleep(2);
        }
        return this;
    }

    /** Replay sessions this archive runs now. */
    int replays() {
        return JournalSourceMain.replays(archive.context().aeron().countersReader());
    }

    @Override
    public void close() {
        CloseHelper.quietClose(pub);
        CloseHelper.quietClose(archive);
        CloseHelper.quietClose(driver);
    }
}
