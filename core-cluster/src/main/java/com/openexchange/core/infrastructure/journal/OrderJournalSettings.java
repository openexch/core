// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.journal;

import com.openexchange.core.infrastructure.persistence.SnapshotModeSettings;
import org.agrona.BitUtil;
import org.agrona.SystemUtil;

import java.util.Map;
import java.util.Objects;

/**
 * Node-local settings of the order journal and of the member archive's replay cap. None decides replicated state: they
 * size the ring and the recorded publication, bound the readiness alarm and the stall log, and cap concurrent replays
 * on this member's archive. Read once at boot; a bad value refuses boot naming the setting. Every value has a default
 * and is printed at boot ({@link #toString}) and shown on {@code /health}.
 */
public final class OrderJournalSettings {

    /** A setting: env name, property name, default text. */
    public enum Key {
        CORE_ORDER_JOURNAL_RING_BYTES("core.order.journal.ring.bytes", "16777216"),
        CORE_ORDER_JOURNAL_TERM_LENGTH("core.order.journal.term.length", "4m"),
        CORE_ORDER_JOURNAL_RING_ALARM_PCT("core.order.journal.ring.alarm.pct", "50"),
        CORE_ORDER_JOURNAL_RING_ALARM_MS("core.order.journal.ring.alarm.ms", "5000"),
        CORE_ORDER_JOURNAL_STALL_LOG_MS("core.order.journal.stall.log.ms", "1000"),
        CORE_ORDER_JOURNAL_WRITER_IDLE_MAX_US("core.order.journal.writer.idle.max.us", "1000"),
        /** How long a closing writer keeps draining the ring and waiting for the archive to record what it offered. */
        CORE_ORDER_JOURNAL_CLOSE_DRAIN_MS("core.order.journal.close.drain.ms", "2000"),
        CORE_ARCHIVE_MAX_CONCURRENT_REPLAYS("core.archive.max.concurrent.replays", "10"),
        /**
         * Replay slots of this member's archive kept free for cluster catch-up: a follower that restarts behind the
         * leader replicates the missing log through one replay on the leader's archive, and in a three-member cluster
         * at most two followers do that at once, hence 2. Readers of the journals get the rest
         * ({@link #readerReplayBudget}). Aeron's archive has one cap for every client, so this is a budget readers are
         * held to, not a slot the archive sets aside: a reader that takes more than its budget can still starve a
         * follower.
         */
        CORE_ARCHIVE_CLUSTER_RESERVED_REPLAYS("core.archive.cluster.reserved.replays", "2"),
        /** The reader's replay stream id whose sender positions the replay-count gauge counts; 0 leaves it off. */
        CORE_ARCHIVE_REPLAY_PROBE_STREAM("core.archive.replay.probe.stream", "0");

        private final String property;
        private final String defaultText;

        Key(final String property, final String defaultText) {
            this.property = property;
            this.defaultText = defaultText;
        }

        public String property() {
            return property;
        }

        public String defaultText() {
            return defaultText;
        }
    }

    /**
     * The smallest ring that takes one record: a ring's largest message is an eighth of its capacity, so the ring is at
     * least eight messages of {@link OrderJournal#MESSAGE_BYTES}, rounded up to a power of two (2048 bytes).
     */
    public static final int MIN_RING_BYTES = BitUtil.findNextPositivePowerOfTwo(8 * OrderJournal.MESSAGE_BYTES);

    private final int ringBytes;
    private final int termLength;
    private final int ringAlarmPct;
    private final long ringAlarmMs;
    private final long stallLogMs;
    private final long writerIdleMaxUs;
    private final long closeDrainMs;
    private final int maxConcurrentReplays;
    private final int clusterReservedReplays;
    private final int replayProbeStream;

    private OrderJournalSettings(final SnapshotModeSettings.Source source) {
        this.ringBytes = (int) size(source, Key.CORE_ORDER_JOURNAL_RING_BYTES, MIN_RING_BYTES, 1L << 30, true);
        this.termLength = (int) size(source, Key.CORE_ORDER_JOURNAL_TERM_LENGTH, 64 * 1024, 1L << 30, true);
        this.ringAlarmPct = (int) range(source, Key.CORE_ORDER_JOURNAL_RING_ALARM_PCT, 1, 100);
        this.ringAlarmMs = range(source, Key.CORE_ORDER_JOURNAL_RING_ALARM_MS, 0, Long.MAX_VALUE / 1_000_000L);
        this.stallLogMs = range(source, Key.CORE_ORDER_JOURNAL_STALL_LOG_MS, 1, Long.MAX_VALUE / 1_000_000L);
        this.writerIdleMaxUs = range(source, Key.CORE_ORDER_JOURNAL_WRITER_IDLE_MAX_US, 1, Long.MAX_VALUE / 1_000L);
        this.closeDrainMs = range(source, Key.CORE_ORDER_JOURNAL_CLOSE_DRAIN_MS, 0, Long.MAX_VALUE / 1_000_000L);
        this.maxConcurrentReplays = (int) range(source, Key.CORE_ARCHIVE_MAX_CONCURRENT_REPLAYS, 1, Integer.MAX_VALUE);
        this.clusterReservedReplays = (int) range(source, Key.CORE_ARCHIVE_CLUSTER_RESERVED_REPLAYS, 0, Integer.MAX_VALUE);
        if (clusterReservedReplays >= maxConcurrentReplays) {
            throw new IllegalArgumentException(Key.CORE_ARCHIVE_CLUSTER_RESERVED_REPLAYS + "=" + clusterReservedReplays
                    + " leaves readers no replay slot: it must be below " + Key.CORE_ARCHIVE_MAX_CONCURRENT_REPLAYS + "="
                    + maxConcurrentReplays);
        }
        this.replayProbeStream = (int) range(source, Key.CORE_ARCHIVE_REPLAY_PROBE_STREAM, Integer.MIN_VALUE,
                Integer.MAX_VALUE);
    }

    /** The settings of this process. @throws IllegalArgumentException naming the first bad setting */
    public static OrderJournalSettings fromEnv() {
        return parse(SnapshotModeSettings.Source.ENVIRONMENT);
    }

    public static OrderJournalSettings parse(final SnapshotModeSettings.Source source) {
        return new OrderJournalSettings(Objects.requireNonNull(source, "source"));
    }

    /** Defaults with the given overrides; test convenience. */
    public static OrderJournalSettings with(final Map<Key, String> values) {
        final Map<Key, String> copy = Map.copyOf(values);
        return parse((env, prop) -> copy.get(Key.valueOf(env)));
    }

    public static OrderJournalSettings defaults() {
        return with(Map.of());
    }

    public int ringBytes() {
        return ringBytes;
    }

    public int termLength() {
        return termLength;
    }

    public int ringAlarmPct() {
        return ringAlarmPct;
    }

    public long ringAlarmMs() {
        return ringAlarmMs;
    }

    public long stallLogMs() {
        return stallLogMs;
    }

    public long writerIdleMaxUs() {
        return writerIdleMaxUs;
    }

    public long closeDrainMs() {
        return closeDrainMs;
    }

    public int maxConcurrentReplays() {
        return maxConcurrentReplays;
    }

    /** Replay slots kept free on this member's archive for cluster catch-up. */
    public int clusterReservedReplays() {
        return clusterReservedReplays;
    }

    /**
     * Replays readers of the journals may hold on this member's archive at once: the cap minus the slots kept for
     * cluster catch-up. Advertised on {@code /health}; a reader that holds more can starve a follower's catch-up.
     */
    public int readerReplayBudget() {
        return maxConcurrentReplays - clusterReservedReplays;
    }

    public int replayProbeStream() {
        return replayProbeStream;
    }

    /** The recorded publication's channel. */
    public String channel() {
        return "aeron:ipc?term-length=" + termLength;
    }

    @Override
    public String toString() {
        return Key.CORE_ORDER_JOURNAL_RING_BYTES + "=" + ringBytes + " " + Key.CORE_ORDER_JOURNAL_TERM_LENGTH + "="
                + termLength + " " + Key.CORE_ORDER_JOURNAL_RING_ALARM_PCT + "=" + ringAlarmPct + " "
                + Key.CORE_ORDER_JOURNAL_RING_ALARM_MS + "=" + ringAlarmMs + " " + Key.CORE_ORDER_JOURNAL_STALL_LOG_MS
                + "=" + stallLogMs + " " + Key.CORE_ORDER_JOURNAL_WRITER_IDLE_MAX_US + "=" + writerIdleMaxUs + " "
                + Key.CORE_ORDER_JOURNAL_CLOSE_DRAIN_MS + "=" + closeDrainMs + " "
                + Key.CORE_ARCHIVE_MAX_CONCURRENT_REPLAYS + "=" + maxConcurrentReplays + " "
                + Key.CORE_ARCHIVE_CLUSTER_RESERVED_REPLAYS + "=" + clusterReservedReplays + " readerReplayBudget="
                + readerReplayBudget() + " "
                + Key.CORE_ARCHIVE_REPLAY_PROBE_STREAM + "=" + replayProbeStream;
    }

    private static String text(final SnapshotModeSettings.Source source, final Key key) {
        final String raw = source.get(key.name(), key.property());
        return raw == null || raw.isBlank() ? key.defaultText() : raw.trim();
    }

    private static long size(final SnapshotModeSettings.Source source, final Key key, final long min, final long max,
                             final boolean powerOfTwo) {
        final String v = text(source, key);
        final long value;
        try {
            value = SystemUtil.parseSize(key.name(), v);
        } catch (final RuntimeException notASize) {
            throw new IllegalArgumentException(key.name() + "='" + v + "' is not a size");
        }
        if (value < min || value > max) {
            throw new IllegalArgumentException(key.name() + "=" + value + " out of range [" + min + "," + max + "]");
        }
        if (powerOfTwo && !BitUtil.isPowerOfTwo(value)) {
            throw new IllegalArgumentException(key.name() + "=" + value + " is not a power of two");
        }
        return value;
    }

    private static long range(final SnapshotModeSettings.Source source, final Key key, final long min, final long max) {
        final String v = text(source, key);
        final long value;
        try {
            value = Long.parseLong(v);
        } catch (final NumberFormatException notANumber) {
            throw new IllegalArgumentException(key.name() + "='" + v + "' is not an integer");
        }
        if (value < min || value > max) {
            throw new IllegalArgumentException(key.name() + "=" + value + " out of range [" + min + "," + max + "]");
        }
        return value;
    }
}
