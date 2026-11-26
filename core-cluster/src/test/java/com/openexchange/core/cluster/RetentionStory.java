// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.cluster.journal.JournalProfile;
import com.openexchange.cluster.journal.JournalReaderTool;
import com.openexchange.cluster.journal.JournalRetentionAgent;
import com.openexchange.cluster.shadow.ShadowReplay;
import com.openexchange.core.infrastructure.InfrastructureConstants;
import com.openexchange.core.infrastructure.generated.TimeInForce;
import com.openexchange.core.infrastructure.journal.CoreJournalProfiles;
import com.openexchange.core.infrastructure.journal.MoneyJournal;
import com.openexchange.core.infrastructure.journal.MoneyJournalWriterAgent;
import com.openexchange.core.infrastructure.journal.OrderJournalSettings;
import com.openexchange.core.infrastructure.persistence.ServiceProbe;
import io.aeron.archive.client.AeronArchive;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.SystemNanoClock;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * A three-member core with the production order journal (64 KiB term) and money journal on every member, 64 KiB
 * archive segments, the journal retention settings as system properties, an order and deposit load, a reader that
 * really reads a journal from the members' archives, and a pusher that delivers that reader's cursor to each member
 * with the kit's command at least one step after it read it. Everything a test asserts about a purge is read back
 * from a member's archive.
 */
final class RetentionStory implements AutoCloseable {

    static final int SEG = 64 * 1024;
    static final int ORDER_FRAME = 256;
    static final long GENESIS = ShadowClusterRig.GENESIS;
    /** The money journal's term is a constant of the engine, and a recording's segment is never below its term. */
    static final int MONEY_SEG = 4 * 1024 * 1024;

    /** One recording as a member's archive lists it. */
    record Rec(long id, long start, long end, boolean active) {
        long bytes() {
            return Math.max(0, end - start);
        }
    }

    final ShadowClusterRig rig;
    final OrderJournalReplicationClusterTest.Wiring wiring;
    final Map<Integer, AgentRunner> moneyWriters = new HashMap<>();
    final List<String> lines = new CopyOnWriteArrayList<>();
    private final List<String> properties = new ArrayList<>();
    private final PrintStream originalOut = System.out;
    private final PrintStream originalErr = System.err;
    OrderLoad load;
    ShadowClusterRig.Member leader;
    ShadowClusterRig.Member f;
    ShadowClusterRig.Member g;
    private int nextOrder = 1;
    private int users;

    /** Records every line that names the journal retention or the reader tool, and passes everything on. */
    private final class Capture extends OutputStream {
        private final PrintStream next;
        private final ByteArrayOutputStream line = new ByteArrayOutputStream();

        Capture(final PrintStream next) {
            this.next = next;
        }

        @Override
        public synchronized void write(final int b) {
            next.write(b);
            if (b == '\n') {
                final String s = line.toString();
                line.reset();
                if (s.contains("[JOURNAL-RE") || s.contains("order journal hole") || s.contains("journal retention")) {
                    lines.add(s);
                }
            } else {
                line.write(b);
            }
        }

        @Override
        public void flush() {
            next.flush();
        }
    }

    /**
     * @param properties system property name and value pairs, set before the first member boots and cleared in
     *                   {@link #close}
     */
    RetentionStory(final int port, final String name, final String... properties) {
        for (int i = 0; i < properties.length; i += 2) {
            System.setProperty(properties[i], properties[i + 1]);
            this.properties.add(properties[i]);
        }
        System.setOut(new PrintStream(new Capture(originalOut), true));
        System.setErr(new PrintStream(new Capture(originalErr), true));
        rig = new ShadowClusterRig(port, name, 3, "64k", SEG);
        wiring = new OrderJournalReplicationClusterTest.Wiring(rig, OrderJournalSettings.with(
                Map.of(OrderJournalSettings.Key.CORE_ORDER_JOURNAL_TERM_LENGTH, "64k")));
        for (final ShadowClusterRig.Member m : rig.members) {
            final MoneyJournal[] ring = {null};
            final java.util.function.Consumer<com.openexchange.core.infrastructure.persistence.CoreClusteredService>
                    order = m.beforeLaunch;
            m.beforeLaunch = svc -> {
                order.accept(svc);
                ring[0] = new MoneyJournal(16 * 1024 * 1024);
                svc.setMoneyJournalSink(ring[0]);
            };
            final java.util.function.Consumer<ShadowClusterRig.Member> orderWriter = m.beforeContainer;
            m.beforeContainer = mm -> {
                orderWriter.accept(mm);
                final AgentRunner w = new AgentRunner(new BackoffIdleStrategy(), Throwable::printStackTrace, null,
                        new MoneyJournalWriterAgent(ring[0], mm.cfg.aeronArchiveContext().clone()::clone,
                                InfrastructureConstants.MONEY_JOURNAL_CHANNEL,
                                InfrastructureConstants.MONEY_JOURNAL_STREAM_ID));
                CloseHelper.quietClose(moneyWriters.put(mm.id, w));
                AgentRunner.startOnThread(w);
            };
        }
    }

    /** Boots, finds the leader, ADOPTs, opens the gate and funds {@code users} users (ten orders each). */
    RetentionStory boot(final int users) throws InterruptedException {
        this.users = users;
        rig.bootAll();
        leader = rig.awaitLeader(null);
        f = rig.followerOtherThan(leader, null);
        g = rig.followerOtherThan(leader, f);
        load = OrderLoad.prepare(rig, g, users);
        return this;
    }

    @Override
    public void close() {
        try {
            wiring.stopAll();
            moneyWriters.values().forEach(CloseHelper::quietClose);
            rig.close();
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
            properties.forEach(System::clearProperty);
            com.openexchange.cluster.journal.JournalRetention.Seams.reset();
        }
    }

    // ------------------------------------------------------------------ load

    /** {@code n} more resting orders, each one order journal record; waits for every answer. */
    void orders(final int n) {
        if (nextOrder - 1 + n > users * OrderLoad.ORDERS_PER_USER) {
            throw new IllegalStateException("the story funded " + users + " users: no more than "
                    + users * OrderLoad.ORDERS_PER_USER + " orders");
        }
        load.orders(rig, nextOrder, n, TimeInForce.GTC, 0L);
        nextOrder += n;
    }

    private long depositCorr = 5_000_000;

    /** {@code n} deposits to the funded users, each one money journal record once the journal is armed. */
    void deposits(final int n) {
        final CoreRestoreRig.Encoders enc = new CoreRestoreRig.Encoders();
        int offered = 0;
        while (offered < n) {
            final int batch = Math.min(4_000, n - offered);
            for (int i = 0; i < batch; i++) {
                depositCorr++;
                CoreRestoreRig.offer(load.tool.session, enc.buf, enc.deposit(depositCorr, 1 + (depositCorr % users),
                        CoreRestoreRig.usd(), ShadowClusterRig.ONE), rig.sessions());
            }
            offered += batch;
            final long last = depositCorr;
            rig.await(() -> load.tool.in.lastDepositAckCorr >= last, 300_000, "deposit ack " + last);
        }
    }

    /** ADMIN SetMoneyJournal on the log: every live member arms its money journal. */
    void armMoneyJournal() {
        rig.armMoneyJournal(load.admin);
    }

    /** The money journal's head as the leader's engine has it. */
    long moneyHead() {
        return rig.awaitLeader(null).service.engine().getJournalSeq();
    }

    /** The order journal's head as the leader's engine has it. */
    long orderHead() {
        return rig.awaitLeader(null).service.engine().getEventSeq();
    }

    /** Stops a member's journal writers before the member is killed. */
    void stopWriters(final ShadowClusterRig.Member m) {
        wiring.stop(m.id);
        CloseHelper.quietClose(moneyWriters.remove(m.id));
    }

    // ------------------------------------------------------------------ the archive, read back

    List<Rec> recordings(final ShadowClusterRig.Member m, final JournalProfile profile) {
        final AeronArchive archive = rig.archive(m);
        final List<long[]> rows = new ArrayList<>();
        archive.listRecordingsForUri(0, 10_000, profile.channelFragment(), profile.streamId(),
                (c, k, recordingId, st, sp, startPosition, stopPosition, i, t, se, mm, sid, str, ch, oc, src) ->
                        rows.add(new long[] {recordingId, startPosition, stopPosition}));
        final List<Rec> out = new ArrayList<>();
        for (final long[] r : rows) {
            long end = r[2];
            boolean active = false;
            if (end == AeronArchive.NULL_POSITION) {
                end = archive.getRecordingPosition(r[0]);
                if (end == AeronArchive.NULL_POSITION) {
                    end = archive.getStopPosition(r[0]);
                } else {
                    active = true;
                }
            }
            out.add(new Rec(r[0], r[1], end, active));
        }
        return out;
    }

    /** The one active recording of the journal on {@code m}; fails when there is none or more than one. */
    Rec active(final ShadowClusterRig.Member m, final JournalProfile profile) {
        Rec found = null;
        for (final Rec r : recordings(m, profile)) {
            if (r.active()) {
                if (found != null) {
                    throw new AssertionError("member " + m.id + ": two active " + profile.journal() + " recordings");
                }
                found = r;
            }
        }
        if (found == null) {
            throw new AssertionError("member " + m.id + ": no active " + profile.journal() + " recording");
        }
        return found;
    }

    long retained(final ShadowClusterRig.Member m, final JournalProfile profile) {
        long sum = 0;
        for (final Rec r : recordings(m, profile)) {
            sum += r.bytes();
        }
        return sum;
    }

    int recordingsWithBytes(final ShadowClusterRig.Member m, final JournalProfile profile) {
        int n = 0;
        for (final Rec r : recordings(m, profile)) {
            if (r.bytes() > 0) {
                n++;
            }
        }
        return n;
    }

    /** Every record {@code m}'s archive still holds of the journal: journalSeq to the number of copies. */
    TreeMap<Long, Integer> held(final ShadowClusterRig.Member m, final JournalProfile profile) {
        final TreeMap<Long, Integer> seqs = new TreeMap<>();
        final ShadowReplay replay = new ShadowReplay(rig.aeron(m), rig.archive(m), SystemNanoClock.INSTANCE);
        for (final Rec r : recordings(m, profile)) {
            if (r.bytes() > 0) {
                try {
                    replay.readAll(r.id(), r.start(), r.end(), (buffer, offset, length, h) ->
                            seqs.merge(buffer.getLong(offset + 8, ByteOrder.LITTLE_ENDIAN), 1, Integer::sum),
                            System.nanoTime() + 30_000_000_000L);
                } catch (final io.aeron.archive.client.ArchiveException purgedUnderTheRead) {
                    // the member purged between the listing and the replay: the caller reads again
                    return null;
                }
            }
        }
        return seqs;
    }

    // ------------------------------------------------------------------ the member's own view

    JournalRetentionAgent.Status status(final ShadowClusterRig.Member m, final JournalProfile profile) {
        final JournalRetentionAgent agent = ServiceProbe.journalRetention(m.service);
        if (agent == null) {
            throw new AssertionError("member " + m.id + " runs no journal retention");
        }
        return agent.status(profile.journal());
    }

    /** The value of {@code core_journal_retention_<name>{journal="..."}} on {@code m}'s {@code /metrics}. */
    long metric(final ShadowClusterRig.Member m, final JournalProfile profile, final String name) {
        final String series = "core_journal_retention_" + name + "{journal=\"" + profile.journal() + "\"} ";
        for (final String line : ServiceProbe.metrics(m.service).split("\n")) {
            if (line.startsWith(series)) {
                return Long.parseLong(line.substring(series.length()).trim());
            }
        }
        throw new AssertionError("member " + m.id + ": no series " + series);
    }

    boolean said(final String text) {
        return lines.stream().anyMatch(l -> l.contains(text));
    }

    // ------------------------------------------------------------------ the reader files

    /** The kit's command on {@code m}'s cluster directory; returns its exit code, its lines go to the capture. */
    int tool(final ShadowClusterRig.Member m, final String command, final String... flags) {
        final String[] args = new String[3 + flags.length];
        args[0] = command;
        args[1] = "--cluster-dir";
        args[2] = m.clusterDir().getAbsolutePath();
        System.arraycopy(flags, 0, args, 3, flags.length);
        return JournalReaderTool.run(args, System.out, System.err, System::currentTimeMillis);
    }

    void set(final ShadowClusterRig.Member m, final JournalProfile profile, final String reader, final long applied) {
        final int exit = tool(m, "set", "--engine", "core", "--cluster-id",
                String.valueOf(m.cfg.consensusModuleContext().clusterId()), "--genesis-id", String.valueOf(GENESIS),
                "--journal", profile.journal(), "--reader", reader, "--applied-journal-seq", String.valueOf(applied),
                "--source", "test-pusher");
        if (exit != 0) {
            throw new AssertionError("JournalReaderTool set on member " + m.id + " exit " + exit + ": " + lines);
        }
    }

    void setAll(final JournalProfile profile, final String reader, final long applied) {
        for (final ShadowClusterRig.Member m : rig.live()) {
            set(m, profile, reader, applied);
        }
    }

    // ------------------------------------------------------------------ a reader and its pusher

    /**
     * A reader of one journal: it reads the members' archives in turn and applies records in sequence, one after the
     * other from 1. Its cursor is what it has applied. A next record that no member holds any more is a gap: the
     * retention deleted a record this reader had not applied.
     */
    final class Reader {
        final JournalProfile profile;
        final String name;
        volatile long applied;
        /** Test seam: a reader that stopped applying (dead or stuck). */
        boolean frozen;
        private int turn;
        private int pollsSinceProgress;
        /** Records read and not yet applied (their predecessor has not been read). */
        private final java.util.TreeSet<Long> pending = new java.util.TreeSet<>();
        /** This reader's own resume memo: per member and recording, the position it has read up to. */
        private final Map<String, Long> memo = new HashMap<>();

        Reader(final JournalProfile profile, final String name) {
            this.profile = profile;
            this.name = name;
        }

        /** One read of one member, from where this reader left each of its recordings; applies what follows. */
        void poll() {
            if (frozen) {
                return;
            }
            final List<ShadowClusterRig.Member> live = rig.live();
            final ShadowClusterRig.Member source = live.get(turn++ % live.size());
            final ShadowReplay replay = new ShadowReplay(rig.aeron(source), rig.archive(source),
                    SystemNanoClock.INSTANCE);
            for (final Rec r : recordings(source, profile)) {
                final String key = source.id + "/" + source.starts + "/" + r.id();
                // a memo below the recording's start is dropped: the member purged what lay between
                final long from = Math.max(r.start(), memo.getOrDefault(key, 0L));
                if (r.end() <= from) {
                    continue;
                }
                try {
                    replay.readAll(r.id(), from, r.end(), (buffer, offset, length, h) -> {
                        final long seq = buffer.getLong(offset + 8, ByteOrder.LITTLE_ENDIAN);
                        if (seq > applied) {
                            pending.add(seq);
                        }
                    }, System.nanoTime() + 30_000_000_000L);
                    memo.put(key, r.end());
                } catch (final io.aeron.archive.client.ArchiveException purgedUnderTheRead) {
                    // the member purged between the listing and the replay: the next turn lists again
                }
            }
            final long before = applied;
            long next = applied;
            while (pending.remove(next + 1)) {
                next++;
            }
            applied = next;
            pollsSinceProgress = next > before ? 0 : pollsSinceProgress + 1;
            if (!pending.isEmpty() && pollsSinceProgress >= 2 * live.size()) {
                throw new AssertionError("reader " + name + " gap: its next " + profile.journal() + " record "
                        + (applied + 1) + " is on no member (the lowest record above it that a member holds is "
                        + pending.first() + ")");
            }
        }
    }

    /**
     * Delivers a reader's cursor to every live member: each step writes the value the previous step read (so a file
     * always carries a cursor at least one step old), and member {@code late} gets it one step later still.
     */
    final class Pusher {
        final Reader reader;
        final ShadowClusterRig.Member late;
        private long read = -1;
        private long older = -1;
        /** Test seam: this is added to every cursor written (a pusher that sends a wrong number). */
        long skew;
        boolean stopped;

        Pusher(final Reader reader, final ShadowClusterRig.Member late) {
            this.reader = reader;
            this.late = late;
        }

        void step() {
            if (stopped) {
                return;
            }
            for (final ShadowClusterRig.Member m : rig.live()) {
                final long value = m == late ? older : read;
                if (value >= 0) {
                    set(m, reader.profile, reader.name, value + skew);
                }
            }
            older = read;
            read = reader.applied;
        }
    }

    /**
     * Runs the readers and their pushers (a pusher step every {@code stepMs}) and the rig's sessions until
     * {@code done}; fails with {@code why} after {@code timeoutMs}.
     */
    void pump(final List<Reader> readers, final List<Pusher> pushers, final long stepMs, final BooleanSupplier done,
              final long timeoutMs, final Supplier<String> why) {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        long nextStep = 0;
        while (true) {
            readers.forEach(Reader::poll);
            if (System.currentTimeMillis() >= nextStep) {
                pushers.forEach(Pusher::step);
                nextStep = System.currentTimeMillis() + stepMs;
            }
            if (done.getAsBoolean()) {
                return;
            }
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError(why.get());
            }
            rig.pumpMs(50);
        }
    }

    String describe(final JournalProfile profile) {
        final StringBuilder sb = new StringBuilder();
        for (final ShadowClusterRig.Member m : rig.live()) {
            JournalRetentionAgent.Status s = null;
            try {
                final JournalRetentionAgent agent = ServiceProbe.journalRetention(m.service);
                s = agent == null ? null : agent.status(profile.journal());
            } catch (final AssertionError noRetention) {
                // a build without journal retention: the archive's listing is all there is to say
            }
            sb.append("[member ").append(m.id).append(' ').append(profile.journal()).append(' ')
                    .append(recordings(m, profile)).append(' ')
                    .append(s == null || s.report == null ? "no pass" : String.valueOf(s.report)).append("] ");
        }
        return sb.toString();
    }

    static JournalProfile order() {
        return CoreJournalProfiles.ORDER;
    }

    static JournalProfile money() {
        return CoreJournalProfiles.MONEY;
    }
}
