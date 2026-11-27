// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.journal;

import org.agrona.concurrent.NanoClock;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * How many bytes the journal tail readers of one member may ask its archive to replay per time window
 * ({@code JOURNAL_TAIL_READ_BUDGET_BYTES} per {@code JOURNAL_TAIL_READ_WINDOW_MS}), shared by every reader of the
 * process: the restore's money and order journal reads and the housekeeping's periodic read.
 *
 * <p>Why: a tail read replays a range of each journal recording through a replay publication the archive creates in
 * the media driver's directory (on {@code /dev/shm}). The log buffer is sparse, so it costs about the replayed bytes,
 * and it stays there until the driver frees the closed publication (its linger, 5 s by default, plus the conductor's
 * next pass). A first look at a recording replays up to one journal term; hundreds of first looks within a few
 * seconds would hold hundreds of terms of tmpfs at once, enough to fill a small {@code /dev/shm} and kill the member
 * with SIGBUS. The budget spreads them out.</p>
 *
 * <p>The rule: a reader asks {@link #acquire} for the bytes of its next replay before it starts it. The request is
 * granted when the current window has granted nothing yet or when it fits in what is left of the window's budget;
 * otherwise the reader waits for the next window (never past its own deadline). After the replay the reader reports
 * what was actually replayed ({@link #settle}), so an estimate that was too high gives the difference back. A window
 * therefore grants at most the budget, or one replay above it when that replay is the window's first: at most
 * {@code max(budget, the largest replay)}, and a tail replay is at most one journal term. With a window longer than
 * the time a replay's buffer stays in the driver directory, at most two windows' replays are there at once: the
 * worst case transient is {@code 2 * max(budget, journal term length)}.</p>
 *
 * <p>Thread safe; waiting happens outside the lock.</p>
 */
public final class TailReadBudget {

    private final long budgetBytes;
    private final long windowNs;
    private final NanoClock clock;

    private long windowStartNs;
    private boolean windowOpen;
    private long used;
    private long grantedTotal;
    private long waitedNs;

    /**
     * @param budgetBytes the bytes a window grants (a request larger than this is granted alone, as a window's first)
     * @param windowMs    the window length
     */
    public TailReadBudget(final long budgetBytes, final long windowMs, final NanoClock clock) {
        if (budgetBytes <= 0 || windowMs <= 0) {
            throw new IllegalArgumentException("budgetBytes=" + budgetBytes + " windowMs=" + windowMs);
        }
        this.budgetBytes = budgetBytes;
        this.windowNs = TimeUnit.MILLISECONDS.toNanos(windowMs);
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** A read's deadline passed while it waited for room in the budget. */
    public static final class DeadlineException extends RuntimeException {
        public DeadlineException(final String message) {
            super(message);
        }
    }

    /**
     * {@link #acquire}, or the reader's deadline as an exception.
     *
     * @throws DeadlineException the deadline passed while waiting
     */
    public void acquireOrThrow(final long bytes, final long deadlineNs) {
        if (!acquire(bytes, deadlineNs)) {
            throw new DeadlineException("deadline while waiting for the tail read budget (" + this + ")");
        }
    }

    /** True for a read failure that is a deadline: the replay's own, or the wait for this budget. */
    public static boolean isDeadline(final Throwable t) {
        return t instanceof DeadlineException || t instanceof com.openexchange.cluster.shadow.ShadowReplay.ReplayTimeoutException;
    }

    /** No bound: every request is granted at once. For tests and tools that are not a member. */
    public static TailReadBudget unbounded() {
        return new TailReadBudget(Long.MAX_VALUE, 1, org.agrona.concurrent.SystemNanoClock.INSTANCE);
    }

    /**
     * Waits until {@code bytes} may be replayed and takes them from the current window.
     *
     * @return false when {@code deadlineNs} (of this budget's clock) passed first: nothing was taken
     */
    public boolean acquire(final long bytes, final long deadlineNs) {
        if (bytes <= 0) {
            return true;
        }
        while (true) {
            final long waitNs;
            synchronized (this) {
                final long now = clock.nanoTime();
                if (!windowOpen || now - windowStartNs >= windowNs) {
                    windowOpen = true;
                    windowStartNs = now;
                    used = 0;
                }
                if (used == 0 || used + bytes <= budgetBytes) {
                    used += bytes;
                    grantedTotal += bytes;
                    return true;
                }
                if (now - deadlineNs >= 0) {
                    return false;
                }
                waitNs = Math.min(windowStartNs + windowNs - now, deadlineNs - now);
            }
            final long t0 = clock.nanoTime();
            LockSupport.parkNanos(Math.max(1, waitNs));
            synchronized (this) {
                waitedNs += clock.nanoTime() - t0;
            }
            if (Thread.currentThread().isInterrupted()) {
                return false;
            }
        }
    }

    /**
     * Corrects the current window after a replay granted {@code granted} bytes actually asked the archive for
     * {@code replayed}: an estimate that was too high gives the rest back; one that was too low takes the difference.
     */
    public synchronized void settle(final long granted, final long replayed) {
        final long delta = replayed - Math.max(0, granted);
        used = Math.max(0, used + delta);
        grantedTotal += delta;
    }

    public long budgetBytes() {
        return budgetBytes;
    }

    public long windowMs() {
        return TimeUnit.NANOSECONDS.toMillis(windowNs);
    }

    /** The bytes granted and settled over the life of this budget. */
    public synchronized long grantedBytesTotal() {
        return grantedTotal;
    }

    /** How long readers waited for a window, over the life of this budget. */
    public synchronized long waitedMsTotal() {
        return TimeUnit.NANOSECONDS.toMillis(waitedNs);
    }

    /**
     * The replay a {@link com.openexchange.cluster.journal.RecordingTail} look is expected to start: from the
     * remembered position when it lies above the floor and at or below {@code stop}, else from the floor (the base of
     * the term holding {@code stop - 1}, or the start). An estimate: {@link #settle} corrects it.
     */
    public static long estimate(final long start, final long stop, final int termLength, final long resume) {
        if (stop <= start) {
            return 0;
        }
        final long floor = Math.max(start, (stop - 1) & -(long) termLength);
        return resume > floor && resume <= stop ? stop - resume : stop - floor;
    }

    @Override
    public String toString() {
        return "budget=" + budgetBytes + "B/" + windowMs() + "ms";
    }
}
