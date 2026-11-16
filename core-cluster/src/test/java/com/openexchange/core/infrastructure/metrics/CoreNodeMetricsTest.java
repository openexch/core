// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.metrics;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** The service-thread cost histogram and its Prometheus rendering, off the cluster. */
public class CoreNodeMetricsTest {

    private static int bucketOf(final long nanos) {
        int i = 0;
        while (i < CoreNodeMetrics.BUCKET_UPPER_NS.length && nanos > CoreNodeMetrics.BUCKET_UPPER_NS[i]) {
            i++;
        }
        return i;
    }

    @Test
    public void thresholdsFallOnExactBucketBoundaries() {
        final CoreNodeMetrics m = new CoreNodeMetrics(0);
        m.recordMessage(1, 5_000L, 100L, 10L);        // exactly 5 us: le="0.000005" bucket
        m.recordMessage(1, 5_001L, 100L, 11L);        // just over: next bucket
        m.recordMessage(1, 250L, 100L, 12L);          // first bucket
        m.recordMessage(1, 20_000_000_000L, 100L, 13L); // beyond the ladder: +Inf slot
        final CoreNodeMetrics.Histogram h = m.dispatch();
        assertEquals(1, h.bucketCount(bucketOf(5_000L)));
        assertEquals(bucketOf(5_000L) + 1, bucketOf(5_001L));
        assertEquals(1, h.bucketCount(bucketOf(5_001L)));
        assertEquals(1, h.bucketCount(0));
        assertEquals(1, h.bucketCount(CoreNodeMetrics.BUCKETS - 1));
        assertEquals(4, h.count());
        assertEquals(5_000L + 5_001L + 250L + 20_000_000_000L, h.sumNanos());
        assertEquals(20_000_000_000L, h.maxNanos());
        assertEquals("0.000005", CoreNodeMetrics.BUCKET_LE[bucketOf(5_000L)]);
        assertEquals("0.00005", CoreNodeMetrics.BUCKET_LE[bucketOf(50_000L)]);
        assertEquals("10", CoreNodeMetrics.BUCKET_LE[CoreNodeMetrics.BUCKET_LE.length - 1]);
    }

    @Test
    public void perTemplateCostAndOutOfRangeTemplatesShareTheLastSlot() {
        final CoreNodeMetrics m = new CoreNodeMetrics(0);
        m.recordMessage(1, 1_000L, 10L, 1L);
        m.recordMessage(1, 3_000L, 10L, 2L);
        m.recordMessage(5, 7_000L, 10L, 3L);
        m.recordMessage(999, 9_000L, 10L, 4L);
        m.recordMessage(-1, 11_000L, 10L, 5L);
        assertEquals(2, m.templateCount(1));
        assertEquals(4_000L, m.templateSumNanos(1));
        assertEquals(3_000L, m.templateMaxNanos(1));
        assertEquals(1, m.templateCount(5));
        assertEquals(2, m.templateCount(CoreNodeMetrics.TEMPLATE_SLOTS - 1));
        assertEquals(11_000L, m.templateMaxNanos(CoreNodeMetrics.TEMPLATE_SLOTS - 1));
        assertEquals(5L, m.lastLogPosition());
        assertEquals(5, m.dispatch().count());
        assertEquals(5, m.drain().count());
    }

    @Test
    public void samplingShiftRecordsOneInTwoToTheShift() {
        final CoreNodeMetrics every = new CoreNodeMetrics(0);
        for (int i = 0; i < 8; i++) {
            assertTrue(every.shouldSample());
        }
        final CoreNodeMetrics quarter = new CoreNodeMetrics(2);
        int sampled = 0;
        for (int i = 0; i < 16; i++) {
            if (quarter.shouldSample()) {
                sampled++;
            }
        }
        assertEquals(4, sampled);
        assertEquals(2, quarter.sampleShift());
    }

    @Test(expected = IllegalArgumentException.class)
    public void sampleShiftOutsideRangeIsRefused() {
        new CoreNodeMetrics(17);
    }

    @Test
    public void publishSequenceAdvancesEveryPublishEveryMessagesAndOnTimerDrain() {
        final CoreNodeMetrics m = new CoreNodeMetrics(0);
        final long before = m.acquire();
        for (int i = 0; i < CoreNodeMetrics.PUBLISH_EVERY - 1; i++) {
            m.recordMessage(1, 1L, 1L, i);
        }
        assertEquals(before, m.acquire());
        m.recordMessage(1, 1L, 1L, 99L);
        assertEquals(before + 1, m.acquire());
        m.recordTimerDrain(5L);
        assertEquals(before + 2, m.acquire());
    }

    @Test
    public void rendersPrometheusTextWithHistogramsTemplatesAndSnapshotSeries() {
        final CoreNodeMetrics m = new CoreNodeMetrics(0);
        m.setRole(2);
        m.setMemberId(1);
        m.recordMessage(1, 4_000L, 500L, 77L);
        m.recordMessage(3, 60_000L, 500L, 78L);
        m.recordTimerDrain(900L);
        final CoreMetricsBody body = new CoreMetricsBody(m)
                .counter("core_test_total", "A counter", () -> 42L)
                .gauge("core_test_gauge", "A gauge", () -> -7L);
        String out = body.render();
        assertTrue(out, out.contains("# TYPE core_test_total counter\ncore_test_total 42\n"));
        assertTrue(out, out.contains("# TYPE core_test_gauge gauge\ncore_test_gauge -7\n"));
        assertTrue(out, out.contains("core_cluster_role 2\n"));
        assertTrue(out, out.contains("core_member_id 1\n"));
        assertTrue(out, out.contains("core_log_position 78\n"));
        assertTrue(out, out.contains("core_apply_sample_shift 0\n"));
        assertTrue(out, out.contains("# TYPE core_apply_dispatch_seconds histogram\n"));
        // 4 us is <= 5 us; 60 us is > 50 us, so the 5 us bucket holds 1 and the 50 us bucket still 1
        assertTrue(out, out.contains("core_apply_dispatch_seconds_bucket{le=\"0.000005\"} 1\n"));
        assertTrue(out, out.contains("core_apply_dispatch_seconds_bucket{le=\"0.00005\"} 1\n"));
        assertTrue(out, out.contains("core_apply_dispatch_seconds_bucket{le=\"0.0001\"} 2\n"));
        assertTrue(out, out.contains("core_apply_dispatch_seconds_bucket{le=\"+Inf\"} 2\n"));
        assertTrue(out, out.contains("core_apply_dispatch_seconds_count 2\n"));
        assertTrue(out, out.contains("core_apply_dispatch_seconds_max 6.0E-5\n"));
        assertTrue(out, out.contains("core_apply_drain_seconds_count 2\n"));
        assertTrue(out, out.contains("core_timer_drain_seconds_count 1\n"));
        assertTrue(out, out.contains("core_apply_template_dispatch_count{template=\"1\"} 1\n"));
        assertTrue(out, out.contains("core_apply_template_dispatch_count{template=\"3\"} 1\n"));
        assertFalse(out, out.contains("template=\"2\""));
        assertTrue(out, out.contains("core_snapshot_age_seconds -1\n"));
        assertTrue(out, out.contains("core_snapshot_last_bytes -1\n"));
        assertTrue(out, out.contains("core_snapshot_last_record_seconds -1\n"));

        m.stampSnapshot(System.currentTimeMillis(), 178_537L, 22L, 8_000_000L, 1_000_000L, 17_000_000L, 261_856L,
                3_613_310_092L);
        out = body.render();
        assertTrue(out, out.contains("core_snapshot_age_seconds 0\n"));
        assertTrue(out, out.contains("core_snapshot_last_bytes 178537\n"));
        assertTrue(out, out.contains("core_snapshot_last_chunks 22\n"));
        assertTrue(out, out.contains("core_snapshot_last_log_position 261856\n"));
        assertTrue(out, out.contains("core_snapshot_last_serialize_seconds 0.008\n"));
        assertTrue(out, out.contains("core_snapshot_last_record_seconds 0.017\n"));
        assertTrue(out, out.contains("# TYPE core_snapshot_state_hash gauge\ncore_snapshot_state_hash 3613310092\n"));
    }

    @Test
    public void theHashSeriesIsOmittedUntilAHashIsKnownAndAStubSnapshotStampsNone() {
        final CoreNodeMetrics m = new CoreNodeMetrics(0);
        final CoreMetricsBody body = new CoreMetricsBody(m);
        String out = body.render();
        assertFalse(out, out.contains("core_snapshot_state_hash"));
        assertTrue(out, out.contains("core_snapshot_last_log_position -1\n"));

        m.stampSnapshot(System.currentTimeMillis(), 120L, 1L, 1L, 1L, 0L, 4_096L, 77L);
        out = body.render();
        assertTrue(out, out.contains("core_snapshot_state_hash 77\n"));

        // a stub names where the state lives; it carries no state to hash, so the series goes away
        m.stampSnapshot(System.currentTimeMillis(), 90L, 1L, 1L, 1L, 0L, 8_192L, CoreNodeMetrics.UNKNOWN_HASH);
        out = body.render();
        assertTrue(out, out.contains("core_snapshot_last_log_position 8192\n"));
        assertFalse(out, out.contains("core_snapshot_state_hash"));
    }

    @Test
    public void aRestoreStampsThePairButNotTheAgeOrSize() {
        final CoreNodeMetrics m = new CoreNodeMetrics(0);
        m.stampRestoredSnapshot(14_848L, 0xFFFF_FFFFL);
        final String out = new CoreMetricsBody(m).render();
        assertTrue(out, out.contains("core_snapshot_last_log_position 14848\n"));
        assertTrue(out, out.contains("core_snapshot_state_hash 4294967295\n"));
        assertTrue("this node took no snapshot: " + out, out.contains("core_snapshot_age_seconds -1\n"));
        assertTrue(out, out.contains("core_snapshot_last_bytes -1\n"));
    }

    @Test
    public void theHelpTextPromisesOnlyWhatAStubRestoreDelivers() {
        final String out = new CoreMetricsBody(new CoreNodeMetrics(0)).render();
        // a stub restore stamps nothing, so the position names a snapshot taken or a FULL image restored
        assertTrue(out, out.contains("# HELP core_snapshot_last_log_position Cluster log position of the last snapshot "
                + "this node took, or restored a full image from (-1 = never)\n"));
        final CoreNodeMetrics m = new CoreNodeMetrics(0);
        m.stampRestoredSnapshot(1L, 2L);
        final String hashed = new CoreMetricsBody(m).render();
        // the hash covers the codec layout too: two builds with different layouts differ at equal state
        assertTrue(hashed, hashed.contains("# HELP core_snapshot_state_hash CRC32C of the payload of the snapshot at "
                + "core_snapshot_last_log_position, unsigned decimal; equal on every healthy member running the same "
                + "snapshot layout\n"));
    }

    @Test
    public void aStampedIdentityIsImmutableAndReplacedAsOneReference() {
        final CoreNodeMetrics m = new CoreNodeMetrics(0);
        assertEquals(-1L, m.snapshotIdentity().logPosition());
        assertFalse(m.snapshotIdentity().hasHash());
        m.stampRestoredSnapshot(100L, 5L);
        final CoreNodeMetrics.SnapshotIdentity first = m.snapshotIdentity();
        m.stampSnapshot(1L, 1L, 1L, 1L, 1L, 1L, 200L, 6L);
        assertEquals("a holder already read never changes", 100L, first.logPosition());
        assertEquals(5L, first.stateHash());
        assertEquals(200L, m.snapshotIdentity().logPosition());
        assertEquals(6L, m.snapshotIdentity().stateHash());
        assertEquals(200L, m.lastSnapshotLogPosition());
    }

    @Test
    public void aScraperNeverSeesAPositionPairedWithAnotherSnapshotsHash() throws Exception {
        final CoreNodeMetrics m = new CoreNodeMetrics(0);
        final int stamps = 200_000;
        final java.util.concurrent.atomic.AtomicReference<String> torn = new java.util.concurrent.atomic.AtomicReference<>();
        final java.util.concurrent.atomic.AtomicBoolean done = new java.util.concurrent.atomic.AtomicBoolean();
        final Thread scraper = new Thread(() -> {
            while (!done.get() && torn.get() == null) {
                final CoreNodeMetrics.SnapshotIdentity id = m.snapshotIdentity();
                if (id.logPosition() >= 0 && id.stateHash() != (id.logPosition() * 31) % 0xFFFF_FFFFL) {
                    torn.set(id.toString());
                }
            }
        });
        scraper.start();
        for (long p = 1; p <= stamps; p++) {
            if ((p & 1) == 0) {
                m.stampSnapshot(1L, 1L, 1L, 1L, 1L, 1L, p, (p * 31) % 0xFFFF_FFFFL);
            } else {
                m.stampRestoredSnapshot(p, (p * 31) % 0xFFFF_FFFFL);
            }
        }
        done.set(true);
        scraper.join(10_000);
        assertEquals("torn pair seen by the scraper", null, torn.get());
    }
}
