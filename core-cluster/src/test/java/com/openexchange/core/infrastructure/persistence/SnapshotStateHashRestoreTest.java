// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.cluster.SnapshotEnvelope;
import com.openexchange.core.application.engine.CoreEngine;
import com.openexchange.core.domain.commands.DepositCommand;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

/**
 * A full-image restore reports the loaded image's state hash (the CRC32C the kit envelope carries over the payload,
 * unsigned decimal), so a restarted node shows the identity of the snapshot it runs on. Off the cluster:
 * {@code cluster} is null here, so the position reads -1; the cluster test checks the position at load.
 */
public class SnapshotStateHashRestoreTest {

    private static void deposit(final CoreEngine engine, final long userId, final long amount, final long position) {
        final DepositCommand d = new DepositCommand();
        d.reset();
        d.setUserId(userId);
        d.setAssetId(0);
        d.setAmount(amount);
        engine.applyCommand(CoreEngine.CMD_DEPOSIT, d, position);
    }

    private static final class Image {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        int length;
    }

    /** A service whose engine applied {@code extra + 2} deposits, and its snapshot image. */
    private static Image imageOf(final int extra) {
        final CoreClusteredService writer = new CoreClusteredService();
        writer.failFast = () -> { };
        writer.engine().setEventSink(new SnapshotEnvelopeAdoptionTest.NoopSink());
        deposit(writer.engine(), 7, 1_000, 10);
        deposit(writer.engine(), 8, 2_000, 11);
        for (int i = 0; i < extra; i++) {
            deposit(writer.engine(), 9, 1, 12 + i);
        }
        final Image image = new Image();
        image.length = CoreClusteredService.encodeSnapshot(writer.engine(), image.buffer, new SnapshotEnvelope());
        return image;
    }

    private static String restoredHash(final Image image) {
        final CoreClusteredService reader = new CoreClusteredService();
        final int[] stops = new int[1];
        reader.failFast = () -> stops[0]++;
        reader.restoreFromImage(image.buffer, image.length);
        assertEquals("the image restores", 0, stops[0]);
        return ServiceProbe.seriesValue(ServiceProbe.nodeMetricsText(reader), "core_snapshot_state_hash");
    }

    @Test
    public void aFullRestoreReportsTheImageCrcAsTheStateHash() {
        final Image image = imageOf(0);
        final long crc = SnapshotEnvelope.sealedCrc(image.buffer, image.length);
        final CoreClusteredService reader = new CoreClusteredService();
        reader.failFast = () -> { };
        reader.restoreFromImage(image.buffer, image.length);
        final String body = ServiceProbe.nodeMetricsText(reader);
        assertEquals(body, Long.toString(crc), ServiceProbe.seriesValue(body, "core_snapshot_state_hash"));
        assertEquals("a restore is not a snapshot this node took", "-1",
                ServiceProbe.seriesValue(body, "core_snapshot_age_seconds"));
        assertEquals("a restore is not a snapshot this node took", "-1",
                ServiceProbe.seriesValue(body, "core_snapshot_last_bytes"));
    }

    @Test
    public void theSameStateRestoresToTheSameHashAndOneMoreAppliedCommandToAnother() {
        final String a = restoredHash(imageOf(1));
        final String b = restoredHash(imageOf(1));
        final String c = restoredHash(imageOf(2));
        assertEquals("two engines with the same applied commands report the same hash", a, b);
        assertNotEquals("one more applied command changes the hash", a, c);
    }

    @Test
    public void beforeAnySnapshotTheHashSeriesIsOmittedAndThePositionReadsMinusOne() {
        final String body = ServiceProbe.nodeMetricsText(new CoreClusteredService());
        assertNull(body, ServiceProbe.seriesValue(body, "core_snapshot_state_hash"));
        assertEquals(body, "-1", ServiceProbe.seriesValue(body, "core_snapshot_last_log_position"));
    }
}
