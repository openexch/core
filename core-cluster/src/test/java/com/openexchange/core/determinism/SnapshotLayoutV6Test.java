// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.determinism;

import com.openexchange.core.application.engine.CoreEngine;
import com.openexchange.core.application.engine.order.OrderEventRing;
import com.openexchange.core.infrastructure.persistence.BalanceSnapshotCodec;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Snapshot layout v6: the tag is -6, the limits record is section 13 and the order-event ring is section 14, both
 * mandatory on every behaviour. A v5 image is refused (the cut is a clean genesis). A ring section that disagrees with
 * the limits record, with the image's eventSeq or with its own records is refused, never half restored.
 */
public class SnapshotLayoutV6Test {

    private static final List<String> STORY = List.of(
            "CLOCK 1700000000000",
            "V3GENESIS behavior=4",
            "GATE market=0 open=1",
            "LIMITS ver=1 ttlMs=600000 cap=40 ring=5",
            "DEPOSIT u=1 asset=0 amt=100000.0",
            "CREATE u=1 market=1 side=0 type=0 px=100.0 qty=1.0 rid=1",
            "CREATE u=1 market=1 side=0 type=0 px=101.0 qty=1.0 rid=2",
            "CREATE u=1 market=1 side=0 type=0 px=102.0 qty=1.0 rid=3",
            "CREATE u=1 market=1 side=0 type=0 px=103.0 qty=1.0 rid=4",
            "CREATE u=1 market=1 side=0 type=0 px=104.0 qty=1.0 rid=5",
            "CREATE u=1 market=1 side=0 type=0 px=105.0 qty=1.0 rid=6",
            "CREATE u=1 market=1 side=0 type=0 px=106.0 qty=1.0 rid=7");

    private static byte[] image(final CoreEngine engine) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final int length = BalanceSnapshotCodec.serialize(engine, buf, 0); // first: the buffer grows while it is written
        return Arrays.copyOf(buf.byteArray(), length);
    }

    private static CoreEngine restore(final byte[] image) {
        final CoreEngine fresh = new CoreEngine();
        BalanceSnapshotCodec.deserialize(new UnsafeBuffer(image), 0, image.length, fresh);
        fresh.verifyRestoredInvariants();
        return fresh;
    }

    private static CoreEngine story() {
        final CoreScenarioRunner runner = CoreScenarioRunner.newRunner();
        runner.execAll(STORY);
        return runner.engine();
    }

    /** {offset of the section body, body length} for section {@code id}; fails when absent. */
    private static int[] section(final byte[] image, final int id) {
        final UnsafeBuffer b = new UnsafeBuffer(image);
        int p = 8 + 7 * 8 + 1;
        final int count = b.getInt(p);
        p += 4;
        for (int i = 0; i < count; i++) {
            final int sectionId = b.getShort(p);
            final int length = b.getInt(p + 2);
            if (sectionId == id) {
                return new int[] {p + 6, length};
            }
            p += 6 + length;
        }
        throw new AssertionError("no section " + id + " in the image");
    }

    private static void assertRefused(final byte[] image, final String text) {
        try {
            restore(image);
        } catch (final IllegalStateException expected) {
            assertTrue("refusal names '" + text + "', got: " + expected.getMessage(), expected.getMessage().contains(text));
            return;
        }
        fail("the image was restored; expected a refusal naming '" + text + "'");
    }

    @Test
    public void theImageCarriesTheTagTheLimitsAndTheWrappedRing() {
        final CoreEngine engine = story();
        final byte[] image = image(engine);
        final UnsafeBuffer b = new UnsafeBuffer(image);
        assertEquals(-6L, BalanceSnapshotCodec.LAYOUT_V6_TAG);
        assertEquals("the v6 tag leads the payload", BalanceSnapshotCodec.LAYOUT_V6_TAG, b.getLong(0));
        final int[] limits = section(image, BalanceSnapshotCodec.SECTION_LIMITS);
        assertEquals(BalanceSnapshotCodec.LIMITS_BYTES, limits[1]);
        assertEquals(1L, b.getLong(limits[0]));
        assertEquals(600_000L, b.getLong(limits[0] + 8));
        assertEquals(40, b.getInt(limits[0] + 16));
        assertEquals(5, b.getInt(limits[0] + 20));
        final int[] ring = section(image, BalanceSnapshotCodec.SECTION_ORDER_EVENT_RING);
        assertEquals("seven events through a ring of five", 7L, engine.getEventSeq());
        assertEquals(BalanceSnapshotCodec.ORDER_EVENT_RING_HEAD_BYTES + 5 * OrderEventRing.RECORD_BYTES, ring[1]);
        assertEquals("capacity", 5, b.getInt(ring[0]));
        assertEquals("firstSeq", 3L, b.getLong(ring[0] + 4));
        assertEquals("count", 5, b.getInt(ring[0] + 12));
        assertEquals(167, OrderEventRing.RECORD_BYTES);
        for (int i = 0; i < 5; i++) {
            assertEquals("records in ascending eventSeq", 3L + i, b.getLong(ring[0] + 16 + i * OrderEventRing.RECORD_BYTES,
                    java.nio.ByteOrder.LITTLE_ENDIAN));
        }
    }

    @Test
    public void restoreThenSerializeIsTheSameImageAndTheSameReplay() {
        final CoreEngine engine = story();
        final byte[] image = image(engine);
        final CoreEngine restored = restore(image);
        assertArrayEquals("the image is a function of state", image, image(restored));
        final RecordingCoreSink a = new RecordingCoreSink();
        final RecordingCoreSink b = new RecordingCoreSink();
        engine.setEventSink(a);
        restored.setEventSink(b);
        for (final CoreEngine e : new CoreEngine[] {engine, restored}) {
            e.orders().applyRequestOrderEvents(1L, 2, 1_024);
            e.orders().applyRequestOrderEvents(2L, 0, 1_024);
            e.requestCoreConfig(3L);
        }
        assertEquals("the replies are equal, record bytes included", a.events(), b.events());
        assertEquals("five replayed, two terminators, one report", 8, a.events().size());
    }

    @Test
    public void aV5ImageIsRefused() {
        final byte[] image = image(story());
        new UnsafeBuffer(image).putLong(0, BalanceSnapshotCodec.LAYOUT_V5_TAG);
        assertRefused(image, "reads only v6");
    }

    @Test
    public void anImageWithoutTheNewSectionsIsRefused() {
        final byte[] image = image(story());
        final UnsafeBuffer b = new UnsafeBuffer(image);
        final int countAt = 8 + 7 * 8 + 1;
        final int[] ring = section(image, BalanceSnapshotCodec.SECTION_ORDER_EVENT_RING);
        final byte[] noRing = Arrays.copyOf(image, ring[0] - 6);
        new UnsafeBuffer(noRing).putInt(countAt, b.getInt(countAt) - 1);
        assertRefused(noRing, "no ORDER_EVENT_RING section (14)");
        final int[] limits = section(image, BalanceSnapshotCodec.SECTION_LIMITS);
        final byte[] neither = Arrays.copyOf(image, limits[0] - 6);
        new UnsafeBuffer(neither).putInt(countAt, b.getInt(countAt) - 2);
        assertRefused(neither, "no LIMITS section (13)");
    }

    @Test
    public void aRingThatDisagreesWithTheImageIsRefused() {
        final byte[] image = image(story());
        final int[] ring = section(image, BalanceSnapshotCodec.SECTION_ORDER_EVENT_RING);
        final int[] limits = section(image, BalanceSnapshotCodec.SECTION_LIMITS);

        byte[] bad = image.clone();
        new UnsafeBuffer(bad).putInt(ring[0], 6); // capacity differs from the limits record
        assertRefused(bad, "ORDER_EVENT_RING section: capacity 6 (limits 5)");

        bad = image.clone();
        new UnsafeBuffer(bad).putLong(ring[0] + 4, 2L); // the records would not end at the image's eventSeq
        assertRefused(bad, "ORDER_EVENT_RING section");

        bad = image.clone();
        new UnsafeBuffer(bad).putLong(ring[0] + 16 + OrderEventRing.RECORD_BYTES, 99L, java.nio.ByteOrder.LITTLE_ENDIAN);
        assertRefused(bad, "record 1 carries eventSeq 99, expected 4");

        bad = image.clone();
        new UnsafeBuffer(bad).putInt(limits[0] + 16, 0); // a per-user cap below 1
        assertRefused(bad, "LIMITS section");

        bad = image.clone();
        new UnsafeBuffer(bad).putLong(limits[0] + 8, 0L); // a TTL below 1
        assertRefused(bad, "LIMITS section");
    }

    @Test
    public void aGenesisImageCarriesTheDefaultRecordAndAnEmptyRing() {
        final byte[] image = image(new CoreEngine());
        final UnsafeBuffer b = new UnsafeBuffer(image);
        final int[] limits = section(image, BalanceSnapshotCodec.SECTION_LIMITS);
        assertEquals(0L, b.getLong(limits[0]));
        assertEquals(900_000L, b.getLong(limits[0] + 8));
        assertEquals(256, b.getInt(limits[0] + 16));
        assertEquals(65_536, b.getInt(limits[0] + 20));
        final int[] ring = section(image, BalanceSnapshotCodec.SECTION_ORDER_EVENT_RING);
        assertEquals("an empty ring is its head alone", BalanceSnapshotCodec.ORDER_EVENT_RING_HEAD_BYTES, ring[1]);
        assertEquals(1L, b.getLong(ring[0] + 4));
        assertArrayEquals(image, image(restore(image)));
    }
}
