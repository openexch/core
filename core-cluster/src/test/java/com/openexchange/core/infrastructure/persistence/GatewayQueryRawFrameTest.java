// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.core.application.engine.feed.V3Rig;
import com.openexchange.core.infrastructure.generated.MessageHeaderEncoder;
import org.junit.Test;

import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The four ingress templates a gateway needs beyond order entry, written as raw bytes at their frozen offsets (no
 * generated codec, so a layout change in the schema breaks this file): RequestOrderEvents 54, RequestUserBalances 55,
 * RequestCoreConfig 56 and the ADMIN record CoreLimitsSet 77. Each must be a known template that the core answers;
 * a frame that is dropped as unknown has no reply, which a gateway can only report as an unknown outcome.
 */
public class GatewayQueryRawFrameTest {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;
    private static final int HEADER = 8;

    private static int header(final V3Rig r, final int templateId, final int blockLength) {
        r.buffer().setMemory(0, HEADER + blockLength, (byte) 0);
        r.buffer().putShort(0, (short) blockLength, LE);
        r.buffer().putShort(2, (short) templateId, LE);
        r.buffer().putShort(4, (short) MessageHeaderEncoder.SCHEMA_ID, LE);
        r.buffer().putShort(6, (short) MessageHeaderEncoder.SCHEMA_VERSION, LE);
        return HEADER + blockLength;
    }

    private static void assertAnswered(final V3Rig r, final int templateId, final int length) {
        r.mark();
        final long unknownBefore = r.demuxer.unknownTemplateCount();
        final long decodeBefore = r.demuxer.decodeRejectCount();
        r.dispatchTrusted(length);
        assertEquals("template " + templateId + " is not a known template", unknownBefore, r.demuxer.unknownTemplateCount());
        assertEquals("template " + templateId + " was dropped at decode", decodeBefore, r.demuxer.decodeRejectCount());
        assertTrue("template " + templateId + " was not answered", !r.events().isEmpty());
    }

    @Test
    public void requestOrderEventsIsAnswered() {
        final V3Rig r = V3Rig.open();
        final int length = header(r, 54, 20);
        r.buffer().putLong(HEADER, 541L, LE);      // correlationId
        r.buffer().putLong(HEADER + 8, 0L, LE);    // fromEventSeq (exclusive)
        r.buffer().putInt(HEADER + 16, 16, LE);    // maxEvents
        assertAnswered(r, 54, length);
    }

    @Test
    public void requestUserBalancesIsAnswered() {
        final V3Rig r = V3Rig.open();
        r.deposit(1, 0, 100_000_000L);
        final int length = header(r, 55, 16);
        r.buffer().putLong(HEADER, 551L, LE);      // correlationId
        r.buffer().putLong(HEADER + 8, 1L, LE);    // userId
        assertAnswered(r, 55, length);
    }

    @Test
    public void requestCoreConfigIsAnswered() {
        final V3Rig r = V3Rig.open();
        final int length = header(r, 56, 8);
        r.buffer().putLong(HEADER, 561L, LE);      // correlationId
        assertAnswered(r, 56, length);
    }

    @Test
    public void coreLimitsSetIsAnswered() {
        final V3Rig r = V3Rig.open();
        final int length = header(r, 77, 28);
        r.buffer().putLong(HEADER, 771L, LE);          // correlationId
        r.buffer().putInt(HEADER + 8, 1, LE);          // configVersion
        r.buffer().putLong(HEADER + 12, 900_000L, LE); // requestTtlMs
        r.buffer().putInt(HEADER + 20, 256, LE);       // requestPerUserCap
        r.buffer().putInt(HEADER + 24, 65_536, LE);    // orderEventRingCapacity
        assertAnswered(r, 77, length);
    }
}
