// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure;

import com.openexchange.core.application.engine.CoreEngine;
import com.openexchange.core.application.projection.SettlementProjector;
import com.openexchange.core.domain.ConfigDecision;
import com.openexchange.core.domain.CoreRejectReason;
import com.openexchange.core.domain.FeedHaltReason;
import com.openexchange.core.domain.GateReason;
import com.openexchange.core.domain.OrderFaultKind;
import com.openexchange.core.domain.ReleaseResult;
import com.openexchange.core.domain.ReleaseScope;
import com.openexchange.core.domain.SettleRejectReason;
import com.openexchange.core.infrastructure.generated.AdmissionStateDecoder;
import com.openexchange.core.infrastructure.generated.BoolFlag;
import com.openexchange.core.infrastructure.generated.CoreConfigDecoder;
import com.openexchange.core.infrastructure.generated.DepositEncoder;
import com.openexchange.core.infrastructure.generated.FeedHaltDecoder;
import com.openexchange.core.infrastructure.generated.FeedLatchClearDecoder;
import com.openexchange.core.infrastructure.generated.FeedPositionReportDecoder;
import com.openexchange.core.infrastructure.generated.GateReasonsDecoder;
import com.openexchange.core.infrastructure.generated.MeCommandOutcomeDecoder;
import com.openexchange.core.infrastructure.generated.MeCommandReleaseDecoder;
import com.openexchange.core.infrastructure.generated.MeDispatchBatchDecoder;
import com.openexchange.core.infrastructure.generated.MeDispatchDecoder;
import com.openexchange.core.infrastructure.generated.MeEngineConfigDecoder;
import com.openexchange.core.infrastructure.generated.MeJournalTerminalDecoder;
import com.openexchange.core.infrastructure.generated.MeJournalTradeBatchDecoder;
import com.openexchange.core.infrastructure.generated.MeJournalTradeDecoder;
import com.openexchange.core.infrastructure.generated.MeJournalTradeEncoder;
import com.openexchange.core.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.core.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.core.infrastructure.generated.OrderEventBatchDecoder;
import com.openexchange.core.infrastructure.generated.OrderEventDecoder;
import com.openexchange.core.infrastructure.generated.SessionClass;
import com.openexchange.core.infrastructure.persistence.CoreSbeDemuxer;
import com.openexchange.core.infrastructure.persistence.CoreSbeDemuxerAccess;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Every block length, offset and enum value the money schema freezes, checked against the generated codecs. Version 8
 * added templates and one enum value and moved nothing: the v7 additions still sit after the unchanged v6 prefix of each
 * template, the v8 templates are new ids, the domain enums carry the wire codes. Version 9 appends one field, genesisId,
 * to the replies 116 and 118 after their v8 root block and moves nothing else. Version 10 appends one field,
 * maxWireVersion, to CoreConfig 70 (after genesisId, root block 24 to 26) and to the reply 118 (after genesisId, root
 * block 57 to 59), and moves nothing else. A frame below version 10 is refused by the jar range (schema reject, counted).
 */
public class MoneySchemaV10LayoutTest {

    private static final int HEADER = MessageHeaderEncoder.ENCODED_LENGTH;

    @Test
    public void schemaIdentity() {
        assertEquals(2, MessageHeaderDecoder.SCHEMA_ID);
        assertEquals(10, MessageHeaderDecoder.SCHEMA_VERSION);
        assertEquals("0.10", MessageHeaderDecoder.SEMANTIC_VERSION);
    }

    /** Version 8: the gateway queries 54 to 56, the limits record 77 and the replies 115 to 118. */
    @Test
    public void gatewayQueryTemplates() {
        final String g = "com.openexchange.core.infrastructure.generated.";
        // {decoder, templateId, root block length}
        final Object[][] roots = {
            {"RequestOrderEventsDecoder", 54, 20}, {"RequestUserBalancesDecoder", 55, 16}, {"RequestCoreConfigDecoder", 56, 8},
            {"CoreLimitsSetDecoder", 77, 28}, {"OrderEventsPageDecoder", 115, 8}, {"OrderEventsEndDecoder", 116, 44},
            {"UserBalancesDecoder", 117, 25}, {"CoreConfigReportDecoder", 118, 59},
        };
        for (final Object[] row : roots) {
            try {
                final Class<?> c = Class.forName(g + row[0]);
                assertEquals(row[0] + " templateId", row[1], c.getField("TEMPLATE_ID").get(null));
                assertEquals(row[0] + " block length", row[2], c.getField("BLOCK_LENGTH").get(null));
                assertEquals(row[0] + " is generated from the v10 schema", 10, c.getField("SCHEMA_VERSION").get(null));
                // the ingress templates are in the demuxer's table; the replies are egress and are not
                assertEquals(row[0] + " in the ingress table", (Integer) row[1] < 100 ? row[2] : (Object) (-1),
                        CoreSbeDemuxerAccess.minimumBlockLength((Integer) row[1]));
            } catch (final ReflectiveOperationException e) {
                throw new AssertionError(row[0] + ": " + e);
            }
        }
        // 54
        assertEquals(0, com.openexchange.core.infrastructure.generated.RequestOrderEventsDecoder.correlationIdEncodingOffset());
        assertEquals(8, com.openexchange.core.infrastructure.generated.RequestOrderEventsDecoder.fromEventSeqEncodingOffset());
        assertEquals(16, com.openexchange.core.infrastructure.generated.RequestOrderEventsDecoder.maxEventsEncodingOffset());
        // 55
        assertEquals(8, com.openexchange.core.infrastructure.generated.RequestUserBalancesDecoder.userIdEncodingOffset());
        // 77
        assertEquals(8, com.openexchange.core.infrastructure.generated.CoreLimitsSetDecoder.configVersionEncodingOffset());
        assertEquals(12, com.openexchange.core.infrastructure.generated.CoreLimitsSetDecoder.requestTtlMsEncodingOffset());
        assertEquals(20, com.openexchange.core.infrastructure.generated.CoreLimitsSetDecoder.requestPerUserCapEncodingOffset());
        assertEquals(24, com.openexchange.core.infrastructure.generated.CoreLimitsSetDecoder.orderEventRingCapacityEncodingOffset());
        // 115: an entry is the OrderEvent block, field for field
        assertEquals(OrderEventDecoder.BLOCK_LENGTH,
                com.openexchange.core.infrastructure.generated.OrderEventsPageDecoder.EventsDecoder.sbeBlockLength());
        assertEquals(167, OrderEventDecoder.BLOCK_LENGTH);
        assertEquals(OrderEventBatchDecoder.EventsDecoder.sbeBlockLength(),
                com.openexchange.core.infrastructure.generated.OrderEventsPageDecoder.EventsDecoder.sbeBlockLength());
        final String[] eventFields = {"eventSeq", "logPosition", "orderId", "userId", "marketId", "eventKind", "status", "reason",
            "side", "orderType", "timeInForce", "price", "stopPrice", "quantity", "filledQty", "remainingQty", "lastPrice",
            "lastQty", "tradeId", "meOrderId", "holdAssetId", "holdAmount", "holdRemaining", "clientOrderIdHigh",
            "clientOrderIdLow", "createdAtMs", "updatedAtMs", "meReason"};
        for (final String f : eventFields) {
            try {
                assertEquals("115 entry field " + f + " sits where 102 has it",
                        OrderEventDecoder.class.getMethod(f + "EncodingOffset").invoke(null),
                        com.openexchange.core.infrastructure.generated.OrderEventsPageDecoder.EventsDecoder.class
                                .getMethod(f + "EncodingOffset").invoke(null));
            } catch (final ReflectiveOperationException e) {
                throw new AssertionError(f + ": " + e);
            }
        }
        assertEquals("115 at 44 entries fits the 8192 MTU", 7368, HEADER + 8 + 4 + 44 * 167);
        // 116
        assertEquals(8, com.openexchange.core.infrastructure.generated.OrderEventsEndDecoder.countEncodingOffset());
        assertEquals(12, com.openexchange.core.infrastructure.generated.OrderEventsEndDecoder.firstAvailableSeqEncodingOffset());
        assertEquals(20, com.openexchange.core.infrastructure.generated.OrderEventsEndDecoder.lastSeqEncodingOffset());
        assertEquals(28, com.openexchange.core.infrastructure.generated.OrderEventsEndDecoder.nextFromSeqEncodingOffset());
        // v9: genesisId appended after the v8 block of 36
        assertEquals(36, com.openexchange.core.infrastructure.generated.OrderEventsEndDecoder.genesisIdEncodingOffset());
        assertEquals(9, com.openexchange.core.infrastructure.generated.OrderEventsEndDecoder.genesisIdSinceVersion());
        // 117
        assertEquals(16, com.openexchange.core.infrastructure.generated.UserBalancesDecoder.knownEncodingOffset());
        assertEquals(17, com.openexchange.core.infrastructure.generated.UserBalancesDecoder.asOfLogPositionEncodingOffset());
        assertEquals(20, com.openexchange.core.infrastructure.generated.UserBalancesDecoder.BalancesDecoder.sbeBlockLength());
        // 118
        assertEquals(8, com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder.configVersionEncodingOffset());
        assertEquals(12, com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder.behaviorVersionEncodingOffset());
        assertEquals(16, com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder.limitsVersionEncodingOffset());
        assertEquals(20, com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder.requestTtlMsEncodingOffset());
        assertEquals(28, com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder.requestPerUserCapEncodingOffset());
        assertEquals(32, com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder.orderEventRingCapacityEncodingOffset());
        assertEquals(36, com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder.gateStateEncodingOffset());
        assertEquals(37, com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder.gateReasonsEncodingOffset());
        assertEquals(41, com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder.asOfLogPositionEncodingOffset());
        // v9: genesisId appended to the root block after asOfLogPosition (v8 block 49), before the groups
        assertEquals(49, com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder.genesisIdEncodingOffset());
        assertEquals(9, com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder.genesisIdSinceVersion());
        // v10: maxWireVersion appended after genesisId (v9 block 57), before the groups
        assertEquals(57, com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder.maxWireVersionEncodingOffset());
        assertEquals(2, com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder.maxWireVersionEncodingLength());
        assertEquals(12, com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder.maxWireVersionId());
        assertEquals(10, com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder.maxWireVersionSinceVersion());
        assertEquals(102, com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder.MarketsDecoder.sbeBlockLength());
        assertEquals(60, com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder.MarketsDecoder.minQuantityEncodingOffset());
        assertEquals(92, com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder.MarketsDecoder.riskConfigVersionEncodingOffset());
        assertEquals(96, com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder.MarketsDecoder.gateStateEncodingOffset());
        assertEquals(97, com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder.MarketsDecoder.gateReasonsEncodingOffset());
        assertEquals(101, com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder.MarketsDecoder.circuitBreakerEncodingOffset());
        assertEquals(13, com.openexchange.core.infrastructure.generated.CoreConfigReportDecoder.AssetsDecoder.sbeBlockLength());
        assertEquals("118 with five markets and four assets is one small frame", true, HEADER + 59 + 4 + 5 * 102 + 4 + 4 * 13 < 1408);
        // the new ConfigKind value, wire and domain
        assertEquals(4, com.openexchange.core.infrastructure.generated.ConfigKind.LIMITS.value());
        assertEquals(4, com.openexchange.core.domain.ConfigKind.LIMITS.code());
    }

    /** 4.2: the ME feed templates, appended fields after the v6 block (101, 53, 124). */
    @Test
    public void feedIngressTails() {
        assertEquals(109, MeJournalTradeDecoder.BLOCK_LENGTH);
        assertEquals(101, MeJournalTradeDecoder.journalSeqEncodingOffset());
        assertEquals(15, MeJournalTradeDecoder.journalSeqId());
        assertEquals(7, MeJournalTradeDecoder.journalSeqSinceVersion());

        assertEquals(109, MeJournalTradeBatchDecoder.TradesDecoder.sbeBlockLength());
        assertEquals(101, MeJournalTradeBatchDecoder.TradesDecoder.journalSeqEncodingOffset());
        assertEquals("62 at cap 64 fits the 8192 MTU", 6988, HEADER + 4 + 64 * MeJournalTradeBatchDecoder.TradesDecoder.sbeBlockLength());

        assertEquals(62, MeJournalTerminalDecoder.BLOCK_LENGTH);
        assertEquals(53, MeJournalTerminalDecoder.journalSeqEncodingOffset());
        assertEquals(9, MeJournalTerminalDecoder.journalSeqId());
        assertEquals(61, MeJournalTerminalDecoder.rejectReasonEncodingOffset());
        assertEquals(10, MeJournalTerminalDecoder.rejectReasonId());
        assertEquals(1, MeJournalTerminalDecoder.rejectReasonEncodingLength());

        assertEquals(153, MeCommandOutcomeDecoder.BLOCK_LENGTH);
        assertEquals(124, MeCommandOutcomeDecoder.journalSeqEncodingOffset());
        assertEquals(132, MeCommandOutcomeDecoder.timeInForceEncodingOffset());
        assertEquals(133, MeCommandOutcomeDecoder.expectedOldRemainingEncodingOffset());
        assertEquals(141, MeCommandOutcomeDecoder.ledgerSizeEncodingOffset());
        assertEquals(145, MeCommandOutcomeDecoder.ledgerCapacityEncodingOffset());
        assertEquals(149, MeCommandOutcomeDecoder.behaviorVersionEncodingOffset());
        assertEquals(4, MeCommandOutcomeDecoder.behaviorVersionEncodingLength());
        assertEquals(22, MeCommandOutcomeDecoder.journalSeqId());
        assertEquals(27, MeCommandOutcomeDecoder.behaviorVersionId());
    }

    /** 4.3 / 4.3a: the new templates 66, 67, 68 and 76. */
    @Test
    public void newTemplates() {
        assertEquals(66, FeedHaltDecoder.TEMPLATE_ID);
        assertEquals(41, FeedHaltDecoder.BLOCK_LENGTH);
        assertEquals(0, FeedHaltDecoder.correlationIdEncodingOffset());
        assertEquals(8, FeedHaltDecoder.reasonEncodingOffset());
        assertEquals(9, FeedHaltDecoder.meEpochSeenEncodingOffset());
        assertEquals(17, FeedHaltDecoder.expectedJournalSeqEncodingOffset());
        assertEquals(25, FeedHaltDecoder.seenJournalSeqEncodingOffset());
        assertEquals(33, FeedHaltDecoder.bridgeIdentityEncodingOffset());

        assertEquals(67, MeCommandReleaseDecoder.TEMPLATE_ID);
        assertEquals(44, MeCommandReleaseDecoder.BLOCK_LENGTH);
        assertEquals(0, MeCommandReleaseDecoder.egressSeqEncodingOffset());
        assertEquals(8, MeCommandReleaseDecoder.journalSeqEncodingOffset());
        assertEquals(16, MeCommandReleaseDecoder.meEpochEncodingOffset());
        assertEquals(24, MeCommandReleaseDecoder.releaseSeqEncodingOffset());
        assertEquals(32, MeCommandReleaseDecoder.ledgerSizeEncodingOffset());
        assertEquals(36, MeCommandReleaseDecoder.ledgerCapacityEncodingOffset());
        assertEquals(40, MeCommandReleaseDecoder.tombstoneRangesEncodingOffset());
        assertEquals(20, MeCommandReleaseDecoder.releasesDecoderId());
        assertEquals(22, MeCommandReleaseDecoder.ReleasesDecoder.sbeBlockLength());
        assertEquals(0, MeCommandReleaseDecoder.ReleasesDecoder.commandIdHighEncodingOffset());
        assertEquals(8, MeCommandReleaseDecoder.ReleasesDecoder.idLowFloorEncodingOffset());
        assertEquals(16, MeCommandReleaseDecoder.ReleasesDecoder.scopeEncodingOffset());
        assertEquals(17, MeCommandReleaseDecoder.ReleasesDecoder.resultEncodingOffset());
        assertEquals(18, MeCommandReleaseDecoder.ReleasesDecoder.removedEncodingOffset());
        assertEquals("frame max at cap 48", 1112, HEADER + MeCommandReleaseDecoder.BLOCK_LENGTH + 4
                + 48 * MeCommandReleaseDecoder.ReleasesDecoder.sbeBlockLength());

        assertEquals(68, MeEngineConfigDecoder.TEMPLATE_ID);
        assertEquals(39, MeEngineConfigDecoder.BLOCK_LENGTH);
        assertEquals(0, MeEngineConfigDecoder.egressSeqEncodingOffset());
        assertEquals(8, MeEngineConfigDecoder.journalSeqEncodingOffset());
        assertEquals(16, MeEngineConfigDecoder.meEpochEncodingOffset());
        assertEquals(24, MeEngineConfigDecoder.configVersionEncodingOffset());
        assertEquals(28, MeEngineConfigDecoder.behaviorVersionEncodingOffset());
        assertEquals(32, MeEngineConfigDecoder.ledgerCapacityEncodingOffset());
        assertEquals(36, MeEngineConfigDecoder.maxWireVersionEncodingOffset());
        assertEquals(2, MeEngineConfigDecoder.maxWireVersionEncodingLength());
        assertEquals(38, MeEngineConfigDecoder.decisionEncodingOffset());

        assertEquals(76, FeedLatchClearDecoder.TEMPLATE_ID);
        assertEquals(17, FeedLatchClearDecoder.BLOCK_LENGTH);
        assertEquals(0, FeedLatchClearDecoder.correlationIdEncodingOffset());
        assertEquals(8, FeedLatchClearDecoder.haltReasonEncodingOffset());
        assertEquals(9, FeedLatchClearDecoder.journalSeqEncodingOffset());
    }

    /** 4.4: egress and admin tails. */
    @Test
    public void egressAndAdminTails() {
        assertEquals(43, FeedPositionReportDecoder.BLOCK_LENGTH);
        assertEquals(25, FeedPositionReportDecoder.meEpochEncodingOffset());
        assertEquals(33, FeedPositionReportDecoder.lastAppliedJournalSeqEncodingOffset());
        assertEquals(41, FeedPositionReportDecoder.feedHaltedEncodingOffset());
        assertEquals(42, FeedPositionReportDecoder.haltReasonEncodingOffset());

        assertEquals(26, CoreConfigDecoder.BLOCK_LENGTH);
        assertEquals(16, CoreConfigDecoder.genesisIdEncodingOffset());
        assertEquals(4, CoreConfigDecoder.genesisIdId());
        // v10: the wire ceiling appended after genesisId
        assertEquals(24, CoreConfigDecoder.maxWireVersionEncodingOffset());
        assertEquals(2, CoreConfigDecoder.maxWireVersionEncodingLength());
        assertEquals(5, CoreConfigDecoder.maxWireVersionId());
        assertEquals(10, CoreConfigDecoder.maxWireVersionSinceVersion());
        assertEquals(26, CoreSbeDemuxerAccess.minimumBlockLength(CoreConfigDecoder.TEMPLATE_ID));

        assertEquals(167, OrderEventDecoder.BLOCK_LENGTH);
        assertEquals(166, OrderEventDecoder.meReasonEncodingOffset());
        assertEquals(28, OrderEventDecoder.meReasonId());
        assertEquals(167, OrderEventBatchDecoder.EventsDecoder.sbeBlockLength());
        assertEquals(166, OrderEventBatchDecoder.EventsDecoder.meReasonEncodingOffset());
        assertEquals("103 at cap 48 inside the 8 KiB scratch", 8028, HEADER + 4 + 48 * OrderEventBatchDecoder.EventsDecoder.sbeBlockLength());

        assertEquals(89, MeDispatchDecoder.BLOCK_LENGTH);
        assertEquals(80, MeDispatchDecoder.releaseScopeEncodingOffset());
        assertEquals(81, MeDispatchDecoder.expectedOldRemainingEncodingOffset());
        assertEquals(89, MeDispatchBatchDecoder.DispatchesDecoder.sbeBlockLength());
        assertEquals(80, MeDispatchBatchDecoder.DispatchesDecoder.releaseScopeEncodingOffset());
        assertEquals(81, MeDispatchBatchDecoder.DispatchesDecoder.expectedOldRemainingEncodingOffset());

        assertEquals(27, AdmissionStateDecoder.BLOCK_LENGTH);
        assertEquals(23, AdmissionStateDecoder.gateReasonsEncodingOffset());
        assertEquals(4, GateReasonsDecoder.ENCODED_LENGTH);
    }

    /** 4.1: wire enum values, and the domain enums carry the same codes (the publisher maps by code). */
    @Test
    public void enumValuesAndDomainCodes() {
        assertEquals(7, SessionClass.REPLICA.value());
        assertEquals(7, com.openexchange.core.infrastructure.generated.GateReason.ME_LEG_LIVE.value());
        assertEquals(8, com.openexchange.core.infrastructure.generated.GateReason.ME_BEHAVIOR.value());
        for (final GateReason g : GateReason.values()) {
            assertEquals(g.name(), g.code(), com.openexchange.core.infrastructure.generated.GateReason.valueOf(g.name()).value());
            assertEquals(g, GateReason.fromCode(g.code()));
        }
        assertEquals(9, GateReason.values().length);
        assertEquals(null, GateReason.fromCode(9));

        assertEquals(40, CoreRejectReason.FEED_HALTED.code());
        assertEquals(41, CoreRejectReason.ME_LANE_INACTIVE.code());
        assertEquals(42, CoreRejectReason.ME_INVALID.code());
        for (final CoreRejectReason c : CoreRejectReason.values()) {
            if (c.adminAckOnly()) {
                // An AdminAck-only code (AdminAck.reason is a uint8): never in the schema enum, no schema change
                for (final com.openexchange.core.infrastructure.generated.CoreRejectReason g
                        : com.openexchange.core.infrastructure.generated.CoreRejectReason.values()) {
                    assertTrue(c + " is not a money-schema value", !g.name().equals(c.name()) && g.value() != c.code());
                }
                continue;
            }
            assertEquals(c.name(), c.code(), com.openexchange.core.infrastructure.generated.CoreRejectReason.valueOf(c.name()).value());
        }
        assertEquals(44, CoreRejectReason.DUPLICATE_IDENTITY.code());
        // HALT_HOLDER_PENDING is AdminAck-only, code 45, absent from the schema enum
        final CoreRejectReason pending = CoreRejectReason.valueOf("HALT_HOLDER_PENDING");
        assertEquals(45, pending.code());
        assertTrue("HALT_HOLDER_PENDING is AdminAck-only", pending.adminAckOnly());
        for (final OrderFaultKind k : OrderFaultKind.values()) {
            assertEquals(k.name(), k.code(), com.openexchange.core.infrastructure.generated.OrderFaultKind.valueOf(k.name()).value());
        }
        assertEquals(6, OrderFaultKind.FEED_HALT.code());
        for (final SettleRejectReason r : SettleRejectReason.values()) {
            assertEquals(r.name(), r.ordinal(), com.openexchange.core.infrastructure.generated.SettleRejectReason.valueOf(r.name()).value());
        }
        assertEquals(8, com.openexchange.core.infrastructure.generated.SettleRejectReason.FEED_UNBOUND.value());

        assertEquals(6, FeedHaltReason.values().length);
        for (final FeedHaltReason f : FeedHaltReason.values()) {
            assertEquals(f.name(), f.code(), com.openexchange.core.infrastructure.generated.FeedHaltReason.valueOf(f.name()).value());
        }
        assertEquals(5, FeedHaltReason.SETTLE_REFUSED.code());
        for (final ReleaseScope v : ReleaseScope.values()) {
            assertEquals(v.name(), v.code(), com.openexchange.core.infrastructure.generated.ReleaseScope.valueOf(v.name()).value());
        }
        assertEquals(2, ReleaseScope.values().length);
        for (final ReleaseResult v : ReleaseResult.values()) {
            assertEquals(v.name(), v.code(), com.openexchange.core.infrastructure.generated.ReleaseResult.valueOf(v.name()).value());
        }
        assertEquals(3, ReleaseResult.values().length);
        for (final ConfigDecision v : ConfigDecision.values()) {
            assertEquals(v.name(), v.code(), com.openexchange.core.infrastructure.generated.ConfigDecision.valueOf(v.name()).value());
        }
        assertEquals(4, ConfigDecision.values().length);
        assertEquals(null, ConfigDecision.fromCode(4));

        // GateReasons: one choice per GateReason value, bit = value
        assertTrue(GateReasonsDecoder.admin(1));
        assertTrue(GateReasonsDecoder.applyFault(1 << 6));
        assertTrue(GateReasonsDecoder.meLegLive(1 << 7));
        assertTrue(GateReasonsDecoder.meBehavior(1 << 8));
    }

    /** A v6 to v9 producer's frames are dropped by the jar range (floor 10), counted, never decoded. */
    @Test
    public void v6ShapedFramesAreSchemaRejects() {
        final CoreEngine engine = new CoreEngine();
        engine.setEventSink(new com.openexchange.core.determinism.RecordingCoreSink());
        final CoreSbeDemuxer demuxer = new CoreSbeDemuxer(engine, new SettlementProjector(engine));
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[512]);
        // {templateId, v6 root block}
        final int[][] v6 = {{61, 101}, {63, 53}, {64, 124}, {70, 16}};
        for (final int[] t : v6) {
            buf.setMemory(0, buf.capacity(), (byte) 0);
            new MessageHeaderEncoder().wrap(buf, 0).blockLength(t[1]).templateId(t[0])
                    .schemaId(MessageHeaderEncoder.SCHEMA_ID).version(6);
            final int len = HEADER + t[1] + (t[0] == 70 ? 8 : 0);
            demuxer.dispatchTrusted(buf, 0, len, 1L);
        }
        final int len = HEADER + new DepositEncoder().wrapAndApplyHeader(buf, 0, new MessageHeaderEncoder())
                .correlationId(1).userId(1).assetId(0).amount(1).requestIdHigh(0).requestIdLow(0).encodedLength();
        buf.putShort(6, (short) 6, ByteOrder.LITTLE_ENDIAN);
        demuxer.dispatchTrusted(buf, 0, len, 1L);
        // the same well-formed deposit under a v7, a v8 and a v9 header, and a version this build has not heard of
        buf.putShort(6, (short) 7, ByteOrder.LITTLE_ENDIAN);
        demuxer.dispatchTrusted(buf, 0, len, 1L);
        buf.putShort(6, (short) 8, ByteOrder.LITTLE_ENDIAN);
        demuxer.dispatchTrusted(buf, 0, len, 1L);
        buf.putShort(6, (short) 9, ByteOrder.LITTLE_ENDIAN);
        demuxer.dispatchTrusted(buf, 0, len, 1L);
        buf.putShort(6, (short) 11, ByteOrder.LITTLE_ENDIAN);
        demuxer.dispatchTrusted(buf, 0, len, 1L);
        assertEquals(9, demuxer.schemaRejectCount());
        assertEquals("nothing of a refused frame was applied", 0, engine.accountCount());
        // and under its own version it is applied
        buf.putShort(6, (short) 10, ByteOrder.LITTLE_ENDIAN);
        demuxer.dispatchTrusted(buf, 0, len, 1L);
        assertEquals(9, demuxer.schemaRejectCount());
        assertEquals(1, engine.accountCount());
        engine.account(1).available(0);
        assertEquals(0, demuxer.decodeRejectCount());
    }

    /** The v6 fields of a v7 frame are where a v6 decoder reads them: the prefix bytes equal the v6 layout. */
    @Test
    public void v7FramesKeepTheV6PrefixByteForByte() {
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[256]);
        final MeJournalTradeEncoder e = new MeJournalTradeEncoder().wrapAndApplyHeader(buf, 0, new MessageHeaderEncoder())
                .egressSeq(1).tradeId(2).marketId(3).takerOrderId(4).takerUserId(5).makerOrderId(6).makerUserId(7)
                .price(8).quantity(9).takerIsBuy(BoolFlag.TRUE).meTimestamp(10).takerOmsOrderId(11).makerOmsOrderId(12)
                .meEpoch(13).journalSeq(14);
        assertEquals(109, e.encodedLength());
        // the v6 layout (block 101): meEpoch was the last field at 93
        assertEquals(93, MeJournalTradeDecoder.meEpochEncodingOffset());
        assertEquals(13L, buf.getLong(HEADER + 93, ByteOrder.LITTLE_ENDIAN));
        assertEquals(14L, buf.getLong(HEADER + 101, ByteOrder.LITTLE_ENDIAN));
        final MeJournalTradeDecoder d = new MeJournalTradeDecoder().wrapAndApplyHeader(buf, 0, new MessageHeaderDecoder());
        assertEquals(2L, d.tradeId());
        assertEquals(12L, d.makerOmsOrderId());
        assertEquals(13L, d.meEpoch());
        assertEquals(14L, d.journalSeq());
        // the other appended templates: the last v6 field keeps its v6 offset (taken from the a9a865d codecs) and the
        // first v7 field starts at the v6 block length
        final int[][] lastV6AndFirstV7 = {
            // {last v6 field offset (v6), its v7 offset, v6 block, first v7 field offset}
            {116, MeCommandOutcomeDecoder.meEpochEncodingOffset(), 124, MeCommandOutcomeDecoder.journalSeqEncodingOffset()},
            {45, MeJournalTerminalDecoder.meEpochEncodingOffset(), 53, MeJournalTerminalDecoder.journalSeqEncodingOffset()},
            {24, FeedPositionReportDecoder.journalEnabledEncodingOffset(), 25, FeedPositionReportDecoder.meEpochEncodingOffset()},
            {15, AdmissionStateDecoder.logPositionEncodingOffset(), 23, AdmissionStateDecoder.gateReasonsEncodingOffset()},
            {158, OrderEventDecoder.updatedAtMsEncodingOffset(), 166, OrderEventDecoder.meReasonEncodingOffset()},
            {79, MeDispatchDecoder.timeInForceEncodingOffset(), 80, MeDispatchDecoder.releaseScopeEncodingOffset()},
            {12, CoreConfigDecoder.behaviorVersionEncodingOffset(), 16, CoreConfigDecoder.genesisIdEncodingOffset()},
        };
        for (final int[] row : lastV6AndFirstV7) {
            assertEquals("v6 prefix unchanged", row[0], row[1]);
            assertEquals("v7 appended after the v6 block", row[2], row[3]);
        }
    }
}
