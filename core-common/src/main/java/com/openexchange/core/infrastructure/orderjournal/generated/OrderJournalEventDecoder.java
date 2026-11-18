/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.core.infrastructure.orderjournal.generated;

import org.agrona.DirectBuffer;


/**
 * One order event. liquidity: 0 NONE, 1 TAKER, 2 MAKER (set on a FILL with tradeId above 0; TAKER when orderId is the trade's takerOmsOrderId, MAKER when it is the makerOmsOrderId). meEpoch, meJournalSeq, meLogPosition (the ME record's egressSeq) and meTimestampMs name the ME record whose apply emitted the event and are 0 when the event is not ME-caused.
 */
@SuppressWarnings("all")
public final class OrderJournalEventDecoder
{
    public static final int BLOCK_LENGTH = 208;
    public static final int TEMPLATE_ID = 1;
    public static final int SCHEMA_ID = 5;
    public static final int SCHEMA_VERSION = 1;
    public static final String SEMANTIC_VERSION = "0.1";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final OrderJournalEventDecoder parentMessage = this;
    private DirectBuffer buffer;
    private int offset;
    private int limit;
    int actingBlockLength;
    int actingVersion;

    public int sbeBlockLength()
    {
        return BLOCK_LENGTH;
    }

    public int sbeTemplateId()
    {
        return TEMPLATE_ID;
    }

    public int sbeSchemaId()
    {
        return SCHEMA_ID;
    }

    public int sbeSchemaVersion()
    {
        return SCHEMA_VERSION;
    }

    public String sbeSemanticType()
    {
        return "";
    }

    public DirectBuffer buffer()
    {
        return buffer;
    }

    public int offset()
    {
        return offset;
    }

    public OrderJournalEventDecoder wrap(
        final DirectBuffer buffer,
        final int offset,
        final int actingBlockLength,
        final int actingVersion)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        this.actingBlockLength = actingBlockLength;
        this.actingVersion = actingVersion;
        limit(offset + actingBlockLength);

        return this;
    }

    public OrderJournalEventDecoder wrapAndApplyHeader(
        final DirectBuffer buffer,
        final int offset,
        final MessageHeaderDecoder headerDecoder)
    {
        headerDecoder.wrap(buffer, offset);

        final int templateId = headerDecoder.templateId();
        if (TEMPLATE_ID != templateId)
        {
            throw new IllegalStateException("Invalid TEMPLATE_ID: " + templateId);
        }

        return wrap(
            buffer,
            offset + MessageHeaderDecoder.ENCODED_LENGTH,
            headerDecoder.blockLength(),
            headerDecoder.version());
    }

    public OrderJournalEventDecoder sbeRewind()
    {
        return wrap(buffer, offset, actingBlockLength, actingVersion);
    }

    public int sbeDecodedLength()
    {
        final int currentLimit = limit();
        sbeSkip();
        final int decodedLength = encodedLength();
        limit(currentLimit);

        return decodedLength;
    }

    public int actingVersion()
    {
        return actingVersion;
    }

    public int encodedLength()
    {
        return limit - offset;
    }

    public int limit()
    {
        return limit;
    }

    public void limit(final int limit)
    {
        this.limit = limit;
    }

    public static int journalSeqId()
    {
        return 1;
    }

    public static int journalSeqSinceVersion()
    {
        return 0;
    }

    public static int journalSeqEncodingOffset()
    {
        return 0;
    }

    public static int journalSeqEncodingLength()
    {
        return 8;
    }

    public static String journalSeqMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long journalSeqNullValue()
    {
        return -9223372036854775808L;
    }

    public static long journalSeqMinValue()
    {
        return -9223372036854775807L;
    }

    public static long journalSeqMaxValue()
    {
        return 9223372036854775807L;
    }

    public long journalSeq()
    {
        return buffer.getLong(offset + 0, BYTE_ORDER);
    }


    public static int epochId()
    {
        return 2;
    }

    public static int epochSinceVersion()
    {
        return 0;
    }

    public static int epochEncodingOffset()
    {
        return 8;
    }

    public static int epochEncodingLength()
    {
        return 8;
    }

    public static String epochMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long epochNullValue()
    {
        return -9223372036854775808L;
    }

    public static long epochMinValue()
    {
        return -9223372036854775807L;
    }

    public static long epochMaxValue()
    {
        return 9223372036854775807L;
    }

    public long epoch()
    {
        return buffer.getLong(offset + 8, BYTE_ORDER);
    }


    public static int logPositionId()
    {
        return 3;
    }

    public static int logPositionSinceVersion()
    {
        return 0;
    }

    public static int logPositionEncodingOffset()
    {
        return 16;
    }

    public static int logPositionEncodingLength()
    {
        return 8;
    }

    public static String logPositionMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long logPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long logPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long logPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public long logPosition()
    {
        return buffer.getLong(offset + 16, BYTE_ORDER);
    }


    public static int orderIdId()
    {
        return 4;
    }

    public static int orderIdSinceVersion()
    {
        return 0;
    }

    public static int orderIdEncodingOffset()
    {
        return 24;
    }

    public static int orderIdEncodingLength()
    {
        return 8;
    }

    public static String orderIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long orderIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long orderIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long orderIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long orderId()
    {
        return buffer.getLong(offset + 24, BYTE_ORDER);
    }


    public static int userIdId()
    {
        return 5;
    }

    public static int userIdSinceVersion()
    {
        return 0;
    }

    public static int userIdEncodingOffset()
    {
        return 32;
    }

    public static int userIdEncodingLength()
    {
        return 8;
    }

    public static String userIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long userIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long userIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long userIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long userId()
    {
        return buffer.getLong(offset + 32, BYTE_ORDER);
    }


    public static int marketIdId()
    {
        return 6;
    }

    public static int marketIdSinceVersion()
    {
        return 0;
    }

    public static int marketIdEncodingOffset()
    {
        return 40;
    }

    public static int marketIdEncodingLength()
    {
        return 4;
    }

    public static String marketIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static int marketIdNullValue()
    {
        return -2147483648;
    }

    public static int marketIdMinValue()
    {
        return -2147483647;
    }

    public static int marketIdMaxValue()
    {
        return 2147483647;
    }

    public int marketId()
    {
        return buffer.getInt(offset + 40, BYTE_ORDER);
    }


    public static int eventKindId()
    {
        return 7;
    }

    public static int eventKindSinceVersion()
    {
        return 0;
    }

    public static int eventKindEncodingOffset()
    {
        return 44;
    }

    public static int eventKindEncodingLength()
    {
        return 1;
    }

    public static String eventKindMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static short eventKindNullValue()
    {
        return (short)255;
    }

    public static short eventKindMinValue()
    {
        return (short)0;
    }

    public static short eventKindMaxValue()
    {
        return (short)254;
    }

    public short eventKind()
    {
        return ((short)(buffer.getByte(offset + 44) & 0xFF));
    }


    public static int statusId()
    {
        return 8;
    }

    public static int statusSinceVersion()
    {
        return 0;
    }

    public static int statusEncodingOffset()
    {
        return 45;
    }

    public static int statusEncodingLength()
    {
        return 1;
    }

    public static String statusMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static short statusNullValue()
    {
        return (short)255;
    }

    public static short statusMinValue()
    {
        return (short)0;
    }

    public static short statusMaxValue()
    {
        return (short)254;
    }

    public short status()
    {
        return ((short)(buffer.getByte(offset + 45) & 0xFF));
    }


    public static int reasonId()
    {
        return 9;
    }

    public static int reasonSinceVersion()
    {
        return 0;
    }

    public static int reasonEncodingOffset()
    {
        return 46;
    }

    public static int reasonEncodingLength()
    {
        return 1;
    }

    public static String reasonMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static short reasonNullValue()
    {
        return (short)255;
    }

    public static short reasonMinValue()
    {
        return (short)0;
    }

    public static short reasonMaxValue()
    {
        return (short)254;
    }

    public short reason()
    {
        return ((short)(buffer.getByte(offset + 46) & 0xFF));
    }


    public static int sideId()
    {
        return 10;
    }

    public static int sideSinceVersion()
    {
        return 0;
    }

    public static int sideEncodingOffset()
    {
        return 47;
    }

    public static int sideEncodingLength()
    {
        return 1;
    }

    public static String sideMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static short sideNullValue()
    {
        return (short)255;
    }

    public static short sideMinValue()
    {
        return (short)0;
    }

    public static short sideMaxValue()
    {
        return (short)254;
    }

    public short side()
    {
        return ((short)(buffer.getByte(offset + 47) & 0xFF));
    }


    public static int orderTypeId()
    {
        return 11;
    }

    public static int orderTypeSinceVersion()
    {
        return 0;
    }

    public static int orderTypeEncodingOffset()
    {
        return 48;
    }

    public static int orderTypeEncodingLength()
    {
        return 1;
    }

    public static String orderTypeMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static short orderTypeNullValue()
    {
        return (short)255;
    }

    public static short orderTypeMinValue()
    {
        return (short)0;
    }

    public static short orderTypeMaxValue()
    {
        return (short)254;
    }

    public short orderType()
    {
        return ((short)(buffer.getByte(offset + 48) & 0xFF));
    }


    public static int timeInForceId()
    {
        return 12;
    }

    public static int timeInForceSinceVersion()
    {
        return 0;
    }

    public static int timeInForceEncodingOffset()
    {
        return 49;
    }

    public static int timeInForceEncodingLength()
    {
        return 1;
    }

    public static String timeInForceMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static short timeInForceNullValue()
    {
        return (short)255;
    }

    public static short timeInForceMinValue()
    {
        return (short)0;
    }

    public static short timeInForceMaxValue()
    {
        return (short)254;
    }

    public short timeInForce()
    {
        return ((short)(buffer.getByte(offset + 49) & 0xFF));
    }


    public static int priceId()
    {
        return 13;
    }

    public static int priceSinceVersion()
    {
        return 0;
    }

    public static int priceEncodingOffset()
    {
        return 50;
    }

    public static int priceEncodingLength()
    {
        return 8;
    }

    public static String priceMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long priceNullValue()
    {
        return -9223372036854775808L;
    }

    public static long priceMinValue()
    {
        return -9223372036854775807L;
    }

    public static long priceMaxValue()
    {
        return 9223372036854775807L;
    }

    public long price()
    {
        return buffer.getLong(offset + 50, BYTE_ORDER);
    }


    public static int stopPriceId()
    {
        return 14;
    }

    public static int stopPriceSinceVersion()
    {
        return 0;
    }

    public static int stopPriceEncodingOffset()
    {
        return 58;
    }

    public static int stopPriceEncodingLength()
    {
        return 8;
    }

    public static String stopPriceMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long stopPriceNullValue()
    {
        return -9223372036854775808L;
    }

    public static long stopPriceMinValue()
    {
        return -9223372036854775807L;
    }

    public static long stopPriceMaxValue()
    {
        return 9223372036854775807L;
    }

    public long stopPrice()
    {
        return buffer.getLong(offset + 58, BYTE_ORDER);
    }


    public static int quantityId()
    {
        return 15;
    }

    public static int quantitySinceVersion()
    {
        return 0;
    }

    public static int quantityEncodingOffset()
    {
        return 66;
    }

    public static int quantityEncodingLength()
    {
        return 8;
    }

    public static String quantityMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long quantityNullValue()
    {
        return -9223372036854775808L;
    }

    public static long quantityMinValue()
    {
        return -9223372036854775807L;
    }

    public static long quantityMaxValue()
    {
        return 9223372036854775807L;
    }

    public long quantity()
    {
        return buffer.getLong(offset + 66, BYTE_ORDER);
    }


    public static int filledQtyId()
    {
        return 16;
    }

    public static int filledQtySinceVersion()
    {
        return 0;
    }

    public static int filledQtyEncodingOffset()
    {
        return 74;
    }

    public static int filledQtyEncodingLength()
    {
        return 8;
    }

    public static String filledQtyMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long filledQtyNullValue()
    {
        return -9223372036854775808L;
    }

    public static long filledQtyMinValue()
    {
        return -9223372036854775807L;
    }

    public static long filledQtyMaxValue()
    {
        return 9223372036854775807L;
    }

    public long filledQty()
    {
        return buffer.getLong(offset + 74, BYTE_ORDER);
    }


    public static int remainingQtyId()
    {
        return 17;
    }

    public static int remainingQtySinceVersion()
    {
        return 0;
    }

    public static int remainingQtyEncodingOffset()
    {
        return 82;
    }

    public static int remainingQtyEncodingLength()
    {
        return 8;
    }

    public static String remainingQtyMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long remainingQtyNullValue()
    {
        return -9223372036854775808L;
    }

    public static long remainingQtyMinValue()
    {
        return -9223372036854775807L;
    }

    public static long remainingQtyMaxValue()
    {
        return 9223372036854775807L;
    }

    public long remainingQty()
    {
        return buffer.getLong(offset + 82, BYTE_ORDER);
    }


    public static int lastPriceId()
    {
        return 18;
    }

    public static int lastPriceSinceVersion()
    {
        return 0;
    }

    public static int lastPriceEncodingOffset()
    {
        return 90;
    }

    public static int lastPriceEncodingLength()
    {
        return 8;
    }

    public static String lastPriceMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long lastPriceNullValue()
    {
        return -9223372036854775808L;
    }

    public static long lastPriceMinValue()
    {
        return -9223372036854775807L;
    }

    public static long lastPriceMaxValue()
    {
        return 9223372036854775807L;
    }

    public long lastPrice()
    {
        return buffer.getLong(offset + 90, BYTE_ORDER);
    }


    public static int lastQtyId()
    {
        return 19;
    }

    public static int lastQtySinceVersion()
    {
        return 0;
    }

    public static int lastQtyEncodingOffset()
    {
        return 98;
    }

    public static int lastQtyEncodingLength()
    {
        return 8;
    }

    public static String lastQtyMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long lastQtyNullValue()
    {
        return -9223372036854775808L;
    }

    public static long lastQtyMinValue()
    {
        return -9223372036854775807L;
    }

    public static long lastQtyMaxValue()
    {
        return 9223372036854775807L;
    }

    public long lastQty()
    {
        return buffer.getLong(offset + 98, BYTE_ORDER);
    }


    public static int tradeIdId()
    {
        return 20;
    }

    public static int tradeIdSinceVersion()
    {
        return 0;
    }

    public static int tradeIdEncodingOffset()
    {
        return 106;
    }

    public static int tradeIdEncodingLength()
    {
        return 8;
    }

    public static String tradeIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long tradeIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long tradeIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long tradeIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long tradeId()
    {
        return buffer.getLong(offset + 106, BYTE_ORDER);
    }


    public static int meOrderIdId()
    {
        return 21;
    }

    public static int meOrderIdSinceVersion()
    {
        return 0;
    }

    public static int meOrderIdEncodingOffset()
    {
        return 114;
    }

    public static int meOrderIdEncodingLength()
    {
        return 8;
    }

    public static String meOrderIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long meOrderIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long meOrderIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long meOrderIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long meOrderId()
    {
        return buffer.getLong(offset + 114, BYTE_ORDER);
    }


    public static int holdAssetIdId()
    {
        return 22;
    }

    public static int holdAssetIdSinceVersion()
    {
        return 0;
    }

    public static int holdAssetIdEncodingOffset()
    {
        return 122;
    }

    public static int holdAssetIdEncodingLength()
    {
        return 4;
    }

    public static String holdAssetIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static int holdAssetIdNullValue()
    {
        return -2147483648;
    }

    public static int holdAssetIdMinValue()
    {
        return -2147483647;
    }

    public static int holdAssetIdMaxValue()
    {
        return 2147483647;
    }

    public int holdAssetId()
    {
        return buffer.getInt(offset + 122, BYTE_ORDER);
    }


    public static int holdAmountId()
    {
        return 23;
    }

    public static int holdAmountSinceVersion()
    {
        return 0;
    }

    public static int holdAmountEncodingOffset()
    {
        return 126;
    }

    public static int holdAmountEncodingLength()
    {
        return 8;
    }

    public static String holdAmountMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long holdAmountNullValue()
    {
        return -9223372036854775808L;
    }

    public static long holdAmountMinValue()
    {
        return -9223372036854775807L;
    }

    public static long holdAmountMaxValue()
    {
        return 9223372036854775807L;
    }

    public long holdAmount()
    {
        return buffer.getLong(offset + 126, BYTE_ORDER);
    }


    public static int holdRemainingId()
    {
        return 24;
    }

    public static int holdRemainingSinceVersion()
    {
        return 0;
    }

    public static int holdRemainingEncodingOffset()
    {
        return 134;
    }

    public static int holdRemainingEncodingLength()
    {
        return 8;
    }

    public static String holdRemainingMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long holdRemainingNullValue()
    {
        return -9223372036854775808L;
    }

    public static long holdRemainingMinValue()
    {
        return -9223372036854775807L;
    }

    public static long holdRemainingMaxValue()
    {
        return 9223372036854775807L;
    }

    public long holdRemaining()
    {
        return buffer.getLong(offset + 134, BYTE_ORDER);
    }


    public static int clientOrderIdHighId()
    {
        return 25;
    }

    public static int clientOrderIdHighSinceVersion()
    {
        return 0;
    }

    public static int clientOrderIdHighEncodingOffset()
    {
        return 142;
    }

    public static int clientOrderIdHighEncodingLength()
    {
        return 8;
    }

    public static String clientOrderIdHighMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long clientOrderIdHighNullValue()
    {
        return -9223372036854775808L;
    }

    public static long clientOrderIdHighMinValue()
    {
        return -9223372036854775807L;
    }

    public static long clientOrderIdHighMaxValue()
    {
        return 9223372036854775807L;
    }

    public long clientOrderIdHigh()
    {
        return buffer.getLong(offset + 142, BYTE_ORDER);
    }


    public static int clientOrderIdLowId()
    {
        return 26;
    }

    public static int clientOrderIdLowSinceVersion()
    {
        return 0;
    }

    public static int clientOrderIdLowEncodingOffset()
    {
        return 150;
    }

    public static int clientOrderIdLowEncodingLength()
    {
        return 8;
    }

    public static String clientOrderIdLowMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long clientOrderIdLowNullValue()
    {
        return -9223372036854775808L;
    }

    public static long clientOrderIdLowMinValue()
    {
        return -9223372036854775807L;
    }

    public static long clientOrderIdLowMaxValue()
    {
        return 9223372036854775807L;
    }

    public long clientOrderIdLow()
    {
        return buffer.getLong(offset + 150, BYTE_ORDER);
    }


    public static int createdAtMsId()
    {
        return 27;
    }

    public static int createdAtMsSinceVersion()
    {
        return 0;
    }

    public static int createdAtMsEncodingOffset()
    {
        return 158;
    }

    public static int createdAtMsEncodingLength()
    {
        return 8;
    }

    public static String createdAtMsMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long createdAtMsNullValue()
    {
        return -9223372036854775808L;
    }

    public static long createdAtMsMinValue()
    {
        return -9223372036854775807L;
    }

    public static long createdAtMsMaxValue()
    {
        return 9223372036854775807L;
    }

    public long createdAtMs()
    {
        return buffer.getLong(offset + 158, BYTE_ORDER);
    }


    public static int updatedAtMsId()
    {
        return 28;
    }

    public static int updatedAtMsSinceVersion()
    {
        return 0;
    }

    public static int updatedAtMsEncodingOffset()
    {
        return 166;
    }

    public static int updatedAtMsEncodingLength()
    {
        return 8;
    }

    public static String updatedAtMsMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long updatedAtMsNullValue()
    {
        return -9223372036854775808L;
    }

    public static long updatedAtMsMinValue()
    {
        return -9223372036854775807L;
    }

    public static long updatedAtMsMaxValue()
    {
        return 9223372036854775807L;
    }

    public long updatedAtMs()
    {
        return buffer.getLong(offset + 166, BYTE_ORDER);
    }


    public static int meReasonId()
    {
        return 29;
    }

    public static int meReasonSinceVersion()
    {
        return 0;
    }

    public static int meReasonEncodingOffset()
    {
        return 174;
    }

    public static int meReasonEncodingLength()
    {
        return 1;
    }

    public static String meReasonMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static short meReasonNullValue()
    {
        return (short)255;
    }

    public static short meReasonMinValue()
    {
        return (short)0;
    }

    public static short meReasonMaxValue()
    {
        return (short)254;
    }

    public short meReason()
    {
        return ((short)(buffer.getByte(offset + 174) & 0xFF));
    }


    public static int liquidityId()
    {
        return 30;
    }

    public static int liquiditySinceVersion()
    {
        return 0;
    }

    public static int liquidityEncodingOffset()
    {
        return 175;
    }

    public static int liquidityEncodingLength()
    {
        return 1;
    }

    public static String liquidityMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static short liquidityNullValue()
    {
        return (short)255;
    }

    public static short liquidityMinValue()
    {
        return (short)0;
    }

    public static short liquidityMaxValue()
    {
        return (short)254;
    }

    public short liquidity()
    {
        return ((short)(buffer.getByte(offset + 175) & 0xFF));
    }


    public static int meEpochId()
    {
        return 31;
    }

    public static int meEpochSinceVersion()
    {
        return 0;
    }

    public static int meEpochEncodingOffset()
    {
        return 176;
    }

    public static int meEpochEncodingLength()
    {
        return 8;
    }

    public static String meEpochMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long meEpochNullValue()
    {
        return -9223372036854775808L;
    }

    public static long meEpochMinValue()
    {
        return -9223372036854775807L;
    }

    public static long meEpochMaxValue()
    {
        return 9223372036854775807L;
    }

    public long meEpoch()
    {
        return buffer.getLong(offset + 176, BYTE_ORDER);
    }


    public static int meJournalSeqId()
    {
        return 32;
    }

    public static int meJournalSeqSinceVersion()
    {
        return 0;
    }

    public static int meJournalSeqEncodingOffset()
    {
        return 184;
    }

    public static int meJournalSeqEncodingLength()
    {
        return 8;
    }

    public static String meJournalSeqMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long meJournalSeqNullValue()
    {
        return -9223372036854775808L;
    }

    public static long meJournalSeqMinValue()
    {
        return -9223372036854775807L;
    }

    public static long meJournalSeqMaxValue()
    {
        return 9223372036854775807L;
    }

    public long meJournalSeq()
    {
        return buffer.getLong(offset + 184, BYTE_ORDER);
    }


    public static int meLogPositionId()
    {
        return 33;
    }

    public static int meLogPositionSinceVersion()
    {
        return 0;
    }

    public static int meLogPositionEncodingOffset()
    {
        return 192;
    }

    public static int meLogPositionEncodingLength()
    {
        return 8;
    }

    public static String meLogPositionMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long meLogPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long meLogPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long meLogPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public long meLogPosition()
    {
        return buffer.getLong(offset + 192, BYTE_ORDER);
    }


    public static int meTimestampMsId()
    {
        return 34;
    }

    public static int meTimestampMsSinceVersion()
    {
        return 0;
    }

    public static int meTimestampMsEncodingOffset()
    {
        return 200;
    }

    public static int meTimestampMsEncodingLength()
    {
        return 8;
    }

    public static String meTimestampMsMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long meTimestampMsNullValue()
    {
        return -9223372036854775808L;
    }

    public static long meTimestampMsMinValue()
    {
        return -9223372036854775807L;
    }

    public static long meTimestampMsMaxValue()
    {
        return 9223372036854775807L;
    }

    public long meTimestampMs()
    {
        return buffer.getLong(offset + 200, BYTE_ORDER);
    }


    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        final OrderJournalEventDecoder decoder = new OrderJournalEventDecoder();
        decoder.wrap(buffer, offset, actingBlockLength, actingVersion);

        return decoder.appendTo(new StringBuilder()).toString();
    }

    public StringBuilder appendTo(final StringBuilder builder)
    {
        if (null == buffer)
        {
            return builder;
        }

        final int originalLimit = limit();
        limit(offset + actingBlockLength);
        builder.append("[OrderJournalEvent](sbeTemplateId=");
        builder.append(TEMPLATE_ID);
        builder.append("|sbeSchemaId=");
        builder.append(SCHEMA_ID);
        builder.append("|sbeSchemaVersion=");
        if (parentMessage.actingVersion != SCHEMA_VERSION)
        {
            builder.append(parentMessage.actingVersion);
            builder.append('/');
        }
        builder.append(SCHEMA_VERSION);
        builder.append("|sbeBlockLength=");
        if (actingBlockLength != BLOCK_LENGTH)
        {
            builder.append(actingBlockLength);
            builder.append('/');
        }
        builder.append(BLOCK_LENGTH);
        builder.append("):");
        builder.append("journalSeq=");
        builder.append(this.journalSeq());
        builder.append('|');
        builder.append("epoch=");
        builder.append(this.epoch());
        builder.append('|');
        builder.append("logPosition=");
        builder.append(this.logPosition());
        builder.append('|');
        builder.append("orderId=");
        builder.append(this.orderId());
        builder.append('|');
        builder.append("userId=");
        builder.append(this.userId());
        builder.append('|');
        builder.append("marketId=");
        builder.append(this.marketId());
        builder.append('|');
        builder.append("eventKind=");
        builder.append(this.eventKind());
        builder.append('|');
        builder.append("status=");
        builder.append(this.status());
        builder.append('|');
        builder.append("reason=");
        builder.append(this.reason());
        builder.append('|');
        builder.append("side=");
        builder.append(this.side());
        builder.append('|');
        builder.append("orderType=");
        builder.append(this.orderType());
        builder.append('|');
        builder.append("timeInForce=");
        builder.append(this.timeInForce());
        builder.append('|');
        builder.append("price=");
        builder.append(this.price());
        builder.append('|');
        builder.append("stopPrice=");
        builder.append(this.stopPrice());
        builder.append('|');
        builder.append("quantity=");
        builder.append(this.quantity());
        builder.append('|');
        builder.append("filledQty=");
        builder.append(this.filledQty());
        builder.append('|');
        builder.append("remainingQty=");
        builder.append(this.remainingQty());
        builder.append('|');
        builder.append("lastPrice=");
        builder.append(this.lastPrice());
        builder.append('|');
        builder.append("lastQty=");
        builder.append(this.lastQty());
        builder.append('|');
        builder.append("tradeId=");
        builder.append(this.tradeId());
        builder.append('|');
        builder.append("meOrderId=");
        builder.append(this.meOrderId());
        builder.append('|');
        builder.append("holdAssetId=");
        builder.append(this.holdAssetId());
        builder.append('|');
        builder.append("holdAmount=");
        builder.append(this.holdAmount());
        builder.append('|');
        builder.append("holdRemaining=");
        builder.append(this.holdRemaining());
        builder.append('|');
        builder.append("clientOrderIdHigh=");
        builder.append(this.clientOrderIdHigh());
        builder.append('|');
        builder.append("clientOrderIdLow=");
        builder.append(this.clientOrderIdLow());
        builder.append('|');
        builder.append("createdAtMs=");
        builder.append(this.createdAtMs());
        builder.append('|');
        builder.append("updatedAtMs=");
        builder.append(this.updatedAtMs());
        builder.append('|');
        builder.append("meReason=");
        builder.append(this.meReason());
        builder.append('|');
        builder.append("liquidity=");
        builder.append(this.liquidity());
        builder.append('|');
        builder.append("meEpoch=");
        builder.append(this.meEpoch());
        builder.append('|');
        builder.append("meJournalSeq=");
        builder.append(this.meJournalSeq());
        builder.append('|');
        builder.append("meLogPosition=");
        builder.append(this.meLogPosition());
        builder.append('|');
        builder.append("meTimestampMs=");
        builder.append(this.meTimestampMs());

        limit(originalLimit);

        return builder;
    }
    
    public OrderJournalEventDecoder sbeSkip()
    {
        sbeRewind();

        return this;
    }
}
