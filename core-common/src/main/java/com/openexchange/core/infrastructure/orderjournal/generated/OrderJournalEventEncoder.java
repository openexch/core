/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.core.infrastructure.orderjournal.generated;

import org.agrona.MutableDirectBuffer;


/**
 * One order event. liquidity: 0 NONE, 1 TAKER, 2 MAKER (set on a FILL with tradeId above 0; TAKER when orderId is the trade's takerOmsOrderId, MAKER when it is the makerOmsOrderId). meEpoch, meJournalSeq, meLogPosition (the ME record's egressSeq) and meTimestampMs name the ME record whose apply emitted the event and are 0 when the event is not ME-caused.
 */
@SuppressWarnings("all")
public final class OrderJournalEventEncoder
{
    public static final int BLOCK_LENGTH = 208;
    public static final int TEMPLATE_ID = 1;
    public static final int SCHEMA_ID = 5;
    public static final int SCHEMA_VERSION = 1;
    public static final String SEMANTIC_VERSION = "0.1";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final OrderJournalEventEncoder parentMessage = this;
    private MutableDirectBuffer buffer;
    private int offset;
    private int limit;

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

    public MutableDirectBuffer buffer()
    {
        return buffer;
    }

    public int offset()
    {
        return offset;
    }

    public OrderJournalEventEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public OrderJournalEventEncoder wrapAndApplyHeader(
        final MutableDirectBuffer buffer, final int offset, final MessageHeaderEncoder headerEncoder)
    {
        headerEncoder
            .wrap(buffer, offset)
            .blockLength(BLOCK_LENGTH)
            .templateId(TEMPLATE_ID)
            .schemaId(SCHEMA_ID)
            .version(SCHEMA_VERSION);

        return wrap(buffer, offset + MessageHeaderEncoder.ENCODED_LENGTH);
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

    public OrderJournalEventEncoder journalSeq(final long value)
    {
        buffer.putLong(offset + 0, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder epoch(final long value)
    {
        buffer.putLong(offset + 8, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder logPosition(final long value)
    {
        buffer.putLong(offset + 16, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder orderId(final long value)
    {
        buffer.putLong(offset + 24, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder userId(final long value)
    {
        buffer.putLong(offset + 32, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder marketId(final int value)
    {
        buffer.putInt(offset + 40, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder eventKind(final short value)
    {
        buffer.putByte(offset + 44, (byte)value);
        return this;
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

    public OrderJournalEventEncoder status(final short value)
    {
        buffer.putByte(offset + 45, (byte)value);
        return this;
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

    public OrderJournalEventEncoder reason(final short value)
    {
        buffer.putByte(offset + 46, (byte)value);
        return this;
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

    public OrderJournalEventEncoder side(final short value)
    {
        buffer.putByte(offset + 47, (byte)value);
        return this;
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

    public OrderJournalEventEncoder orderType(final short value)
    {
        buffer.putByte(offset + 48, (byte)value);
        return this;
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

    public OrderJournalEventEncoder timeInForce(final short value)
    {
        buffer.putByte(offset + 49, (byte)value);
        return this;
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

    public OrderJournalEventEncoder price(final long value)
    {
        buffer.putLong(offset + 50, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder stopPrice(final long value)
    {
        buffer.putLong(offset + 58, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder quantity(final long value)
    {
        buffer.putLong(offset + 66, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder filledQty(final long value)
    {
        buffer.putLong(offset + 74, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder remainingQty(final long value)
    {
        buffer.putLong(offset + 82, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder lastPrice(final long value)
    {
        buffer.putLong(offset + 90, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder lastQty(final long value)
    {
        buffer.putLong(offset + 98, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder tradeId(final long value)
    {
        buffer.putLong(offset + 106, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder meOrderId(final long value)
    {
        buffer.putLong(offset + 114, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder holdAssetId(final int value)
    {
        buffer.putInt(offset + 122, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder holdAmount(final long value)
    {
        buffer.putLong(offset + 126, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder holdRemaining(final long value)
    {
        buffer.putLong(offset + 134, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder clientOrderIdHigh(final long value)
    {
        buffer.putLong(offset + 142, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder clientOrderIdLow(final long value)
    {
        buffer.putLong(offset + 150, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder createdAtMs(final long value)
    {
        buffer.putLong(offset + 158, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder updatedAtMs(final long value)
    {
        buffer.putLong(offset + 166, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder meReason(final short value)
    {
        buffer.putByte(offset + 174, (byte)value);
        return this;
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

    public OrderJournalEventEncoder liquidity(final short value)
    {
        buffer.putByte(offset + 175, (byte)value);
        return this;
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

    public OrderJournalEventEncoder meEpoch(final long value)
    {
        buffer.putLong(offset + 176, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder meJournalSeq(final long value)
    {
        buffer.putLong(offset + 184, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder meLogPosition(final long value)
    {
        buffer.putLong(offset + 192, value, BYTE_ORDER);
        return this;
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

    public OrderJournalEventEncoder meTimestampMs(final long value)
    {
        buffer.putLong(offset + 200, value, BYTE_ORDER);
        return this;
    }


    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        return appendTo(new StringBuilder()).toString();
    }

    public StringBuilder appendTo(final StringBuilder builder)
    {
        if (null == buffer)
        {
            return builder;
        }

        final OrderJournalEventDecoder decoder = new OrderJournalEventDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
