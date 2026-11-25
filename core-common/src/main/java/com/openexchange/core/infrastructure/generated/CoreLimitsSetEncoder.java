/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.core.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * ADMIN: the core limits record, replicated and snapshotted, monotonic configVersion (equal version and equal values = DUPLICATE, a version at or below the recorded one = STALE). requestTtlMs: how long a requestId decision is remembered; applies to entries recorded after this record. requestPerUserCap: remembered requests per user above which CreateOrder and AmendOrder are TOO_MANY_PENDING_REQUESTS; applies at once and evicts nothing. orderEventRingCapacity: events kept for RequestOrderEvents; a smaller ring keeps the newest events at the apply, 0 switches replay off. requestTtlMs below 1, requestPerUserCap below 1, a negative orderEventRingCapacity, or one above 12859183 (the most one snapshot section's 32-bit length field can describe; this bounds that field only and does not mean an image holding that many events can be written) is REJECTED reason INVALID. Genesis record: version 0, 900000 ms, 256, 65536. Reply ORIGIN AdminAck{refTemplateId 77} and BROADCAST ConfigApplied{LIMITS, marketId 0}.
 */
@SuppressWarnings("all")
public final class CoreLimitsSetEncoder
{
    public static final int BLOCK_LENGTH = 28;
    public static final int TEMPLATE_ID = 77;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 10;
    public static final String SEMANTIC_VERSION = "0.10";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final CoreLimitsSetEncoder parentMessage = this;
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

    public CoreLimitsSetEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public CoreLimitsSetEncoder wrapAndApplyHeader(
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

    public static int correlationIdId()
    {
        return 1;
    }

    public static int correlationIdSinceVersion()
    {
        return 0;
    }

    public static int correlationIdEncodingOffset()
    {
        return 0;
    }

    public static int correlationIdEncodingLength()
    {
        return 8;
    }

    public static String correlationIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long correlationIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long correlationIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long correlationIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public CoreLimitsSetEncoder correlationId(final long value)
    {
        buffer.putLong(offset + 0, value, BYTE_ORDER);
        return this;
    }


    public static int configVersionId()
    {
        return 2;
    }

    public static int configVersionSinceVersion()
    {
        return 0;
    }

    public static int configVersionEncodingOffset()
    {
        return 8;
    }

    public static int configVersionEncodingLength()
    {
        return 4;
    }

    public static String configVersionMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long configVersionNullValue()
    {
        return 4294967295L;
    }

    public static long configVersionMinValue()
    {
        return 0L;
    }

    public static long configVersionMaxValue()
    {
        return 4294967294L;
    }

    public CoreLimitsSetEncoder configVersion(final long value)
    {
        buffer.putInt(offset + 8, (int)value, BYTE_ORDER);
        return this;
    }


    public static int requestTtlMsId()
    {
        return 3;
    }

    public static int requestTtlMsSinceVersion()
    {
        return 0;
    }

    public static int requestTtlMsEncodingOffset()
    {
        return 12;
    }

    public static int requestTtlMsEncodingLength()
    {
        return 8;
    }

    public static String requestTtlMsMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long requestTtlMsNullValue()
    {
        return -9223372036854775808L;
    }

    public static long requestTtlMsMinValue()
    {
        return -9223372036854775807L;
    }

    public static long requestTtlMsMaxValue()
    {
        return 9223372036854775807L;
    }

    public CoreLimitsSetEncoder requestTtlMs(final long value)
    {
        buffer.putLong(offset + 12, value, BYTE_ORDER);
        return this;
    }


    public static int requestPerUserCapId()
    {
        return 4;
    }

    public static int requestPerUserCapSinceVersion()
    {
        return 0;
    }

    public static int requestPerUserCapEncodingOffset()
    {
        return 20;
    }

    public static int requestPerUserCapEncodingLength()
    {
        return 4;
    }

    public static String requestPerUserCapMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static int requestPerUserCapNullValue()
    {
        return -2147483648;
    }

    public static int requestPerUserCapMinValue()
    {
        return -2147483647;
    }

    public static int requestPerUserCapMaxValue()
    {
        return 2147483647;
    }

    public CoreLimitsSetEncoder requestPerUserCap(final int value)
    {
        buffer.putInt(offset + 20, value, BYTE_ORDER);
        return this;
    }


    public static int orderEventRingCapacityId()
    {
        return 5;
    }

    public static int orderEventRingCapacitySinceVersion()
    {
        return 0;
    }

    public static int orderEventRingCapacityEncodingOffset()
    {
        return 24;
    }

    public static int orderEventRingCapacityEncodingLength()
    {
        return 4;
    }

    public static String orderEventRingCapacityMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static int orderEventRingCapacityNullValue()
    {
        return -2147483648;
    }

    public static int orderEventRingCapacityMinValue()
    {
        return -2147483647;
    }

    public static int orderEventRingCapacityMaxValue()
    {
        return 2147483647;
    }

    public CoreLimitsSetEncoder orderEventRingCapacity(final int value)
    {
        buffer.putInt(offset + 24, value, BYTE_ORDER);
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

        final CoreLimitsSetDecoder decoder = new CoreLimitsSetDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
