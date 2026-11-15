/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.core.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * ADMIN: the core limits record, replicated and snapshotted, monotonic configVersion (equal version and equal values = DUPLICATE, a version at or below the recorded one = STALE). requestTtlMs: how long a requestId decision is remembered; applies to entries recorded after this record. requestPerUserCap: remembered requests per user above which CreateOrder and AmendOrder are TOO_MANY_PENDING_REQUESTS; applies at once and evicts nothing. orderEventRingCapacity: events kept for RequestOrderEvents; a smaller ring keeps the newest events at the apply, 0 switches replay off. requestTtlMs below 1, requestPerUserCap below 1, a negative orderEventRingCapacity, or one above 12859183 (the most one snapshot section's 32-bit length field can describe; this bounds that field only and does not mean an image holding that many events can be written) is REJECTED reason INVALID. Genesis record: version 0, 900000 ms, 256, 65536. Reply ORIGIN AdminAck{refTemplateId 77} and BROADCAST ConfigApplied{LIMITS, marketId 0}.
 */
@SuppressWarnings("all")
public final class CoreLimitsSetDecoder
{
    public static final int BLOCK_LENGTH = 28;
    public static final int TEMPLATE_ID = 77;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 8;
    public static final String SEMANTIC_VERSION = "0.8";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final CoreLimitsSetDecoder parentMessage = this;
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

    public CoreLimitsSetDecoder wrap(
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

    public CoreLimitsSetDecoder wrapAndApplyHeader(
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

    public CoreLimitsSetDecoder sbeRewind()
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

    public long correlationId()
    {
        return buffer.getLong(offset + 0, BYTE_ORDER);
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

    public long configVersion()
    {
        return (buffer.getInt(offset + 8, BYTE_ORDER) & 0xFFFF_FFFFL);
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

    public long requestTtlMs()
    {
        return buffer.getLong(offset + 12, BYTE_ORDER);
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

    public int requestPerUserCap()
    {
        return buffer.getInt(offset + 20, BYTE_ORDER);
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

    public int orderEventRingCapacity()
    {
        return buffer.getInt(offset + 24, BYTE_ORDER);
    }


    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        final CoreLimitsSetDecoder decoder = new CoreLimitsSetDecoder();
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
        builder.append("[CoreLimitsSet](sbeTemplateId=");
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
        builder.append("correlationId=");
        builder.append(this.correlationId());
        builder.append('|');
        builder.append("configVersion=");
        builder.append(this.configVersion());
        builder.append('|');
        builder.append("requestTtlMs=");
        builder.append(this.requestTtlMs());
        builder.append('|');
        builder.append("requestPerUserCap=");
        builder.append(this.requestPerUserCap());
        builder.append('|');
        builder.append("orderEventRingCapacity=");
        builder.append(this.orderEventRingCapacity());

        limit(originalLimit);

        return builder;
    }
    
    public CoreLimitsSetDecoder sbeSkip()
    {
        sbeRewind();

        return this;
    }
}
