/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.core.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * Terminates a RequestOrderEvents reply. count = events in this reply; firstAvailableSeq = the oldest eventSeq the ring holds (lastSeq + 1 when it holds none); lastSeq = the newest eventSeq the core has emitted; nextFromSeq = the cursor for the next request (fromEventSeq + count). The replay is complete when nextFromSeq equals lastSeq. An overrun is count 0 with firstAvailableSeq above fromEventSeq + 1; a negative fromEventSeq is always answered as an overrun (nextFromSeq negative). nextFromSeq above lastSeq means the request's cursor was ahead of this core: the replay can never complete from it, and the client must treat the reply as a resync.
 */
@SuppressWarnings("all")
public final class OrderEventsEndDecoder
{
    public static final int BLOCK_LENGTH = 44;
    public static final int TEMPLATE_ID = 116;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 10;
    public static final String SEMANTIC_VERSION = "0.10";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final OrderEventsEndDecoder parentMessage = this;
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

    public OrderEventsEndDecoder wrap(
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

    public OrderEventsEndDecoder wrapAndApplyHeader(
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

    public OrderEventsEndDecoder sbeRewind()
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


    public static int countId()
    {
        return 2;
    }

    public static int countSinceVersion()
    {
        return 0;
    }

    public static int countEncodingOffset()
    {
        return 8;
    }

    public static int countEncodingLength()
    {
        return 4;
    }

    public static String countMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static int countNullValue()
    {
        return -2147483648;
    }

    public static int countMinValue()
    {
        return -2147483647;
    }

    public static int countMaxValue()
    {
        return 2147483647;
    }

    public int count()
    {
        return buffer.getInt(offset + 8, BYTE_ORDER);
    }


    public static int firstAvailableSeqId()
    {
        return 3;
    }

    public static int firstAvailableSeqSinceVersion()
    {
        return 0;
    }

    public static int firstAvailableSeqEncodingOffset()
    {
        return 12;
    }

    public static int firstAvailableSeqEncodingLength()
    {
        return 8;
    }

    public static String firstAvailableSeqMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long firstAvailableSeqNullValue()
    {
        return -9223372036854775808L;
    }

    public static long firstAvailableSeqMinValue()
    {
        return -9223372036854775807L;
    }

    public static long firstAvailableSeqMaxValue()
    {
        return 9223372036854775807L;
    }

    public long firstAvailableSeq()
    {
        return buffer.getLong(offset + 12, BYTE_ORDER);
    }


    public static int lastSeqId()
    {
        return 4;
    }

    public static int lastSeqSinceVersion()
    {
        return 0;
    }

    public static int lastSeqEncodingOffset()
    {
        return 20;
    }

    public static int lastSeqEncodingLength()
    {
        return 8;
    }

    public static String lastSeqMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long lastSeqNullValue()
    {
        return -9223372036854775808L;
    }

    public static long lastSeqMinValue()
    {
        return -9223372036854775807L;
    }

    public static long lastSeqMaxValue()
    {
        return 9223372036854775807L;
    }

    public long lastSeq()
    {
        return buffer.getLong(offset + 20, BYTE_ORDER);
    }


    public static int nextFromSeqId()
    {
        return 5;
    }

    public static int nextFromSeqSinceVersion()
    {
        return 0;
    }

    public static int nextFromSeqEncodingOffset()
    {
        return 28;
    }

    public static int nextFromSeqEncodingLength()
    {
        return 8;
    }

    public static String nextFromSeqMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long nextFromSeqNullValue()
    {
        return -9223372036854775808L;
    }

    public static long nextFromSeqMinValue()
    {
        return -9223372036854775807L;
    }

    public static long nextFromSeqMaxValue()
    {
        return 9223372036854775807L;
    }

    public long nextFromSeq()
    {
        return buffer.getLong(offset + 28, BYTE_ORDER);
    }


    public static int genesisIdId()
    {
        return 6;
    }

    public static int genesisIdSinceVersion()
    {
        return 9;
    }

    public static int genesisIdEncodingOffset()
    {
        return 36;
    }

    public static int genesisIdEncodingLength()
    {
        return 8;
    }

    public static String genesisIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long genesisIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long genesisIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long genesisIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long genesisId()
    {
        if (parentMessage.actingVersion < 9)
        {
            return -9223372036854775808L;
        }

        return buffer.getLong(offset + 36, BYTE_ORDER);
    }


    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        final OrderEventsEndDecoder decoder = new OrderEventsEndDecoder();
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
        builder.append("[OrderEventsEnd](sbeTemplateId=");
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
        builder.append("count=");
        builder.append(this.count());
        builder.append('|');
        builder.append("firstAvailableSeq=");
        builder.append(this.firstAvailableSeq());
        builder.append('|');
        builder.append("lastSeq=");
        builder.append(this.lastSeq());
        builder.append('|');
        builder.append("nextFromSeq=");
        builder.append(this.nextFromSeq());
        builder.append('|');
        builder.append("genesisId=");
        builder.append(this.genesisId());

        limit(originalLimit);

        return builder;
    }
    
    public OrderEventsEndDecoder sbeSkip()
    {
        sbeRewind();

        return this;
    }
}
