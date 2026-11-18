/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.core.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * Terminates a RequestOrderEvents reply. count = events in this reply; firstAvailableSeq = the oldest eventSeq the ring holds (lastSeq + 1 when it holds none); lastSeq = the newest eventSeq the core has emitted; nextFromSeq = the cursor for the next request (fromEventSeq + count). The replay is complete when nextFromSeq equals lastSeq. An overrun is count 0 with firstAvailableSeq above fromEventSeq + 1; a negative fromEventSeq is always answered as an overrun (nextFromSeq negative). nextFromSeq above lastSeq means the request's cursor was ahead of this core: the replay can never complete from it, and the client must treat the reply as a resync.
 */
@SuppressWarnings("all")
public final class OrderEventsEndEncoder
{
    public static final int BLOCK_LENGTH = 44;
    public static final int TEMPLATE_ID = 116;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 9;
    public static final String SEMANTIC_VERSION = "0.9";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final OrderEventsEndEncoder parentMessage = this;
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

    public OrderEventsEndEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public OrderEventsEndEncoder wrapAndApplyHeader(
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

    public OrderEventsEndEncoder correlationId(final long value)
    {
        buffer.putLong(offset + 0, value, BYTE_ORDER);
        return this;
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

    public OrderEventsEndEncoder count(final int value)
    {
        buffer.putInt(offset + 8, value, BYTE_ORDER);
        return this;
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

    public OrderEventsEndEncoder firstAvailableSeq(final long value)
    {
        buffer.putLong(offset + 12, value, BYTE_ORDER);
        return this;
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

    public OrderEventsEndEncoder lastSeq(final long value)
    {
        buffer.putLong(offset + 20, value, BYTE_ORDER);
        return this;
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

    public OrderEventsEndEncoder nextFromSeq(final long value)
    {
        buffer.putLong(offset + 28, value, BYTE_ORDER);
        return this;
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

    public OrderEventsEndEncoder genesisId(final long value)
    {
        buffer.putLong(offset + 36, value, BYTE_ORDER);
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

        final OrderEventsEndDecoder decoder = new OrderEventsEndDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
