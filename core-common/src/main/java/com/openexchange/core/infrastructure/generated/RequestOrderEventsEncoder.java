/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.core.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * read-only: GATEWAY / ADMIN with an any-user key (the events are exchange-wide; a user-scoped key is NOT_OWNER). Replays order events from the bounded, snapshotted event ring: every event with eventSeq above fromEventSeq (exclusive cursor), in order, as OrderEventsPage frames then one OrderEventsEnd. maxEvents: 0 = head probe (End only, no ring visit), 1..4096 = page size, above 4096 clamped to 4096, negative = OrderReject INVALID naming 54. When fromEventSeq + 1 is below the first event the ring still holds the reply is OrderEventsEnd alone with count 0: an overrun is explicit, never a partial reply. A negative fromEventSeq is answered the same way, as an overrun with nextFromSeq equal to that negative fromEventSeq, even when the ring holds every event; a client starting from the beginning sends 0. A fromEventSeq at or above lastSeq is answered with OrderEventsEnd alone, count 0, nextFromSeq = fromEventSeq; when it is above lastSeq the cursor is ahead of this core (for example a cursor kept across a new genesis), nextFromSeq never becomes equal to lastSeq, and the client must treat the reply as a resync.
 */
@SuppressWarnings("all")
public final class RequestOrderEventsEncoder
{
    public static final int BLOCK_LENGTH = 20;
    public static final int TEMPLATE_ID = 54;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 8;
    public static final String SEMANTIC_VERSION = "0.8";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final RequestOrderEventsEncoder parentMessage = this;
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

    public RequestOrderEventsEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public RequestOrderEventsEncoder wrapAndApplyHeader(
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

    public RequestOrderEventsEncoder correlationId(final long value)
    {
        buffer.putLong(offset + 0, value, BYTE_ORDER);
        return this;
    }


    public static int fromEventSeqId()
    {
        return 2;
    }

    public static int fromEventSeqSinceVersion()
    {
        return 0;
    }

    public static int fromEventSeqEncodingOffset()
    {
        return 8;
    }

    public static int fromEventSeqEncodingLength()
    {
        return 8;
    }

    public static String fromEventSeqMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long fromEventSeqNullValue()
    {
        return -9223372036854775808L;
    }

    public static long fromEventSeqMinValue()
    {
        return -9223372036854775807L;
    }

    public static long fromEventSeqMaxValue()
    {
        return 9223372036854775807L;
    }

    public RequestOrderEventsEncoder fromEventSeq(final long value)
    {
        buffer.putLong(offset + 8, value, BYTE_ORDER);
        return this;
    }


    public static int maxEventsId()
    {
        return 3;
    }

    public static int maxEventsSinceVersion()
    {
        return 0;
    }

    public static int maxEventsEncodingOffset()
    {
        return 16;
    }

    public static int maxEventsEncodingLength()
    {
        return 4;
    }

    public static String maxEventsMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static int maxEventsNullValue()
    {
        return -2147483648;
    }

    public static int maxEventsMinValue()
    {
        return -2147483647;
    }

    public static int maxEventsMaxValue()
    {
        return 2147483647;
    }

    public RequestOrderEventsEncoder maxEvents(final int value)
    {
        buffer.putInt(offset + 16, value, BYTE_ORDER);
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

        final RequestOrderEventsDecoder decoder = new RequestOrderEventsDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
