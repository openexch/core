/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.core.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * ORIGIN reply to RequestUserBalances, one frame. known = an account record exists for the user. balances: one entry per asset with a nonzero available or locked amount, ascending assetId; an absent asset is zero.
 */
@SuppressWarnings("all")
public final class UserBalancesEncoder
{
    public static final int BLOCK_LENGTH = 25;
    public static final int TEMPLATE_ID = 117;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 10;
    public static final String SEMANTIC_VERSION = "0.10";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final UserBalancesEncoder parentMessage = this;
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

    public UserBalancesEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public UserBalancesEncoder wrapAndApplyHeader(
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

    public UserBalancesEncoder correlationId(final long value)
    {
        buffer.putLong(offset + 0, value, BYTE_ORDER);
        return this;
    }


    public static int userIdId()
    {
        return 2;
    }

    public static int userIdSinceVersion()
    {
        return 0;
    }

    public static int userIdEncodingOffset()
    {
        return 8;
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

    public UserBalancesEncoder userId(final long value)
    {
        buffer.putLong(offset + 8, value, BYTE_ORDER);
        return this;
    }


    public static int knownId()
    {
        return 3;
    }

    public static int knownSinceVersion()
    {
        return 0;
    }

    public static int knownEncodingOffset()
    {
        return 16;
    }

    public static int knownEncodingLength()
    {
        return 1;
    }

    public static String knownMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public UserBalancesEncoder known(final BoolFlag value)
    {
        buffer.putByte(offset + 16, (byte)value.value());
        return this;
    }

    public static int asOfLogPositionId()
    {
        return 4;
    }

    public static int asOfLogPositionSinceVersion()
    {
        return 0;
    }

    public static int asOfLogPositionEncodingOffset()
    {
        return 17;
    }

    public static int asOfLogPositionEncodingLength()
    {
        return 8;
    }

    public static String asOfLogPositionMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long asOfLogPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long asOfLogPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long asOfLogPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public UserBalancesEncoder asOfLogPosition(final long value)
    {
        buffer.putLong(offset + 17, value, BYTE_ORDER);
        return this;
    }


    private final BalancesEncoder balances = new BalancesEncoder(this);

    public static long balancesId()
    {
        return 20;
    }

    public BalancesEncoder balancesCount(final int count)
    {
        balances.wrap(buffer, count);
        return balances;
    }

    public static final class BalancesEncoder
    {
        public static final int HEADER_SIZE = 4;
        private final UserBalancesEncoder parentMessage;
        private MutableDirectBuffer buffer;
        private int count;
        private int index;
        private int offset;
        private int initialLimit;

        BalancesEncoder(final UserBalancesEncoder parentMessage)
        {
            this.parentMessage = parentMessage;
        }

        public void wrap(final MutableDirectBuffer buffer, final int count)
        {
            if (count < 0 || count > 65534)
            {
                throw new IllegalArgumentException("count outside allowed range: count=" + count);
            }

            if (buffer != this.buffer)
            {
                this.buffer = buffer;
            }

            index = 0;
            this.count = count;
            final int limit = parentMessage.limit();
            initialLimit = limit;
            parentMessage.limit(limit + HEADER_SIZE);
            buffer.putShort(limit + 0, (short)20, BYTE_ORDER);
            buffer.putShort(limit + 2, (short)count, BYTE_ORDER);
        }

        public BalancesEncoder next()
        {
            if (index >= count)
            {
                throw new java.util.NoSuchElementException();
            }

            offset = parentMessage.limit();
            parentMessage.limit(offset + sbeBlockLength());
            ++index;

            return this;
        }

        public int resetCountToIndex()
        {
            count = index;
            buffer.putShort(initialLimit + 2, (short)count, BYTE_ORDER);

            return count;
        }

        public static int countMinValue()
        {
            return 0;
        }

        public static int countMaxValue()
        {
            return 65534;
        }

        public static int sbeHeaderSize()
        {
            return HEADER_SIZE;
        }

        public static int sbeBlockLength()
        {
            return 20;
        }

        public static int assetIdId()
        {
            return 1;
        }

        public static int assetIdSinceVersion()
        {
            return 0;
        }

        public static int assetIdEncodingOffset()
        {
            return 0;
        }

        public static int assetIdEncodingLength()
        {
            return 4;
        }

        public static String assetIdMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static int assetIdNullValue()
        {
            return -2147483648;
        }

        public static int assetIdMinValue()
        {
            return -2147483647;
        }

        public static int assetIdMaxValue()
        {
            return 2147483647;
        }

        public BalancesEncoder assetId(final int value)
        {
            buffer.putInt(offset + 0, value, BYTE_ORDER);
            return this;
        }


        public static int availableId()
        {
            return 2;
        }

        public static int availableSinceVersion()
        {
            return 0;
        }

        public static int availableEncodingOffset()
        {
            return 4;
        }

        public static int availableEncodingLength()
        {
            return 8;
        }

        public static String availableMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static long availableNullValue()
        {
            return -9223372036854775808L;
        }

        public static long availableMinValue()
        {
            return -9223372036854775807L;
        }

        public static long availableMaxValue()
        {
            return 9223372036854775807L;
        }

        public BalancesEncoder available(final long value)
        {
            buffer.putLong(offset + 4, value, BYTE_ORDER);
            return this;
        }


        public static int lockedId()
        {
            return 3;
        }

        public static int lockedSinceVersion()
        {
            return 0;
        }

        public static int lockedEncodingOffset()
        {
            return 12;
        }

        public static int lockedEncodingLength()
        {
            return 8;
        }

        public static String lockedMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static long lockedNullValue()
        {
            return -9223372036854775808L;
        }

        public static long lockedMinValue()
        {
            return -9223372036854775807L;
        }

        public static long lockedMaxValue()
        {
            return 9223372036854775807L;
        }

        public BalancesEncoder locked(final long value)
        {
            buffer.putLong(offset + 12, value, BYTE_ORDER);
            return this;
        }

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

        final UserBalancesDecoder decoder = new UserBalancesDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
