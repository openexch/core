/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.core.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * ORIGIN reply to RequestUserBalances, one frame. known = an account record exists for the user. balances: one entry per asset with a nonzero available or locked amount, ascending assetId; an absent asset is zero.
 */
@SuppressWarnings("all")
public final class UserBalancesDecoder
{
    public static final int BLOCK_LENGTH = 25;
    public static final int TEMPLATE_ID = 117;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 8;
    public static final String SEMANTIC_VERSION = "0.8";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final UserBalancesDecoder parentMessage = this;
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

    public UserBalancesDecoder wrap(
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

    public UserBalancesDecoder wrapAndApplyHeader(
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

    public UserBalancesDecoder sbeRewind()
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

    public long userId()
    {
        return buffer.getLong(offset + 8, BYTE_ORDER);
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

    public short knownRaw()
    {
        return ((short)(buffer.getByte(offset + 16) & 0xFF));
    }

    public BoolFlag known()
    {
        return BoolFlag.get(((short)(buffer.getByte(offset + 16) & 0xFF)));
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

    public long asOfLogPosition()
    {
        return buffer.getLong(offset + 17, BYTE_ORDER);
    }


    private final BalancesDecoder balances = new BalancesDecoder(this);

    public static long balancesDecoderId()
    {
        return 20;
    }

    public static int balancesDecoderSinceVersion()
    {
        return 0;
    }

    public BalancesDecoder balances()
    {
        balances.wrap(buffer);
        return balances;
    }

    public static final class BalancesDecoder
        implements Iterable<BalancesDecoder>, java.util.Iterator<BalancesDecoder>
    {
        public static final int HEADER_SIZE = 4;
        private final UserBalancesDecoder parentMessage;
        private DirectBuffer buffer;
        private int count;
        private int index;
        private int offset;
        private int blockLength;

        BalancesDecoder(final UserBalancesDecoder parentMessage)
        {
            this.parentMessage = parentMessage;
        }

        public void wrap(final DirectBuffer buffer)
        {
            if (buffer != this.buffer)
            {
                this.buffer = buffer;
            }

            index = 0;
            final int limit = parentMessage.limit();
            parentMessage.limit(limit + HEADER_SIZE);
            blockLength = (buffer.getShort(limit + 0, BYTE_ORDER) & 0xFFFF);
            count = (buffer.getShort(limit + 2, BYTE_ORDER) & 0xFFFF);
        }

        public BalancesDecoder next()
        {
            if (index >= count)
            {
                throw new java.util.NoSuchElementException();
            }

            offset = parentMessage.limit();
            parentMessage.limit(offset + blockLength);
            ++index;

            return this;
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

        public int actingBlockLength()
        {
            return blockLength;
        }

        public int actingVersion()
        {
            return parentMessage.actingVersion;
        }

        public int count()
        {
            return count;
        }

        public java.util.Iterator<BalancesDecoder> iterator()
        {
            return this;
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        public boolean hasNext()
        {
            return index < count;
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

        public int assetId()
        {
            return buffer.getInt(offset + 0, BYTE_ORDER);
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

        public long available()
        {
            return buffer.getLong(offset + 4, BYTE_ORDER);
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

        public long locked()
        {
            return buffer.getLong(offset + 12, BYTE_ORDER);
        }


        public StringBuilder appendTo(final StringBuilder builder)
        {
            if (null == buffer)
            {
                return builder;
            }

            builder.append('(');
            builder.append("assetId=");
            builder.append(this.assetId());
            builder.append('|');
            builder.append("available=");
            builder.append(this.available());
            builder.append('|');
            builder.append("locked=");
            builder.append(this.locked());
            builder.append(')');

            return builder;
        }
        
        public BalancesDecoder sbeSkip()
        {

            return this;
        }
    }

    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        final UserBalancesDecoder decoder = new UserBalancesDecoder();
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
        builder.append("[UserBalances](sbeTemplateId=");
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
        builder.append("userId=");
        builder.append(this.userId());
        builder.append('|');
        builder.append("known=");
        builder.append(this.known());
        builder.append('|');
        builder.append("asOfLogPosition=");
        builder.append(this.asOfLogPosition());
        builder.append('|');
        builder.append("balances=[");
        final int balancesOriginalOffset = balances.offset;
        final int balancesOriginalIndex = balances.index;
        final BalancesDecoder balances = this.balances();
        if (balances.count() > 0)
        {
            while (balances.hasNext())
            {
                balances.next().appendTo(builder);
                builder.append(',');
            }
            builder.setLength(builder.length() - 1);
        }
        balances.offset = balancesOriginalOffset;
        balances.index = balancesOriginalIndex;
        builder.append(']');

        limit(originalLimit);

        return builder;
    }
    
    public UserBalancesDecoder sbeSkip()
    {
        sbeRewind();
        BalancesDecoder balances = this.balances();
        if (balances.count() > 0)
        {
            while (balances.hasNext())
            {
                balances.next();
                balances.sbeSkip();
            }
        }

        return this;
    }
}
