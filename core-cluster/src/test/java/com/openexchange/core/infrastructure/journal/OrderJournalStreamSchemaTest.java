// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.journal;

import com.openexchange.core.domain.EventKind;
import com.openexchange.core.infrastructure.InfrastructureConstants;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Every production writer of the core repo on the two journal streams writes its own schema: 3 on the money journal
 * stream (4101), 5 on the order journal stream (4102). The writers are found in the sources (every recorded
 * publication the core opens), and each one's ring is driven to read the schema id of what it writes.
 */
public class OrderJournalStreamSchemaTest {

    private static final int MONEY_SCHEMA = com.openexchange.core.infrastructure.journal.generated.MessageHeaderEncoder.SCHEMA_ID;
    private static final int ORDER_SCHEMA =
            com.openexchange.core.infrastructure.orderjournal.generated.MessageHeaderEncoder.SCHEMA_ID;

    /** The schema id of one journal message (SBE header: blockLength, templateId, schemaId, version). */
    private static int schemaOf(final byte[] message) {
        return new UnsafeBuffer(message).getShort(4) & 0xFFFF;
    }

    /** The row's check: what a producer writes on {@code stream}. */
    static void assertStreamSchema(final int stream, final byte[] message) {
        final int schema = schemaOf(message);
        if (stream == InfrastructureConstants.ORDER_JOURNAL_STREAM_ID && schema != 5) {
            throw new AssertionError("wrong schema on the order journal stream: " + schema);
        }
        if (stream == InfrastructureConstants.MONEY_JOURNAL_STREAM_ID && schema != 3) {
            throw new AssertionError("wrong schema on the money journal stream: " + schema);
        }
    }

    private static List<byte[]> drain(final org.agrona.concurrent.ringbuffer.OneToOneRingBuffer ring) {
        final List<byte[]> out = new ArrayList<>();
        ring.read((t, b, i, l) -> {
            final byte[] m = new byte[l];
            b.getBytes(i, m);
            out.add(m);
        }, Integer.MAX_VALUE);
        return out;
    }

    private static byte[] moneyMessage() {
        final MoneyJournal money = new MoneyJournal(64 * 1024);
        money.onDeposit(1, 7, 0, 100, 100, 1_000);
        money.onSettle(2, 1, 1, 10, 1, 7, 8, true, 1, 1, 1, 1, 1_000);
        final List<byte[]> m = drain(money.ringBuffer());
        assertEquals(2, m.size());
        return m.get(1);
    }

    private static byte[] orderMessage() {
        final OrderJournal order = new OrderJournal(OrderJournalSettings.with(
                java.util.Map.of(OrderJournalSettings.Key.CORE_ORDER_JOURNAL_RING_BYTES, "65536")));
        order.onOrderEvent(1, 9, 64, new TestOrder(), EventKind.ACCEPTED, com.openexchange.core.domain.CoreRejectReason.NONE,
                0, 0, 0, 5, 1_000, 255, 0, 0, 0, 0, 0);
        final List<byte[]> m = drain(order.ringBuffer());
        assertEquals(1, m.size());
        assertEquals("one message per event, the production size", OrderJournal.MESSAGE_BYTES, m.get(0).length);
        return m.get(0);
    }

    /** The recorded publications the core's production sources open: one per journal, on its own stream. */
    @Test
    public void everyProductionJournalWriterWritesItsOwnSchema() throws IOException {
        final Path main = Paths.get("src/main/java");
        final Pattern recorded = Pattern.compile("addRecordedExclusivePublication\\(([^,]+),\\s*([^)]+)\\)");
        final List<String> writers = new ArrayList<>();
        try (Stream<Path> files = Files.walk(main)) {
            for (final Path f : (Iterable<Path>) files.filter(p -> p.toString().endsWith(".java"))::iterator) {
                final Matcher m = recorded.matcher(Files.readString(f));
                while (m.find()) {
                    writers.add(f.getFileName() + ":" + m.group(2).trim());
                }
            }
        }
        assertEquals("the recorded publications of the core: " + writers,
                List.of("MoneyJournalWriterAgent.java:streamId", "OrderJournalWriterAgent.java:streamId").stream().sorted().toList(),
                writers.stream().sorted().toList());
        // each writer agent is built with its own journal's ring and stream id, and only there
        final String moneyRuntime = Files.readString(main.resolve("com/openexchange/core/infrastructure/journal/MoneyJournalRuntime.java"));
        final String orderRuntime = Files.readString(main.resolve("com/openexchange/core/infrastructure/journal/OrderJournalRuntime.java"));
        assertTrue(moneyRuntime.contains("InfrastructureConstants.MONEY_JOURNAL_STREAM_ID);")
                && !moneyRuntime.contains("ORDER_JOURNAL_STREAM_ID"));
        assertTrue(orderRuntime.contains("InfrastructureConstants.ORDER_JOURNAL_STREAM_ID,")
                && !orderRuntime.contains("MONEY_JOURNAL_STREAM_ID"));

        assertStreamSchema(InfrastructureConstants.MONEY_JOURNAL_STREAM_ID, moneyMessage());
        assertStreamSchema(InfrastructureConstants.ORDER_JOURNAL_STREAM_ID, orderMessage());
        assertEquals(3, MONEY_SCHEMA);
        assertEquals(5, ORDER_SCHEMA);
    }

    /** Control: a test producer of schema 3 on the order journal stream fails with its text. */
    @Test
    public void controlSchema3OnTheOrderJournalStreamIsRefused() {
        try {
            assertStreamSchema(InfrastructureConstants.ORDER_JOURNAL_STREAM_ID, moneyMessage());
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(), expected.getMessage().startsWith("wrong schema on the order journal stream"));
            return;
        }
        fail("schema 3 passed on the order journal stream");
    }

    /** A plain order view for the encoder. */
    static final class TestOrder implements com.openexchange.core.domain.CoreOrderView {
        @Override public long orderId() { return 11; }
        @Override public long userId() { return 7; }
        @Override public int marketId() { return 1; }
        @Override public int statusCode() { return 0; }
        @Override public int sideCode() { return 0; }
        @Override public int orderTypeCode() { return 0; }
        @Override public int timeInForceCode() { return 0; }
        @Override public long price() { return 10; }
        @Override public long stopPrice() { return 0; }
        @Override public long quantity() { return 1; }
        @Override public long filledQty() { return 0; }
        @Override public long remainingQty() { return 1; }
        @Override public long meOrderId() { return 0; }
        @Override public int holdAssetId() { return 0; }
        @Override public long holdAmount() { return 10; }
        @Override public long clientOrderIdHigh() { return 0; }
        @Override public long clientOrderIdLow() { return 0; }
        @Override public long createdAtMs() { return 1_000; }
        @Override public long updatedAtMs() { return 1_000; }
        @Override public long budget() { return 0; }
        @Override public long displayQty() { return 0; }
        @Override public long hiddenQty() { return 0; }
        @Override public long expiresAtMs() { return 0; }
        @Override public long trailingArmPrice() { return 0; }
        @Override public int sliceSeq() { return 0; }
    }
}
