// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.application.engine.order;

import com.openexchange.core.application.engine.feed.V3Rig;
import com.openexchange.core.determinism.CoreEngineEvent;
import com.openexchange.core.domain.CoreRejectReason;
import com.openexchange.core.domain.commands.CreateOrderCommand;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.openexchange.core.application.engine.order.OrderRig.ONE;
import static com.openexchange.core.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * RequestOrderEvents: the replay of order events from the bounded ring. Every event above the asked sequence comes
 * back in order with the fields the broadcast carried, terminal events included; a reply pages by {@code nextFromSeq}
 * until it reaches {@code lastSeq}; {@code maxEvents} 0 is the head probe, above 4096 it is clamped, negative is
 * INVALID naming template 54. When the first event asked for has left the ring the reply is the terminator alone and
 * says where the ring starts. Capacity 0 switches the replay off, which is also the control of this file: the same
 * check must then fail with {@code event <seq> not replayed}.
 */
public class OrderEventReplayTest {

    private static final int T_ORDER_EVENTS = 54;

    /** The fields a live OrderEvent and its replay share, as one comparable line. */
    private static String line(final CoreEngineEvent.OrderEvent e) {
        return e.render().replaceFirst("^ORDEREVENT ", "");
    }

    private static String line(final CoreEngineEvent.OrderEventReplay e) {
        return e.render().replaceFirst("^ORDEREVENTREPLAY corr=\\d+ ", "");
    }

    private static List<String> live(final List<CoreEngineEvent> events) {
        final List<String> out = new ArrayList<>();
        for (final CoreEngineEvent e : events) {
            if (e instanceof CoreEngineEvent.OrderEvent oe) {
                out.add(line(oe));
            }
        }
        return out;
    }

    /** One request; returns the terminator and appends the page to {@code into}. */
    private static CoreEngineEvent.OrderEventsEnd ask(final OrderEngine orders, final com.openexchange.core.determinism.RecordingCoreSink sink,
                                                     final long from, final int max, final List<String> into) {
        final int mark = sink.events().size();
        orders.applyRequestOrderEvents(4_242L, from, max);
        final List<CoreEngineEvent> reply = sink.events().subList(mark, sink.events().size());
        assertTrue("a reply ends with its terminator, got " + reply,
                !reply.isEmpty() && reply.get(reply.size() - 1) instanceof CoreEngineEvent.OrderEventsEnd);
        for (int i = 0; i < reply.size() - 1; i++) {
            assertTrue("only replayed events before the terminator, got " + reply.get(i),
                    reply.get(i) instanceof CoreEngineEvent.OrderEventReplay);
            final CoreEngineEvent.OrderEventReplay r = (CoreEngineEvent.OrderEventReplay) reply.get(i);
            assertEquals("the reply echoes the correlation", 4_242L, r.correlationId());
            into.add(line(r));
        }
        final CoreEngineEvent.OrderEventsEnd end = (CoreEngineEvent.OrderEventsEnd) reply.get(reply.size() - 1);
        assertEquals("count is the events in this reply", reply.size() - 1, end.count());
        assertEquals("nextFromSeq is the cursor plus what was delivered", from + end.count(), end.nextFromSeq());
        return end;
    }

    /** The rule under test: every live event above {@code from} is replayed, in order, paging to lastSeq. */
    private static void assertReplayed(final OrderEngine orders, final com.openexchange.core.determinism.RecordingCoreSink sink,
                                       final List<String> liveEvents, final long from, final int page) {
        final List<String> replayed = new ArrayList<>();
        long cursor = from;
        CoreEngineEvent.OrderEventsEnd end;
        int guard = 0;
        do {
            end = ask(orders, sink, cursor, page, replayed);
            if (end.count() == 0 && end.nextFromSeq() != end.lastSeq()) {
                fail("event " + (cursor + 1) + " not replayed (first available " + end.firstAvailableSeq() + ", last "
                        + end.lastSeq() + ")");
            }
            cursor = end.nextFromSeq();
            assertTrue("paging terminates", ++guard < 100_000);
        } while (cursor != end.lastSeq());
        assertEquals("every live event above " + from + " is replayed in order", liveEvents.subList((int) from, liveEvents.size()),
                replayed);
    }

    /** A full lifecycle at behaviour 4: accepted, acked, partially filled, filled and terminal on both sides. */
    private static V3Rig filledBook() {
        final V3Rig r = V3Rig.openAt(4);
        r.twoParty();
        assertTrue(r.sellHits(1, V3Rig.ONE));
        assertTrue(r.sellHits(2, V3Rig.ONE));
        assertTrue(r.terminal(1, 101, 1, V3Rig.ME_FILLED, 0));
        assertTrue(r.terminal(2, 202, 2, V3Rig.ME_FILLED, 0));
        return r;
    }

    @Test
    public void everyEventAboveTheCursorIsReplayedInOrderTerminalsIncluded() {
        final V3Rig r = filledBook();
        final List<String> liveEvents = live(r.sink.events());
        assertTrue("precondition: the story has a FILLED terminal, got " + liveEvents,
                liveEvents.stream().anyMatch(l -> l.contains("status=FILLED")));
        assertEquals("precondition: dense sequence", liveEvents.size(), r.engine.getEventSeq());
        assertReplayed(r.engine.orders(), r.sink, liveEvents, 0, 1024);
        assertReplayed(r.engine.orders(), r.sink, liveEvents, 3, 1024);
        assertReplayed(r.engine.orders(), r.sink, liveEvents, 0, 1); // one event per page
        assertReplayed(r.engine.orders(), r.sink, liveEvents, 0, 3);
    }

    @Test
    public void controlRingCapacityZeroDoesNotReplay() {
        final V3Rig r = V3Rig.openAt(4);
        r.engine.applyCoreLimitsSet(1L, 1L, 900_000L, 256, 0, r.now); // replay off before the first event
        r.twoParty();
        final List<String> liveEvents = live(r.sink.events());
        try {
            assertReplayed(r.engine.orders(), r.sink, liveEvents, 0, 1024);
        } catch (final AssertionError expected) {
            assertTrue("the control must fail with 'event 1 not replayed', got: " + expected.getMessage(),
                    expected.getMessage().startsWith("event 1 not replayed"));
            return;
        }
        fail("the control did not fail: a ring of capacity 0 replayed events");
    }

    @Test
    public void theHeadProbeAnswersWithTheTerminatorAlone() {
        final V3Rig r = filledBook();
        final long last = r.engine.getEventSeq();
        final List<String> none = new ArrayList<>();
        final CoreEngineEvent.OrderEventsEnd end = ask(r.engine.orders(), r.sink, 2, 0, none);
        assertEquals(new CoreEngineEvent.OrderEventsEnd(4_242L, 0, 1L, last, 2L), end);
        assertTrue(none.isEmpty());
        // a cursor at the head: nothing to deliver, and the cursor says the replay is complete
        final CoreEngineEvent.OrderEventsEnd atHead = ask(r.engine.orders(), r.sink, last, 16, none);
        assertEquals(new CoreEngineEvent.OrderEventsEnd(4_242L, 0, 1L, last, last), atHead);
        // a fresh core: no event yet
        final OrderRig fresh = OrderRig.open();
        assertEquals(new CoreEngineEvent.OrderEventsEnd(4_242L, 0, 1L, 0L, 0L), ask(fresh.orders, fresh.sink, 0, 16, none));
    }

    @Test
    public void aNegativeMaxEventsIsInvalidNamingTheTemplate() {
        final V3Rig r = filledBook();
        r.mark();
        r.engine.orders().applyRequestOrderEvents(77L, 0, -1);
        assertEquals("one reject and nothing else, got " + r.events(), 1, r.events().size());
        final CoreEngineEvent.OrderReject rej = (CoreEngineEvent.OrderReject) r.events().get(0);
        assertEquals(CoreRejectReason.INVALID, rej.reason());
        assertEquals(T_ORDER_EVENTS, rej.refTemplateId());
        assertEquals(77L, rej.correlationId());
        assertEquals(-1L, rej.detail());
    }

    /**
     * A negative fromEventSeq is answered as an overrun: the terminator alone, count 0, nextFromSeq the negative cursor,
     * even though the ring holds every event. The client sends 0 to start from the beginning.
     */
    @Test
    public void aNegativeFromEventSeqIsAnsweredAsAnOverrunEvenWhenTheRingHoldsEverything() {
        final V3Rig r = filledBook();
        final long last = r.engine.getEventSeq();
        final List<String> none = new ArrayList<>();
        assertEquals("precondition: the ring holds every event", last, r.engine.orders().eventRing().count());
        for (final long from : new long[] {-1L, -5L, Long.MIN_VALUE + 1}) {
            final CoreEngineEvent.OrderEventsEnd end = ask(r.engine.orders(), r.sink, from, 16, none);
            assertEquals("fromEventSeq " + from + " is an overrun", new CoreEngineEvent.OrderEventsEnd(4_242L, 0, 1L, last, from),
                    end);
            assertTrue("an overrun: firstAvailableSeq above fromEventSeq + 1", end.firstAvailableSeq() > from + 1);
        }
        assertTrue("nothing replayed", none.isEmpty());
    }

    /**
     * A fromEventSeq above lastSeq (a cursor kept across a new genesis) is answered with the terminator alone and
     * nextFromSeq = fromEventSeq, above lastSeq: the replay can never complete from it, so the client must resync.
     */
    @Test
    public void aCursorAboveLastSeqReturnsNextFromSeqAboveLastSeq() {
        final V3Rig r = filledBook();
        final long last = r.engine.getEventSeq();
        final List<String> none = new ArrayList<>();
        for (final long from : new long[] {last + 1, last + 1_000, Long.MAX_VALUE}) {
            final CoreEngineEvent.OrderEventsEnd end = ask(r.engine.orders(), r.sink, from, 16, none);
            assertEquals("fromEventSeq " + from + " above lastSeq " + last,
                    new CoreEngineEvent.OrderEventsEnd(4_242L, 0, 1L, last, from), end);
            assertTrue("nextFromSeq is above lastSeq (the resync signal)", end.nextFromSeq() > end.lastSeq());
        }
        // the head probe gives the same answer
        assertEquals(new CoreEngineEvent.OrderEventsEnd(4_242L, 0, 1L, last, last + 7),
                ask(r.engine.orders(), r.sink, last + 7, 0, none));
        assertTrue("nothing replayed", none.isEmpty());
    }

    /** {@code n} accepted creates on a v1 core: one ACCEPTED event each. */
    private static OrderRig withEvents(final int n) {
        final OrderRig r = OrderRig.open();
        final int users = Math.max(1, (n + 199) / 200);
        for (int u = 1; u <= users; u++) {
            r.deposit(u, USD, 1_000_000_000L * ONE);
        }
        for (int i = 0; i < n; i++) {
            final CreateOrderCommand c = r.limit(1 + (i % users), 1, 0, ONE, ONE);
            r.create(c);
            r.advance(25); // 40 per second across the users, under every rate limit
        }
        assertEquals("precondition: " + n + " events", n, r.engine.getEventSeq());
        return r;
    }

    @Test
    public void aRequestAboveTheCapIsClampedAndTheCursorContinues() {
        final OrderRig r = withEvents(OrderEngine.ORDER_EVENTS_CAP + 150);
        final List<String> liveEvents = live(r.sink.events());
        final List<String> replayed = new ArrayList<>();
        final CoreEngineEvent.OrderEventsEnd first = ask(r.orders, r.sink, 0, Integer.MAX_VALUE, replayed);
        assertEquals("clamped to the cap", OrderEngine.ORDER_EVENTS_CAP, first.count());
        assertEquals(OrderEngine.ORDER_EVENTS_CAP, first.nextFromSeq());
        final CoreEngineEvent.OrderEventsEnd second = ask(r.orders, r.sink, first.nextFromSeq(), Integer.MAX_VALUE, replayed);
        assertEquals(150, second.count());
        assertEquals("the replay is complete", second.lastSeq(), second.nextFromSeq());
        assertEquals(liveEvents, replayed);
    }

    @Test
    public void anOverrunIsTheTerminatorAloneNamingWhereTheRingStarts() {
        final OrderRig r = OrderRig.open();
        r.engine.applyCoreLimitsSet(1L, 1L, 900_000L, 256, 8, r.now);
        r.deposit(1, USD, 1_000_000 * ONE);
        for (int i = 0; i < 20; i++) {
            r.create(r.limit(1, 1, 0, ONE, ONE));
            r.advance(100);
        }
        final List<String> liveEvents = live(r.sink.events());
        final List<String> page = new ArrayList<>();
        // events 1..12 have left a ring of 8: asking from 0 (or anything below 12) is an overrun, never a partial reply
        for (final long from : new long[] {0, 5, 11, -3}) {
            final CoreEngineEvent.OrderEventsEnd end = ask(r.orders, r.sink, from, 1024, page);
            assertEquals("overrun from " + from, new CoreEngineEvent.OrderEventsEnd(4_242L, 0, 13L, 20L, from), end);
            assertTrue("firstAvailableSeq is above the event asked for", end.firstAvailableSeq() > from + 1);
        }
        assertTrue("an overrun delivers nothing", page.isEmpty());
        // from the ring's edge the newest eight come back, wrapped storage and all
        final CoreEngineEvent.OrderEventsEnd end = ask(r.orders, r.sink, 12, 1024, page);
        assertEquals(new CoreEngineEvent.OrderEventsEnd(4_242L, 8, 13L, 20L, 20L), end);
        assertEquals(liveEvents.subList(12, 20), page);
    }

    @Test
    public void aSmallerRingKeepsTheNewestAndALargerRingKeepsEverything() {
        final OrderRig r = withEvents(40);
        final List<String> liveEvents = live(r.sink.events());
        r.engine.applyCoreLimitsSet(1L, 1L, 900_000L, 256, 10, r.now);
        final List<String> page = new ArrayList<>();
        assertEquals(new CoreEngineEvent.OrderEventsEnd(4_242L, 10, 31L, 40L, 40L), ask(r.orders, r.sink, 30, 1024, page));
        assertEquals("the newest ten survive the shrink", liveEvents.subList(30, 40), page);
        r.engine.applyCoreLimitsSet(2L, 2L, 900_000L, 256, 1_000, r.now);
        page.clear();
        assertEquals("growing drops nothing", new CoreEngineEvent.OrderEventsEnd(4_242L, 10, 31L, 40L, 40L),
                ask(r.orders, r.sink, 30, 1024, page));
        for (int i = 0; i < 25; i++) {
            r.create(r.limit(1, 1, 0, ONE, ONE));
            r.advance(100);
        }
        page.clear();
        final List<String> after = live(r.sink.events());
        assertEquals(new CoreEngineEvent.OrderEventsEnd(4_242L, 35, 31L, 65L, 65L), ask(r.orders, r.sink, 30, 1024, page));
        assertEquals(after.subList(30, 65), page);
        // capacity 0 switches the replay off: nothing is kept and the ring starts after the head
        r.engine.applyCoreLimitsSet(3L, 3L, 900_000L, 256, 0, r.now);
        page.clear();
        assertEquals(new CoreEngineEvent.OrderEventsEnd(4_242L, 0, 66L, 65L, 10L), ask(r.orders, r.sink, 10, 1024, page));
        assertEquals(new CoreEngineEvent.OrderEventsEnd(4_242L, 0, 66L, 65L, 65L), ask(r.orders, r.sink, 65, 1024, page));
    }

    @Test
    public void theReplayedRecordIsTheBroadcastRecordByteForByte() {
        final V3Rig r = filledBook();
        // a publisher over the same engine events: capture each broadcast OrderEvent body, then compare with the ring
        final List<String> broadcastHex = new ArrayList<>();
        final com.openexchange.core.infrastructure.publisher.CoreEventPublisher publisher =
                new com.openexchange.core.infrastructure.publisher.CoreEventPublisher((buffer, offset, length, channel, routing) -> {
                    final int template = buffer.getShort(offset + 2, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
                    if (template == 102) {
                        final StringBuilder hex = new StringBuilder();
                        for (int i = offset + 8; i < offset + length; i++) {
                            hex.append(String.format("%02x", buffer.getByte(i)));
                        }
                        broadcastHex.add(hex.toString());
                    }
                }, () -> false);
        final V3Rig again = V3Rig.openAt(4);
        again.engine.setEventSink(publisher);
        again.twoParty();
        again.sellHits(1, V3Rig.ONE);
        again.sellHits(2, V3Rig.ONE);
        again.terminal(1, 101, 1, V3Rig.ME_FILLED, 0);
        again.terminal(2, 202, 2, V3Rig.ME_FILLED, 0);
        final com.openexchange.core.determinism.RecordingCoreSink recording = new com.openexchange.core.determinism.RecordingCoreSink();
        again.engine.setEventSink(recording);
        again.engine.orders().applyRequestOrderEvents(1L, 0, 1024);
        final List<String> ringHex = new ArrayList<>();
        for (final CoreEngineEvent e : recording.events()) {
            if (e instanceof CoreEngineEvent.OrderEventReplay replay) {
                ringHex.add(replay.recordHex());
            }
        }
        assertEquals("precondition: the same story", r.engine.getEventSeq(), again.engine.getEventSeq());
        assertTrue("non-vacuous", broadcastHex.size() > 4);
        assertEquals("the ring holds the broadcast bytes", broadcastHex, ringHex);
    }
}
