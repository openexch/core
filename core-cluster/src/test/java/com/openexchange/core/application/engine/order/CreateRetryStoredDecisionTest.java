// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.application.engine.order;

import com.openexchange.core.application.engine.gate.OrderEntryGate;
import com.openexchange.core.determinism.CoreEngineEvent;
import com.openexchange.core.determinism.V3Genesis;
import com.openexchange.core.domain.Account;
import com.openexchange.core.domain.CoreRejectReason;
import com.openexchange.core.domain.GateReason;
import com.openexchange.core.domain.GateState;
import com.openexchange.core.domain.OrderStatus;
import com.openexchange.core.domain.commands.CreateOrderCommand;
import org.junit.Test;

import java.util.List;

import static com.openexchange.core.application.engine.order.OrderRig.ONE;
import static com.openexchange.core.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A retried CreateOrder always gets the decision the core stored for its requestId. Under behaviour 4 the requestId
 * lookup runs before the market, shape, time-in-force, flags, expiry, gate and tick/lot/band checks, so a create that
 * was accepted and is retried after the gate closed, or after its GTD deadline passed, is answered with the stored
 * acceptance (an ack marked duplicate, the same orderId) and a stored reject replays as that reject. Nothing is created
 * twice: one order record, one hold, one dispatch. Under behaviour 3 the lookup keeps its old place, which is what the
 * control rows show (they must fail with the named text).
 */
public class CreateRetryStoredDecisionTest {

    private static final int LATEST = 4;
    private static final int MARKET = 1;
    private static final long PRICE = 100 * ONE;

    /** Genesis ladder to {@code behavior}, the feed bound, GLOBAL opened by ADMIN, user 1 funded. */
    static OrderRig rigAt(final int behavior) {
        final OrderRig r = new OrderRig();
        r.engine.coreConfig().setSupportedBehaviorVersions(1, 2, 3, 4);
        for (long v = 1; v <= behavior; v++) {
            r.engine.applyCoreConfig(V3Genesis.config(v, v), r.now);
        }
        assertEquals("precondition: the recorded behaviour", behavior, r.engine.coreConfig().behaviorVersion());
        assertTrue("precondition: the feed binds", V3Genesis.bind(r.engine, 1L, r.now));
        r.engine.applyOrderEntryGate(1L, OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN, r.now);
        assertEquals("precondition: GLOBAL is open", GateState.OPEN, r.engine.gate().state(OrderEntryGate.GLOBAL));
        r.deposit(1, USD, 100_000 * ONE);
        return r;
    }

    private static CoreEngineEvent.OrderAck accept(final OrderRig r, final CreateOrderCommand c) {
        r.markNow();
        r.create(c);
        final CoreEngineEvent.OrderAck ack = r.first(CoreEngineEvent.OrderAck.class);
        assertFalse("precondition: the first offer is a fresh decision", ack.duplicate());
        return ack;
    }

    /** The one-live-order oracle: one record, one hold of the accepted amount, one dispatch. */
    private static void assertOneOrderOneHold(final OrderRig r, final long orderId, final long holdAmount) {
        assertEquals("one live order", 1, r.orders.orders().size());
        assertNotNull("the accepted order is the live one", r.orders.orders().get(orderId));
        final Account a = r.engine.account(1);
        assertEquals("one hold", 1, a.holdCount());
        assertEquals("the hold is the accepted order's", holdAmount, a.holdRemaining(orderId));
        assertEquals("locked equals the one hold", holdAmount, a.locked(USD));
        assertEquals("one CREATE dispatch", 1L, r.engine.getDispatchSeq());
    }

    private static void assertStoredAcceptance(final OrderRig r, final CoreEngineEvent.OrderAck first, final String ifRejected) {
        final List<CoreEngineEvent> e = r.events();
        for (final CoreEngineEvent ev : e) {
            if (ev instanceof CoreEngineEvent.OrderReject rej) {
                fail(ifRejected.replace("{}", rej.reason().name()) + " (" + rej.render() + ")");
            }
        }
        assertEquals("the retry emits the duplicate ack and nothing else, got " + e, 1, e.size());
        final CoreEngineEvent.OrderAck again = r.only(CoreEngineEvent.OrderAck.class);
        assertTrue("the retry is marked duplicate", again.duplicate());
        assertEquals("the retry names the stored orderId", first.orderId(), again.orderId());
    }

    // ---------------------------------------------------------------- the gate closes between offer and retry

    private static void retryAfterAdminGateClosed(final int behavior) {
        final OrderRig r = rigAt(behavior);
        final CreateOrderCommand c = r.limit(1, MARKET, 0, PRICE, ONE);
        final CoreEngineEvent.OrderAck first = accept(r, c);
        r.engine.applyOrderEntryGate(2L, OrderEntryGate.GLOBAL, GateState.SUSPENDED, GateReason.ADMIN, r.now);
        assertEquals("precondition: GLOBAL is suspended", GateState.SUSPENDED, r.engine.gate().state(OrderEntryGate.GLOBAL));
        r.markNow();
        r.create(c);
        assertStoredAcceptance(r, first, "retry answered {} for a stored acceptance");
        assertOneOrderOneHold(r, first.orderId(), PRICE);
    }

    @Test
    public void retryAfterTheAdminGateClosedReturnsTheStoredAcceptance() {
        retryAfterAdminGateClosed(LATEST);
    }

    @Test
    public void controlBehaviour3AnswersGateClosedAfterTheAdminGate() {
        expectFailure(() -> retryAfterAdminGateClosed(3), "retry answered GATE_CLOSED for a stored acceptance");
    }

    private static void retryAfterBridgeHalt(final int behavior) {
        final OrderRig r = rigAt(behavior);
        final CreateOrderCommand c = r.limit(1, MARKET, 0, PRICE, ONE);
        final CoreEngineEvent.OrderAck first = accept(r, c);
        r.engine.suspendGateInternally(OrderEntryGate.GLOBAL, GateReason.BRIDGE_HALT);
        assertEquals("precondition: GLOBAL is suspended", GateState.SUSPENDED, r.engine.gate().state(OrderEntryGate.GLOBAL));
        assertTrue("precondition: the BRIDGE_HALT bit is set",
                (r.engine.gate().reasons(OrderEntryGate.GLOBAL) & (1 << GateReason.BRIDGE_HALT.code())) != 0);
        r.markNow();
        r.create(c);
        assertStoredAcceptance(r, first, "retry answered {} for a stored acceptance");
        assertOneOrderOneHold(r, first.orderId(), PRICE);
    }

    @Test
    public void retryAfterABridgeHaltReturnsTheStoredAcceptance() {
        retryAfterBridgeHalt(LATEST);
    }

    @Test
    public void controlBehaviour3AnswersGateClosedAfterABridgeHalt() {
        expectFailure(() -> retryAfterBridgeHalt(3), "retry answered GATE_CLOSED for a stored acceptance");
    }

    // ---------------------------------------------------------------- the GTD deadline passes between offer and retry

    private static void retryAfterGtdDeadline(final int behavior, final boolean sweep) {
        final OrderRig r = rigAt(behavior);
        final CreateOrderCommand c = r.limit(1, MARKET, 0, PRICE, ONE);
        c.timeInForce = 3; // GTD
        c.expiresAtMs = r.now + 1_000;
        final CoreEngineEvent.OrderAck first = accept(r, c);
        r.advance(2_000);
        if (sweep) {
            r.sweep(); // the deadline fires as it would in the cluster; the order waits for the engine's terminal
        }
        r.markNow();
        r.create(c);
        assertStoredAcceptance(r, first, "retry answered {} for a stored acceptance");
        assertEquals("no second record", 1, r.orders.orders().size());
        assertEquals("no second hold", 1, r.engine.account(1).holdCount());
        assertEquals("locked is the one hold", PRICE, r.engine.account(1).locked(USD));
    }

    @Test
    public void retryAfterTheGtdDeadlineReturnsTheStoredAcceptance() {
        retryAfterGtdDeadline(LATEST, false);
    }

    @Test
    public void retryAfterTheGtdDeadlineFiredReturnsTheStoredAcceptance() {
        retryAfterGtdDeadline(LATEST, true);
    }

    @Test
    public void controlBehaviour3AnswersExpiredAfterTheGtdDeadline() {
        expectFailure(() -> retryAfterGtdDeadline(3, false), "retry answered EXPIRED for a stored acceptance");
    }

    // ---------------------------------------------------------------- a stored reject retried after the gate closed

    private static void storedRejectAfterGateClosed(final int behavior) {
        final OrderRig r = rigAt(behavior);
        final CreateOrderCommand c = r.limit(1, MARKET, 0, PRICE, 2_000 * ONE); // 200 000 USD against 100 000
        r.markNow();
        r.create(c);
        assertEquals("precondition: the first offer is refused for funds", CoreRejectReason.INSUFFICIENT_FUNDS, r.reject().reason());
        assertNotNull("precondition: the reject is stored", r.orders.requests().get(1, c.requestIdHigh, c.requestIdLow));
        r.engine.applyOrderEntryGate(2L, OrderEntryGate.GLOBAL, GateState.SUSPENDED, GateReason.ADMIN, r.now);
        r.markNow();
        r.create(c);
        final CoreRejectReason reason = r.reject().reason();
        assertEquals("retry answered " + reason.name() + " for a stored reject", CoreRejectReason.INSUFFICIENT_FUNDS, reason);
        assertEquals("no record", 0, r.orders.orders().size());
        assertEquals("no hold", 0, r.engine.account(1).holdCount());
    }

    @Test
    public void aStoredRejectReplaysAfterTheGateClosed() {
        storedRejectAfterGateClosed(LATEST);
    }

    @Test
    public void controlBehaviour3AnswersGateClosedForAStoredReject() {
        expectFailure(() -> storedRejectAfterGateClosed(3), "retry answered GATE_CLOSED for a stored reject");
    }

    // ---------------------------------------------------------------- nothing else moved

    @Test
    public void aFirstCreateUnderAClosedGateIsStillGateClosedAndStoresNothing() {
        final OrderRig r = rigAt(LATEST);
        r.engine.applyOrderEntryGate(2L, OrderEntryGate.GLOBAL, GateState.SUSPENDED, GateReason.ADMIN, r.now);
        final CreateOrderCommand c = r.limit(1, MARKET, 0, PRICE, ONE);
        final long requestSeq = r.orders.requests().nextRequestSeq();
        r.markNow();
        r.create(c);
        assertEquals(CoreRejectReason.GATE_CLOSED, r.reject().reason());
        assertNull("a structural refusal is not stored", r.orders.requests().get(1, c.requestIdHigh, c.requestIdLow));
        assertEquals("no request sequence consumed", requestSeq, r.orders.requests().nextRequestSeq());
        assertEquals("no record", 0, r.orders.orders().size());
        assertEquals("no hold", 0, r.engine.account(1).holdCount());
        // the same request after the gate reopens is a first-time create
        r.engine.applyOrderEntryGate(3L, OrderEntryGate.GLOBAL, GateState.OPEN, GateReason.ADMIN, r.now);
        assertFalse(accept(r, c).duplicate());
    }

    @Test
    public void aZeroRequestIdIsStillMissingRequestId() {
        final OrderRig r = rigAt(LATEST);
        final CreateOrderCommand c = r.limit(1, MARKET, 0, PRICE, ONE);
        c.requestIdHigh = 0;
        c.requestIdLow = 0;
        r.markNow();
        r.create(c);
        assertEquals(CoreRejectReason.MISSING_REQUEST_ID, r.reject().reason());
        // and it keeps its place: an unknown market is named before the missing id
        c.marketId = 99;
        r.markNow();
        r.create(c);
        assertEquals(CoreRejectReason.UNKNOWN_MARKET, r.reject().reason());
    }

    @Test
    public void aDifferentPayloadUnderAStoredIdIsAMismatchEvenWhenTheNewPayloadIsMalformed() {
        final OrderRig r = rigAt(LATEST);
        final CreateOrderCommand c = r.limit(1, MARKET, 0, PRICE, ONE);
        final CoreEngineEvent.OrderAck first = accept(r, c);
        c.quantity = 2 * ONE;
        r.markNow();
        r.create(c);
        assertEquals(CoreRejectReason.REQUEST_ID_PAYLOAD_MISMATCH, r.reject().reason());
        c.quantity = 0; // malformed on its own: the stored id answers first
        r.markNow();
        r.create(c);
        assertEquals(CoreRejectReason.REQUEST_ID_PAYLOAD_MISMATCH, r.reject().reason());
        assertOneOrderOneHold(r, first.orderId(), PRICE);
    }

    @Test
    public void aMissIsCheckedInTheOldOrder() {
        // an id the table has never seen falls through to the same checks in the same order as before
        final OrderRig r = rigAt(LATEST);
        final CreateOrderCommand unknownMarket = r.limit(1, 99, 0, PRICE, ONE);
        r.markNow();
        r.create(unknownMarket);
        assertEquals(CoreRejectReason.UNKNOWN_MARKET, r.reject().reason());
        final CreateOrderCommand expired = r.limit(1, MARKET, 0, PRICE, ONE);
        expired.timeInForce = 3;
        expired.expiresAtMs = r.now; // not above cluster time
        r.markNow();
        r.create(expired);
        assertEquals(CoreRejectReason.EXPIRED, r.reject().reason());
        assertNull("EXPIRED on a first offer is not stored", r.orders.requests().get(1, expired.requestIdHigh, expired.requestIdLow));
    }

    @Test
    public void theDuplicateAckCarriesTheLiveStatus() {
        final OrderRig r = rigAt(LATEST);
        final CreateOrderCommand c = r.limit(1, MARKET, 0, PRICE, ONE);
        final CoreEngineEvent.OrderAck first = accept(r, c);
        r.engine.applyOrderEntryGate(2L, OrderEntryGate.GLOBAL, GateState.SUSPENDED, GateReason.ADMIN, r.now);
        r.markNow();
        r.create(c);
        final CoreEngineEvent.OrderAck again = r.only(CoreEngineEvent.OrderAck.class);
        assertEquals(OrderStatus.PENDING_NEW, again.status());
        assertEquals(first.holdAmount(), again.holdAmount());
    }

    private static void expectFailure(final Runnable body, final String text) {
        try {
            body.run();
        } catch (final AssertionError expected) {
            assertTrue("the control must fail with '" + text + "', got: " + expected.getMessage(),
                    expected.getMessage() != null && expected.getMessage().contains(text));
            return;
        }
        fail("the control did not fail: behaviour 3 answered the stored decision, so the rule under test is not what "
                + "decides this row");
    }
}
