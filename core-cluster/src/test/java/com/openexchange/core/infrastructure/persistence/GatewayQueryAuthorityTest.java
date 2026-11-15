// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.persistence;

import com.openexchange.cluster.auth.ClusterPrincipal;
import com.openexchange.core.application.engine.feed.V3Rig;
import com.openexchange.core.auth.CoreSessionClasses;
import com.openexchange.core.determinism.CoreEngineEvent;
import com.openexchange.core.determinism.V3Genesis;
import com.openexchange.core.domain.AdminResult;
import com.openexchange.core.domain.CoreRejectReason;
import com.openexchange.core.domain.GateReason;
import com.openexchange.core.domain.GateState;
import com.openexchange.core.infrastructure.generated.SessionClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The gateway queries through the demuxer, with real session authorities: who may ask (class table, registration,
 * user scope) and what the answer is. RequestUserBalances is user-scoped; RequestOrderEvents needs a key that may act
 * for any user; RequestCoreConfig reports state that was set before the asking session existed; CoreLimitsSet is ADMIN
 * only. The control of the balance row sends the same frame with the scope check out of the way (the in-process
 * trusted path) and must fail with {@code foreign balances returned}.
 */
public class GatewayQueryAuthorityTest {

    private static final long ONE = V3Rig.ONE;
    private final CoreFrameEncoders frames = new CoreFrameEncoders();

    private long session(final V3Rig r, final long id, final SessionClass cls, final long... users) {
        final ClusterPrincipal p = users.length == 0 ? ClusterPrincipal.anyUser(CoreSessionClasses.toKit(cls), (int) id)
                : ClusterPrincipal.forUsers(CoreSessionClasses.toKit(cls), (int) id, users);
        r.engine.sessions().open(id, p.encode());
        return id;
    }

    private long registered(final V3Rig r, final long id, final SessionClass cls, final long... users) {
        session(r, id, cls, users);
        r.dispatch(id, r.load(frames.register(8_000 + id, cls)));
        assertTrue("precondition: session " + id + " is registered", r.engine.sessions().get(id).isRegistered());
        return id;
    }

    private List<CoreEngineEvent> ask(final V3Rig r, final long sessionId, final byte[] frame) {
        r.mark();
        final long dropped = r.demuxer.decodeRejectCount() + r.demuxer.unknownTemplateCount() + r.demuxer.schemaRejectCount();
        r.dispatch(sessionId, r.load(frame));
        assertEquals("the core answers every frame a gateway can send: nothing was dropped", dropped,
                r.demuxer.decodeRejectCount() + r.demuxer.unknownTemplateCount() + r.demuxer.schemaRejectCount());
        return r.events();
    }

    private static void assertRejected(final List<CoreEngineEvent> reply, final int template, final CoreRejectReason reason,
                                       final long correlationId) {
        assertEquals("one reject and nothing else, got " + reply, 1, reply.size());
        final CoreEngineEvent.OrderReject rej = (CoreEngineEvent.OrderReject) reply.get(0);
        assertEquals(reason, rej.reason());
        assertEquals(template, rej.refTemplateId());
        assertEquals(correlationId, rej.correlationId());
    }

    // ---------------------------------------------------------------- RequestUserBalances (55)

    private V3Rig twoFundedUsers() {
        final V3Rig r = V3Rig.openAt(4);
        r.deposit(1, V3Rig.USD, 1_000 * ONE);
        r.deposit(1, V3Rig.BTC, 3 * ONE);
        r.deposit(2, V3Rig.USD, 77 * ONE);
        r.create(r.limit(1, 0, 100 * ONE, 2 * ONE)); // user 1 locks 200 USD
        return r;
    }

    /** The rule: a key scoped to user 1 never reads user 2's balances. */
    private void assertForeignBalancesRefused(final V3Rig r, final long sessionId, final boolean trusted) {
        r.mark();
        final int length = r.load(frames.userBalances(552L, 2L));
        if (trusted) {
            r.dispatchTrusted(length);
        } else {
            r.dispatch(sessionId, length);
        }
        for (final CoreEngineEvent e : r.events()) {
            if (e instanceof CoreEngineEvent.UserBalances) {
                fail("foreign balances returned: " + e.render());
            }
        }
        assertRejected(r.events(), 55, CoreRejectReason.NOT_OWNER, 552L);
    }

    @Test
    public void ownBalancesEqualTheLedgerAndAForeignUserIsNotOwner() {
        final V3Rig r = twoFundedUsers();
        final long scoped = registered(r, 41, SessionClass.GATEWAY, 1L);
        final List<CoreEngineEvent> own = ask(r, scoped, frames.userBalances(551L, 1L));
        assertEquals(List.of(new CoreEngineEvent.UserBalances(551L, 1L, true,
                "0:" + (800 * ONE) + "/" + (200 * ONE) + " 1:" + (3 * ONE) + "/0")), own);
        assertEquals("the rows are the ledger's", 800 * ONE, r.engine.account(1).available(V3Rig.USD));
        assertEquals(200 * ONE, r.engine.account(1).locked(V3Rig.USD));
        assertForeignBalancesRefused(r, scoped, false);
        // an any-user gateway and ADMIN read any user
        final long gateway = registered(r, 42, SessionClass.GATEWAY);
        assertEquals(List.of(new CoreEngineEvent.UserBalances(553L, 2L, true, "0:" + (77 * ONE) + "/0")),
                ask(r, gateway, frames.userBalances(553L, 2L)));
        final long admin = session(r, 43, SessionClass.ADMIN);
        assertEquals(1, ask(r, admin, frames.userBalances(554L, 2L)).size());
    }

    @Test
    public void controlWithoutTheScopeCheckForeignBalancesAreReturned() {
        final V3Rig r = twoFundedUsers();
        try {
            assertForeignBalancesRefused(r, 0L, true);
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(), expected.getMessage().startsWith("foreign balances returned"));
            return;
        }
        fail("the control did not fail: the scope check is not what refuses a foreign user");
    }

    @Test
    public void aUserWithNoAccountIsUnknownAndStaysUnknown() {
        final V3Rig r = twoFundedUsers();
        final long gateway = registered(r, 42, SessionClass.GATEWAY);
        assertEquals(List.of(new CoreEngineEvent.UserBalances(555L, 9_999L, false, "")), ask(r, gateway, frames.userBalances(555L, 9_999L)));
        assertNull("asking never creates an account", r.engine.account(9_999L));
        // an account whose balances are all zero is known, with no rows
        r.deposit(3, V3Rig.USD, ONE);
        final com.openexchange.core.domain.commands.WithdrawCommand w = new com.openexchange.core.domain.commands.WithdrawCommand();
        w.reset();
        w.setUserId(3);
        w.setAssetId(V3Rig.USD);
        w.setAmount(ONE);
        r.engine.applyCommand(com.openexchange.core.application.engine.CoreEngine.CMD_WITHDRAW, w, r.now);
        assertEquals(List.of(new CoreEngineEvent.UserBalances(556L, 3L, true, "")), ask(r, gateway, frames.userBalances(556L, 3L)));
    }

    // ---------------------------------------------------------------- class table, registration, any-user rule

    @Test
    public void theClassTableAndTheAnyUserRule() {
        final V3Rig r = twoFundedUsers();
        final long scoped = registered(r, 41, SessionClass.GATEWAY, 1L);
        final long gateway = registered(r, 42, SessionClass.GATEWAY);
        final long admin = session(r, 43, SessionClass.ADMIN);
        final long forwarder = registered(r, 44, SessionClass.FORWARDER);
        final long tool = session(r, 45, SessionClass.TOOL);
        final long treasury = session(r, 46, SessionClass.TREASURY);
        final long unregistered = session(r, 47, SessionClass.GATEWAY);

        // the event replay is exchange-wide: a user-scoped key owns no such stream
        assertRejected(ask(r, scoped, frames.orderEvents(541L, 0, 16)), 54, CoreRejectReason.NOT_OWNER, 541L);
        assertTrue(ask(r, gateway, frames.orderEvents(542L, 0, 16)).get(0) instanceof CoreEngineEvent.OrderEventReplay);
        assertTrue(ask(r, admin, frames.orderEvents(543L, 0, 0)).get(0) instanceof CoreEngineEvent.OrderEventsEnd);
        // a scoped gateway still reads the config
        assertTrue(ask(r, scoped, frames.coreConfigRequest(561L)).get(0) instanceof CoreEngineEvent.CoreConfigReport);

        for (final long other : new long[] {forwarder, tool, treasury}) {
            assertRejected(ask(r, other, frames.orderEvents(544L, 0, 16)), 54, CoreRejectReason.UNAUTHORIZED_CLASS, 544L);
            assertRejected(ask(r, other, frames.userBalances(557L, 1L)), 55, CoreRejectReason.UNAUTHORIZED_CLASS, 557L);
            assertRejected(ask(r, other, frames.coreConfigRequest(562L)), 56, CoreRejectReason.UNAUTHORIZED_CLASS, 562L);
            assertRejected(ask(r, other, frames.limits(771L, 1, 60_000, 10, 10)), 77, CoreRejectReason.UNAUTHORIZED_CLASS, 771L);
        }
        // a gateway registers before anything but a pre-registration query
        assertRejected(ask(r, unregistered, frames.orderEvents(545L, 0, 16)), 54, CoreRejectReason.UNREGISTERED_SESSION, 545L);
        assertRejected(ask(r, unregistered, frames.userBalances(558L, 1L)), 55, CoreRejectReason.UNREGISTERED_SESSION, 558L);
        assertRejected(ask(r, unregistered, frames.coreConfigRequest(563L)), 56, CoreRejectReason.UNREGISTERED_SESSION, 563L);
        // the limits record is ADMIN only
        assertRejected(ask(r, gateway, frames.limits(772L, 1, 60_000, 10, 10)), 77, CoreRejectReason.UNAUTHORIZED_CLASS, 772L);
        assertEquals("nothing above changed the record", 0L, r.engine.limits().version());
        final List<CoreEngineEvent> set = ask(r, admin, frames.limits(773L, 1, 60_000, 10, 10));
        assertEquals(new CoreEngineEvent.AdminAck(773L, 77, AdminResult.ACCEPTED, 0, 1L), set.get(0));
        assertEquals(10, r.engine.limits().requestPerUserCap());
    }

    // ---------------------------------------------------------------- RequestCoreConfig (56)

    @Test
    public void theConfigReportCarriesStateSetBeforeTheAskingSessionConnected() {
        final V3Rig r = V3Rig.openAt(4);
        final long admin = session(r, 43, SessionClass.ADMIN);
        // before any gateway exists: a risk row, the limits record, a suspended market, a tripped breaker
        r.dispatch(admin, r.load(frames.riskConfig(711L, 2, 3, 7, 25, 1_500)));
        r.dispatch(admin, r.load(frames.limits(774L, 2, 120_000, 32, 4_096)));
        r.adminGate(3, GateState.SUSPENDED, GateReason.ADMIN);
        r.dispatch(admin, r.load(frames.breaker(721L, 4, true)));
        assertTrue("precondition: the breaker of market 4 is tripped", r.engine.orders().market(4).breakerTripped);
        assertEquals("precondition: market 3 is suspended", GateState.SUSPENDED, r.engine.gate().state(3));

        final long gateway = registered(r, 42, SessionClass.GATEWAY); // connects after all of it
        final List<CoreEngineEvent> reply = ask(r, gateway, frames.coreConfigRequest(564L));
        assertEquals(1, reply.size());
        final CoreEngineEvent.CoreConfigReport report = (CoreEngineEvent.CoreConfigReport) reply.get(0);
        assertEquals(564L, report.correlationId());
        assertEquals("the recorded config", 4L, report.configVersion());
        assertEquals(4L, report.behaviorVersion());
        assertEquals("the limits record", 2L, report.limitsVersion());
        assertEquals(120_000L, report.requestTtlMs());
        assertEquals(32, report.requestPerUserCap());
        assertEquals(4_096, report.orderEventRingCapacity());
        assertEquals("the global gate is open", GateState.OPEN.code(), report.gateStateCode());
        assertEquals(0, report.gateReasons());
        final var recorded = r.engine.coreConfig().recorded();
        assertEquals("one row per recorded market", recorded.markets.length, report.markets().size());
        assertEquals("one row per recorded asset", recorded.assets.length, report.assets().size());
        for (int i = 0; i < recorded.markets.length; i++) {
            final var m = recorded.markets[i];
            final var risk = r.engine.orders().market(m.marketId());
            final String expected = String.format("%d:%s base=%d quote=%d tick=%d min=%d max=%d lot=%d minQty=%d maxQty=%d minNotional=%d maxNotional=%d risk=%d gate=%d reasons=0x%x breaker=%d",
                    m.marketId(), m.symbol(), m.baseAssetId(), m.quoteAssetId(), m.tickSize(), m.minPrice(), m.maxPrice(), m.lotSize(),
                    risk.minQuantity, risk.maxQuantity, risk.minNotional, risk.maxNotional, risk.configVersion,
                    m.marketId() == 3 ? GateState.SUSPENDED.code() : GateState.OPEN.code(),
                    m.marketId() == 3 ? 1 << GateReason.ADMIN.code() : 0, m.marketId() == 4 ? 1 : 0);
            assertEquals(expected, report.markets().get(i));
        }
        assertTrue("the risk row set before the session connected is in the report: " + report.markets().get(2),
                report.markets().get(2).contains(" risk=2 "));
        assertEquals(V3Genesis.config(1, 1).assets.length, report.assets().size());

        // the registration reply alone would not have said any of it: only the global gate, breaker off
        // and a report tracks later changes
        r.adminGate(0, GateState.SUSPENDED, GateReason.ADMIN);
        final CoreEngineEvent.CoreConfigReport after = (CoreEngineEvent.CoreConfigReport) ask(r, gateway, frames.coreConfigRequest(565L)).get(0);
        assertEquals(GateState.SUSPENDED.code(), after.gateStateCode());
        assertEquals(1 << GateReason.ADMIN.code(), after.gateReasons());
    }

    @Test
    public void aCoreWithNoRecordedConfigReportsVersionZeroAndNoRows() {
        final com.openexchange.core.application.engine.CoreEngine engine = new com.openexchange.core.application.engine.CoreEngine();
        final com.openexchange.core.determinism.RecordingCoreSink sink = new com.openexchange.core.determinism.RecordingCoreSink();
        engine.setEventSink(sink);
        engine.requestCoreConfig(1L);
        final CoreEngineEvent.CoreConfigReport report = (CoreEngineEvent.CoreConfigReport) sink.events().get(0);
        assertEquals(0L, report.configVersion());
        assertEquals(1L, report.behaviorVersion());
        assertEquals(0L, report.limitsVersion());
        assertEquals(900_000L, report.requestTtlMs());
        assertEquals(256, report.requestPerUserCap());
        assertEquals(65_536, report.orderEventRingCapacity());
        assertEquals("genesis: the global gate is suspended", GateState.SUSPENDED.code(), report.gateStateCode());
        assertEquals(1 << GateReason.GENESIS.code(), report.gateReasons());
        assertTrue(report.markets().isEmpty());
        assertTrue(report.assets().isEmpty());
    }
}
