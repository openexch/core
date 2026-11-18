// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Pins the pure decode -> row/leg mapping: the header columns each movement type stamps (and leaves
 * null), the single-leg mapping for deposit/withdraw/opening, and the settle four-leg explosion
 * (buyer/seller x BASE/QUOTE, asset_id NULL + asset_role set, market_id on the header only). This is
 * the projection contract the Postgres store binds, testable without Aeron or a live PG.
 */
public class MoneyJournalRecordTest {

    @Test
    public void depositMapsToOneLegWithPositiveDelta() {
        MoneyJournalRecord r = MoneyJournalRecord.deposit(7L, 1001L, 2, 500_000L, 1_500_000L, 42L, 9_000L);
        assertEquals(7L, r.journalSeq());
        assertEquals(MoneyJournalRecord.TYPE_DEPOSIT, r.movementType());
        assertEquals(Long.valueOf(42L), r.clusterTimeMs());
        assertEquals(Long.valueOf(1001L), r.userId());
        assertEquals(Integer.valueOf(2), r.assetId());
        assertEquals(Long.valueOf(500_000L), r.amount());
        assertEquals(Long.valueOf(1_500_000L), r.balanceAfter());
        assertNull(r.tradeId());
        assertNull(r.marketId());
        assertNull(r.takerIsBuy());

        assertEquals(1, r.legs().size());
        MoneyJournalRecord.Leg leg = r.legs().get(0);
        assertEquals((short) 0, leg.legNo());
        assertEquals(1001L, leg.userId());
        assertEquals(Integer.valueOf(2), leg.assetId());
        assertNull(leg.assetRole());
        assertEquals(1_500_000L, leg.balanceAfter());
        assertEquals(Long.valueOf(500_000L), leg.delta());
    }

    @Test
    public void withdrawMapsToOneLegWithNegativeDelta() {
        MoneyJournalRecord r = MoneyJournalRecord.withdraw(8L, 1001L, 2, 300_000L, 1_200_000L, 43L, 9_100L);
        assertEquals(MoneyJournalRecord.TYPE_WITHDRAW, r.movementType());
        assertEquals(1, r.legs().size());
        MoneyJournalRecord.Leg leg = r.legs().get(0);
        assertEquals(1_200_000L, leg.balanceAfter());
        assertEquals(Long.valueOf(-300_000L), leg.delta());
        assertNull(leg.assetRole());
        assertEquals(Integer.valueOf(2), leg.assetId());
    }

    @Test
    public void openingMapsToOneLegNoClusterTimeNoDelta() {
        MoneyJournalRecord r = MoneyJournalRecord.opening(1L, 1001L, 3, 9_000_000L, 8_800L);
        assertEquals(MoneyJournalRecord.TYPE_OPENING, r.movementType());
        assertNull("opening epoch has no cluster timestamp", r.clusterTimeMs());
        assertEquals(Long.valueOf(9_000_000L), r.amount());
        assertEquals(Long.valueOf(9_000_000L), r.balanceAfter());
        assertEquals(1, r.legs().size());
        MoneyJournalRecord.Leg leg = r.legs().get(0);
        assertEquals(Integer.valueOf(3), leg.assetId());
        assertNull(leg.assetRole());
        assertEquals(9_000_000L, leg.balanceAfter());
        assertNull("opening is a snapshot, not a movement", leg.delta());
    }

    @Test
    public void settleExplodesIntoFourLegsBuyerAndSellerBaseAndQuote() {
        // buyer=200, seller=300, market=5, taker is the buyer.
        MoneyJournalRecord r = MoneyJournalRecord.settle(
                100L, 55L, 5, 1_000_000L, 2_000_000L, 200L, 300L, true,
                /*buyerBaseAfter*/  11L,
                /*buyerQuoteAfter*/ 22L,
                /*sellerBaseAfter*/ 33L,
                /*sellerQuoteAfter*/44L,
                /*clusterTimeMs*/   77L,
                /*logPosition*/     12_345L);

        assertEquals(MoneyJournalRecord.TYPE_SETTLE, r.movementType());
        assertEquals(12_345L, r.logPosition());
        // Settle carries market_id on the header, not per-leg; single user/asset/amount stay null.
        assertEquals(Long.valueOf(55L), r.tradeId());
        assertEquals(Integer.valueOf(5), r.marketId());
        assertEquals(Long.valueOf(1_000_000L), r.price());
        assertEquals(Long.valueOf(2_000_000L), r.quantity());
        assertEquals(Long.valueOf(200L), r.buyerUserId());
        assertEquals(Long.valueOf(300L), r.sellerUserId());
        assertEquals(Boolean.TRUE, r.takerIsBuy());
        assertNull(r.userId());
        assertNull(r.assetId());
        assertNull(r.amount());
        assertNull(r.balanceAfter());

        List<MoneyJournalRecord.Leg> legs = r.legs();
        assertEquals(4, legs.size());

        assertLeg(legs.get(0), (short) 0, 200L, MoneyJournalRecord.ROLE_BASE, 11L);
        assertLeg(legs.get(1), (short) 1, 200L, MoneyJournalRecord.ROLE_QUOTE, 22L);
        assertLeg(legs.get(2), (short) 2, 300L, MoneyJournalRecord.ROLE_BASE, 33L);
        assertLeg(legs.get(3), (short) 3, 300L, MoneyJournalRecord.ROLE_QUOTE, 44L);

        for (MoneyJournalRecord.Leg leg : legs) {
            assertNull("settle legs carry market_id + role, not a resolved asset_id", leg.assetId());
            assertNull("settle per-leg delta is not derivable from the record", leg.delta());
        }
    }

    @Test
    public void settleTakerIsBuyFalseIsCarried() {
        MoneyJournalRecord r = MoneyJournalRecord.settle(
                101L, 56L, 5, 1L, 1L, 200L, 300L, false, 1L, 2L, 3L, 4L, 78L, 1L);
        assertEquals(Boolean.FALSE, r.takerIsBuy());
        assertTrue(r.legs().stream().allMatch(l -> l.assetId() == null));
    }

    @Test
    public void equalityIsOverEveryProjectedFieldLegsAndLogPositionIncluded() {
        final MoneyJournalRecord a = MoneyJournalRecord.settle(9L, 1L, 2, 3L, 4L, 5L, 6L, true, 7L, 8L, 9L, 10L, 11L, 12L);
        assertEquals(a, MoneyJournalRecord.settle(9L, 1L, 2, 3L, 4L, 5L, 6L, true, 7L, 8L, 9L, 10L, 11L, 12L));
        assertEquals(a, MoneyJournalRecord.stored(9L, 12L, MoneyJournalRecord.TYPE_SETTLE, 11L, null, null, null, null,
                1L, 2, 3L, 4L, 5L, 6L, true, a.legs()));
        assertNotEquals("a leg total", a,
                MoneyJournalRecord.settle(9L, 1L, 2, 3L, 4L, 5L, 6L, true, 7L, 8L, 9L, 99L, 11L, 12L));
        assertNotEquals("the log position", a,
                MoneyJournalRecord.settle(9L, 1L, 2, 3L, 4L, 5L, 6L, true, 7L, 8L, 9L, 10L, 11L, 13L));
        assertNotEquals("the taker side", a,
                MoneyJournalRecord.settle(9L, 1L, 2, 3L, 4L, 5L, 6L, false, 7L, 8L, 9L, 10L, 11L, 12L));
        assertNotEquals("an amount", MoneyJournalRecord.deposit(1L, 2L, 3, 4L, 5L, 6L, 7L),
                MoneyJournalRecord.deposit(1L, 2L, 3, 40L, 5L, 6L, 7L));
        assertNotEquals("no stored row", a, null);
    }

    private static void assertLeg(MoneyJournalRecord.Leg leg, short legNo, long userId, short role,
                                  long balanceAfter) {
        assertEquals(legNo, leg.legNo());
        assertEquals(userId, leg.userId());
        assertEquals(Short.valueOf(role), leg.assetRole());
        assertEquals(balanceAfter, leg.balanceAfter());
    }
}
