// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.journal;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** The archive replay settings: the slots kept for cluster catch-up, validated against the cap, and the readers' budget. */
public class OrderJournalSettingsTest {

    @Test
    public void defaultsKeepTwoReplaySlotsForTheClusterAndAdvertiseTheReadersBudget() {
        final OrderJournalSettings s = OrderJournalSettings.defaults();
        assertEquals(10, s.maxConcurrentReplays());
        assertEquals(2, s.clusterReservedReplays());
        assertEquals(8, s.readerReplayBudget());
        assertTrue(s.toString(), s.toString().contains("CORE_ARCHIVE_MAX_CONCURRENT_REPLAYS=10 "
                + "CORE_ARCHIVE_CLUSTER_RESERVED_REPLAYS=2 readerReplayBudget=8"));
    }

    @Test
    public void noReservationIsASettingNotAnError() {
        final OrderJournalSettings s = OrderJournalSettings.with(Map.of(
                OrderJournalSettings.Key.CORE_ARCHIVE_CLUSTER_RESERVED_REPLAYS, "0"));
        assertEquals(10, s.readerReplayBudget());
    }

    @Test
    public void aReservationThatLeavesReadersNoSlotRefusesBootNamingBothSettings() {
        for (final String reserved : new String[] {"4", "5"}) {
            try {
                OrderJournalSettings.with(Map.of(OrderJournalSettings.Key.CORE_ARCHIVE_MAX_CONCURRENT_REPLAYS, "4",
                        OrderJournalSettings.Key.CORE_ARCHIVE_CLUSTER_RESERVED_REPLAYS, reserved));
                fail("reserved " + reserved + " of 4 was accepted");
            } catch (final IllegalArgumentException expected) {
                assertEquals("CORE_ARCHIVE_CLUSTER_RESERVED_REPLAYS=" + reserved + " leaves readers no replay slot: it must"
                        + " be below CORE_ARCHIVE_MAX_CONCURRENT_REPLAYS=4", expected.getMessage());
            }
        }
        try {
            OrderJournalSettings.with(Map.of(OrderJournalSettings.Key.CORE_ARCHIVE_CLUSTER_RESERVED_REPLAYS, "-1"));
            fail("a negative reservation was accepted");
        } catch (final IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().startsWith("CORE_ARCHIVE_CLUSTER_RESERVED_REPLAYS=-1"));
        }
    }
}
