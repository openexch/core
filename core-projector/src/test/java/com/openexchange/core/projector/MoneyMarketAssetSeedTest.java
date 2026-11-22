// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import com.openexchange.core.domain.Market;
import org.junit.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The market rows V002 seeds into money_market_asset are the core's compiled market table: a settle leg's asset is
 * resolved through them, so a market the table lacks (or one with other assets) would show a balance movement under
 * the wrong asset or none. A market added to {@link Market#ALL} fails this test until a migration adds its row.
 */
public class MoneyMarketAssetSeedTest {

    @Test
    public void seededMarketsEqualTheCompiledMarketTable() throws Exception {
        final String sql;
        try (InputStream in = getClass().getResourceAsStream("/db/migration/V002__balance_movements_view.sql")) {
            assertTrue("V002 on the classpath", in != null);
            sql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        final int from = sql.indexOf("INSERT INTO money_market_asset");
        assertTrue("V002 seeds money_market_asset", from >= 0);
        final String values = sql.substring(from, sql.indexOf(';', from));
        final Matcher row = Pattern.compile("\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)").matcher(values);
        final List<String> seeded = new ArrayList<>();
        while (row.find()) {
            seeded.add(row.group(1) + ":" + row.group(2) + ":" + row.group(3));
        }
        final List<String> compiled = new ArrayList<>();
        for (final Market m : Market.ALL) {
            compiled.add(m.marketId() + ":" + m.baseAsset().id() + ":" + m.quoteAsset().id());
        }
        assertEquals("money_market_asset rows (market:base:quote) against Market.ALL", compiled, seeded);
    }
}
