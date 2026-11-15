// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.application.engine.order;

import com.openexchange.core.determinism.CoreEngineEvent;
import com.openexchange.core.domain.commands.CreateOrderCommand;
import org.junit.Test;

import java.util.Arrays;

import static com.openexchange.core.application.engine.order.OrderRig.ONE;
import static com.openexchange.core.application.engine.order.OrderRig.USD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A measurement, not an acceptance row: what one user-scoped RequestOrderSnapshot costs the service thread at 10 000
 * and at 100 000 open orders. The query walks the whole order table in orderId order and filters by user, so its cost
 * follows the table, not the user's own orders. The numbers are printed for the decision they feed (whether the
 * open-orders query needs a per-user index); nothing here fails on a duration.
 */
public class OrderSnapshotQueryCostTest {

    private static final int ORDERS_PER_USER = 200;

    private static OrderRig withOpenOrders(final int orders) {
        final OrderRig r = OrderRig.open();
        final int users = orders / ORDERS_PER_USER;
        for (int u = 1; u <= users; u++) {
            r.deposit(u, USD, 1_000_000 * ONE);
        }
        for (int i = 0; i < orders; i++) {
            final CreateOrderCommand c = r.limit(1 + (i % users), 1, 0, ONE, ONE);
            r.create(c);
            r.advance(1);
        }
        assertEquals("precondition: every create rests in the table", orders, r.orders.orders().size());
        return r;
    }

    /** {median, p99, max} in nanoseconds of one user-scoped query (one page: the user's 200 orders). */
    private static long[] measure(final OrderRig r, final int samples) {
        final long[] ns = new long[samples];
        for (int i = -200; i < samples; i++) { // 200 warm-up queries
            final long user = 1 + (Math.abs(i) % 37);
            r.markNow();
            final long t0 = System.nanoTime();
            r.orders.applyRequestOrderSnapshot(1L, user, 0L, false);
            final long t1 = System.nanoTime();
            if (i >= 0) {
                ns[i] = t1 - t0;
            }
            assertEquals("the reply is the user's own orders", ORDERS_PER_USER, r.count(CoreEngineEvent.OrderSnapshotEntry.class));
        }
        Arrays.sort(ns);
        return new long[] {ns[samples / 2], ns[samples * 99 / 100], ns[samples - 1]};
    }

    @Test
    public void reportTheApplyCostOfOneOpenOrdersQuery() {
        for (final int orders : new int[] {10_000, 100_000}) {
            final OrderRig r = withOpenOrders(orders);
            final long[] ns = measure(r, 300);
            assertTrue(ns[0] > 0);
            System.out.println(String.format("[query-cost] open orders=%d: one user-scoped RequestOrderSnapshot (200 rows) median=%d us"
                            + " p99=%d us max=%d us (recording sink, one JVM, not pinned)", orders, ns[0] / 1_000, ns[1] / 1_000,
                    ns[2] / 1_000));
        }
    }
}
