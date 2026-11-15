// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.domain;

/**
 * Read-only view of what a config report carries: the recorded config, the limits record, the global gate, and per
 * market the recorded row, the client-facing risk limits and the market's own gate and breaker state. Markets and
 * assets are indexed {@code 0..count-1} in ascending id; both counts are 0 while no config is recorded.
 */
public interface CoreConfigReportView {

    long configVersion();

    long behaviorVersion();

    long limitsVersion();

    long requestTtlMs();

    int requestPerUserCap();

    int orderEventRingCapacity();

    /** The global gate: wire state code and reason bitmask. */
    int gateStateCode();

    int gateReasons();

    int marketCount();

    int marketId(int index);

    String symbol(int index);

    int baseAssetId(int index);

    int quoteAssetId(int index);

    long tickSize(int index);

    long minPrice(int index);

    long maxPrice(int index);

    long lotSize(int index);

    long minQuantity(int index);

    long maxQuantity(int index);

    long minNotional(int index);

    long maxNotional(int index);

    long riskConfigVersion(int index);

    int marketGateStateCode(int index);

    int marketGateReasons(int index);

    boolean circuitBreaker(int index);

    int assetCount();

    int assetId(int index);

    String assetCode(int index);

    int assetScale(int index);
}
