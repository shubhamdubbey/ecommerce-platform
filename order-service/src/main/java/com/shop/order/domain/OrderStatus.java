package com.shop.order.domain;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public enum OrderStatus {
    CREATED,
    STOCK_RESERVED,
    PAYMENT_PENDING,
    CONFIRMED,
    REJECTED,
    PAYMENT_FAILED,
    CANCELLED,
    EXPIRED;

    private static final Map<OrderStatus, Set<OrderStatus>> ALLOWED = new EnumMap<>(OrderStatus.class);

    static {
        ALLOWED.put(CREATED, EnumSet.of(STOCK_RESERVED, REJECTED, CANCELLED, EXPIRED));
        ALLOWED.put(STOCK_RESERVED, EnumSet.of(PAYMENT_PENDING, CANCELLED, EXPIRED));
        ALLOWED.put(PAYMENT_PENDING, EnumSet.of(CONFIRMED, PAYMENT_FAILED, CANCELLED, EXPIRED));
    }

    // this is used to check if a status can be moved to desired state
    public boolean canTransitionTo(OrderStatus target) {
        return ALLOWED.getOrDefault(this, Set.of()).contains(target);
    }

    public boolean isTerminal() {
        return !ALLOWED.containsKey(this);
    }
}