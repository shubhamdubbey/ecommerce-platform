package com.shop.order.domain;

import java.util.UUID;

public class IllegalOrderTransitionException extends RuntimeException {

    public IllegalOrderTransitionException(UUID orderId, OrderStatus from, OrderStatus to) {
        super("Order " + orderId + ": illegal transition " + from + " -> " + to);
    }
}