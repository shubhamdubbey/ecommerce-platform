package com.shop.order.api;

import com.shop.order.domain.Order;
import com.shop.order.domain.OrderStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OrderResponse(
        UUID id,
        UUID customerId,
        OrderStatus status,
        BigDecimal grandTotal,
        String currency,
        long version,
        Instant createdAt,
        Instant expiresAt,
        List<Item> items) {

    public record Item(String sku, int qty, BigDecimal unitPrice) {
    }

    public static OrderResponse from(Order order) {
        return new OrderResponse(
                order.getId(),
                order.getCustomerId(),
                order.getStatus(),
                order.getGrandTotal(),
                order.getCurrency(),
                order.getVersion() == null ? 0L : order.getVersion(),
                order.getCreatedAt(),
                order.getExpiresAt(),
                order.getItems().stream()
                        .map(i -> new Item(i.getSku(), i.getQty(), i.getUnitPrice()))
                        .toList());
    }
}