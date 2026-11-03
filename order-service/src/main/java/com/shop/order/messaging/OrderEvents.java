package com.shop.order.messaging;

import com.shop.events.common.EventMetadata;
import com.shop.events.order.OrderCreated;
import com.shop.order.domain.Order;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

/** Builds Avro events from the domain model. */
public final class OrderEvents {

    private OrderEvents() {
    }

    public static OrderCreated orderCreated(Order order, UUID correlationId) {
        EventMetadata metadata = EventMetadata.newBuilder()
                .setEventId(UUID.randomUUID())
                .setEventType("OrderCreated")
                .setOccurredAt(Instant.now().truncatedTo(ChronoUnit.MILLIS))
                .setCorrelationId(correlationId)
                .setProducer("order-service")
                .build();

        List<com.shop.events.common.OrderItem> items = order.getItems().stream()
                .map(i -> com.shop.events.common.OrderItem.newBuilder()
                        .setSku(i.getSku())
                        .setQty(i.getQty())
                        .setUnitPrice(i.getUnitPrice())
                        .build())
                .toList();

        return OrderCreated.newBuilder()
                .setMetadata(metadata)
                .setOrderId(order.getId())
                .setCustomerId(order.getCustomerId())
                .setItems(items)
                .setGrandTotal(order.getGrandTotal())
                .setCurrency(order.getCurrency())
                .setCouponCode(order.getCouponCode())
                .build();
    }
}