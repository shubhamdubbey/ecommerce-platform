package com.shop.order.service;

import com.shop.order.api.CreateOrderRequest;
import com.shop.order.api.OrderResponse;
import com.shop.order.domain.Order;
import com.shop.order.domain.OrderNotFoundException;
import com.shop.order.pricing.PriceCalculator;
import com.shop.order.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final PriceCalculator priceCalculator;
    private final long expiryMinutes;

    public OrderService(OrderRepository orderRepository,
                        PriceCalculator priceCalculator,
                        @Value("${app.order.expiry-minutes}") long expiryMinutes) {
        this.orderRepository = orderRepository;
        this.priceCalculator = priceCalculator;
        this.expiryMinutes = expiryMinutes;
    }

    @Transactional
    public OrderResponse create(CreateOrderRequest request) {
        Instant expiresAt = Instant.now().plus(expiryMinutes, ChronoUnit.MINUTES);
        Order order = Order.create(request.customerId(), request.currency(), expiresAt);

        for (CreateOrderRequest.Item item : request.items()) {
            order.addItem(item.sku(), item.qty(), priceCalculator.unitPrice(item.sku()));
        }

        orderRepository.saveAndFlush(order);
        log.info("Order {} created for customer {} total {} {}",
                order.getId(), order.getCustomerId(), order.getGrandTotal(), order.getCurrency());
        return OrderResponse.from(order);
    }

    @Transactional(readOnly = true)
    public OrderResponse get(UUID id) {
        return orderRepository.findWithItemsById(id)
                .map(OrderResponse::from)
                .orElseThrow(() -> new OrderNotFoundException(id));
    }
}