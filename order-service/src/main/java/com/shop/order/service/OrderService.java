package com.shop.order.service;

import com.shop.order.api.CreateOrderRequest;
import com.shop.order.api.OrderResponse;
import com.shop.order.domain.Order;
import com.shop.order.domain.OrderNotFoundException;
import com.shop.order.idempotency.IdempotencyKeyRepository;
import com.shop.order.idempotency.IdempotencyKeyRepository.StoredKey;
import com.shop.order.idempotency.IdempotencyKeyReuseException;
import com.shop.order.idempotency.RequestFingerprint;
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
    private final IdempotencyKeyRepository idempotencyKeys;
    private final PriceCalculator priceCalculator;
    private final long expiryMinutes;
    private final long keyTtlHours;

    public OrderService(OrderRepository orderRepository,
                        IdempotencyKeyRepository idempotencyKeys,
                        PriceCalculator priceCalculator,
                        @Value("${app.order.expiry-minutes}") long expiryMinutes,
                        @Value("${app.idempotency.ttl-hours}") long keyTtlHours) {
        this.orderRepository = orderRepository;
        this.idempotencyKeys = idempotencyKeys;
        this.priceCalculator = priceCalculator;
        this.expiryMinutes = expiryMinutes;
        this.keyTtlHours = keyTtlHours;
    }

    @Transactional
    public CreateResult create(String idempotencyKey, CreateOrderRequest request) {
        String hash = RequestFingerprint.of(request);
        UUID orderId = UUID.randomUUID();
        Instant now = Instant.now();

        boolean firstTime = idempotencyKeys.insertIfAbsent(
                request.customerId(), idempotencyKey, hash, orderId,
                now.plus(keyTtlHours, ChronoUnit.HOURS));

        if (!firstTime) {
            return replay(idempotencyKey, request, hash);
        }

        Order order = Order.create(orderId, request.customerId(), request.currency(),
                now.plus(expiryMinutes, ChronoUnit.MINUTES));
        for (CreateOrderRequest.Item item : request.items()) {
            order.addItem(item.sku(), item.qty(), priceCalculator.unitPrice(item.sku()));
        }
        orderRepository.saveAndFlush(order);

        log.info("Order {} created for customer {} total {} {} (key {})",
                order.getId(), order.getCustomerId(), order.getGrandTotal(),
                order.getCurrency(), idempotencyKey);
        return new CreateResult(OrderResponse.from(order), false);
    }

    private CreateResult replay(String idempotencyKey, CreateOrderRequest request, String hash) {
        StoredKey stored = idempotencyKeys.find(request.customerId(), idempotencyKey)
                .orElseThrow(() -> new IllegalStateException(
                        "Idempotency key disappeared: " + idempotencyKey));

        if (!stored.requestHash().equals(hash)) {
            throw new IdempotencyKeyReuseException(idempotencyKey);
        }

        Order existing = orderRepository.findWithItemsById(stored.orderId())
                .orElseThrow(() -> new OrderNotFoundException(stored.orderId()));

        log.info("Idempotent replay: key {} returns existing order {}", idempotencyKey, existing.getId());
        return new CreateResult(OrderResponse.from(existing), true);
    }

    @Transactional(readOnly = true)
    public OrderResponse get(UUID id) {
        return orderRepository.findWithItemsById(id)
                .map(OrderResponse::from)
                .orElseThrow(() -> new OrderNotFoundException(id));
    }
}