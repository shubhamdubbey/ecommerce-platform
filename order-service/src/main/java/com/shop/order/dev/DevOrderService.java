package com.shop.order.dev;

import com.shop.order.api.OrderResponse;
import com.shop.order.domain.Order;
import com.shop.order.domain.OrderNotFoundException;
import com.shop.order.domain.OrderStatus;
import com.shop.order.repository.OrderRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/** DEV ONLY: lets us drive the state machine by hand. Removed when Kafka events drive transitions. */
@Service
@ConditionalOnProperty(name = "app.dev-endpoints.enabled", havingValue = "true")
public class DevOrderService {

    private final OrderRepository orderRepository;

    public DevOrderService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @Transactional
    public OrderResponse transition(UUID id, OrderStatus target, long delayMs) {
        Order order = orderRepository.findWithItemsById(id)
                .orElseThrow(() -> new OrderNotFoundException(id));

        order.transitionTo(target);

        if (delayMs > 0) {
            try {
                Thread.sleep(delayMs);   // simulates a slow handler so we can create a race
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }

        orderRepository.saveAndFlush(order);
        return OrderResponse.from(order);
    }
}