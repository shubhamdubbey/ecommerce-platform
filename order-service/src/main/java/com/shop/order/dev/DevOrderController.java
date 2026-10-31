package com.shop.order.dev;

import com.shop.order.api.OrderResponse;
import com.shop.order.domain.OrderStatus;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/dev/orders")
@ConditionalOnProperty(name = "app.dev-endpoints.enabled", havingValue = "true")
public class DevOrderController {

    public record TransitionRequest(@NotNull OrderStatus to) {
    }

    private final DevOrderService devOrderService;

    public DevOrderController(DevOrderService devOrderService) {
        this.devOrderService = devOrderService;
    }

    @PostMapping("/{id}/transition")
    public OrderResponse transition(@PathVariable UUID id,
                                    @Valid @RequestBody TransitionRequest request,
                                    @RequestParam(defaultValue = "0") long delayMs) {
        return devOrderService.transition(id, request.to(), delayMs);
    }
}