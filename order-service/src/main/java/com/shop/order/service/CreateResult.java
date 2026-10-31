package com.shop.order.service;

import com.shop.order.api.OrderResponse;

public record CreateResult(OrderResponse order, boolean replayed) {
}