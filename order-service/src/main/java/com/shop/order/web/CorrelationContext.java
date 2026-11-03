package com.shop.order.web;

import org.slf4j.MDC;

import java.util.UUID;

public final class CorrelationContext {

    private CorrelationContext() {
    }

    public static UUID currentOrNew() {
        String current = MDC.get(CorrelationIdFilter.MDC_KEY);
        return current != null ? UUID.fromString(current) : UUID.randomUUID();
    }
}