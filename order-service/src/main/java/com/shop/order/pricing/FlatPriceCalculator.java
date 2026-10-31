package com.shop.order.pricing;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/** TEMPORARY: replaced by the real pricing engine in Phase 5. */
@Component
public class FlatPriceCalculator implements PriceCalculator {

    @Override
    public BigDecimal unitPrice(String sku) {
        return new BigDecimal("100.00");
    }
}