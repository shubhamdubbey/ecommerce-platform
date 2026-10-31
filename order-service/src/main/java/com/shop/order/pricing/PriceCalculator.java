package com.shop.order.pricing;

import java.math.BigDecimal;

public interface PriceCalculator {

    BigDecimal unitPrice(String sku);
}