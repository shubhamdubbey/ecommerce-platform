package com.shop.order.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

import java.math.BigDecimal;

/**
 * Explanation:
 *
 * @Entity + @Table map the class to the order_items table from V1.
 * IDENTITY means the database assigns the id (the BIGSERIAL column).
 * @ManyToOne(fetch = LAZY) loads the parent order only when needed. Lazy is the safe default.
 * The constructor is package-private, so only Order (same package) can create items. Nobody can build an orphan item.
 * The protected no-argument constructor exists only because JPA needs one.
 * There are getters but no setters. Items are immutable once created.
 */

@Entity
@Table(name = "order_items")
public class OrderItem {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "order_id", nullable = false)
    private Order order;

    @Column(nullable = false, length = 64)
    private String sku;

    @Column(nullable = false)
    private int qty;

    @Column(name = "unit_price", nullable = false, precision = 12, scale = 2)
    private BigDecimal unitPrice;

    protected OrderItem() {
        // required by JPA
    }

    OrderItem(Order order, String sku, int qty, BigDecimal unitPrice) {
        this.order = order;
        this.sku = sku;
        this.qty = qty;
        this.unitPrice = unitPrice;
    }

    public String getSku() {
        return sku;
    }

    public int getQty() {
        return qty;
    }

    public BigDecimal getUnitPrice() {
        return unitPrice;
    }
}