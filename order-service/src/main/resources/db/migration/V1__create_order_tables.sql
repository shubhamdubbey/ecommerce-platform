CREATE TABLE orders (
                        id            UUID           PRIMARY KEY,
                        customer_id   UUID           NOT NULL,
                        status        VARCHAR(32)    NOT NULL,
                        grand_total   NUMERIC(12, 2) NOT NULL,
                        currency      CHAR(3)        NOT NULL,
                        coupon_code   VARCHAR(64),
                        version       BIGINT         NOT NULL DEFAULT 0,
                        created_at    TIMESTAMPTZ    NOT NULL DEFAULT now(),
                        updated_at    TIMESTAMPTZ    NOT NULL DEFAULT now(),
                        expires_at    TIMESTAMPTZ,
                        CONSTRAINT chk_orders_status CHECK (status IN (
                                                                       'CREATED', 'STOCK_RESERVED', 'PAYMENT_PENDING', 'CONFIRMED',
                                                                       'REJECTED', 'PAYMENT_FAILED', 'CANCELLED', 'EXPIRED'))
);

CREATE TABLE order_items (
                             id          BIGSERIAL      PRIMARY KEY,
                             order_id    UUID           NOT NULL REFERENCES orders (id),
                             sku         VARCHAR(64)    NOT NULL,
                             qty         INTEGER        NOT NULL CHECK (qty > 0),
                             unit_price  NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0)
);

CREATE INDEX idx_orders_customer_created ON orders (customer_id, created_at DESC);
CREATE INDEX idx_orders_status_expires   ON orders (status, expires_at);
CREATE INDEX idx_order_items_order       ON order_items (order_id);