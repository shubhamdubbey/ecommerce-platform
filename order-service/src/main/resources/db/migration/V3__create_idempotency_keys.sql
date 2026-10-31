CREATE TABLE idempotency_keys (
                                  customer_id      UUID         NOT NULL,
                                  idempotency_key  VARCHAR(128) NOT NULL,
                                  request_hash     VARCHAR(64)  NOT NULL,
                                  order_id         UUID         NOT NULL REFERENCES orders (id) DEFERRABLE INITIALLY DEFERRED,
                                  created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
                                  expires_at       TIMESTAMPTZ  NOT NULL,
                                  PRIMARY KEY (customer_id, idempotency_key)
);

CREATE UNIQUE INDEX uq_idempotency_keys_order ON idempotency_keys (order_id);
CREATE INDEX idx_idempotency_keys_expires ON idempotency_keys (expires_at);