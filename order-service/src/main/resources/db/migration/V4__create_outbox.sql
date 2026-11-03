CREATE TABLE outbox (
                        id            BIGSERIAL    PRIMARY KEY,
                        aggregate_id  UUID         NOT NULL,
                        event_type    VARCHAR(100) NOT NULL,
                        topic         VARCHAR(200) NOT NULL,
                        event_key     VARCHAR(100) NOT NULL,
                        payload       TEXT         NOT NULL,
                        created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
                        published_at  TIMESTAMPTZ
);

CREATE INDEX idx_outbox_unpublished
    ON outbox (id) WHERE published_at IS NULL;

CREATE INDEX idx_outbox_aggregate_unpublished
    ON outbox (aggregate_id, id) WHERE published_at IS NULL;