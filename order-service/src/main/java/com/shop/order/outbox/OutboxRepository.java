package com.shop.order.outbox;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Repository
public class OutboxRepository {

    public record OutboxRow(long id, UUID aggregateId, String eventType,
                            String topic, String eventKey, String payload) {
    }

    private final JdbcClient jdbc;

    public OutboxRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void insert(UUID aggregateId, String eventType, String topic, String eventKey, String payload) {
        jdbc.sql("""
                        INSERT INTO outbox (aggregate_id, event_type, topic, event_key, payload)
                        VALUES (:aggregateId, :eventType, :topic, :eventKey, :payload)
                        """)
                .param("aggregateId", aggregateId)
                .param("eventType", eventType)
                .param("topic", topic)
                .param("eventKey", eventKey)
                .param("payload", payload)
                .update();
    }

    /**
     * Claims unpublished rows for this transaction. A row is skipped while an EARLIER unpublished row
     * of the same aggregate exists, so events of one order are always published in order, even with
     * several publisher instances running.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public List<OutboxRow> claimBatch(int limit) {
        return jdbc.sql("""
                        SELECT o.id, o.aggregate_id, o.event_type, o.topic, o.event_key, o.payload
                        FROM outbox o
                        WHERE o.published_at IS NULL
                          AND NOT EXISTS (
                              SELECT 1 FROM outbox earlier
                              WHERE earlier.aggregate_id = o.aggregate_id
                                AND earlier.id < o.id
                                AND earlier.published_at IS NULL)
                        ORDER BY o.id
                        LIMIT :limit
                        FOR UPDATE OF o SKIP LOCKED
                        """)
                .param("limit", limit)
                .query((rs, rowNum) -> new OutboxRow(
                        rs.getLong("id"),
                        rs.getObject("aggregate_id", UUID.class),
                        rs.getString("event_type"),
                        rs.getString("topic"),
                        rs.getString("event_key"),
                        rs.getString("payload")))
                .list();
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void markPublished(List<Long> ids) {
        jdbc.sql("UPDATE outbox SET published_at = now() WHERE id IN (:ids)")
                .param("ids", ids)
                .update();
    }
}