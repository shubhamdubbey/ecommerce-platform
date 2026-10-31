package com.shop.order.idempotency;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

/**
 * Explanation:
 *
 * JdbcClient is Spring’s modern fluent way to run SQL. Here we need Postgres-specific SQL (ON CONFLICT), which is awkward in JPA.
 * ON CONFLICT ... DO NOTHING is the atomic “insert if absent”. update() returns the number of rows inserted: 1 means we claimed the key, 0 means someone else had.
 * Propagation.MANDATORY means “this method must be called inside an existing transaction, or throw”. It enforces the rule that the key row and the order commit together, so nobody can accidentally call it outside one.
 * The PostgreSQL driver doesn’t accept java.time.Instant as a parameter, so we convert to OffsetDateTime in UTC.
 * Both statements run on the same connection as the JPA transaction, because Spring shares the connection.
 */
@Repository
public class IdempotencyKeyRepository {

    public record StoredKey(String requestHash, UUID orderId) {
    }

    private final JdbcClient jdbc;

    public IdempotencyKeyRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Returns true if this call claimed the key, false if the key already existed. */
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean insertIfAbsent(UUID customerId, String key, String requestHash,
                                  UUID orderId, Instant expiresAt) {
        int rows = jdbc.sql("""
                        INSERT INTO idempotency_keys
                            (customer_id, idempotency_key, request_hash, order_id, expires_at)
                        VALUES (:customerId, :key, :hash, :orderId, :expiresAt)
                        ON CONFLICT (customer_id, idempotency_key) DO NOTHING
                        """)
                .param("customerId", customerId)
                .param("key", key)
                .param("hash", requestHash)
                .param("orderId", orderId)
                .param("expiresAt", OffsetDateTime.ofInstant(expiresAt, ZoneOffset.UTC))
                .update();
        return rows == 1;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public Optional<StoredKey> find(UUID customerId, String key) {
        return jdbc.sql("""
                        SELECT request_hash, order_id
                        FROM idempotency_keys
                        WHERE customer_id = :customerId AND idempotency_key = :key
                        """)
                .param("customerId", customerId)
                .param("key", key)
                .query((rs, rowNum) -> new StoredKey(
                        rs.getString("request_hash"),
                        rs.getObject("order_id", UUID.class)))
                .optional();
    }
}