package com.shop.order.outbox;

import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Component
public class OutboxWriter {

    private final OutboxRepository repository;

    public OutboxWriter(OutboxRepository repository) {
        this.repository = repository;
    }

    /** Must be called inside the business transaction, so the event commits or rolls back with it. */
    @Transactional(propagation = Propagation.MANDATORY)
    public void write(UUID aggregateId, String topic, String key, SpecificRecord event) {
        repository.insert(aggregateId, OutboxEventTypes.nameOf(event), topic, key, AvroJson.toJson(event));
    }
}