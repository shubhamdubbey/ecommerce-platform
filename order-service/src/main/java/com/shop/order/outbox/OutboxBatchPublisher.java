package com.shop.order.outbox;

import com.shop.order.outbox.OutboxRepository.OutboxRow;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class OutboxBatchPublisher {

    private final OutboxRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final int batchSize;
    private final long sendTimeoutSeconds;

    public OutboxBatchPublisher(OutboxRepository repository,
                                KafkaTemplate<String, Object> kafkaTemplate,
                                @Value("${app.outbox.batch-size}") int batchSize,
                                @Value("${app.outbox.send-timeout-seconds}") long sendTimeoutSeconds) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.batchSize = batchSize;
        this.sendTimeoutSeconds = sendTimeoutSeconds;
    }

    /**
     * Claims a batch, sends it to Kafka, waits for all acknowledgements, then marks the rows published.
     * If anything fails the transaction rolls back and the rows stay unpublished for the next attempt.
     */
    @Transactional
    public int publishBatch() {
        List<OutboxRow> rows = repository.claimBatch(batchSize);
        if (rows.isEmpty()) {
            return 0;
        }

        List<CompletableFuture<?>> sends = new ArrayList<>(rows.size());
        for (OutboxRow row : rows) {
            SpecificRecord event = AvroJson.fromJson(row.payload(), OutboxEventTypes.classFor(row.eventType()));
            sends.add(kafkaTemplate.send(new ProducerRecord<>(row.topic(), row.eventKey(), event)));
        }

        try {
            CompletableFuture.allOf(sends.toArray(new CompletableFuture[0]))
                    .get(sendTimeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OutboxPublishException("Interrupted while publishing outbox batch", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new OutboxPublishException("Kafka send failed for a batch of " + rows.size() + " event(s)", e);
        }

        repository.markPublished(rows.stream().map(OutboxRow::id).toList());
        return rows.size();
    }
}