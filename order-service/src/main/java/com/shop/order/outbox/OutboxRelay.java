package com.shop.order.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxBatchPublisher batchPublisher;

    public OutboxRelay(OutboxBatchPublisher batchPublisher) {
        this.batchPublisher = batchPublisher;
    }

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms}")
    public void poll() {
        try {
            int published;
            do {
                published = batchPublisher.publishBatch();
                if (published > 0) {
                    log.info("Published {} outbox event(s) to Kafka", published);
                }
            } while (published > 0);
        } catch (Exception e) {
            log.warn("Outbox publishing failed, will retry on the next poll: {}", e.toString());
        }
    }
}