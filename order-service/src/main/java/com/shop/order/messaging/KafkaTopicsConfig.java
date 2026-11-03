package com.shop.order.messaging;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.time.Duration;

/**
 * Explanation: the broker has auto-creation turned off (Phase 1), so the service declares the topics it owns. Spring’s KafkaAdmin creates any missing NewTopic bean at
 * startup and leaves existing ones alone. 12 partitions and 30-day retention follow section 8 of the design (30 days so the read model can be rebuilt). Replicas default
 * to 1 because we have a single local broker, and production would set 3 through configuration.
 */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    public NewTopic orderCreatedTopic(@Value("${app.kafka.partitions:12}") int partitions,
                                      @Value("${app.kafka.replicas:1}") short replicas) {
        return TopicBuilder.name(Topics.ORDER_CREATED)
                .partitions(partitions)
                .replicas(replicas)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(Duration.ofDays(30).toMillis()))
                .build();
    }
}