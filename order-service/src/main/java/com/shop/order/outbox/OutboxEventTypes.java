package com.shop.order.outbox;

import com.shop.events.order.OrderCreated;
import org.apache.avro.specific.SpecificRecord;

import java.util.Map;

/** Allow-list of event types this service may publish. Add each new event here. */
public final class OutboxEventTypes {

    private static final Map<String, Class<? extends SpecificRecord>> TYPES = Map.of(
            "OrderCreated", OrderCreated.class);

    private OutboxEventTypes() {
    }

    public static String nameOf(SpecificRecord event) {
        String name = event.getClass().getSimpleName();
        if (!TYPES.containsKey(name)) {
            throw new IllegalArgumentException("Event type not registered for outbox: " + name);
        }
        return name;
    }

    public static Class<? extends SpecificRecord> classFor(String eventType) {
        Class<? extends SpecificRecord> type = TYPES.get(eventType);
        if (type == null) {
            throw new IllegalStateException("Unknown outbox event type: " + eventType);
        }
        return type;
    }
}