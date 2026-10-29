# Event-Driven E-Commerce Platform: Kafka + RabbitMQ

**Design document, v2.0 (final, ready for implementation)**
Stack: Java 17, Spring Boot 3.x, PostgreSQL, Kafka with Avro and Confluent Schema Registry, RabbitMQ, Redis (cache and rate limit only), Docker Compose

---

## Contents

1. Purpose and goals
2. Architecture overview
3. Why Kafka for business events and RabbitMQ for email
4. Services and responsibilities
5. Order lifecycle (saga)
6. Order creation: the validation pipeline
7. The pricing engine
8. Kafka design
9. Avro and the Schema Registry
10. RabbitMQ design (email only)
11. CQRS: the order read model
12. Data model
13. Concurrency without distributed locks
14. Idempotency at four levels
15. Reliability patterns
16. Caching (Redis, cache-aside)
17. Resilience and protection
18. Background jobs
19. Edge cases and how each is handled
20. Observability
21. Scaling for large traffic
22. Testing strategy
23. Suggested package layout (per service)
24. Build phases
25. How this feeds the payments/UPI project
26. Deferred (not in this version)

---

## 1. Purpose and goals

This project is a production-grade order platform (order, inventory, payment, notification) built to learn Kafka and RabbitMQ the way they behave in real systems, not in a proof of concept. It is the successor to the SNS/SQS-based order-platform, and it is the warm-up for the later payments/UPI project.

**Goals**

1. Use Kafka for every business event (order, inventory, payment lifecycle).
2. Use RabbitMQ only for sending notification emails, and understand why it fits that job better than Kafka.
3. Apply production concepts: idempotency, transactional outbox, inbox, saga with compensation, retries, DLQ, caching, rate limiting, observability.
4. Design for large traffic: horizontal scaling, partitioning, no single-node bottlenecks.
5. Model the order-taking path properly: a validation pipeline and a pricing engine, so the order total is calculated by clear, testable rules.
6. Separate reads from writes with a CQRS read model built from Kafka events.
7. Make every event contract explicit with Avro schemas and a Schema Registry, with enforced compatibility rules.

**Non-goals**

- No Redis distributed locks (see section 13 for what replaces them).
- No real payment gateway. A gateway simulator stands in.
- No frontend beyond Postman/curl and a simple status endpoint.
- No shipment service and no outbound webhooks in this version (section 26).

---

## 2. Architecture overview

```
                      Client
                        |
               API Gateway / Edge
               (auth, rate limit)
                  |            |
           commands            queries
                  v            v
  +--------------------+    +----------------------+
  | Order Service      |    | Order Query Service  |
  |  - validation      |    |  (CQRS read model)   |
  |  - pricing engine  |    |  (query DB)          |
  |  - saga state      |    +----------------------+
  |  (orders DB)       |               ^
  +--------------------+               | projects events
         ^   |                         |
         |   v                         |
  ======================  KAFKA (Avro events)  ======================
         ^   |         ^   |          ^   |             |
         |   v         |   v          |   v             |
  +-----------+  +-----------+   +--------------+       |
  | Inventory |  | Payment   |   | Notification |<------+
  | Service   |  | Service   |   | bridge       |
  | (inv DB)  |  | (pay DB)  |   +------+-------+
  +-----------+  +-----+-----+          |
                       | HTTP           v
                       v            RabbitMQ
               +---------------+        |
               | Gateway       |        v
               | Simulator     |   +-----------+
               +---------------+   | Email     |
                                   | Worker    |
                                   +-----------+

  Schema Registry  <---  every Kafka producer and consumer (Avro schemas)
```

**Rules of the architecture**

- Each service owns its own database. No shared tables, no cross-service joins.
- Services communicate only through events (plus the synchronous client-facing API and the payment gateway call).
- Every state change that must be announced is written to an outbox table in the same transaction as the change.
- Every consumer is idempotent.
- Writes go to the service that owns the data. Reads for screens go to the Order Query Service (CQRS).
- Every Kafka event has an Avro schema, registered and compatibility-checked before it can be used.

---

## 3. Why Kafka for business events and RabbitMQ for email

| Concern | Kafka (business events) | RabbitMQ (email) |
|---|---|---|
| Nature of message | A fact that happened: `OrderCreated` | A job to do: "send this email" |
| Retention | Kept for days or weeks, replayable | Deleted after ack |
| Ordering | Per partition key (`orderId`) | Not needed |
| Consumers | Many independent groups read the same event | One worker pool consumes each job |
| Retry/delay | Must build retry topics yourself | Native: TTL, dead-letter exchange, delay queues |
| Per-message ack and redelivery | Offset based, coarse | Per message, fine grained |
| Priority, TTL per message | No | Yes |
| Audit and rebuild state | Yes, replay the log | No |

Summary: events are history, so Kafka. Emails are tasks with retries and delays, so RabbitMQ.

---

## 4. Services and responsibilities

### 4.1 Order Service
- Exposes `POST /orders`, `POST /orders/{id}/cancel`, `POST /pricing/quote`, and an internal `GET /internal/orders/{id}`. Customer-facing reads are served by the Order Query Service.
- Runs the validation pipeline (section 6) and the pricing engine (section 7) before accepting an order.
- Redeems and releases coupons, and stores the immutable price snapshot with the order.
- Owns the order state machine and saga state.
- Writes orders and outbox rows in one transaction.
- Consumes `inventory.*`, `payment.*` events to advance the order.
- Runs the order-expiry job.

### 4.2 Inventory Service
- Owns stock levels and reservations.
- Consumes `order.created`, `order.cancelled`, `order.expired`, `payment.completed`, `payment.failed`.
- Reserves, releases and commits stock using atomic SQL (section 13).
- Runs the reservation-expiry job.

### 4.3 Payment Service
- Owns payment attempts.
- Consumes `inventory.reserved`, calls the gateway with a payment idempotency key.
- Receives gateway webhooks, publishes `payment.completed` / `payment.failed`.
- Runs the payment reconciliation job.

### 4.4 Notification Service
- Kafka side (bridge): consumes order and payment events and turns relevant ones into email jobs on RabbitMQ.
- RabbitMQ side (worker): consumes email jobs, renders templates, sends via SMTP/SES, records what was sent.

### 4.5 Gateway Simulator
- A small separate app that mimics a payment gateway: random success, failure, timeout, delayed webhook, duplicate webhook. This is what lets you test the ugly paths.

---

### 4.6 Order Query Service
- Read-only CQRS side (section 11). Consumes order, inventory and payment events and projects them into `order_view` and `order_timeline`.
- Serves order details, order history (cursor pagination) and the order timeline.
- Falls back to the Order Service for orders it has not projected yet.

---

## 5. Order lifecycle (saga)

### 5.1 States

`CREATED -> STOCK_RESERVED -> PAYMENT_PENDING -> CONFIRMED`

Failure and terminal states: `REJECTED` (no stock), `PAYMENT_FAILED`, `CANCELLED` (by user), `EXPIRED` (timed out).

### 5.2 Allowed transitions

| From | Event | To |
|---|---|---|
| CREATED | `inventory.reserved` | STOCK_RESERVED |
| CREATED | `inventory.reservation-failed` | REJECTED |
| STOCK_RESERVED | `payment.initiated` | PAYMENT_PENDING |
| PAYMENT_PENDING | `payment.completed` | CONFIRMED |
| PAYMENT_PENDING | `payment.failed` | PAYMENT_FAILED |
| CREATED, STOCK_RESERVED, PAYMENT_PENDING | user cancel | CANCELLED |
| CREATED, STOCK_RESERVED, PAYMENT_PENDING | timeout | EXPIRED |

Any event that would cause an illegal transition is logged and ignored (never applied). Terminal states never change, except the special case in 5.4.

### 5.3 Happy path

1. Client calls `POST /orders` with an `Idempotency-Key`.
2. Order runs the validation pipeline (section 6) and the pricing engine (section 7), then in one transaction saves the order (CREATED) with its price snapshot, redeems the coupon, and writes the outbox row. It returns `202 Accepted` with the order id.
3. Outbox publisher emits `order.created`.
4. Inventory reserves stock, emits `inventory.reserved`.
5. Order moves to STOCK_RESERVED. Payment consumes `inventory.reserved`, creates a payment attempt, emits `payment.initiated`, calls the gateway.
6. Gateway confirms (response or webhook). Payment emits `payment.completed`.
7. Order moves to CONFIRMED and emits `order.confirmed`. Inventory commits the reservation (reserved becomes sold).
8. Notification bridge turns `order.confirmed` into an email job on RabbitMQ. Worker sends the email.
9. In parallel, the Order Query Service projects every event into the read model (section 11).

### 5.4 Compensation paths

- **Out of stock:** `inventory.reservation-failed`, order becomes REJECTED, rejection email sent. No payment is attempted.
- **Payment failed:** `payment.failed`, Inventory releases the reservation, order becomes PAYMENT_FAILED, email sent.
- **User cancel or expiry:** `order.cancelled` / `order.expired`, Inventory releases the reservation, Payment cancels or refunds if needed.
- **Late success after expiry** (payment completes after the order expired): Payment emits `payment.refund-requested`, and the refund is processed automatically. This is the one case where a payment event arrives for a terminal order.

---

In every failure or cancellation path, the Order Service also releases any redeemed coupon in the same transaction as the state change (section 7.6).

---

## 6. Order creation: the validation pipeline

### 6.1 What it is and where it runs

Before an order is accepted, a series of checks must pass. The validation pipeline runs these checks one after another inside the Order Service, synchronously, as part of `POST /orders`.

The order of work for one request:

```
rate limit -> idempotency key lookup -> validation pipeline -> pricing engine -> save order + outbox (one transaction) -> 202 Accepted
```

Idempotency comes first so that a retried request returns the stored response without being validated and priced again.

Validation answers "is this request acceptable?". It does **not** reserve stock. Real stock checking happens only through the atomic inventory update in the saga (section 13), because any stock check done earlier would be stale by the time the order is processed.

### 6.2 Stages and checks

Checks are grouped in stages, cheapest first. A later stage runs only if the earlier stage passed.

| Stage | Check | Data needed | Error code |
|---|---|---|---|
| 1. Shape (in memory) | Cart not empty | request only | `CART_EMPTY` |
| 1. Shape | Quantity per line between 1 and the max (for example 10) | request only | `QTY_OUT_OF_RANGE` |
| 1. Shape | No duplicate SKUs in the cart | request only | `DUPLICATE_SKU` |
| 1. Shape | Required fields and formats (pin code, email, currency) | request only | `INVALID_FIELD` |
| 2. Business (local DB or cache) | Every product exists and is active | catalog | `PRODUCT_NOT_FOUND`, `PRODUCT_INACTIVE` |
| 2. Business | Customer is not blocked | customer table | `CUSTOMER_BLOCKED` |
| 2. Business | Delivery pin code is serviceable | serviceability table | `ADDRESS_NOT_SERVICEABLE` |
| 2. Business | Cart total within allowed range, per-customer limits | computed | `ORDER_LIMIT_EXCEEDED` |
| 3. Remote (optional, with timeout and circuit breaker) | Address verification, basic fraud signal | external call | `ADDRESS_INVALID`, `FRAUD_SUSPECTED` |

### 6.3 Design

This is the Chain of Responsibility pattern, combined with the Specification pattern for composable rules.

```java
public interface OrderValidator {
    ValidationStage stage();                       // SHAPE, BUSINESS, REMOTE
    ValidationResult validate(OrderContext ctx);   // pure: no side effects
}

public final class ValidationPipeline {
    private final List<OrderValidator> validators;  // injected, sorted by stage then @Order

    public ValidationResult run(OrderContext ctx, Mode mode) {
        List<ValidationError> errors = new ArrayList<>();
        for (ValidationStage stage : ValidationStage.values()) {
            for (OrderValidator v : validatorsFor(stage)) {
                ValidationResult r = v.validate(ctx);
                errors.addAll(r.errors());
                if (mode == Mode.FAIL_FAST && !errors.isEmpty()) return ValidationResult.failed(errors);
            }
            if (!errors.isEmpty()) break;          // do not start a more expensive stage
        }
        return errors.isEmpty() ? ValidationResult.ok() : ValidationResult.failed(errors);
    }
}
```

- `OrderContext` is an immutable object holding the request plus data loaded once (for example a `Map<String, Product>` fetched in one query). Validators read from it and never query on their own, which avoids N queries for N checks.
- Each validator is one small class with one job. Adding a rule means adding one class. Existing validators are not touched.
- Spring injects `List<OrderValidator>` automatically, so registering a validator needs no wiring code.

### 6.4 Results and errors

- Stage 1 and stage 2 collect all errors in the stage so the client sees every problem at once. Stage 3 fails fast because it is expensive.
- Response: `422 Unprocessable Entity` with a list of `{code, field, message}`. Malformed JSON stays `400`.
- Error codes are stable and documented, since clients depend on them.
- Remote checks that time out follow a stated policy per check: either fail open (accept and flag the order for review) or fail closed (reject with a retryable error). Address verification fails open, fraud fails closed.

### 6.5 Rules for validators

- No side effects. A validator never writes to the database or publishes events.
- Validation is not a substitute for database constraints. Constraints are still the last line of defence.
- Validation is deterministic for the same context, so every validator is unit-testable without Spring.

---

## 7. The pricing engine

### 7.1 What it is

The pricing engine answers one question: "what should this customer pay for this cart?" It turns a validated cart into a full price breakdown. The rules (discounts, coupons, tax, shipping) change often, so each one is a small separate piece instead of one big `if/else` method.

It runs as a module inside the Order Service. It has no dependencies on Kafka or the web layer, so it could be extracted into its own service later.

### 7.2 Inputs and outputs

**Input:** lines (sku, quantity), customer id, optional coupon code, delivery state and pin code.

**Output:** a `PriceBreakdown`:

```
lines[]          : sku, qty, unitPrice, lineSubtotal, discountsApplied[], taxAmount, lineTotal
adjustments[]    : every discount, fee and tax with its rule id and amount (for transparency)
subtotal         : sum of base line amounts
totalDiscount
shippingFee
totalTax
grandTotal
currency
pricingVersion   : version of the rule set used
computedAt
```

### 7.3 Money handling

- A `Money` value object (`BigDecimal` amount plus currency) is used everywhere. Raw `double` never appears in pricing code.
- Scale 2, one documented rounding mode (`HALF_UP`), applied at one defined point (per line after tax). The same rounding rule is used by the quote endpoint and by order creation, so the numbers never differ.
- The totals can never go negative. If discounts exceed the base amount, the line total is zero.

### 7.4 Calculation steps (fixed order)

| Step | What happens | Example |
|---|---|---|
| 1. Base price | Unit price from the catalog times quantity | 2 x 500 = 1000 |
| 2. Product or category promotion | "20% off shoes" | -200 |
| 3. Quantity discount | "Buy 3 or more, extra 5%" | 0 (not eligible) |
| 4. Coupon | "WELCOME100: flat 100 off on first order" | -100 |
| 5. Shipping | Free above 999 after discounts, else a flat fee | 0 or 50 |
| 6. Tax (GST) | Rate by product category, applied on the discounted price | 12% of 700 = 84 |
| 7. Final rounding and caps | Round, enforce non-negative, enforce max discount cap | grandTotal |

Order matters and is fixed in code and documented: tax is always computed on the amount after discounts, and shipping eligibility is decided on the amount after discounts and before tax.

### 7.5 Rules as data, built from small pieces

- **Strategy:** each rule type is a `PriceAdjuster` implementation (percentage discount, flat discount, quantity discount, free shipping, tax calculator).
- **Decorator / chain:** adjusters are applied in order, each one receiving the running result and returning the new one, and each records an adjustment entry.
- **Specification:** eligibility conditions ("cart value >= 999", "first order", "category in [shoes]", "customer segment = premium") are small composable specifications combined with `and`, `or`, `not`.
- **Rules live in tables**, not code: `promotions`, `coupons`, `tax_rules`, `shipping_rules` (section 12.1). A new festival sale is a new row, not a deployment.
- **Stacking policy** is explicit on each rule: `stackable` flag, priority, and a global maximum discount cap per order. Non-stackable rules lose to the higher-priority one.

### 7.6 Coupon redemption

Pricing itself is pure: the same input always gives the same output and it changes nothing. Redeeming a coupon is a separate, side-effecting step done inside the order creation transaction:

```sql
UPDATE coupons
SET used_count = used_count + 1
WHERE code = :code AND active = true AND used_count < max_uses AND now() BETWEEN valid_from AND valid_to;
```

- Zero rows updated means the coupon just ran out, so the order is rejected with `COUPON_EXHAUSTED`. No locks are needed. The conditional update is atomic.
- A row in `coupon_redemptions(coupon_id, customer_id, order_id)` with a unique constraint enforces per-customer limits and makes redemption idempotent per order.
- **Compensation:** when an order ends in `REJECTED`, `PAYMENT_FAILED`, `CANCELLED` or `EXPIRED`, the Order Service releases the redemption (decrement and delete the redemption row) in the same transaction as the state change. The release is idempotent: it acts only if a redemption row exists for that order.

### 7.7 Quote endpoint

`POST /pricing/quote` runs the same engine for the cart page. It does not redeem coupons and does not save anything. Order creation always recomputes the price on the server. The client never sends a trusted price. A quote is informational, not a promise: if a promotion ends between the quote and the order, the order uses the price at order time.

### 7.8 Price snapshot

The full `PriceBreakdown` is stored with the order in `order_pricing_snapshot` and never modified. Consequences:

- Later catalog or promotion changes do not affect existing orders.
- The Payment Service charges exactly `grandTotal` from the snapshot (carried in `order.created`, so no cross-service lookup is needed).
- Support can see exactly why an order cost what it did.
- Refunds are computed from the snapshot.

### 7.9 Patterns used

Strategy (adjusters), Decorator / chain (running result), Specification (eligibility), Builder (`PriceBreakdown`), Value Object (`Money`), Factory (adjuster selection from rule rows).

---

## 8. Kafka design

### 8.1 Topics

| Topic | Key | Producer | Consumers |
|---|---|---|---|
| `order.created` | orderId | Order | Inventory |
| `order.confirmed` | orderId | Order | Notification bridge, Inventory (commit) |
| `order.cancelled` | orderId | Order | Inventory, Payment, Notification bridge |
| `order.expired` | orderId | Order | Inventory, Payment, Notification bridge |
| `inventory.reserved` | orderId | Inventory | Order, Payment |
| `inventory.reservation-failed` | orderId | Inventory | Order, Notification bridge |
| `inventory.released` | orderId | Inventory | Order (audit) |
| `payment.initiated` | orderId | Payment | Order |
| `payment.completed` | orderId | Payment | Order, Inventory |
| `payment.failed` | orderId | Payment | Order, Inventory, Notification bridge |
| `payment.refund-requested` | orderId | Payment | Payment (refund worker) |

Each topic also gets `<topic>.retry-5s`, `<topic>.retry-1m`, and `<topic>.dlt` where retries make sense.

The Order Query Service consumes all of the order, inventory and payment topics above through its own consumer group.

**Key choice:** all topics use `orderId` as the key, so every event for one order lands in one partition and is processed in order. This single decision removes most race conditions in the saga.

**Sizing (starting point, tune later):** 12 partitions per business topic, replication factor 3 (1 in local Docker), `min.insync.replicas=2` in a real cluster, retention 7 days for inventory and payment topics and 30 days for `order.*` topics (so the read model can be rebuilt, section 11.7).

### 8.2 Event envelope

Events are Avro records. Every event starts with the same `metadata` record (full schema in section 9.5):

| Field | Purpose |
|---|---|
| `eventId` | Dedup key for the inbox table |
| `eventType` | Name of the event, for logs and routing |
| `occurredAt` | When the business fact happened (UTC) |
| `correlationId` | One id per client request, ties the whole saga together for tracing |
| `causationId` | `eventId` of the event that triggered this one, builds the cause-and-effect chain |
| `producer` | Name of the producing service |

There is no `version` field. The Schema Registry tracks versions, and evolution follows the rules in section 9.6.

### 8.3 Event payloads

Each event is `metadata` plus its own fields. Money fields use the Avro decimal type (section 9.4).

| Event | Fields after `metadata` |
|---|---|
| `order.created` | orderId, customerId, items[sku, qty, unitPrice], grandTotal, currency, couponCode (nullable) |
| `order.confirmed` | orderId, customerId, grandTotal, currency |
| `order.cancelled` / `order.expired` | orderId, reason |
| `inventory.reserved` | orderId, reservationId, items[sku, qty], expiresAt |
| `inventory.reservation-failed` | orderId, failedSkus[], reason |
| `inventory.released` | orderId, reservationId, reason |
| `payment.initiated` | orderId, paymentId, amount, currency |
| `payment.completed` | orderId, paymentId, amount, gatewayReference, method |
| `payment.failed` | orderId, paymentId, reasonCode, retryable |
| `payment.refund-requested` | orderId, paymentId, amount, reason |

### 8.4 Producer configuration

- `acks=all`
- `enable.idempotence=true`
- `max.in.flight.requests.per.connection=5` (safe with idempotence)
- `delivery.timeout.ms` generous, `retries` effectively unlimited, bounded by the delivery timeout
- `linger.ms` small (5 to 20) with batching for throughput
- `compression.type=lz4` or `zstd`
- `value.serializer=KafkaAvroSerializer` with the Schema Registry URL (details in section 9.7)

### 8.5 Consumer configuration

- `enable.auto.commit=false`, manual commit after the DB transaction commits
- `isolation.level=read_committed` if transactions are used
- `max.poll.records` tuned so one batch finishes well within `max.poll.interval.ms`
- Cooperative sticky assignor to reduce rebalance pain
- `KafkaAvroDeserializer` wrapped in `ErrorHandlingDeserializer`, `specific.avro.reader=true` (section 9.7)
- Concurrency per service equals partitions per instance, never more consumers than partitions

---

## 9. Avro and the Schema Registry

### 9.1 Why Avro

- Every Kafka event has a formal schema, so a producer cannot silently send a payload that breaks consumers.
- Events are compact binary, much smaller than JSON.
- Schemas can evolve safely under compatibility rules that the registry enforces.
- Producers and consumers are decoupled in time: an old consumer can read a new event and a new consumer can read an old event.

### 9.2 Moving parts

- **Schema Registry** (Confluent Schema Registry, a container in Docker Compose). Stores every schema version and hands out an integer schema id.
- **`event-contracts` module** (shared Maven module or its own repository) holding all `.avsc` files. The `avro-maven-plugin` generates Java classes from them. Services depend on a versioned artifact of this module.
- **Avro serializer/deserializer** in every Kafka producer and consumer.

### 9.3 Wire format and subjects

- A message value is: 1 magic byte, 4 bytes of schema id, then the Avro binary payload. The consumer reads the id, fetches (and caches) the writer schema, and resolves it against its own reader schema.
- **Subject naming:** `TopicNameStrategy`, so the subject is `<topic>-value` (for example `order.created-value`). One event type per topic keeps this simple.
- **Keys** stay plain strings (`orderId`), so partitioning and log viewing stay simple.

### 9.4 Type conventions (decide once, follow everywhere)

| Concept | Avro type |
|---|---|
| Identifiers | `string` with `logicalType: uuid` |
| Timestamps | `long` with `logicalType: timestamp-millis` (UTC) |
| Money | `bytes` with `logicalType: decimal`, precision 12, scale 2. Never `float` or `double` |
| Optional field | union `["null", T]` with `"default": null` (null first) |
| Closed set of values | `enum` with a `default` symbol (for example `UNKNOWN`) so new symbols do not break old readers |
| Open set of values | `string` |
| Lists | `array` |

### 9.5 Schemas

Shared metadata record, embedded in every event (this replaces the JSON envelope; there is no `version` field, because the registry tracks versions):

```json
{
  "type": "record", "name": "EventMetadata", "namespace": "com.shop.events.common",
  "fields": [
    {"name": "eventId",       "type": {"type": "string", "logicalType": "uuid"}},
    {"name": "eventType",     "type": "string"},
    {"name": "occurredAt",    "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "correlationId", "type": {"type": "string", "logicalType": "uuid"}},
    {"name": "causationId",   "type": ["null", {"type": "string", "logicalType": "uuid"}], "default": null},
    {"name": "producer",      "type": "string"}
  ]
}
```

Example event (`order.created`):

```json
{
  "type": "record", "name": "OrderCreated", "namespace": "com.shop.events.order",
  "fields": [
    {"name": "metadata",   "type": "com.shop.events.common.EventMetadata"},
    {"name": "orderId",    "type": {"type": "string", "logicalType": "uuid"}},
    {"name": "customerId", "type": {"type": "string", "logicalType": "uuid"}},
    {"name": "items", "type": {"type": "array", "items": {
        "type": "record", "name": "OrderItem", "fields": [
          {"name": "sku",       "type": "string"},
          {"name": "qty",       "type": "int"},
          {"name": "unitPrice", "type": {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}}
        ]}}},
    {"name": "grandTotal",  "type": {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}},
    {"name": "currency",    "type": "string"},
    {"name": "couponCode",  "type": ["null", "string"], "default": null}
  ]
}
```

Every other event follows the same shape: `metadata` first, then its own fields (listed in section 8.3).

### 9.6 Compatibility mode and evolution rules

- **Compatibility mode: `BACKWARD_TRANSITIVE`** for all subjects. In simple words: a consumer using the newest schema can read every older event still in the topic. Because Kafka keeps events for days or weeks, and the read model may replay them, "every older version" matters, which is why the transitive variant is used.
- **Deployment order under BACKWARD:** upgrade consumers first, then producers.

| Change | Allowed? | Rule |
|---|---|---|
| Add a field | Yes | Must have a default (use `["null", T]` with default null for optional) |
| Remove a field | Yes | Only if it had a default |
| Rename a field | Carefully | Use `aliases`, or add the new field and deprecate the old one |
| Change a type | Rarely | Only Avro-legal promotions (int to long, float to double). Otherwise add a new field |
| Reuse a field name with a new meaning | Never | Add a new field |
| Add an enum symbol | Yes | Only if the enum has a default symbol |
| Change a logical type or decimal scale | Never | Breaking, requires a new topic |

A truly breaking change means a new topic (for example `order.created.v2`), with producers dual-writing for a transition period.

### 9.7 Client configuration

Producer:

```properties
key.serializer=org.apache.kafka.common.serialization.StringSerializer
value.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
schema.registry.url=http://schema-registry:8081
auto.register.schemas=false        # production: schemas are registered by CI, not at runtime
use.latest.version=true
```

Consumer:

```properties
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
value.deserializer=org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
spring.deserializer.value.delegate.class=io.confluent.kafka.serializers.KafkaAvroDeserializer
specific.avro.reader=true          # deserialize into generated classes, not GenericRecord
schema.registry.url=http://schema-registry:8081
```

`ErrorHandlingDeserializer` turns an undecodable message into a handled error so it goes to the dead letter topic instead of blocking the partition.

### 9.8 Outbox and Avro

The outbox row stores the event in **Avro JSON encoding**, not the binary form. The outbox poller converts it to the Avro record and serializes it with `KafkaAvroSerializer` only when publishing.

Reason: serializing needs the Schema Registry. If that happened inside the business transaction, a registry outage would fail order creation. With conversion at publish time, orders are always accepted and events are published once the registry is back.

### 9.9 CI/CD and registry operations

- Schemas are registered by the pipeline, never by running applications in production (`auto.register.schemas=false`). In local development auto-registration is allowed for speed.
- The build runs a **compatibility check** (the registry's compatibility test via the Schema Registry Maven plugin) on every schema change. An incompatible schema fails the pull request.
- Schemas are reviewed like API contracts. Changes to `event-contracts` need an owner approval.
- Run the registry with more than one instance in production. Its data lives in a compacted Kafka topic (`_schemas`) with a replication factor of 3.

### 9.10 Failure behaviour

| Situation | What happens |
|---|---|
| Registry is down | Running producers and consumers keep working with their cached schema ids. New schemas cannot be registered. The outbox poller retries. Order creation is not affected (section 9.8) |
| Producer sends an incompatible schema | Registration is rejected (and the CI check catches it first) |
| Consumer sees an event with extra fields | Ignored, the reader schema decides which fields it uses |
| Consumer sees an event missing a new field | The reader schema default fills it in |
| Corrupt or unknown payload | `ErrorHandlingDeserializer` routes it to the DLT with the original bytes preserved (the DLT uses a byte-array serializer so nothing is re-encoded) |

---

## 10. RabbitMQ design (email only)

### 10.1 Topology

```
 notifications.exchange (topic)
    |-- email.order-confirmed  --> queue: email.order-confirmed.q
    |-- email.order-rejected   --> queue: email.order-rejected.q
    |-- email.payment-failed   --> queue: email.payment-failed.q
    |-- email.order-cancelled  --> queue: email.order-cancelled.q

 Each main queue has:
    x-dead-letter-exchange = notifications.retry.exchange

 notifications.retry.exchange --> email.retry.q
    x-message-ttl = 30000
    x-dead-letter-exchange = notifications.exchange   (returns to main queue)

 After N failed attempts (tracked in the x-death header or an attempt counter):
    publish to email.parking-lot.q (manual inspection, no auto retry)
```

### 10.2 Behaviour

- **Publisher confirms** and `mandatory` flag on, so the bridge knows the broker really accepted the message. The Kafka offset is committed only after the confirm.
- **Durable** exchanges and queues, **persistent** messages.
- **Manual ack** in the worker, ack only after the email provider accepts the message (or after it is permanently rejected).
- **Prefetch** set low (10 to 50) for fair distribution across worker instances.
- **Retry:** transient failure (SMTP timeout) goes to the retry queue with backoff. Permanent failure (invalid address) goes straight to the parking lot.
- **Priority:** payment-failed and confirmation emails can use a higher priority than marketing-style mails.
- **Idempotent worker:** an `email_log` table with a unique key `(orderId, templateType)`. If a redelivery arrives after a successful send, the worker acks without sending again.

### 10.3 The bridge, step by step

1. Notification bridge consumes `order.confirmed` from Kafka.
2. Builds an email job (recipient, template, data, `orderId`, `correlationId`).
3. Publishes to RabbitMQ with publisher confirm.
4. On confirm, commits the Kafka offset. On failure, does not commit, so the event is retried through the Kafka retry topics.

---

### 10.4 Message format

Email jobs on RabbitMQ are small JSON documents (recipient, template id, template data, `orderId`, `correlationId`). They are internal, short-lived work items, so they do not need the Schema Registry. The bridge and the worker share one versioned DTO class for the job.

---

## 11. CQRS: the order read model

### 11.1 The problem and the idea

A screen such as "My Orders" needs the order, its items, its payment status and its price breakdown. That data is spread over several services and databases. Calling them all for every screen load is slow and fragile, and one slow service breaks the whole screen.

CQRS (Command Query Responsibility Segregation) means writing and reading can be two different models.

- **Command side:** Order, Inventory and Payment services do the real work and enforce the rules.
- **Query side:** the **Order Query Service** keeps one table shaped exactly like what the screens need. It is built entirely from Kafka events. Nobody else writes to it.

### 11.2 Components

- **Order Query Service:** a separate Spring Boot service with its own database (PostgreSQL to start; the model would also suit a document store or a search index later).
- **Projector:** Kafka consumers in the group `order-query-projector`, reading `order.*`, `inventory.*` and `payment.*` topics.
- **Query API:** read-only REST endpoints.

### 11.3 Read schema

```
order_view(
  order_id PK, customer_id, status, items JSONB,
  subtotal, total_discount, shipping_fee, total_tax, grand_total, currency, coupon_code,
  payment_status, payment_method, payment_updated_at,
  inventory_status, inventory_updated_at,
  status_rank, created_at, updated_at, last_event_at
)
order_timeline(order_id, event_id PK, event_type, from_status, to_status, occurred_at, details JSONB)
inbox(event_id PK, consumer, processed_at)
```

Indexes: `order_view(customer_id, created_at DESC, order_id)` for the history list, `order_timeline(order_id, occurred_at)`.

### 11.4 Projection rules

Events for one order come from different topics, so there is no single global order across them. The projector is built to survive that:

1. **Idempotent.** Insert `eventId` into `inbox` in the same transaction as the update. A duplicate is skipped.
2. **Upsert by `order_id`.** If a `payment.*` event arrives before `order.created`, the projector creates a stub row, and `order.created` fills in the rest later.
3. **Each handler updates only its own columns.** A payment event touches only payment columns, an inventory event only inventory columns.
4. **Per-section timestamp guard.** An update applies only if the event's `occurredAt` is not older than the section's last update (`WHERE payment_updated_at IS NULL OR payment_updated_at <= :eventTs`). A late, older event cannot overwrite a newer one.
5. **Status never moves backwards.** Overall `status` is derived from the saga state machine ranks (`status_rank`). An update that would lower the rank is ignored for the status column, but is still written to the timeline.
6. **Every event is appended to `order_timeline`**, which powers an order tracking history.
7. Failures follow the same retry topic and DLT approach as other consumers.

### 11.5 Query API

- `GET /orders/{id}`: the full order view.
- `GET /customers/{id}/orders?limit=20&cursor=...`: order history with **cursor (keyset) pagination** on `(created_at, order_id)`. Offset pagination is avoided because it becomes slow and unstable on large tables.
- `GET /orders/{id}/timeline`: status history.
- Read-only. This service never publishes business events and never calls back into the write side to change anything.

### 11.6 Consistency

The read side is **eventually consistent**. A status change shows up after the projector processes the event, usually within milliseconds, but it can lag under load.

**Read-your-own-write:** right after `POST /orders`, a client may poll before the projector has created the row. If the Query Service finds no row for an order id, it falls back to one internal call to the Order Service (`GET /internal/orders/{id}`, with a timeout and circuit breaker) and returns that. After a few seconds the view is the normal source.

**The write side never trusts the read side.** The Payment Service, for example, never reads `order_view` to decide anything. Business decisions are made only from the owning service's own data.

### 11.7 Rebuilding the view

Because the view comes from events, it can be dropped and rebuilt: start a new consumer group from the earliest offset into a fresh table (for example `order_view_v2`), wait for lag to reach zero, then switch reads to the new table.

Limit: Kafka retention. To keep a full rebuild possible, `order.*` topics use 30 day retention, and the rebuild procedure for older data is to bootstrap from a snapshot export of the Order DB and then catch up from Kafka. Document and test this procedure.

### 11.8 Operating it

- Metrics: projector consumer lag, projection time per event, number of stub rows older than 1 minute (a sign of a missing `order.created`), fallback-to-write-side calls.
- Scaling: more query instances for reads (stateless), more projector instances up to the partition count, read replicas of the query database.
- If the Query Service is down, order processing is unaffected. It catches up from Kafka when it returns.

---

## 12. Data model

### 12.1 Order DB

```
orders(id PK, customer_id, status, grand_total, currency, coupon_code, version, created_at, updated_at, expires_at)
order_items(id PK, order_id FK, sku, qty, unit_price)
order_pricing_snapshot(order_id PK, breakdown JSONB, pricing_version, computed_at)   -- immutable
coupon_redemptions(id PK, coupon_id, customer_id, order_id UNIQUE, redeemed_at)
idempotency_keys(key PK, request_hash, response_body, status_code, created_at, expires_at)
outbox(id PK, aggregate_id, topic, event_key, payload [Avro JSON], headers, created_at, published_at NULL)
inbox(event_id PK, consumer, processed_at)
```

Catalog and pricing rule tables (Order DB):

```
products(sku PK, name, category, base_price, currency, tax_category, active)
promotions(id PK, name, type, conditions JSONB, value, priority, stackable, valid_from, valid_to, active)
coupons(code PK, type, value, conditions JSONB, max_uses, used_count, per_customer_limit, valid_from, valid_to, active)
tax_rules(id PK, tax_category, region, rate)
shipping_rules(id PK, min_cart_value, fee, region)
serviceable_pins(pin_code PK, state)
customers(id PK, status)
```

### 12.2 Inventory DB

```
inventory(sku PK, available, reserved, sold, version)
reservations(id PK, order_id UNIQUE, status [RESERVED|COMMITTED|RELEASED], expires_at, created_at)
reservation_items(reservation_id FK, sku, qty)
outbox(...), inbox(...)
```

Invariant: `available + reserved + sold = total` for every SKU, and every change goes through one of the atomic operations in section 13.

### 12.3 Payment DB

```
payments(id PK, order_id, status [INITIATED|PENDING|COMPLETED|FAILED|REFUNDED], amount, gateway_ref, idempotency_key UNIQUE, attempt_no, version, created_at, updated_at)
payment_attempts(id PK, payment_id FK, request, response, result, created_at)
outbox(...), inbox(...)
```

### 12.4 Notification DB

```
email_log(id PK, order_id, template_type, recipient, status, attempt_count, provider_message_id, sent_at, UNIQUE(order_id, template_type))
inbox(...)
```

---

### 12.5 Order Query DB

See section 11.3 for `order_view`, `order_timeline` and `inbox`.

---

## 13. Concurrency without distributed locks

There are no Redis locks. Correctness comes from the database, from Kafka's ordering, and from constraints. These are the same tools large systems rely on.

### 13.1 Atomic conditional update (inventory)

Never read, check, then write. Do it in one statement:

```sql
UPDATE inventory
SET available = available - :qty,
    reserved  = reserved  + :qty,
    version   = version + 1
WHERE sku = :sku AND available >= :qty;
```

If zero rows are updated, there is not enough stock: reject. The database serialises concurrent updates on the same row, so two orders racing for the last unit cannot both win. For multi-item orders, process SKUs in sorted order inside one transaction to avoid deadlocks, or fail the whole reservation if any item fails.

### 13.2 Optimistic locking

`@Version` on orders and payments. A lost update raises `OptimisticLockException`, and the handler retries the whole unit of work (re-read, re-validate transition, re-apply).

### 13.3 Kafka partition ordering as the serialiser

Because every event for an order uses `orderId` as the key, one consumer thread handles one order at a time. No cross-thread race on the same order's saga.

### 13.4 Unique constraints as the final guard

- `reservations.order_id UNIQUE` prevents double reservation.
- `payments.idempotency_key UNIQUE` prevents double payment attempts.
- `inbox.event_id` prevents double processing.
- `email_log(order_id, template_type) UNIQUE` prevents double email.
- `coupon_redemptions(order_id) UNIQUE` plus the conditional `used_count < max_uses` update prevent double or over redemption of a coupon (section 7.6).

### 13.5 Row claiming for background jobs

When several instances run the same scheduled job (outbox poller, expiry job), each claims distinct rows:

```sql
SELECT * FROM outbox
WHERE published_at IS NULL
ORDER BY id
LIMIT 100
FOR UPDATE SKIP LOCKED;
```

Instances never block each other and never process the same row. This replaces ShedLock-style or Redis-lock coordination.

### 13.6 Where Redis still helps

Redis is used for **caching** and **rate limiting** only. Both tolerate loss: if Redis is down, reads fall back to the database and rate limiting degrades gracefully. It is never the source of truth for stock or money.

---

## 14. Idempotency at four levels

1. **API level.** `POST /orders` requires an `Idempotency-Key`. Store key, request hash, and response. Same key with the same hash returns the stored response. Same key with a different hash returns `422`. An in-flight duplicate returns `409` (or waits). Keys expire after 24 hours.
2. **Event level (inbox).** In one transaction: insert `eventId` into `inbox`, apply the business change, write the outbox row. A duplicate event violates the primary key and is skipped, and the offset is still committed.
3. **State level.** The state machine rejects transitions that are already applied or illegal, so replaying an event is harmless.
4. **External level.** The payment idempotency key is sent to the gateway on every attempt for the same payment, so retries cannot double charge. Refunds also carry their own key.

---

## 15. Reliability patterns

### 15.1 Transactional outbox

```
BEGIN
  update orders ...
  insert into outbox (topic, key, payload)
COMMIT
-- separate publisher reads outbox (FOR UPDATE SKIP LOCKED), sends to Kafka, marks published_at
```

Guarantee: no event is lost if Kafka is down, and no event is published for a rolled-back change. Delivery is at-least-once, which is why consumers are idempotent. A cleanup job deletes published rows older than N days. The outbox payload is stored as Avro JSON and converted to Avro binary only when the poller publishes (section 9.8), so business transactions never depend on the Schema Registry being available. Upgrade path: replace the poller with Debezium CDC.

### 15.2 Inbox (idempotent consumer)

Covered in section 14. The inbox insert and the business change commit together, then the offset is committed. If the process crashes between DB commit and offset commit, the event is redelivered and skipped by the inbox.

### 15.3 Retry and dead letter topics

```
orders topic --fail--> retry-5s --fail--> retry-1m --fail--> dlt
```

- **Retryable:** timeouts, optimistic lock conflicts, temporary DB errors.
- **Non-retryable (go to DLT immediately):** deserialisation errors, schema violations, unknown event version.
- The DLT message carries the original headers plus error class, message, and attempt count.
- A small admin tool or endpoint can replay a DLT message after the bug is fixed.
- Retry topics use non-blocking retry, so one bad message never stalls the partition.

### 15.4 Poison messages

Configure the error-handling deserializer so a bad payload becomes a handled error, not an infinite poll loop.

### 15.5 Graceful shutdown

On SIGTERM: stop polling, finish in-flight records, commit offsets, flush the producer, ack or nack RabbitMQ messages, then exit.

---

## 16. Caching (Redis, cache-aside)

| Data | Strategy |
|---|---|
| Product details | Cache-aside, TTL 10 min with random jitter (avoids synchronized expiry) |
| Stock for display | Cache-aside, short TTL (5 to 10 s). Display only. The real check is always the atomic DB update |
| Order status for polling | Cache, invalidated on state change events |
| Nonexistent products | Negative cache with short TTL, blocks repeated misses |
| Product prices and active promotions (quote endpoint) | Cache-aside, TTL 60 s, evicted on admin change. Order creation always prices from the database inside its transaction, never from cache |
| Hot keys | Request coalescing inside the app so one miss triggers one database load, not a thundering herd |

Important distinction: cached stock may be slightly stale and that is acceptable for browsing. It is never trusted for reservation.

---

## 17. Resilience and protection

- **Circuit breaker, timeout, retry with backoff and jitter, bulkhead** (Resilience4j) around the payment gateway call.
- **Rate limiting** (token bucket, per customer and per IP) on `POST /orders`, backed by Redis, failing open if Redis is unavailable.
- **Backpressure:** bounded consumer concurrency and `max.poll.records`, so a slow dependency slows consumption instead of exhausting memory.
- **Timeouts everywhere:** HTTP, DB, Kafka, RabbitMQ.
- **Health endpoints:** liveness and readiness, readiness checks DB and broker connectivity.
- **Load shedding:** under extreme load, return `429` or `503` early instead of accepting orders that cannot be processed.

---

## 18. Background jobs

All jobs are safe to run on multiple instances because they claim rows with `FOR UPDATE SKIP LOCKED`.

| Job | Service | What it does |
|---|---|---|
| Outbox publisher | Each producing service | Publishes unsent outbox rows, marks them sent |
| Outbox cleanup | Each producing service | Deletes old published rows |
| Order expiry | Order | Moves stale CREATED / STOCK_RESERVED / PAYMENT_PENDING orders to EXPIRED, emits `order.expired` |
| Reservation expiry | Inventory | Releases reservations past `expires_at` (safety net if events were lost) |
| Payment reconciliation | Payment | For payments stuck in PENDING, asks the gateway for the real status and emits the right event |
| Inbox cleanup | All | Deletes inbox rows older than the max replay window |
| Coupon and promotion expiry | Order | Deactivates coupons and promotions past `valid_to` |
| Stub-row check | Order Query | Alerts on stub rows older than 1 minute (missing `order.created`) |

---

## 19. Edge cases and how each is handled

| Case | Handling |
|---|---|
| Client retries `POST /orders` | Idempotency key returns the original response |
| Same event delivered twice | Inbox primary key skips it |
| Events arrive out of order | State machine ignores illegal transitions; per-order key ordering makes this rare |
| Two orders race for the last unit | Atomic conditional UPDATE, one wins, the other gets `reservation-failed` |
| Reservation made, payment service down | Reservation TTL and reservation-expiry job release stock |
| Payment succeeded but response lost | Gateway webhook or reconciliation job finds the true status |
| Duplicate gateway webhook | Idempotent payment state update, inbox on the webhook id |
| Webhook arrives after order expired | `payment.refund-requested`, automatic refund |
| Release event processed twice | Release is idempotent: only acts if the reservation is RESERVED |
| Crash after DB commit, before offset commit | Redelivery, inbox skips |
| Crash after publish, before marking outbox row | Duplicate event, consumers dedupe |
| Poison message | Non-retryable, straight to DLT, partition keeps moving |
| Rebalance mid-processing | Idempotent handlers make reprocessing safe |
| Kafka unavailable | Writes succeed, outbox grows, publisher catches up later |
| RabbitMQ unavailable | Bridge cannot confirm, offset is not committed, event retried via Kafka retry topics |
| Email provider outage | RabbitMQ retry queue with backoff, then parking lot |
| Invalid email address | Permanent failure, parking lot, no retry |
| Price changes between cart and order | Order stores a price snapshot at creation, payment uses the stored total |
| Partial multi-item reservation | All-or-nothing in one DB transaction |
| User cancels while payment in flight | Cancel marks the order, payment result then triggers refund if it still succeeds |
| Clock skew | Ordering decisions use versions and offsets, not timestamps |
| Client sends a manipulated price | Ignored. The server always recomputes the price from catalog and rules |
| Several validation errors in one request | Stage 1 and 2 collect all errors and return them together |
| Two orders redeem the last use of a coupon | Atomic conditional UPDATE, one wins, the other gets `COUPON_EXHAUSTED` |
| Coupon redeemed but the order fails or expires | Redemption released in the same transaction as the state change, idempotently |
| Promotion ends between quote and order | Order uses the price at order time, the quote is not binding |
| Rounding differences between quote and order | One `Money` type, one rounding rule, covered by tests |
| Order status polled before the projector caught up | Query Service falls back to the Order Service for a missing row |
| Payment event reaches the read model before `order.created` | Projector creates a stub row, filled in when `order.created` arrives |
| Out-of-order or duplicate events in the projector | Inbox dedupe, per-section timestamp guard, status never moves backwards |
| Order Query Service down | Writes unaffected, it catches up from Kafka |
| Incompatible schema pushed by a producer | CI compatibility check fails the build, registry rejects registration |
| Consumer meets an event written with a newer or older schema | Avro reader/writer schema resolution handles it under BACKWARD_TRANSITIVE |
| Schema Registry unavailable | Cached schema ids keep services running, outbox publishing retries, order creation is unaffected |
| Message too large | Events carry ids and essential fields only, never whole documents |

---

## 20. Observability

- **Correlation:** `correlationId` created at the edge, propagated in HTTP headers, Kafka headers, and RabbitMQ headers, and written to every log line (MDC).
- **Distributed tracing:** OpenTelemetry with Jaeger or Tempo, one trace across the saga.
- **Metrics (Micrometer + Prometheus + Grafana):**
  - Kafka consumer lag per group and partition
  - Outbox backlog size and oldest unpublished age
  - DLT message count and retry topic depth
  - Saga end-to-end duration, orders per state
  - Reservation expiry count, refund count
  - RabbitMQ queue depth, retry queue depth, parking lot depth
  - Gateway call latency and circuit breaker state
  - Projector lag, stub rows, and fallback calls in the Order Query Service
  - Validation rejections by error code, pricing calculation latency, coupon redemptions and releases
  - Schema Registry request errors and deserialization failures
- **Alerts:** growing consumer lag, outbox age above threshold, any message in DLT or parking lot, orders stuck in a non-terminal state beyond the SLA.
- **Logging:** structured JSON, no PII or card data in logs.

---

## 21. Scaling for large traffic

- **Stateless services:** scale horizontally behind a load balancer. All state is in the database or the brokers.
- **Partition count is the ceiling** for consumer parallelism. Choose it for the target throughput (for example 12 to 48 partitions for hot topics).
- **Hot SKU problem:** one extremely popular SKU makes a single inventory row a contention point. Mitigation options: split stock into N sub-rows (stock sharding) and reserve from a random shard, with fallback to the next shard. Treat as an advanced phase.
- **Read scaling:** the CQRS read side scales on its own: stateless query instances, projector instances up to the partition count, and read replicas of the query database. Hot reads can also be cached.
- **Database:** indexes on `orders(customer_id, created_at)`, `outbox(published_at)`, `reservations(expires_at)`, `payments(status, updated_at)`. Partition or archive old orders and outbox/inbox tables.
- **Kafka:** batching and compression on producers, right-sized consumer fetch settings.
- **RabbitMQ:** more worker instances plus prefetch tuning. Quorum queues for durability in a clustered setup.
- **Async API:** `POST /orders` returns `202` quickly, the client polls `GET /orders/{id}` or receives a notification, so slow downstream steps never hold HTTP threads.

---

## 22. Testing strategy

1. **Unit tests:** state machine transitions (every legal and illegal pair), idempotency key logic, atomic inventory SQL, each validator on its own, each price adjuster and specification, rounding and non-negative totals.
2. **Integration tests with Testcontainers:** Postgres, Kafka, RabbitMQ, Redis. Test outbox to Kafka to inbox end to end.
3. **Concurrency tests:** 1000 parallel orders against 10 units of stock. Assert exactly 10 reservations and the stock invariant holds.
4. **Failure injection (the most valuable part):**
   - Kill a consumer mid-processing and verify no loss or duplicate effect.
   - Send duplicate and out-of-order events.
   - Stop Kafka, create orders, restart Kafka, verify catch-up.
   - Stop RabbitMQ, verify bridge backs off and recovers.
   - Make the gateway simulator time out, return duplicates, and send late webhooks.
5. **Load test** (k6 or Gatling): measure throughput, p99 latency, consumer lag under sustained load.
6. **Schema tests:** compatibility check of every `.avsc` against the registry in CI, and tests that old sample events still deserialize with the newest schema.
7. **Pricing tests:** a table of carts with the exact expected breakdown (golden tests), plus a concurrency test where many orders race for a coupon's last uses.
8. **Projection tests:** replay events in shuffled order and with duplicates, and assert the read model ends in the same correct state. Test a full rebuild.

---

## 23. Suggested package layout (per service)

```
com.shop.<service>
  api            controllers, request/response DTOs, exception handler
  domain         entities, state machine, domain services
  repository     JPA repositories, custom atomic queries
  messaging
    inbound      Kafka listeners (one per topic), error handlers
    outbound     outbox publisher, event factory
  outbox         outbox entity, poller
  inbox          inbox entity, dedupe helper
  config         Kafka, RabbitMQ, Redis, Resilience4j config
  job            scheduled jobs
```

Extra packages in the Order Service: `validation` (validators, pipeline, `OrderContext`) and `pricing` (`Money`, adjusters, specifications, `PriceBreakdown`, quote service, coupon redemption).

Order Query Service layout: `projection` (one handler per event, inbox, timestamp guards), `view` (entities, repositories), `api` (controllers, cursor pagination), `fallback` (client for the Order Service).

Patterns in use: State, Strategy (payment methods, price adjusters), Chain of Responsibility (validation pipeline), Specification (eligibility rules), Decorator (running price result), Builder, Value Object (`Money`), Factory (event creation, adjuster selection), Template Method (base consumer with inbox and error handling), Adapter (gateway client), CQRS, Outbox, Inbox, Saga, Circuit Breaker, Retry, Cache-Aside, Token Bucket.

---

## 24. Build phases

| Phase | Deliverable |
|---|---|
| 1 | Docker Compose: Kafka, Schema Registry, RabbitMQ, Postgres (per service plus the query DB), Redis, Kafka UI, RabbitMQ management UI |
| 2 | `event-contracts` module: `EventMetadata` and all event schemas (`.avsc`), generated classes, compatibility check in the build |
| 3 | Order Service core: `POST /orders`, idempotency key, state machine, outbox with Avro JSON payload, outbox publisher with `KafkaAvroSerializer` |
| 4 | Validation pipeline: stages, validators, error responses, unit tests per validator |
| 5 | Pricing engine: `Money`, adjusters, specifications, rule tables, quote endpoint, coupon redemption and release, price snapshot |
| 6 | Inventory Service: atomic reserve, release and commit, inbox, `order.created` consumer |
| 7 | Payment Service and Gateway Simulator: attempts, idempotency, webhook, events |
| 8 | Full saga happy path and compensation paths (including coupon release) |
| 9 | Retry topics, DLT, poison message handling, DLT replay tool |
| 10 | Notification bridge, RabbitMQ topology, retry and parking-lot queues, email worker |
| 11 | Order Query Service: projector, `order_view`, timeline, cursor pagination, write-side fallback, rebuild procedure |
| 12 | Expiry, reconciliation, and cleanup jobs |
| 13 | Caching and rate limiting |
| 14 | Observability: metrics, tracing, dashboards, alerts |
| 15 | Failure-injection tests and load tests |
| 16 | Advanced: stock sharding for hot SKUs, Debezium CDC for the outbox |

Rule for every phase: it is done only when its tests pass, including at least one failure scenario (duplicate event, crash, or dependency down).

---

## 25. How this feeds the payments/UPI project

Almost everything here carries over directly: outbox, inbox, idempotency at every level, saga with compensation, retry and DLT topics, reconciliation, observability, Avro contracts, and the CQRS read model for statements. The payments project then adds a double-entry ledger, a fraud service, statements, and the bank gateway simulator on top of an engine you already understand.

---

## 26. Deferred (not in this version)

- Outbound webhooks for merchants (HMAC signing, retries, per-endpoint circuit breaker)
- Shipment / fulfilment service with returns and refund flows
- Search service (OpenSearch) fed from product events
- Full event sourcing of the order aggregate
- Kafka Streams analytics and exactly-once transactional processing
- Field-level encryption and data deletion flow for personal data
- Canary / blue-green deployments and feature flags

These can be added later without changing the core, because the events and the read model are already in place.
