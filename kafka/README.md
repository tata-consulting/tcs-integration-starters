# Kafka Integration Starter (Node.js)

Producer and consumer patterns for Kafka with Avro schemas, Confluent Schema Registry, and dead-letter queue (DLQ) handling.

## Patterns

### Producer
- Avro serialization via Confluent Schema Registry
- Automatic schema registration from `schemas/` directory on startup
- Retry: up to 3 attempts with exponential backoff
- DLQ fallback: on exhausted retries, message is published to `<topic>.DLQ` with error headers

### Consumer
- Consumer group with manual offset commit
- Avro deserialization via Schema Registry; falls back to JSON for non-Avro messages
- Error handling: failed messages are published to `<topic>.DLQ` before committing offset
- Graceful shutdown on SIGTERM/SIGINT

### DLQ (Dead-Letter Queue)

The DLQ pattern prevents poison messages from blocking the consumer. Failed messages are published to `<original-topic>.DLQ` with the following headers:

| Header | Value |
|--------|-------|
| `dlq-original-topic` | Original topic name |
| `dlq-error-message` | Error message string |
| `dlq-error-timestamp` | ISO 8601 timestamp of failure |
| `dlq-retry-count` | Number of retries attempted |

To reprocess DLQ messages, consume from `<topic>.DLQ` and republish to the original topic after fixing the underlying issue.

## Local Dev

Prerequisites: Docker and Docker Compose.

```bash
# Start Kafka (KRaft mode), Schema Registry, and Kafdrop UI
make start

# Create the required topics
make create-topics

# Open Kafdrop UI to browse topics and messages
open http://localhost:9000
```

Environment variables for local dev:
```
KAFKA_BROKERS=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8081
KAFKA_CLIENT_ID=my-service-local
```

## Schema Registration

Place Avro schema files (`.avsc`) in `schemas/`. Call `initializeSchemas()` on startup:

```typescript
import { initializeProducer, initializeSchemas } from './producer';

await initializeProducer();
await initializeSchemas(); // registers all .avsc files in schemas/
```

Schemas are registered with subject `<namespace>.<name>-value` (value subject convention).

## Usage

### Producer

```typescript
import { initializeProducer, initializeSchemas, sendMessage } from './producer';

await initializeProducer();
await initializeSchemas();

await sendMessage('orders.created', order.id, {
  orderId: order.id,
  customerId: order.customerId,
  totalAmount: order.total,
  createdAt: Date.now(),
  items: order.items,
}, schemaId);
```

### Consumer

```typescript
import { startConsumer } from './consumer';

await startConsumer(
  ['orders.created'],
  'inventory-service-group',
  async (key, value) => {
    const order = value as OrderCreated;
    await reserveStock(order);
  },
);
```

## Tests

```bash
npm test
```

Tests use kafkajs mocks - no broker required.
