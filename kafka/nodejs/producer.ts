import { Kafka, Producer, Message, RecordMetadata } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import * as fs from 'fs';
import * as path from 'path';

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID ?? 'tcs-producer',
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
  ssl: process.env.KAFKA_SSL === 'true',
  sasl:
    process.env.KAFKA_USERNAME && process.env.KAFKA_PASSWORD
      ? {
          mechanism: 'scram-sha-512',
          username: process.env.KAFKA_USERNAME,
          password: process.env.KAFKA_PASSWORD,
        }
      : undefined,
});

const registry = new SchemaRegistry({
  host: process.env.SCHEMA_REGISTRY_URL ?? 'http://localhost:8081',
});

let producer: Producer;

export async function initializeProducer(): Promise<void> {
  producer = kafka.producer({
    allowAutoTopicCreation: false,
    transactionTimeout: 30000,
  });
  await producer.connect();
}

/**
 * Register all Avro schemas from the schemas/ directory on startup.
 * This ensures schemas are available in the registry before producing messages.
 */
export async function initializeSchemas(): Promise<void> {
  const schemasDir = path.join(__dirname, 'schemas');
  const schemaFiles = fs.readdirSync(schemasDir).filter((f) => f.endsWith('.avsc'));

  for (const file of schemaFiles) {
    const schema = JSON.parse(fs.readFileSync(path.join(schemasDir, file), 'utf-8'));
    const subject = `${schema.namespace}.${schema.name}-value`;
    await registry.register({ type: 'AVRO', schema: JSON.stringify(schema) }, { subject });
    console.log(`Registered schema: ${subject}`);
  }
}

/**
 * Send a message to a Kafka topic with Avro serialization.
 * On failure after 3 retries, publishes to the DLQ topic with error metadata headers.
 */
export async function sendMessage(
  topic: string,
  key: string,
  value: unknown,
  schemaId?: number,
): Promise<RecordMetadata[]> {
  const encodedValue = schemaId
    ? await registry.encode(schemaId, value)
    : Buffer.from(JSON.stringify(value));

  const message: Message = {
    key,
    value: encodedValue,
    headers: {
      'content-type': 'application/avro',
      'source-service': process.env.KAFKA_CLIENT_ID ?? 'unknown',
    },
  };

  let lastError: Error | undefined;

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const result = await producer.send({ topic, messages: [message] });
      return result;
    } catch (err) {
      lastError = err as Error;
      console.error(`Send attempt ${attempt}/3 failed for topic ${topic}: ${lastError.message}`);
      if (attempt < 3) {
        await sleep(Math.pow(2, attempt) * 100);
      }
    }
  }

  // All retries exhausted - publish to DLQ
  await publishToDlq(topic, message, lastError!);
  throw lastError;
}

/**
 * Publish a failed message to the DLQ topic.
 * DLQ topic naming: <original-topic>.DLQ
 * Error metadata is attached as message headers for reprocessing tooling.
 */
async function publishToDlq(
  originalTopic: string,
  message: Message,
  error: Error,
): Promise<void> {
  const dlqTopic = `${originalTopic}.DLQ`;

  const dlqMessage: Message = {
    ...message,
    headers: {
      ...((message.headers as Record<string, string>) ?? {}),
      'dlq-original-topic': originalTopic,
      'dlq-error-message': error.message,
      'dlq-error-timestamp': new Date().toISOString(),
      'dlq-retry-count': '3',
    },
  };

  try {
    await producer.send({ topic: dlqTopic, messages: [dlqMessage] });
    console.warn(`Message published to DLQ topic: ${dlqTopic}`);
  } catch (dlqErr) {
    // DLQ publish failed - log and continue. The original error is the primary concern.
    console.error(`Failed to publish to DLQ ${dlqTopic}: ${(dlqErr as Error).message}`);
  }
}

/**
 * Health check: verify connectivity to the Kafka broker.
 */
export async function checkHealth(): Promise<boolean> {
  const admin = kafka.admin();
  try {
    await admin.connect();
    await admin.listTopics();
    return true;
  } catch {
    return false;
  } finally {
    await admin.disconnect().catch(() => undefined);
  }
}

export async function shutdownProducer(): Promise<void> {
  await producer?.disconnect();
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
