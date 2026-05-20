import { Kafka, Consumer, EachMessagePayload, KafkaMessage } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID ?? 'tcs-consumer',
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

type MessageHandler = (key: string | null, value: unknown) => Promise<void>;

let consumer: Consumer;

export async function startConsumer(
  topics: string[],
  groupId: string,
  handler: MessageHandler,
): Promise<void> {
  consumer = kafka.consumer({
    groupId,
    // Manual offset commit for at-least-once reliability.
    // autoCommit is false: we only commit after successful processing.
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
  });

  await consumer.connect();

  for (const topic of topics) {
    await consumer.subscribe({ topic, fromBeginning: false });
  }

  await consumer.run({
    autoCommit: false,
    eachMessage: async (payload: EachMessagePayload) => {
      const { topic, partition, message } = payload;
      const offset = message.offset;

      try {
        const key = message.key?.toString() ?? null;
        const value = message.value ? await decodeMessage(message) : null;

        await handler(key, value);

        // Commit offset only after successful processing.
        // This ensures the message is not considered processed if the handler throws.
        await consumer.commitOffsets([
          { topic, partition, offset: (BigInt(offset) + 1n).toString() },
        ]);
      } catch (err) {
        console.error(
          `Processing failed for ${topic}[${partition}]@${offset}: ${(err as Error).message}`,
        );
        await publishToDlq(topic, message, err as Error);

        // Commit offset even on failure to avoid infinite reprocessing of poison messages.
        // The message is preserved in the DLQ for later inspection and reprocessing.
        await consumer.commitOffsets([
          { topic, partition, offset: (BigInt(offset) + 1n).toString() },
        ]);
      }
    },
  });
}

async function decodeMessage(message: KafkaMessage): Promise<unknown> {
  if (!message.value) return null;

  // Attempt Avro decoding; fall back to raw JSON for non-Avro messages.
  try {
    return await registry.decode(message.value);
  } catch {
    return JSON.parse(message.value.toString());
  }
}

async function publishToDlq(
  originalTopic: string,
  message: KafkaMessage,
  error: Error,
): Promise<void> {
  const dlqTopic = `${originalTopic}.DLQ`;

  // Import producer inline to avoid circular dependency
  const { sendMessage } = await import('./producer');

  const dlqPayload = {
    originalTopic,
    originalKey: message.key?.toString() ?? null,
    originalValue: message.value?.toString() ?? null,
    originalOffset: message.offset,
    errorMessage: error.message,
    errorTimestamp: new Date().toISOString(),
    headers: Object.fromEntries(
      Object.entries(message.headers ?? {}).map(([k, v]) => [
        k,
        Buffer.isBuffer(v) ? v.toString() : String(v),
      ]),
    ),
  };

  try {
    await sendMessage(dlqTopic, message.key?.toString() ?? 'unknown', dlqPayload);
    console.warn(`Message sent to DLQ: ${dlqTopic}`);
  } catch (dlqErr) {
    console.error(`Failed to publish to DLQ ${dlqTopic}: ${(dlqErr as Error).message}`);
  }
}

export async function shutdownConsumer(): Promise<void> {
  await consumer?.disconnect();
}

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM received - shutting down consumer');
  await shutdownConsumer();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('SIGINT received - shutting down consumer');
  await shutdownConsumer();
  process.exit(0);
});
