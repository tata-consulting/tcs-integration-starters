import { jest } from '@jest/globals';

// Mock kafkajs before importing producer
const mockSend = jest.fn();
const mockConnect = jest.fn();
const mockDisconnect = jest.fn();
const mockAdminConnect = jest.fn();
const mockAdminDisconnect = jest.fn();
const mockListTopics = jest.fn();

jest.mock('kafkajs', () => ({
  Kafka: jest.fn().mockImplementation(() => ({
    producer: jest.fn().mockReturnValue({
      connect: mockConnect,
      disconnect: mockDisconnect,
      send: mockSend,
    }),
    admin: jest.fn().mockReturnValue({
      connect: mockAdminConnect,
      disconnect: mockAdminDisconnect,
      listTopics: mockListTopics,
    }),
  })),
}));

jest.mock('@kafkajs/confluent-schema-registry', () => ({
  SchemaRegistry: jest.fn().mockImplementation(() => ({
    register: jest.fn().mockResolvedValue({ id: 1 }),
    encode: jest.fn().mockResolvedValue(Buffer.from('encoded')),
  })),
}));

// Mock fs for schema registration test
jest.mock('fs', () => ({
  readdirSync: jest.fn().mockReturnValue(['example.avsc']),
  readFileSync: jest.fn().mockReturnValue(
    JSON.stringify({
      type: 'record',
      name: 'OrderCreated',
      namespace: 'io.tcs.platform.orders',
      fields: [],
    }),
  ),
}));

import { initializeProducer, sendMessage, initializeSchemas, checkHealth } from '../producer';

describe('Kafka Producer', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockConnect.mockResolvedValue(undefined);
    mockDisconnect.mockResolvedValue(undefined);
  });

  describe('sendMessage', () => {
    it('sends a message successfully on first attempt', async () => {
      await initializeProducer();
      mockSend.mockResolvedValueOnce([{ topicName: 'test-topic', partition: 0, offset: '0' }]);

      const result = await sendMessage('test-topic', 'key-1', { orderId: '123' });

      expect(mockSend).toHaveBeenCalledTimes(1);
      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          topic: 'test-topic',
          messages: expect.arrayContaining([expect.objectContaining({ key: 'key-1' })]),
        }),
      );
      expect(result).toHaveLength(1);
    });

    it('publishes to DLQ after 3 failed send attempts', async () => {
      await initializeProducer();
      const sendError = new Error('broker unavailable');
      // First 3 calls fail (retries), 4th call succeeds (DLQ publish)
      mockSend
        .mockRejectedValueOnce(sendError)
        .mockRejectedValueOnce(sendError)
        .mockRejectedValueOnce(sendError)
        .mockResolvedValueOnce([{ topicName: 'test-topic.DLQ', partition: 0, offset: '0' }]);

      await expect(sendMessage('test-topic', 'key-1', { orderId: '123' })).rejects.toThrow(
        'broker unavailable',
      );

      // 3 failed attempts + 1 DLQ publish
      expect(mockSend).toHaveBeenCalledTimes(4);

      // Verify DLQ message has error headers
      const dlqCall = mockSend.mock.calls[3];
      const dlqMessage = (dlqCall[0] as { messages: Array<{ headers: Record<string, string> }> }).messages[0];
      expect(dlqMessage.headers['dlq-original-topic']).toBe('test-topic');
      expect(dlqMessage.headers['dlq-error-message']).toBe('broker unavailable');
      expect(dlqMessage.headers['dlq-retry-count']).toBe('3');
    });

    it('does not throw if DLQ publish also fails', async () => {
      await initializeProducer();
      const sendError = new Error('broker down');
      mockSend.mockRejectedValue(sendError);

      await expect(sendMessage('test-topic', 'key-1', {})).rejects.toThrow('broker down');
      // Should not throw a different error even when DLQ also fails
    });
  });

  describe('initializeSchemas', () => {
    it('registers schemas from the schemas directory on startup', async () => {
      const { SchemaRegistry } = await import('@kafkajs/confluent-schema-registry');
      const mockRegister = jest.fn().mockResolvedValue({ id: 1 });
      (SchemaRegistry as jest.Mock).mockImplementationOnce(() => ({
        register: mockRegister,
        encode: jest.fn(),
      }));

      await initializeSchemas();

      // One schema file in the mock (example.avsc)
      expect(mockRegister).toHaveBeenCalledTimes(1);
      expect(mockRegister).toHaveBeenCalledWith(
        expect.objectContaining({ type: 'AVRO' }),
        expect.objectContaining({ subject: 'io.tcs.platform.orders.OrderCreated-value' }),
      );
    });
  });

  describe('checkHealth', () => {
    it('returns true when broker is reachable', async () => {
      mockAdminConnect.mockResolvedValue(undefined);
      mockListTopics.mockResolvedValue(['test-topic']);
      mockAdminDisconnect.mockResolvedValue(undefined);

      const healthy = await checkHealth();
      expect(healthy).toBe(true);
    });

    it('returns false when broker is unreachable', async () => {
      mockAdminConnect.mockRejectedValue(new Error('connection refused'));

      const healthy = await checkHealth();
      expect(healthy).toBe(false);
    });
  });
});
