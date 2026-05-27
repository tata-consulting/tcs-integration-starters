import { jest } from '@jest/globals';

// Mock axios before importing resilient-client
const mockRequest = jest.fn();

jest.mock('axios', () => ({
  default: {
    create: jest.fn().mockReturnValue({
      request: mockRequest,
    }),
  },
}));

import { ResilientHttpClient } from '../resilient-client';

describe('ResilientHttpClient', () => {
  let client: ResilientHttpClient;

  beforeEach(() => {
    jest.clearAllMocks();
    client = new ResilientHttpClient({
      baseURL: 'https://api.example.com',
      timeout: 5000,
      circuitBreaker: {
        volumeThreshold: 3, // Lower threshold for testing
        errorThresholdPercentage: 50,
        resetTimeout: 100, // Short reset for testing
      },
    });
  });

  describe('GET - successful request', () => {
    it('returns response data on success', async () => {
      mockRequest.mockResolvedValueOnce({ data: { id: 1, name: 'test' } });

      const result = await client.get<{ id: number; name: string }>('/users/1');

      expect(result).toEqual({ id: 1, name: 'test' });
      expect(mockRequest).toHaveBeenCalledTimes(1);
    });

    it('uses cached response for identical GET requests', async () => {
      mockRequest.mockResolvedValue({ data: { id: 1 } });

      await client.get('/users/1');
      await client.get('/users/1');

      // Second call served from cache
      expect(mockRequest).toHaveBeenCalledTimes(1);
    });
  });

  describe('Retry on 429 and 5xx', () => {
    it('retries on 429 and succeeds on third attempt', async () => {
      const rateLimitError = { response: { status: 429 } };
      mockRequest
        .mockRejectedValueOnce(rateLimitError)
        .mockRejectedValueOnce(rateLimitError)
        .mockResolvedValueOnce({ data: { ok: true } });

      const result = await client.get<{ ok: boolean }>('/resource');

      expect(result).toEqual({ ok: true });
      expect(mockRequest).toHaveBeenCalledTimes(3);
    });

    it('retries on 503 and fails after 3 attempts', async () => {
      const serverError = { response: { status: 503 } };
      mockRequest.mockRejectedValue(serverError);

      await expect(client.get('/resource')).rejects.toEqual(serverError);
      expect(mockRequest).toHaveBeenCalledTimes(3);
    });

    it('does not retry on 400 client error', async () => {
      const clientError = { response: { status: 400 } };
      mockRequest.mockRejectedValueOnce(clientError);

      await expect(client.get('/resource')).rejects.toEqual(clientError);
      expect(mockRequest).toHaveBeenCalledTimes(1);
    });
  });

  describe('Circuit breaker', () => {
    it('opens circuit after failure threshold is reached', async () => {
      const serverError = { response: { status: 500 } };
      // Need to exceed volumeThreshold (3) with enough failures to meet errorThresholdPercentage (50%)
      mockRequest.mockRejectedValue(serverError);

      // Fire enough requests to trip the circuit
      // volumeThreshold=3, so after 3 calls all failing (100% > 50%), circuit opens
      const requests = Array.from({ length: 5 }, () =>
        client.get('/resource').catch(() => undefined),
      );
      await Promise.all(requests);

      // Wait a tick for the breaker state to update
      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(client.isCircuitOpen()).toBe(true);
    });

    it('rejects fast when circuit is open', async () => {
      const serverError = { response: { status: 500 } };
      mockRequest.mockRejectedValue(serverError);

      // Trip the circuit
      for (let i = 0; i < 5; i++) {
        await client.get('/resource').catch(() => undefined);
      }
      await new Promise((resolve) => setTimeout(resolve, 50));

      // Reset mock to succeed - circuit should still be open and reject fast
      mockRequest.mockResolvedValue({ data: {} });
      mockRequest.mockClear();

      await expect(client.get('/resource')).rejects.toThrow();
      // Should not have called the underlying axios at all (fast rejection)
      expect(mockRequest).toHaveBeenCalledTimes(0);
    });
  });

  describe('Correlation ID propagation', () => {
    it('injects x-correlation-id header on every request', async () => {
      mockRequest.mockResolvedValueOnce({ data: {} });

      await client.get('/resource');

      const callArgs = mockRequest.mock.calls[0][0] as { headers: Record<string, string> };
      expect(callArgs.headers['x-correlation-id']).toBeDefined();
      expect(typeof callArgs.headers['x-correlation-id']).toBe('string');
    });
  });
});
