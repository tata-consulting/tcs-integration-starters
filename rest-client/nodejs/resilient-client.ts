import axios, { AxiosInstance, AxiosRequestConfig, AxiosResponse } from 'axios';
import CircuitBreaker from 'opossum';
import { randomUUID } from 'crypto';
import { getCorrelationId } from './correlation-context';

export interface ResilientHttpClientOptions {
  baseURL: string;
  timeout?: number;
  circuitBreaker?: {
    // Percentage of failures that trips the circuit (default: 50)
    errorThresholdPercentage?: number;
    // Time in ms a request may take before it's considered a failure (default: 5000)
    timeout?: number;
    // Time in ms to wait before allowing a test request in half-open state (default: 30000)
    resetTimeout?: number;
    // Minimum number of calls before error percentage is evaluated (default: 10)
    // Setting this too low trips the circuit on startup noise.
    volumeThreshold?: number;
  };
  cache?: {
    // TTL in ms for cached GET responses (default: 60000)
    ttl?: number;
  };
}

interface CacheEntry {
  data: unknown;
  expiresAt: number;
}

/**
 * Resilient HTTP client with:
 * - Exponential backoff + jitter for 429 and 5xx responses (max 3 retries)
 * - Circuit breaker via opossum (closed -> open -> half-open)
 * - Correlation ID propagation via X-Correlation-ID header
 * - GET response caching with TTL
 * - OpenTelemetry trace header injection
 */
export class ResilientHttpClient {
  private readonly axios: AxiosInstance;
  private readonly breaker: CircuitBreaker;
  private readonly cache = new Map<string, CacheEntry>();
  private readonly cacheTtl: number;

  constructor(options: ResilientHttpClientOptions) {
    this.axios = axios.create({
      baseURL: options.baseURL,
      timeout: options.timeout ?? 10_000,
    });

    this.cacheTtl = options.cache?.ttl ?? 60_000;

    // Wrap the internal request function with a circuit breaker.
    // The breaker transitions: closed (normal) -> open (failing) -> half-open (testing).
    this.breaker = new CircuitBreaker(this.executeRequest.bind(this), {
      errorThresholdPercentage: options.circuitBreaker?.errorThresholdPercentage ?? 50,
      timeout: options.circuitBreaker?.timeout ?? 5_000,
      resetTimeout: options.circuitBreaker?.resetTimeout ?? 30_000,
      // Minimum calls before the error percentage is evaluated.
      // Prevents tripping on 1-2 startup failures before traffic ramps up.
      volumeThreshold: options.circuitBreaker?.volumeThreshold ?? 10,
    });

    this.breaker.on('open', () => {
      console.warn(`[ResilientHttpClient] Circuit breaker OPEN for ${options.baseURL}`);
    });
    this.breaker.on('halfOpen', () => {
      console.info(`[ResilientHttpClient] Circuit breaker HALF-OPEN - sending test request`);
    });
    this.breaker.on('close', () => {
      console.info(`[ResilientHttpClient] Circuit breaker CLOSED - resuming normal traffic`);
    });
  }

  async get<T>(path: string, config?: AxiosRequestConfig): Promise<T> {
    const cacheKey = `GET:${path}:${JSON.stringify(config?.params ?? {})}`;
    const cached = this.cache.get(cacheKey);

    if (cached && cached.expiresAt > Date.now()) {
      return cached.data as T;
    }

    const response = await this.breaker.fire('GET', path, undefined, config) as AxiosResponse<T>;

    this.cache.set(cacheKey, { data: response.data, expiresAt: Date.now() + this.cacheTtl });
    return response.data;
  }

  async post<T>(path: string, body: unknown, config?: AxiosRequestConfig): Promise<T> {
    const response = await this.breaker.fire('POST', path, body, config) as AxiosResponse<T>;
    return response.data;
  }

  async put<T>(path: string, body: unknown, config?: AxiosRequestConfig): Promise<T> {
    const response = await this.breaker.fire('PUT', path, body, config) as AxiosResponse<T>;
    return response.data;
  }

  async delete<T>(path: string, config?: AxiosRequestConfig): Promise<T> {
    const response = await this.breaker.fire('DELETE', path, undefined, config) as AxiosResponse<T>;
    return response.data;
  }

  private async executeRequest(
    method: string,
    path: string,
    body: unknown,
    config?: AxiosRequestConfig,
  ): Promise<AxiosResponse> {
    const correlationId = getCorrelationId() ?? randomUUID();

    const requestConfig: AxiosRequestConfig = {
      ...config,
      headers: {
        ...config?.headers,
        'x-correlation-id': correlationId,
        // OpenTelemetry W3C trace context propagation headers.
        // If a trace context exists in the current span, inject it here.
        // This is a no-op placeholder - wire in your OTel SDK propagator.
        ...(this.getOtelHeaders()),
      },
    };

    return this.withRetry(() =>
      this.axios.request({ method, url: path, data: body, ...requestConfig }),
    );
  }

  /**
   * Retry with exponential backoff and jitter for 429 (rate limit) and 5xx (server errors).
   * Does not retry 4xx client errors (except 429).
   */
  private async withRetry<T>(fn: () => Promise<T>, maxAttempts = 3): Promise<T> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return await fn();
      } catch (err) {
        const status = (err as { response?: { status: number } }).response?.status;
        const isRetryable = status === 429 || (status !== undefined && status >= 500);

        if (!isRetryable || attempt === maxAttempts) {
          throw err;
        }

        lastError = err as Error;
        const baseDelay = Math.pow(2, attempt) * 100;
        const jitter = Math.random() * baseDelay;
        const delay = baseDelay + jitter;

        console.warn(
          `[ResilientHttpClient] Attempt ${attempt}/${maxAttempts} failed (${status}), ` +
          `retrying in ${Math.round(delay)}ms`,
        );
        await sleep(delay);
      }
    }

    throw lastError;
  }

  private getOtelHeaders(): Record<string, string> {
    // Wire in your OpenTelemetry propagator here.
    // Example with @opentelemetry/api:
    //   const carrier: Record<string, string> = {};
    //   propagation.inject(context.active(), carrier);
    //   return carrier;
    return {};
  }

  /** Expose breaker state for health checks */
  isCircuitOpen(): boolean {
    return this.breaker.opened;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
