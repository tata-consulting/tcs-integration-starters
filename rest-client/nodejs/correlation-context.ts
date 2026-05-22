import { AsyncLocalStorage } from 'async_hooks';
import { randomUUID } from 'crypto';
import type { Request, Response, NextFunction } from 'express';

export interface CorrelationContext {
  correlationId: string;
}

export const correlationStorage = new AsyncLocalStorage<CorrelationContext>();

/**
 * Get the current correlation ID from AsyncLocalStorage.
 * Returns undefined if called outside a request context.
 */
export function getCorrelationId(): string | undefined {
  return correlationStorage.getStore()?.correlationId;
}

/**
 * Express middleware that reads X-Correlation-ID from the inbound request.
 * If not present, generates a new UUID. Stores it in AsyncLocalStorage so
 * all downstream code in the same request context can read it without threading it manually.
 */
export function correlationMiddleware(req: Request, res: Response, next: NextFunction): void {
  const correlationId =
    (req.headers['x-correlation-id'] as string | undefined) ?? randomUUID();

  res.setHeader('x-correlation-id', correlationId);

  correlationStorage.run({ correlationId }, () => {
    next();
  });
}
