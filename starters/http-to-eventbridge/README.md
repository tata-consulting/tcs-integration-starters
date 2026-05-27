# HTTP to EventBridge starter

This starter demonstrates a simple integration boundary where an HTTP request is validated, normalized, and forwarded as an event for downstream processing.

## Assets

- `openapi.yaml` - contract for the HTTP ingress
- `handler.py` - starter event transformation logic

## Next steps

- Add authentication and request signing
- Add idempotency storage for repeated requests
- Expand the emitted event schema with business metadata
