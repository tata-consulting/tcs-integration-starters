# Integration starter kickoff notes - 2026-05-27

## Purpose

Seed the integration starter catalog with a concrete pattern that consulting teams can explain and extend quickly.

## Decisions

- Start with HTTP to eventing because it is a common modernization pattern.
- Keep the event transformation logic small and readable so teams can adapt it to specific domains.
- Track non-functional requirements such as retries and idempotency in issue #2.

## Action items

- Add authentication examples for external-facing use cases.
- Add retry and dead-letter guidance for downstream consumers.
- Expand the starter catalog with file, batch, and SaaS integration patterns.
