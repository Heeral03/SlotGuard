# SlotGuard

A distributed, high-concurrency seat/resource booking engine built to eliminate race conditions under real concurrent load, with automatic fair waitlisting and demand-spike admission control.

## Core Features

- **Atomic locking**: Single-threaded Redis Lua scripts (`holdSeat`, `confirmHold`) make seat holds and confirmations atomic, preventing race conditions under concurrent requests.
- **Database backstop**: A PostgreSQL partial unique index (`WHERE status = 'CONFIRMED'`) guarantees no duplicate confirmed bookings, even if Redis state drifts.
- **Idempotent retries**: Idempotency-key middleware ensures safe request retries after network failures, preventing duplicate confirmations.
- **Rate limiting**: A microsecond-precision Token Bucket rate limiter (Redis Lua) throttles per-user request bursts without server crashes.
- **Automatic waitlist reassignment**: Expired holds are detected via BullMQ delayed jobs (no polling). If a hold expires unclaimed, the next user on a Redis sorted-set waitlist is automatically reassigned the seat, with a new expiry cycle scheduled for them.
- **Virtual waiting room**: Optional admission-control layer that queues users fairly (FIFO, Redis sorted set) and admits them in controlled batches at a fixed interval, preventing demand-spike overload on the core booking flow.
- **Health checks**: `GET /health` actively probes PostgreSQL and Redis connectivity.
- **Graceful shutdown**: Handles `SIGINT`/`SIGTERM` with a double-signal guard and a hard fallback timeout; drains in-flight work before closing HTTP server, BullMQ worker, queue, Redis, and Postgres connections in order.
- **Job monitoring**: Bull Board dashboard (`/admin/queues`, HTTP Basic Auth protected) for inspecting queue and job state.

## Tech Stack

Node.js, Express, Redis (ioredis, Lua scripting), PostgreSQL, BullMQ, JWT, k6 (load testing), Terraform (local infra provisioning).

## Verified Results

- Load-tested with k6 to 1000 concurrent users: 100% success, zero double-bookings.
- Diagnosed a Postgres connection pool bottleneck (default max of 10) causing p95 latency to exceed 500ms under load; tuned pool size to 50, cutting p95 latency by 36% (503ms to 322ms).
- Verified atomic single-seat locking under 20 simultaneous network-level requests via k6 (exactly 1 success, 19 correct rejections).
- Verified idempotent retry behavior manually: identical requests with the same idempotency key return identical cached responses without re-executing business logic.
- Verified end-to-end waitlist reassignment: an expired hold is automatically detected and reassigned to the next waitlisted user, who can then successfully confirm.
- Verified waiting room batching under 8 concurrent users: exactly `ADMISSION_BATCH_SIZE` users admitted per cycle, remainder correctly queued and admitted in the following cycle.

## Setup

### Prerequisites

- Node.js v18+
- Redis
- PostgreSQL
- Terraform (optional, for local container provisioning)

### Environment Variables