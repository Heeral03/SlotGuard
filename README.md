# SlotGuard

A distributed, high-concurrency seat/resource booking engine built to eliminate race conditions under real concurrent load, with automatic fair waitlisting, real-time SSE event notifications, and demand-spike admission control.

## System Architecture

```mermaid
flowchart TD
    subgraph Clients["Clients & Edge"]
        C1["HTTP Client (User A)"]
        C2["HTTP / SSE Client (User B)"]
    end

    subgraph LoadBalancer["Multi-Instance API Cluster"]
        S1["Server Instance A\n(PORT 3000)"]
        S2["Server Instance B\n(PORT 3001)"]
    end

    subgraph Middleware["Middleware Pipeline"]
        M1["Auth (JWT)"]
        M2["Virtual Waiting Room"]
        M3["Token Bucket Rate Limiter"]
        M4["Idempotency Cache"]
    end

    subgraph Redis["Redis (Shared State & Pub/Sub)"]
        R1[("Atomic Lua Scripts\nholdSeat / confirmHold")]
        R2[("Sorted Sets\nwaitlist:seatId / queue:waiting")]
        R3[("Pub/Sub Channels\nqueue:admissions / queue:reassignments")]
        R4[("BullMQ Queue\nhold-expiry")]
    end

    subgraph Workers["Distributed Background Workers"]
        W1["Worker Instance 1\n(WORKER_ID worker-1)"]
        W2["Worker Instance 2\n(WORKER_ID worker-2)"]
    end

    subgraph Postgres["PostgreSQL Storage"]
        DB[("Database: bookings Table\nUnique Index (seat_id WHERE status='CONFIRMED')")]
    end

    %% Client Interactions
    C1 -->|"POST /slots/:id/hold"| S1
    C2 -->|"GET /queue/stream"| S2
    C2 -->|"POST /slots/:id/waitlist"| S2
    C2 -->|"POST /slots/:id/confirm"| S2

    %% Server Internal Routing & Redis State
    S1 --> M1 --> M2 --> M3 --> R1
    S2 --> M1 --> M2 --> M3 --> R1
    S2 --> M4
    
    %% Stream & Waitlist
    S2 -->|"Subscribe SSE"| R3
    S2 -->|"ZADD Waitlist"| R2
    
    %% Hold Queue Scheduling
    S1 -->|"Schedule Expiry Job"| R4
    S2 -->|"Schedule Expiry Job"| R4

    %% Worker Operations
    R4 -->|"Job Lock Claim"| W1
    R4 -->|"Job Lock Claim"| W2
    W1 -->|"Check DB & Pop Waitlist"| DB
    W1 -->|"Hold & Publish Reassignment"| R1
    W1 -->|"Publish Reassignment"| R3
    W2 -->|"Check DB & Pop Waitlist"| DB
    W2 -->|"Hold & Publish Reassignment"| R1
    W2 -->|"Publish Reassignment"| R3
    
    %% Confirmation Transaction
    S2 -->|"ACID Transaction"| DB
```

## Core Features

- **Atomic locking**: Single-threaded Redis Lua scripts (`holdSeat`, `confirmHold`) make seat holds and confirmations atomic, preventing race conditions under concurrent requests.
- **Database backstop**: A PostgreSQL partial unique index (`WHERE status = 'CONFIRMED'`) guarantees no duplicate confirmed bookings, even if Redis state drifts.
- **Idempotent retries**: Idempotency-key middleware ensures safe request retries after network failures, preventing duplicate confirmations.
- **Rate limiting**: A microsecond-precision Token Bucket rate limiter (Redis Lua) throttles per-user request bursts without server crashes.
- **Automatic waitlist reassignment**: Expired holds are detected via BullMQ delayed jobs (no polling). If a hold expires unclaimed, the next user on a Redis sorted-set waitlist is automatically reassigned the seat, with a new expiry cycle scheduled for them.
- **Real-time SSE event streaming**: Live event stream (`GET /api/v1/queue/stream`) pushes instant status updates to clients (e.g. `seat_reassigned`, `admitted`) via Redis Pub/Sub without polling overhead.
- **Virtual waiting room**: Optional admission-control layer that queues users fairly (FIFO, Redis sorted set) and admits them in controlled batches at a fixed interval, preventing demand-spike overload on the core booking flow.
- **Multi-instance cluster support**: Configurable HTTP ports (`PORT`) and worker instance identifiers (`WORKER_ID`) allow running multiple server nodes and background worker workers sharing Redis and PostgreSQL state.
- **Health checks**: `GET /health` actively probes PostgreSQL and Redis connectivity.
- **Graceful shutdown**: Handles `SIGINT`/`SIGTERM` with a double-signal guard and a hard fallback timeout; drains in-flight work before closing HTTP server, BullMQ worker, queue, Redis, and Postgres connections in order.
- **Job monitoring**: Bull Board dashboard (`/admin/queues`, HTTP Basic Auth protected) for inspecting queue and job state.

## Tech Stack

Node.js, Express, Redis (ioredis, Lua scripting, Pub/Sub), PostgreSQL, BullMQ, Server-Sent Events (SSE), JWT, k6 (load testing), Terraform (local infra provisioning).

## Verified Results

- **Multi-Instance Shared State**: Verified across separate server instances (`PORT=3000` & `PORT=3001`). Holding a seat on Server A returned `200 OK`, while attempting to hold the same seat via Server B immediately returned `409 Conflict`, proving global Redis state enforcement across distinct Node processes.
- **Distributed Worker Job Locking**: Verified across multiple background workers (`WORKER_ID=worker-1` & `WORKER_ID=worker-2`). Expiry jobs for concurrent holds were distributed evenly across workers with zero double-processing or duplicate claims due to BullMQ distributed Redis locks.
- **Real-Time SSE Reassignment**: Verified instant event pushing over open SSE streams (`data: {"status":"seat_reassigned","seatId":"..."}`). Waitlisted users receive instant notifications the moment a seat hold expires.
- **Load-tested with k6**: Evaluated up to 1000 concurrent users with 100% success and zero double-bookings.
- **Postgres Pool Optimization**: Diagnosed a Postgres connection pool bottleneck (default max of 10) causing p95 latency to exceed 500ms under load; tuned pool size to 50, cutting p95 latency by 36% (503ms to 322ms).
- **Atomic Single-Seat Locking**: Verified under 20 simultaneous network-level requests via k6 (exactly 1 success, 19 correct rejections).
- **Idempotent Retry Behavior**: Verified identical requests with the same idempotency key return identical cached responses without re-executing business logic.
- **Waiting Room Batching**: Verified batching under 8 concurrent users: exactly `ADMISSION_BATCH_SIZE` users admitted per cycle, remainder correctly queued and admitted in following cycles.

## Setup

### Prerequisites

- Node.js v18+
- Redis
- PostgreSQL
- Terraform (optional, for local container provisioning)

### Environment Variables

```env
DATABASE_URL=postgresql://username:password@localhost:5432/slotguard
REDIS_URL=redis://localhost:6379
JWT_SECRET=your_secret_here
ADMIN_USER=admin
ADMIN_PASS=admin
WAITING_ROOM_ENABLED=false
PORT=3000
WORKER_ID=worker-1
```

### Database Schema

```sql
CREATE TABLE IF NOT EXISTS bookings (
    id SERIAL PRIMARY KEY,
    seat_id VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_confirmed_seat
ON bookings (seat_id)
WHERE status = 'CONFIRMED';
```

### Running Single-Instance

```bash
npm install

# Terminal 1: API server
node src/server.js

# Terminal 2: Background worker
node src/worker.js
```

### Running Multi-Instance Cluster

```bash
# Terminal 1: Server A (Port 3000)
PORT=3000 node src/server.js

# Terminal 2: Server B (Port 3001)
PORT=3001 node src/server.js

# Terminal 3: Worker Instance 1
WORKER_ID=worker-1 node src/worker.js

# Terminal 4: Worker Instance 2
WORKER_ID=worker-2 node src/worker.js
```

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/v1/slots/:id/hold` | Attempt to hold a seat |
| POST | `/api/v1/slots/:id/confirm` | Confirm a held seat (idempotent) |
| POST | `/api/v1/slots/:id/waitlist` | Join the waitlist for a held seat |
| POST | `/api/v1/queue/join` | Join the virtual waiting room |
| GET | `/api/v1/queue/status` | Check waiting room status/position |
| GET | `/api/v1/queue/stream` | Real-time SSE stream for admission and reassignment events |
| GET | `/health` | Service health check |
| GET | `/admin/queues` | Bull Board job dashboard (Basic Auth) |

## Load Testing

k6 scripts (`race-test.js`, `capacity-test.js`, `throughtput-test.js`) validate correctness and measure throughput/latency under controlled concurrency. Run with:

```bash
k6 run <script-name>.js
```