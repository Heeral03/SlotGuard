# SlotGuard

A distributed, high-concurrency seat/resource booking engine built to eliminate race conditions under real concurrent load, with automatic fair waitlisting, real-time SSE event notifications, and demand-spike admission control.

## System Architecture

### Overview

```mermaid
flowchart TD
    subgraph Layer1["1. CLIENT LAYER"]
        direction TB
        C1["HTTP Client\n(Hold / Confirm / Waitlist)"]
        C2["SSE Stream Client\n(Real-Time Listener)"]
    end

    subgraph Layer2["2. MULTI-INSTANCE API CLUSTER"]
        direction TB
        S1["Server Instance A (PORT 3000)"]
        S2["Server Instance B (PORT 3001)"]
        MW["Middleware Pipeline\n(Auth ➔ Waiting Room ➔ Rate Limiter ➔ Idempotency)"]
    end

    subgraph Layer3["3. SHARED REDIS IN-MEMORY LAYER"]
        direction TB
        LUA[("Atomic Lua Scripts\n(holdSeat / confirmHold)")]
        KEYS[("Redis Keys & Sorted Sets\n(seat:* / waitlist:* / queue:*)")]
        PUBSUB[("Pub/Sub Channels\n(queue:admissions / queue:reassignments)")]
        QUEUE[("BullMQ Queue\n(hold-expiry delayed jobs)")]
    end

    subgraph Layer4["4. DISTRIBUTED WORKER CLUSTER"]
        direction TB
        W1["Worker Instance 1 (WORKER_ID worker-1)"]
        W2["Worker Instance 2 (WORKER_ID worker-2)"]
    end

    subgraph Layer5["5. PERSISTENCE STORAGE LAYER"]
        direction TB
        DB[("PostgreSQL Database\n(bookings table + Partial Unique Index)")]
    end

    Layer1 --> Layer2
    Layer2 --> MW
    MW --> Layer3
    Layer3 --> Layer4
    Layer4 --> Layer5
    Layer2 --> Layer5
```

### Core Execution Flows

#### 1. Seat Hold & Confirmation Flow
```mermaid
sequenceDiagram
    autonumber
    actor User as Client
    participant API as Express API Node
    participant Redis as Redis State
    participant DB as PostgreSQL DB

    User->>API: 1. POST /api/v1/slots/:id/hold
    API->>Redis: 2. Execute Lua holdSeat()
    Redis-->>API: 3. Return Success (1) or Conflict (0)
    API->>Redis: 4. Schedule delayed hold-expiry job (60s)
    API-->>User: 5. 200 Held / 409 Conflict

    User->>API: 6. POST /api/v1/slots/:id/confirm (Idempotency-Key)
    API->>Redis: 7. Execute Lua confirmHold()
    API->>DB: 8. ACID Transaction: INSERT INTO bookings
    API-->>User: 9. 201 Booking Confirmed
```

#### 2. Hold Expiry, Waitlist Reassignment & SSE Event Push Flow
```mermaid
sequenceDiagram
    autonumber
    actor UserB as Waitlisted User
    participant SSE as SSE Stream Server Node
    participant Worker as BullMQ Worker Node
    participant Redis as Redis Pub/Sub & Keys

    UserB->>SSE: 1. GET /api/v1/queue/stream
    SSE->>Redis: 2. Subscribe (queue:reassignments)

    Note over Worker: 3. 60s Hold Expiry Job Triggers
    Worker->>Redis: 4. ZPOPMIN waitlist:seatId
    Worker->>Redis: 5. Reassign hold via Lua holdSeat(UserB)
    Worker->>Redis: 6. PUBLISH queue:reassignments { UserB, seatId }
    Redis-->>SSE: 7. Deliver Pub/Sub Message
    SSE-->>UserB: 8. Push SSE Event: {"status":"seat_reassigned","seatId":"..."}
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