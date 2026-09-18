# SlotGuard

A distributed, high-concurrency seat/resource booking engine built to eliminate race conditions under real concurrent load, with automatic fair waitlisting, real-time SSE event notifications, and demand-spike admission control.

## System Architecture

### Overview

```mermaid
flowchart TD
    subgraph Client["Client"]
        C1["HTTP request"]
        C2["SSE stream (open connection)"]
    end

    subgraph API["API server (horizontally scalable)"]
        MW["Auth, waiting room, rate limit, idempotency"]
    end

    subgraph Redis["Redis"]
        RL[("Atomic locks<br/>holdSeat / confirmHold")]
        RQ[("Sorted sets<br/>seat waitlist, admission queue")]
        RP[("Pub/Sub<br/>admissions, reassignments")]
        RB[("BullMQ queue<br/>hold-expiry jobs")]
    end

    subgraph Worker["Background worker (horizontally scalable)"]
        WJ["Expiry detection, reassignment, admission cycle"]
    end

    subgraph DB["PostgreSQL"]
        PB[("bookings table<br/>partial unique index")]
    end

    C1 -->|"hold / confirm / waitlist / join"| MW
    MW --> RL
    MW --> RQ
    MW -->|"schedule expiry job"| RB
    MW -->|"ACID write"| PB

    C2 -->|"GET /queue/stream"| API
    API -->|"subscribe"| RP
    RP -->|"push: admitted / reassigned"| API
    API -->|"SSE event"| C2

    RB -->|"job pickup, locked per-job"| WJ
    WJ -->|"check status"| PB
    WJ -->|"pop next in line"| RQ
    WJ -->|"acquire new hold"| RL
    WJ -->|"publish event"| RP

    classDef store fill:#efe9ff,stroke:#7f77dd,color:#26215c
    classDef compute fill:#e6f1fb,stroke:#378add,color:#042c53
    class RL,RQ,RP,RB,PB store
    class MW,WJ compute
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

## Verified Benchmarks & Metrics

- **Throughput & Capacity (1,000 Concurrent VUs)**:
  - **Throughput**: **`825.11 req/sec`**
  - **Success Rate**: **`100.00%`** (1,000 / 1,000 succeeded)
  - **Error Rate**: **`0.00%`**
  - **Avg Latency**: **`662.47 ms`**
  - **p90 / p95 Latency**: **`1.02 s`** / **`1.07 s`**
  - **Min Latency**: **`149.49 ms`**

- **Race Condition & Single-Seat Lock Precision (50 Simultaneous VUs)**:
  - **Contention**: 50 simultaneous VUs racing for the exact same seat ID
  - **Lock Precision**: **`100.00%`** (Exactly **1** hold granted, **49** instantly rejected with `409 Conflict`)
  - **Winner Lock Latency**: **`13.76 ms`**
  - **Double-Booking Error Rate**: **`0.00%`**

- **Multi-Instance Cluster Scaling**: Verified across 3 containerized API servers and 2 workers (`docker compose up --scale server=3 --scale worker=2`). Holding a seat on Server Instance 2 (`port 3003`) returned `200 OK`, while attempting to hold the same seat via Server Instance 3 (`port 3004`) immediately returned `409 Conflict`, proving global Redis state enforcement across distinct Node processes.

- **Distributed Worker Job Locking**: Verified across multiple background workers (`worker-1` & `worker-2`). Expiry jobs for concurrent holds were distributed evenly across workers with zero double-processing or duplicate claims due to BullMQ distributed Redis locks.

- **Real-Time SSE Reassignment**: Verified instant event pushing over open SSE streams (`data: {"status":"seat_reassigned","seatId":"..."}`). Waitlisted users receive instant notifications the moment a seat hold expires.

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

### Running with Docker Compose & Nginx Load Balancer

The containerized stack includes PostgreSQL, Redis, horizontally scaled API servers (`server`), background workers (`worker`), and an **Nginx Reverse Proxy Load Balancer** (`http://localhost:8080`) providing upstream HTTP keep-alive connection pooling across server replicas:

```bash
# Spin up cluster with 3 server replicas, 2 worker replicas, and Nginx load balancer
docker compose up --build -d --scale server=3 --scale worker=2
```

#### Multi-Instance Tuning & Architectural Insights:
- **Database Connection Pool**: Configured `DB_POOL_MAX=15` per container. Because both `server.js` and `worker.js` import the same database module (`src/db.js`), 3 server replicas and 2 worker replicas previously instantiated 5 independent connection pools ($5 \times 50 = \mathbf{250}$ requested connections), exceeding PostgreSQL's default `max_connections = 100` cap and causing connection timeouts under load. Setting `max: 15` per process ($5 \times 15 = 75$ total connections) keeps total pool size safely under PostgreSQL's limit.
- **Connection-Level vs Request-Level Round-Robin**: Nginx's default load-balancing algorithm operates at the **TCP connection level**, not per HTTP request. With client keep-alive enabled, a single TCP connection carries multiple HTTP requests pinned to the same backend instance. Enabling `keepalive 64;` in Nginx upstream settings prevents socket backlog exhaustion (111 Connection Refused) while maintaining persistent upstream connections.

### Running Manually

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