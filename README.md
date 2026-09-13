# SlotGuard

SlotGuard is a high-concurrency seat booking and rate-limiting engine built with **Express**, **Redis**, and **PostgreSQL**. It guarantees atomic seat holds, prevents double-booking race conditions, and enforces microsecond-precision token-bucket rate limits across API endpoints.

---

## Tech Stack

- **Runtime**: Node.js / ES Modules
- **Framework**: Express.js
- **In-Memory Store**: Redis (`ioredis` with embedded Lua scripting)
- **Database**: PostgreSQL (`pg` pool, ACID transactions, partial unique indexes)
- **Authentication**: JSON Web Tokens (`jsonwebtoken`)
- **Environment**: Configured via `.env` (`dotenv`)
- **Testing**: Asynchronous concurrency & rate-limiting proof suite (`Promise.all`)

---

## Key Features

- **Atomic Seat Holds**: Uses Redis Lua scripts (`holdSeat`) to acquire seat locks atomically with a 60-second TTL.
- **ACID Transaction Confirmations**: Converts Redis holds into permanent PostgreSQL database records inside atomic transactions (`confirmHold`).
- **PostgreSQL Partial Unique Index Safety Net**: Leverages PostgreSQL `idx_unique_confirmed_seat` partial unique index (`WHERE status = 'CONFIRMED'`) as an immutable database-level safeguard against double booking.
- **Token Bucket Rate Limiting**: Implements token bucket rate limiting in Redis Lua (`checkRateLimit`) to prevent user starvation during burst traffic.
- **User Isolation & Fairness**: Enforces strict per-user rate limit isolation under heavy multi-user concurrency.

---

## Performance & Benchmark Metrics

Benchmarked under high concurrency (`Promise.all` simultaneous burst testing):

### 1. Burst Concurrency & Rate-Limiting Accuracy

| Test Scenario | Total Concurrency | Limit Enforced | Allowed (200) | Rate-Limited (429) | Throughput | Accuracy |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Single User Burst (Small)** | 50 simultaneous reqs | 5 / min | **5** | 45 | **1,141.3 req/sec** | **100% PASS ✅** |
| **Single User Burst (Large)** | 500 simultaneous reqs | 5 / min | **5** | 495 | **1,760.7 req/sec** | **100% PASS ✅** |
| **Multi-User Fairness** | 50 users x 10 reqs (500 total) | 5 / user | **250 (5 per user)** | 250 | **1,627.4 req/sec** | **100% PASS ✅** |

### 2. Latency Metrics

- **50 Burst Reqs**: Min `31.48 ms` | p50 `34.33 ms` | p95 `57.00 ms` | Max `125.29 ms`
- **500 Burst Reqs**: Min `74.31 ms` | p50 `236.52 ms` | p95 `279.45 ms` | Max `284.03 ms`

### 3. Database Safety Net (PostgreSQL Partial Index)
- **Duplicate Prevention**: 100% duplicate seat attempt protection via `idx_unique_confirmed_seat` partial unique index, triggering PostgreSQL constraint error code `23505` (`unique_violation`).

---

## PostgreSQL Database Schema

```sql
CREATE TABLE bookings (
    id SERIAL PRIMARY KEY,
    seat_id VARCHAR(50) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('PENDING', 'CONFIRMED', 'CANCELLED', 'EXPIRED')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- THE MAGIC SAFETY NET: Partial Unique Index
CREATE UNIQUE INDEX idx_unique_confirmed_seat 
ON bookings (seat_id) 
WHERE status = 'CONFIRMED';
```

---

## Architecture & Workflow

```
[ Client ] 
   │
   ├──► 1. POST /api/v1/slots/:id/hold (Auth -> Rate Limit -> Redis Lua holdSeat)
   │
   └──► 2. POST /api/v1/slots/:id/confirm (Auth -> Redis Lua confirmHold -> PostgreSQL ACID Insert)
                                                                                  │
                                                                                  ▼
                                                              [ Partial Unique Index Safety Net ]
```

1. **Hold Request (`POST /api/v1/slots/:id/hold`)**
   - Validates JWT authentication token.
   - Applies token bucket rate limiter in Redis.
   - Executes atomic Lua script to hold seat in Redis with a 60-second TTL.

2. **Confirm Request (`POST /api/v1/slots/:id/confirm`)**
   - Validates JWT authentication token.
   - Atomically verifies Redis hold ownership and removes hold key.
   - Executes ACID database transaction to insert record into PostgreSQL `bookings` table.
   - Enforces `idx_unique_confirmed_seat` partial unique index to guarantee no seat is double-booked.

---

## Setup & Running

### Environment Configuration (`.env`)

```env
DATABASE_URL=postgresql://heeral:postgres@localhost:5432/booking_system
REDIS_URL=redis://localhost:6379
```

### Installation & Execution

```bash
# Install dependencies
npm install

# Start development server
npm run dev

# Run Concurrency & Benchmark Test Suite
node src/Test.js

# Run Connection & Flow Integration Verification
node src/test_flow.js
```
