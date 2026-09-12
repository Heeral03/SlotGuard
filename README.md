# SlotGuard

SlotGuard is a high-concurrency seat booking and rate-limiting engine built with Express, Redis, and PostgreSQL. It guarantees atomic seat holds, prevents double-booking race conditions, and enforces token-bucket rate limits across API endpoints.

## Features

- **Atomic Seat Holding**: Uses Redis Lua scripts (`holdSeat`) to acquire seat locks atomically with a 60-second TTL.
- **Race-Condition Free Confirmation**: Converts Redis holds into permanent PostgreSQL database records inside ACID transactions (`confirmHold`).
- **Post-Confirmation Lock Security**: Retains confirmed state in Redis and verifies against PostgreSQL to prevent expired holds from allowing double bookings.
- **Token Bucket Rate Limiting**: Implements microsecond-precision token bucket rate limiting in Redis Lua (`checkRateLimit`) to prevent user starvation during frequent polling.
- **Partial Unique Index Safety Net**: Leverages PostgreSQL `idx_unique_confirmed_seat` unique partial index (`WHERE status = 'CONFIRMED'`) as an immutable database-level safeguard.

## Architecture & Workflow

1. **Hold Request (`POST /api/v1/slots/:id/hold`)**
   - Authenticates JWT bearer token.
   - Evaluates token bucket rate limit.
   - Atomically attempts to set key `seat:<id>` in Redis for the user.
   - Verifies seat is not already confirmed in PostgreSQL.

2. **Confirm Request (`POST /api/v1/slots/:id/confirm`)**
   - Authenticates JWT bearer token.
   - Evaluates token bucket rate limit.
   - Atomically verifies Redis hold ownership and updates key status to `CONFIRMED`.
   - Inserts booking record into PostgreSQL inside an ACID transaction.

## Setup & Environment

Create a `.env` file in the root directory:

```env
DATABASE_URL=postgresql://username:password@localhost:5432/booking_system
REDIS_URL=redis://localhost:6379
```

Database schema requirements:

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

## Running the Application

Install dependencies:
```bash
npm install
```

Start dev server:
```bash
npm run dev
```

Run test suite:
```bash
node tests/test_suite.js
```
