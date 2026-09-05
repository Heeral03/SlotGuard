# SlotGuard — Event-Driven Public Service Appointment Engine

[![Python](https://img.shields.io/badge/Python-3.12-blue.svg)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110-green.svg)](https://fastapi.tiangolo.com)
[![Redis](https://img.shields.io/badge/Redis-Lua%20%2B%20PubSub%20%2B%20ZSET-red.svg)](https://redis.io)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue.svg)](https://postgresql.org)
[![Pytest](https://img.shields.io/badge/Pytest-Suite%20Passed-brightgreen.svg)](tests/)
[![Docker](https://img.shields.io/badge/Docker%20Compose-Ready-blue.svg)](docker-compose.yml)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**SlotGuard** is an event-driven, fault-tolerant public service appointment engine engineered in Python, FastAPI, Redis (Lua scripts + Pub/Sub + Sorted Sets), PostgreSQL, and Prometheus.

It is designed to solve **"Refresh Stampedes"** in high-demand public systems (CoWIN vaccine booking, Passport slots, US Visa appointments, Tatkal bookings) where high-concurrency burst traffic attempts to claim scarce slots simultaneously.

---

## 🏛 Enterprise Architecture & Resilience Design

```
                     ┌─────────────────────────────────────────┐
                     │            Client REST Requests         │
                     └────────────────────┬────────────────────┘
                                          │
                                          ▼
                     ┌─────────────────────────────────────────┐
                     │           FastAPI Web Gateway           │
                     └────────────────────┬────────────────────┘
                                          │
                   ┌──────────────────────┴──────────────────────┐
                   │                                             │
                   ▼                                             ▼
  ┌─────────────────────────────────┐           ┌─────────────────────────────────┐
  │     Redis Atomic Hold Tier      │           │     Postgres Storage Tier       │
  ├─────────────────────────────────┤           ├─────────────────────────────────┤
  │ • Check-and-Set Lua Scripts     │           │ • Threaded Connection Pool      │
  │ • ZSET TTL Hold Tracking        │           │ • ACID Insert + Outbox Table    │
  │ • Fair FIFO Waitlists (ZSET)    │           │ • Partial Unique Index Backstop │
  └────────────────┬────────────────┘           └────────────────┬────────────────┘
                   │                                             │
                   ▼                                             ▼
  ┌─────────────────────────────────┐           ┌─────────────────────────────────┐
  │ Dual-Layer Expiry Worker Daemon │           │ CDC Transactional Outbox Worker │
  ├─────────────────────────────────┤           ├─────────────────────────────────┤
  │ • Pub/Sub Fast Path Event       │           │ • Non-blocking SELECT FOR       │
  │ • ZSET Scanner Fail-safe (100ms)│           │   UPDATE SKIP LOCKED Poller     │
  └─────────────────────────────────┘           └─────────────────────────────────┘
```

### Key Engineering Features:

1. **Threaded PostgreSQL Connection Pooling (`config.py`)**:
   - Replaces naive per-request connections with `psycopg2.pool.ThreadedConnectionPool` (5 to 100 pooled connections).
   - Drastically cuts TCP connection handshake overhead, enabling fast concurrent HTTP throughput.

2. **Dual-Layer Deterministic Expiry Worker (`expiry_worker.py`)**:
   - **Fast Path**: Listens to Redis Pub/Sub `__keyevent@0__:expired` events.
   - **Fail-Safe Scanner**: Periodically scans `holds:ttl` Redis Sorted Set (`ZRANGEBYSCORE`) via Lua (`scan_expired_holds.lua`) every 100ms.
   - **Guarantees 100% queue progression even if Pub/Sub events are dropped or worker daemons restart.**

3. **Zero-TOCTOU Atomic Confirmation (`confirm_check.lua`)**:
   - Atomically checks state and locks key as `CONFIRMING` in Redis before acquiring DB transactions.
   - Eliminates Time-of-Check to Time-of-Use race conditions where a hold could expire mid-DB insert.

4. **Transactional Outbox & CDC Poller (`outbox_worker.py`)**:
   - Writes `SLOT_CONFIRMED` events into PostgreSQL `outbox` table in the **exact same ACID transaction** as the booking insert.
   - CDC Poller worker processes events using `SELECT ... FOR UPDATE SKIP LOCKED` for non-blocking multi-worker scaling.

5. **Partial Unique Index Database Backstop (`schema.sql`)**:
   - Postgres `CREATE UNIQUE INDEX idx_no_double_booking ON bookings (seat_id) WHERE status = 'CONFIRMED'`.
   - Hard backstop ensuring absolute zero double-bookings even under catastrophic cache loss.

---

## 📊 Empirical Benchmarks & Verification (100% Passed)

| Component / Test Suite | Concurrency / Setup | Performance Metrics | Status |
|---|---|---|---|
| **Pytest Test Suite** | 9 Integration & Engine Tests | **9/9 PASSED (0.73s)** | ✅ PASS |
| **Single-Slot Engine Benchmark** | 10,000 users vs 100 slots (100:1 ratio) | **2,409.3 ops/sec**, 0 double bookings | ✅ PASS |
| **Concurrent Expiry Stress Test** | 50 simultaneous TTL expiries | **50/50 Auto-Offers Processed in 19.2ms** | ✅ PASS |
| **FastAPI REST API Benchmark** | 500 citizens, 50 slots, 50 threads | **83.9 ops/sec**, 0 double bookings | ✅ PASS |
| **Naive Baseline (SELECT-then-INSERT)** | 500 users vs 10 slots (with 5ms delay) | 26 DB rows (**9/10 slots oversold**) | ❌ FAILED |

---

## 🚀 Quickstart & One-Click Deployment

### Option A: Docker Compose (Recommended)
Spin up PostgreSQL, Redis, FastAPI (4 Uvicorn workers), CDC Outbox Worker, Expiry Worker, and Prometheus with a single command:

```bash
docker compose up --build
```

### Option B: Local Setup & Running Pytest

```bash
# 1. Clone repo & setup virtual environment
git clone https://github.com/Heeral03/SlotGuard.git
cd SlotGuard

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Run test suite
PYTHONPATH=. pytest tests/ -v

# 3. Run benchmarks
python3 load_test.py
python3 load_test_expiry.py
python3 load_test_api.py
```

---

## 💻 REST API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Health check (Pooled Redis & PostgreSQL connection checks) |
| `GET` | `/metrics` | Prometheus scrapable metrics endpoint |
| `POST` | `/api/v1/slots/{id}/hold` | Atomic single-slot hold (check-and-set via Redis Lua) |
| `POST` | `/api/v1/slots/{id}/confirm` | Zero-TOCTOU atomic confirm + Transactional Outbox record |
| `POST` | `/api/v1/slots/{id}/release` | Release hold (triggers auto-offer to next waitlisted citizen) |
| `POST` | `/api/v1/slots/{id}/waitlist/join` | Join fair FIFO waitlist queue |
| `POST` | `/api/v1/slots/{id}/waitlist/leave` | Leave waitlist queue |
| `GET` | `/api/v1/slots/{id}/waitlist` | Query ordered waitlist positions |
| `POST` | `/api/v1/slots/hold-multi` | Atomic multi-slot family hold (all-or-nothing Lua script) |
| `POST` | `/api/v1/slots/confirm-multi` | Multi-slot transactional confirmation |

---

## 📜 License
Distributed under the MIT License. See `LICENSE` for details.
