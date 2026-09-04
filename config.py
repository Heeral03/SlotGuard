"""
Configuration for the Hold & Confirm Ticketing System.
Centralises Redis, Postgres, and application constants.
"""

import redis
import psycopg2

# ── Redis ────────────────────────────────────────────────────────────────
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB   = 0

_redis_pool = redis.ConnectionPool(
    host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, max_connections=250
)

def get_redis_client() -> redis.Redis:
    """Return a Redis client backed by a shared connection pool."""
    return redis.Redis(connection_pool=_redis_pool)

# ── Postgres ─────────────────────────────────────────────────────────────
PG_DSN = {
    "dbname": "booking_system",
    "user":   "heeral",
    # No 'host' → uses Unix socket → peer authentication (no password needed)
}

def get_pg_connection():
    """Return a new Postgres connection."""
    return psycopg2.connect(**PG_DSN)

# ── Application constants ───────────────────────────────────────────────
HOLD_TTL_SECONDS = 120   # how long a seat hold lasts before auto-expiry
