"""
Configuration for the Hold & Confirm Ticketing System.
Centralises Redis, Postgres, and application constants.
"""

import os
import contextlib
import redis
import psycopg2
from psycopg2 import pool

# ── Redis ────────────────────────────────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

_redis_pool = redis.ConnectionPool(
    host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, max_connections=500
)

def get_redis_client() -> redis.Redis:
    """Return a Redis client backed by a shared connection pool."""
    return redis.Redis(connection_pool=_redis_pool)

# ── Postgres ─────────────────────────────────────────────────────────────
PG_HOST = os.getenv("PG_HOST", "")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "booking_system")
PG_USER = os.getenv("PG_USER", "heeral")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")

PG_DSN = {"dbname": PG_DB}
if PG_USER:
    PG_DSN["user"] = PG_USER
if PG_HOST:
    PG_DSN["host"] = PG_HOST
if PG_PORT:
    PG_DSN["port"] = PG_PORT
if PG_PASSWORD:
    PG_DSN["password"] = PG_PASSWORD

_pg_pool: pool.ThreadedConnectionPool | None = None


def get_pg_pool() -> pool.ThreadedConnectionPool:
    """Lazy initialize and return the global ThreadedConnectionPool."""
    global _pg_pool
    if _pg_pool is None or _pg_pool.closed:
        _pg_pool = pool.ThreadedConnectionPool(
            minconn=5,
            maxconn=100,
            **PG_DSN
        )
    return _pg_pool


@contextlib.contextmanager
def get_pg_conn():
    """Context manager to check out a pooled connection and return it on exit."""
    p = get_pg_pool()
    conn = p.getconn()
    try:
        yield conn
    finally:
        p.putconn(conn)


def get_pg_connection():
    """Return a raw Postgres connection (for startup or standalone scripts)."""
    return psycopg2.connect(**PG_DSN)

# ── Application constants ───────────────────────────────────────────────
HOLD_TTL_SECONDS = int(os.getenv("HOLD_TTL_SECONDS", 120))

