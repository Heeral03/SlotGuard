"""
Pytest configuration and shared fixtures for SlotGuard test suite.
"""

import pytest
from config import get_redis_client, get_pg_conn


@pytest.fixture(autouse=True)
def clean_databases():
    """Ensure Redis and PostgreSQL tables are fresh before each test run."""
    r = get_redis_client()
    r.flushdb()

    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE bookings, outbox RESTART IDENTITY;")
        conn.commit()

    yield

    r.flushdb()
