"""
Naive Baseline — SELECT-then-UPDATE booking (NO atomic hold)
────────────────────────────────────────────────────────────
Deliberately racy implementation that checks availability with SELECT,
then books with INSERT — exactly the pattern that causes overselling
in production. Used as a comparison baseline for the Lua-based system.
"""

from __future__ import annotations

import psycopg2
import psycopg2.errors
import psycopg2.pool

from config import PG_DSN

# Separate table so we don't interfere with the real bookings
_SETUP_SQL = """
CREATE TABLE IF NOT EXISTS naive_bookings (
    id         SERIAL PRIMARY KEY,
    seat_id    INT NOT NULL,
    user_id    INT NOT NULL,
    status     TEXT NOT NULL DEFAULT 'CONFIRMED',
    created_at TIMESTAMP DEFAULT NOW()
);
"""

# NOTE: No unique index — this is the "naive" version.
# Double-bookings are the expected failure mode.

_naive_pool = psycopg2.pool.ThreadedConnectionPool(
    minconn=2, maxconn=40, **PG_DSN
)


def setup_naive_table():
    """Create the naive_bookings table if it doesn't exist."""
    conn = _naive_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(_SETUP_SQL)
        conn.commit()
    finally:
        _naive_pool.putconn(conn)


def cleanup_naive(seat_ids: list[int]):
    """Wipe naive_bookings test data."""
    conn = _naive_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM naive_bookings WHERE seat_id = ANY(%s)",
                (seat_ids,),
            )
        conn.commit()
    finally:
        _naive_pool.putconn(conn)


def naive_book_seat(seat_id: int, user_id: int) -> bool:
    """
    The classic race-condition-prone booking:
      1. SELECT to check if anyone has this seat  (not locked!)
      2. Simulate app-level processing (payment check, validation)
      3. If free → INSERT

    The gap between SELECT and INSERT is where double-booking happens.
    Uses a fresh connection per call to avoid pool exhaustion under load.
    """
    import time as _time
    conn = psycopg2.connect(**PG_DSN)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            # Step 1 — check (NOT atomic, NOT locked)
            cur.execute(
                "SELECT COUNT(*) FROM naive_bookings WHERE seat_id = %s",
                (seat_id,),
            )
            count = cur.fetchone()[0]

            if count > 0:
                return False  # someone already booked (maybe)

            # ── Race window ──────────────────────────────────────────
            # Simulates real-world processing between read and write:
            # payment validation, session checks, API calls, etc.
            # This is where concurrent threads see stale SELECT results.
            _time.sleep(0.005)  # 5ms — realistic app processing

            # Step 2 — book  (race: another thread can INSERT here too)
            cur.execute(
                "INSERT INTO naive_bookings (seat_id, user_id) VALUES (%s, %s)",
                (seat_id, user_id),
            )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        return False
    finally:
        conn.close()


def audit_naive_bookings(seat_ids: list[int]) -> dict:
    """
    Count how many seats were double-booked.
    Returns {total_bookings, unique_seats, oversold_seats, oversold_list}.
    """
    conn = _naive_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM naive_bookings WHERE seat_id = ANY(%s)",
                (seat_ids,),
            )
            total = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(DISTINCT seat_id) FROM naive_bookings WHERE seat_id = ANY(%s)",
                (seat_ids,),
            )
            unique = cur.fetchone()[0]

            cur.execute("""
                SELECT seat_id, COUNT(*) AS cnt
                FROM naive_bookings
                WHERE seat_id = ANY(%s)
                GROUP BY seat_id
                HAVING COUNT(*) > 1
                ORDER BY cnt DESC
                LIMIT 20
            """, (seat_ids,))
            oversold = cur.fetchall()

        return {
            "total_bookings": total,
            "unique_seats_booked": unique,
            "oversold_seats": len(oversold),
            "oversold_examples": [(sid, cnt) for sid, cnt in oversold],
        }
    finally:
        _naive_pool.putconn(conn)
