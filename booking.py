"""
Hold & Confirm Booking Engine
─────────────────────────────
Core:
  hold_seat / confirm_booking / get_seat_status / release_hold

Waitlist (fair queue):
  join_waitlist / leave_waitlist / get_waitlist

Multi-seat (atomic):
  hold_seats / confirm_seats / release_holds
"""

from __future__ import annotations

import json
import os
import time
import uuid
from typing import Tuple, Optional, List

import psycopg2
import psycopg2.errors
import redis

from config import get_redis_client, get_pg_connection, HOLD_TTL_SECONDS, PG_DSN
from metrics import (
    SLOTGUARD_HOLDS_TOTAL,
    SLOTGUARD_CONFIRMS_TOTAL,
    SLOTGUARD_WAITLIST_JOIN_TOTAL,
    SLOTGUARD_HOLD_LATENCY,
    SLOTGUARD_CONFIRM_LATENCY,
    SLOTGUARD_WAITLIST_DEPTH,
)


# ── Load Lua scripts once ────────────────────────────────────────────────
_LUA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lua")


def _load_lua(filename: str) -> str:
    with open(os.path.join(_LUA_DIR, filename)) as f:
        return f.read()


_redis: redis.Redis = get_redis_client()

_hold_script = _redis.register_script(_load_lua("hold.lua"))
_waitlist_offer_script = _redis.register_script(_load_lua("waitlist_offer.lua"))
_hold_multi_script = _redis.register_script(_load_lua("hold_multi.lua"))


# ═══════════════════════════════════════════════════════════════════════
#  SINGLE-SEAT HOLD & CONFIRM (unchanged API, enhanced release)
# ═══════════════════════════════════════════════════════════════════════

def hold_seat(seat_id: int, user_id: int, ttl: int | None = None) -> bool:
    """Attempt to place a temporary hold on a single seat."""
    ttl = ttl or HOLD_TTL_SECONDS
    result = _hold_script(keys=[f"seat:{seat_id}"], args=[user_id, ttl])
    return result == 1


def confirm_booking(
    seat_id: int, user_id: int, profile: bool = False,
    booking_group_id: str | None = None,
) -> Tuple[bool, str, Optional[dict]]:
    """
    Confirm a previously held seat.

    Returns (success, message, timings).
    """
    timings: dict = {} if profile else None

    key = f"seat:{seat_id}"

    t0 = time.perf_counter() if profile else 0
    current = _redis.get(key)
    if profile:
        timings["redis_check_s"] = time.perf_counter() - t0

    if current is None:
        return False, "Hold expired — no active hold on this seat", timings

    expected = f"HELD:{user_id}".encode()
    if current != expected:
        return False, f"Hold invalid (current state: {current.decode()})", timings

    t1 = time.perf_counter() if profile else 0
    conn = psycopg2.connect(**PG_DSN)
    if profile:
        timings["pg_conn_acquire_s"] = time.perf_counter() - t1

    try:
        t2 = time.perf_counter() if profile else 0
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO bookings (seat_id, user_id, status, booking_group_id) "
                "VALUES (%s, %s, 'CONFIRMED', %s)",
                (seat_id, user_id, booking_group_id),
            )
            outbox_payload = json.dumps({"seat_id": seat_id, "user_id": user_id, "confirmed_at": time.time()})
            cur.execute(
                "INSERT INTO outbox (event_type, aggregate_id, payload) VALUES ('SLOT_CONFIRMED', %s, %s)",
                (str(seat_id), outbox_payload),
            )
        conn.commit()

        if profile:
            timings["pg_insert_commit_s"] = time.perf_counter() - t2
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        return False, "Seat already confirmed (Postgres constraint caught race)", timings
    except Exception as exc:
        conn.rollback()
        return False, f"Database error: {exc}", timings
    finally:
        conn.close()

    t3 = time.perf_counter() if profile else 0
    _redis.set(key, "CONFIRMED")
    if profile:
        timings["redis_promote_s"] = time.perf_counter() - t3

    return True, "Booking confirmed", timings


def get_seat_status(seat_id: int) -> str:
    """Return FREE / HELD:<user_id> / CONFIRMED."""
    val = _redis.get(f"seat:{seat_id}")
    if val is None:
        return "FREE"
    return val.decode()


def release_hold(seat_id: int, user_id: int, offer_waitlist: bool = True) -> bool:
    """
    Release a hold. If offer_waitlist=True (default), the next user in the
    waitlist is automatically offered the seat.
    """
    key = f"seat:{seat_id}"
    current = _redis.get(key)
    expected = f"HELD:{user_id}".encode()

    if current != expected:
        return False

    _redis.delete(key)

    if offer_waitlist:
        _offer_to_next_in_waitlist(seat_id)

    return True


# ═══════════════════════════════════════════════════════════════════════
#  WAITLIST QUEUE (fair FIFO)
# ═══════════════════════════════════════════════════════════════════════

def join_waitlist(seat_id: int, user_id: int) -> Tuple[bool, str]:
    """
    Add user to the waitlist for a seat.
    Only makes sense if the seat is currently held or confirmed.
    Score = timestamp → FIFO ordering.
    """
    status = get_seat_status(seat_id)
    if status == "FREE":
        return False, "Seat is free — hold it directly instead"

    wl_key = f"waitlist:{seat_id}"
    # ZADD NX: only add if not already in the set
    added = _redis.zadd(wl_key, {str(user_id): time.time()}, nx=True)
    if added:
        return True, f"Added to waitlist (position: {_redis.zrank(wl_key, str(user_id)) + 1})"
    return False, "Already on the waitlist"


def leave_waitlist(seat_id: int, user_id: int) -> bool:
    """Remove user from the waitlist."""
    return _redis.zrem(f"waitlist:{seat_id}", str(user_id)) > 0


def get_waitlist(seat_id: int) -> List[int]:
    """Return ordered list of user_ids waiting for this seat (FIFO)."""
    members = _redis.zrange(f"waitlist:{seat_id}", 0, -1)
    return [int(m) for m in members]


def _offer_to_next_in_waitlist(
    seat_id: int, ttl: int | None = None,
) -> Optional[int]:
    """
    Internal: atomically pop the next waitlisted user and place a hold for them.
    Returns the offered user_id, or None if the waitlist is empty.
    """
    ttl = ttl or HOLD_TTL_SECONDS
    result = _waitlist_offer_script(
        keys=[f"seat:{seat_id}", f"waitlist:{seat_id}"],
        args=[ttl],
    )
    if result == 0:
        return None
    # result is the user_id as bytes
    return int(result)


# ═══════════════════════════════════════════════════════════════════════
#  MULTI-SEAT ATOMIC BOOKING (all-or-nothing)
# ═══════════════════════════════════════════════════════════════════════

def hold_seats(
    seat_ids: List[int], user_id: int, ttl: int | None = None,
) -> Tuple[bool, str]:
    """
    Atomically hold ALL requested seats, or NONE.

    Returns (success, message).
    If success=False, message lists which seats were taken.
    """
    ttl = ttl or HOLD_TTL_SECONDS
    keys = [f"seat:{sid}" for sid in seat_ids]

    result = _hold_multi_script(
        keys=keys,
        args=[user_id, ttl, len(seat_ids)],
    )

    # result is [status, detail]
    status_code = result[0]
    detail = result[1].decode() if isinstance(result[1], bytes) else str(result[1])

    if status_code == 1:
        return True, f"All {len(seat_ids)} seats held"
    else:
        return False, f"Some seats taken: {detail}"


def confirm_seats(
    seat_ids: List[int], user_id: int,
) -> Tuple[bool, str]:
    """
    Confirm ALL held seats in a single Postgres transaction.
    All-or-nothing: if any seat fails, the entire transaction rolls back.

    Seats are linked by a shared booking_group_id.
    """
    # Step 1 — verify all holds in Redis
    for sid in seat_ids:
        current = _redis.get(f"seat:{sid}")
        expected = f"HELD:{user_id}".encode()
        if current != expected:
            status = current.decode() if current else "FREE"
            return False, f"Seat {sid} not held by user {user_id} (state: {status})"

    # Step 2 — single Postgres transaction for all seats
    group_id = str(uuid.uuid4())
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            for sid in seat_ids:
                cur.execute(
                    "INSERT INTO bookings (seat_id, user_id, status, booking_group_id) "
                    "VALUES (%s, %s, 'CONFIRMED', %s)",
                    (sid, user_id, group_id),
                )
                outbox_payload = json.dumps({"seat_id": sid, "user_id": user_id, "group_id": group_id, "confirmed_at": time.time()})
                cur.execute(
                    "INSERT INTO outbox (event_type, aggregate_id, payload) VALUES ('SLOT_CONFIRMED', %s, %s)",
                    (str(sid), outbox_payload),
                )
        conn.commit()
    except psycopg2.IntegrityError as e:
        conn.rollback()
        return False, f"DB IntegrityError: One or more seats already confirmed ({e})"
    except Exception as e:
        conn.rollback()
        return False, f"DB Error: {e}"
    finally:
        conn.close()

    # Step 3 — promote all seats to CONFIRMED in Redis
    for sid in seat_ids:
        r = get_redis_client()
        r.set(f"seat:{sid}", "CONFIRMED")

    SLOTGUARD_CONFIRMS_TOTAL.labels(status="success").inc()
    return True, f"All {len(seat_ids)} seats confirmed (group: {group_id})"


def release_holds(
    seat_ids: List[int], user_id: int, offer_waitlist: bool = True
) -> Tuple[int, int]:
    """
    Release holds on multiple seats. Returns (released_count, failed_count).
    Optionally offers each released seat to the next waitlisted user.
    """
    released = 0
    failed = 0
    for sid in seat_ids:
        if release_hold(sid, user_id, offer_waitlist=offer_waitlist):
            released += 1
        else:
            failed += 1
    return released, failed
