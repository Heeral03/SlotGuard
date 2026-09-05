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

from config import get_redis_client, get_pg_conn, get_pg_connection, HOLD_TTL_SECONDS
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
_confirm_check_script = _redis.register_script(_load_lua("confirm_check.lua"))
_scan_expired_script = _redis.register_script(_load_lua("scan_expired_holds.lua"))


# ═══════════════════════════════════════════════════════════════════════
#  SINGLE-SEAT HOLD & CONFIRM (Atomic check-and-set + Pooled DB)
# ═══════════════════════════════════════════════════════════════════════

def hold_seat(seat_id: int, user_id: int, ttl: int | None = None) -> bool:
    """Attempt to place a temporary hold on a single seat."""
    ttl = ttl or HOLD_TTL_SECONDS
    result = _hold_script(keys=[f"seat:{seat_id}"], args=[user_id, ttl])
    if result == 1:
        SLOTGUARD_HOLDS_TOTAL.labels(status="success").inc()
        return True
    SLOTGUARD_HOLDS_TOTAL.labels(status="conflict").inc()
    return False


def confirm_booking(
    seat_id: int, user_id: int, profile: bool = False,
    booking_group_id: str | None = None,
) -> Tuple[bool, str, Optional[dict]]:
    """
    Confirm a previously held seat atomically without TOCTOU race conditions.
    Uses pooled DB connections for maximum HTTP throughput.

    Returns (success, message, timings).
    """
    timings: dict = {} if profile else None
    key = f"seat:{seat_id}"

    # Step 1 — Atomic Redis Check & State Lock (Prevents TTL expiration during DB write)
    t0 = time.perf_counter() if profile else 0
    check_ok = _confirm_check_script(keys=[key], args=[user_id])
    if profile:
        timings["redis_check_s"] = time.perf_counter() - t0

    if check_ok == 0:
        current = _redis.get(key)
        status_msg = current.decode() if current else "FREE/EXPIRED"
        return False, f"Hold invalid or expired (current state: {status_msg})", timings

    # Step 2 — Pooled Postgres ACID Insert + Transactional Outbox
    t1 = time.perf_counter() if profile else 0
    try:
        with get_pg_conn() as conn:
            if profile:
                timings["pg_conn_acquire_s"] = time.perf_counter() - t1

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
        # Revert Redis state if Postgres unique constraint catches a duplicate
        _redis.set(key, f"HELD:{user_id}")
        return False, "Seat already confirmed (Postgres constraint caught race)", timings
    except Exception as exc:
        _redis.set(key, f"HELD:{user_id}")
        return False, f"Database error: {exc}", timings

    # Step 3 — Promote state in Redis
    t3 = time.perf_counter() if profile else 0
    _redis.set(key, "CONFIRMED")
    if profile:
        timings["redis_promote_s"] = time.perf_counter() - t3

    SLOTGUARD_CONFIRMS_TOTAL.labels(status="success").inc()
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
    _redis.zrem("holds:ttl", str(seat_id))

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
    added = _redis.zadd(wl_key, {str(user_id): time.time()}, nx=True)
    if added:
        SLOTGUARD_WAITLIST_JOIN_TOTAL.inc()
        depth = _redis.zcard(wl_key)
        SLOTGUARD_WAITLIST_DEPTH.labels(slot_id=str(seat_id)).set(depth)
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
    if result == 0 or result is None:
        return None
    return int(result)


# ═══════════════════════════════════════════════════════════════════════
#  MULTI-SEAT ATOMIC BOOKING (all-or-nothing)
# ═══════════════════════════════════════════════════════════════════════

def hold_seats(
    seat_ids: List[int], user_id: int, ttl: int | None = None,
) -> Tuple[bool, str]:
    """
    Atomically hold ALL requested seats, or NONE.
    """
    ttl = ttl or HOLD_TTL_SECONDS
    keys = [f"seat:{sid}" for sid in seat_ids]

    result = _hold_multi_script(
        keys=keys,
        args=[user_id, ttl, len(seat_ids)],
    )

    status_code = result[0]
    detail = result[1].decode() if isinstance(result[1], bytes) else str(result[1])

    if status_code == 1:
        SLOTGUARD_HOLDS_TOTAL.labels(status="success").inc()
        return True, f"All {len(seat_ids)} seats held"
    else:
        SLOTGUARD_HOLDS_TOTAL.labels(status="conflict").inc()
        return False, f"Some seats taken: {detail}"


def confirm_seats(
    seat_ids: List[int], user_id: int,
) -> Tuple[bool, str]:
    """
    Confirm ALL held seats in a single Postgres transaction using pooled connections.
    All-or-nothing atomic verification.
    """
    # Step 1 — Verify and lock state for all seats atomically in Redis
    for sid in seat_ids:
        check_ok = _confirm_check_script(keys=[f"seat:{sid}"], args=[user_id])
        if check_ok == 0:
            return False, f"Seat {sid} not held by user {user_id} or expired"

    # Step 2 — Single Postgres transaction using pool
    group_id = str(uuid.uuid4())
    try:
        with get_pg_conn() as conn:
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
        for sid in seat_ids:
            _redis.set(f"seat:{sid}", f"HELD:{user_id}")
        return False, f"DB IntegrityError: One or more seats already confirmed ({e})"
    except Exception as e:
        for sid in seat_ids:
            _redis.set(f"seat:{sid}", f"HELD:{user_id}")
        return False, f"DB Error: {e}"

    # Step 3 — Promote all seats to CONFIRMED in Redis
    for sid in seat_ids:
        _redis.set(f"seat:{sid}", "CONFIRMED")

    SLOTGUARD_CONFIRMS_TOTAL.labels(status="success").inc()
    return True, f"All {len(seat_ids)} seats confirmed (group: {group_id})"


def release_holds(
    seat_ids: List[int], user_id: int, offer_waitlist: bool = True
) -> Tuple[int, int]:
    """Release holds on multiple seats."""
    released = 0
    failed = 0
    for sid in seat_ids:
        if release_hold(sid, user_id, offer_waitlist=offer_waitlist):
            released += 1
        else:
            failed += 1
    return released, failed
