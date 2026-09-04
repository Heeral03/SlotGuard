#!/usr/bin/env python3
"""
Hold & Confirm — Correctness Test Suite
────────────────────────────────────────
Runs the 5 manual verification steps from the spec, plus a bonus
release-hold test. Uses a SHORT TTL (3 s) so the expiry check
completes quickly.

Usage:
    python3 test_booking.py
"""

import sys
import time

import redis as _redis_mod

from config import get_redis_client, get_pg_connection
from booking import hold_seat, confirm_booking, get_seat_status, release_hold

# ── Helpers ──────────────────────────────────────────────────────────────

_pass = 0
_fail = 0

def check(label: str, got, expected):
    global _pass, _fail
    ok = got == expected
    tag = "\033[32mPASS\033[0m" if ok else "\033[31mFAIL\033[0m"
    print(f"  [{tag}] {label}")
    if not ok:
        print(f"         expected: {expected!r}")
        print(f"              got: {got!r}")
        _fail += 1
    else:
        _pass += 1


def cleanup(redis_client, seat_ids: list[int]):
    """Remove test keys from Redis and test rows from Postgres."""
    for sid in seat_ids:
        redis_client.delete(f"seat:{sid}")

    conn = get_pg_connection()
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM bookings WHERE seat_id = ANY(%s)",
            (seat_ids,),
        )
    conn.commit()
    conn.close()


# ── Tests ────────────────────────────────────────────────────────────────

SHORT_TTL = 3  # seconds (for the expiry test)

def test_hold_and_confirm():
    """Tests 1-4 from the spec."""
    print("\n── Test Group 1: Hold → Conflict → Confirm → Re-confirm ──")

    # 1. hold_seat(100, user=1)  →  True
    result = hold_seat(100, 1)
    check("hold_seat(100, user=1) → True", result, True)

    # 2. hold_seat(100, user=2)  →  False (already held)
    result = hold_seat(100, 2)
    check("hold_seat(100, user=2) → False  (already held)", result, False)

    # Status should be HELD:1
    status = get_seat_status(100)
    check("seat 100 status → HELD:1", status, "HELD:1")

    # 3. confirm_booking(100, user=1)  →  True
    ok, msg, _ = confirm_booking(100, 1)
    check("confirm_booking(100, user=1) → True", ok, True)

    # Status should be CONFIRMED
    status = get_seat_status(100)
    check("seat 100 status → CONFIRMED", status, "CONFIRMED")

    # 4. confirm_booking(100, user=1) again  →  False
    ok, msg, _ = confirm_booking(100, 1)
    check("confirm_booking(100, user=1) again → False", ok, False)
    print(f"         reason: {msg}")


def test_ttl_expiry():
    """Test 5: hold expires, seat becomes available again."""
    print(f"\n── Test Group 2: TTL Expiry ({SHORT_TTL}s) ──")

    # Hold seat 200 with short TTL
    result = hold_seat(200, 10, ttl=SHORT_TTL)
    check("hold_seat(200, user=10, ttl=3s) → True", result, True)

    status = get_seat_status(200)
    check("seat 200 status → HELD:10", status, "HELD:10")

    # Wait for TTL to expire
    print(f"  ... waiting {SHORT_TTL + 1}s for TTL to expire ...")
    time.sleep(SHORT_TTL + 1)

    # Seat should be FREE now
    status = get_seat_status(200)
    check("seat 200 status → FREE (expired)", status, "FREE")

    # Another user can now hold it
    result = hold_seat(200, 20, ttl=SHORT_TTL)
    check("hold_seat(200, user=20) → True  (after expiry)", result, True)


def test_release_hold():
    """Bonus: manual release of a hold."""
    print("\n── Test Group 3: Manual Release ──")

    hold_seat(300, 50, ttl=60)
    check("hold_seat(300, user=50) → True", True, True)

    # Wrong user can't release
    result = release_hold(300, 99)
    check("release_hold(300, user=99) → False", result, False)

    # Correct user can release
    result = release_hold(300, 50)
    check("release_hold(300, user=50) → True", result, True)

    status = get_seat_status(300)
    check("seat 300 status → FREE", status, "FREE")


def test_postgres_backstop():
    """Verify the Postgres unique index catches double-confirms."""
    print("\n── Test Group 4: Postgres Backstop ──")

    # Hold and confirm seat 400 normally
    hold_seat(400, 60)
    ok, _, _ = confirm_booking(400, 60)
    check("confirm_booking(400, user=60) → True", ok, True)

    # Now manually force a Redis state to simulate a race condition:
    # set the key to HELD:70 even though Postgres already has a CONFIRMED row
    r = get_redis_client()
    r.set("seat:400", "HELD:70")

    ok, msg, _ = confirm_booking(400, 70)
    check("confirm_booking(400, user=70) → False  (Postgres backstop)", ok, False)
    print(f"         reason: {msg}")


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Hold & Confirm — Correctness Tests")
    print("=" * 60)

    r = get_redis_client()
    test_seats = [100, 200, 300, 400]

    # Clean slate
    cleanup(r, test_seats)

    try:
        test_hold_and_confirm()
        test_ttl_expiry()
        test_release_hold()
        test_postgres_backstop()
    finally:
        # Clean up after tests
        cleanup(r, test_seats)

    print("\n" + "=" * 60)
    print(f"  Results: {_pass} passed, {_fail} failed")
    print("=" * 60)

    sys.exit(1 if _fail else 0)


if __name__ == "__main__":
    main()
