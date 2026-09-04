"""
Waitlist Queue Correctness Suite
──────────────────────────────────
Tests for fair waitlist queuing (FIFO ordering, duplicate prevention,
auto-offer on release, leave waitlist).
"""

import sys
import time
from config import get_redis_client, get_pg_connection
from booking import (
    hold_seat,
    get_seat_status,
    release_hold,
    join_waitlist,
    leave_waitlist,
    get_waitlist,
    confirm_booking,
)

passed_count = 0
failed_count = 0


def check(description: str, actual, expected):
    global passed_count, failed_count
    if actual == expected:
        print(f"  [PASS] {description}")
        passed_count += 1
    else:
        print(f"  [FAIL] {description}")
        print(f"         expected: {expected}")
        print(f"         got:      {actual}")
        failed_count += 1


def cleanup(seat_ids: list[int]):
    r = get_redis_client()
    for sid in seat_ids:
        r.delete(f"seat:{sid}")
        r.delete(f"waitlist:{sid}")

    conn = get_pg_connection()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM bookings WHERE seat_id = ANY(%s)", (seat_ids,))
    conn.commit()
    conn.close()


def run_tests():
    seats = [500, 501, 502]
    cleanup(seats)

    print("\n" + "=" * 60)
    print("  Waitlist Queue — Correctness Tests")
    print("=" * 60)

    # ── Test 1: Cannot join waitlist if seat is FREE ──
    print("\n── Test Group 1: Join Free Seat ──")
    ok, msg = join_waitlist(500, user_id=10)
    check("join_waitlist on FREE seat → False", ok, False)

    # ── Test 2: Join waitlist on HELD seat & FIFO ordering ──
    print("\n── Test Group 2: FIFO Waitlist Ordering ──")
    hold_seat(500, user_id=1)
    check("hold_seat(500, user=1) → True", get_seat_status(500), "HELD:1")

    ok1, _ = join_waitlist(500, user_id=10)
    check("user=10 joins waitlist → True", ok1, True)

    # small sleep to ensure distinct timestamps
    time.sleep(0.01)

    ok2, _ = join_waitlist(500, user_id=11)
    check("user=11 joins waitlist → True", ok2, True)

    wl = get_waitlist(500)
    check("Waitlist FIFO order → [10, 11]", wl, [10, 11])

    # Duplicate join check
    ok_dup, _ = join_waitlist(500, user_id=10)
    check("user=10 re-joining waitlist → False", ok_dup, False)
    check("Waitlist still [10, 11]", get_waitlist(500), [10, 11])

    # ── Test 3: Leave Waitlist ──
    print("\n── Test Group 3: Leave Waitlist ──")
    left = leave_waitlist(500, user_id=11)
    check("user=11 leave_waitlist → True", left, True)
    check("Waitlist after leave → [10]", get_waitlist(500), [10])

    # Re-add user 11
    join_waitlist(500, user_id=11)

    # ── Test 4: Auto-Offer on Release ──
    print("\n── Test Group 4: Auto-Offer on Release ──")
    # Current status: seat 500 HELD:1, waitlist: [10, 11]
    rel = release_hold(500, user_id=1, offer_waitlist=True)
    check("release_hold(500, user=1) → True", rel, True)

    # Seat should immediately be HELD:10 (auto-offered to user 10)
    status_after_release = get_seat_status(500)
    check("seat 500 status after release → HELD:10", status_after_release, "HELD:10")
    check("user 10 popped from waitlist → [11] remains", get_waitlist(500), [11])

    # Confirm user 10's booking
    ok_conf, _, _ = confirm_booking(500, user_id=10)
    check("confirm_booking(500, user=10) → True", ok_conf, True)
    check("seat 500 status → CONFIRMED", get_seat_status(500), "CONFIRMED")

    # ── Cleanup ──
    cleanup(seats)

    print("\n" + "=" * 60)
    print(f"  Results: {passed_count} passed, {failed_count} failed")
    print("=" * 60 + "\n")

    sys.exit(0 if failed_count == 0 else 1)


if __name__ == "__main__":
    run_tests()
