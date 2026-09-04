"""
Multi-Seat Atomic Booking Suite
──────────────────────────────────
Tests for all-or-nothing multi-seat holds and transactional multi-seat confirmations.
"""

import sys
from config import get_redis_client, get_pg_connection
from booking import (
    hold_seats,
    confirm_seats,
    release_holds,
    get_seat_status,
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
    seats = [600, 601, 602, 603]
    cleanup(seats)

    print("\n" + "=" * 60)
    print("  Multi-Seat Atomic Booking — Correctness Tests")
    print("=" * 60)

    # ── Test 1: Atomic Hold Both Seats ──
    print("\n── Test Group 1: Atomic Multi-Hold Success ──")
    ok1, msg1 = hold_seats([600, 601], user_id=1)
    check("hold_seats([600, 601], user=1) → True", ok1, True)
    check("seat 600 status → HELD:1", get_seat_status(600), "HELD:1")
    check("seat 601 status → HELD:1", get_seat_status(601), "HELD:1")

    # ── Test 2: All-or-Nothing Partial Conflict ──
    print("\n── Test Group 2: All-or-Nothing Partial Conflict ──")
    # Seat 601 is taken. Attempt to hold 601 and 602.
    ok2, msg2 = hold_seats([601, 602], user_id=2)
    check("hold_seats([601, 602], user=2) → False", ok2, False)
    check("seat 601 remains HELD:1", get_seat_status(601), "HELD:1")
    check("seat 602 remains FREE (not held partially)", get_seat_status(602), "FREE")

    # ── Test 3: Transactional Multi-Seat Confirm ──
    print("\n── Test Group 3: Transactional Multi-Seat Confirm ──")
    ok3, msg3 = confirm_seats([600, 601], user_id=1)
    check("confirm_seats([600, 601], user=1) → True", ok3, True)
    check("seat 600 status → CONFIRMED", get_seat_status(600), "CONFIRMED")
    check("seat 601 status → CONFIRMED", get_seat_status(601), "CONFIRMED")

    # Verify database has linked UUID booking_group_id
    conn = get_pg_connection()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT seat_id, booking_group_id FROM bookings "
            "WHERE seat_id IN (600, 601) AND status = 'CONFIRMED'"
        )
        rows = cur.fetchall()
        check("Postgres confirmed row count", len(rows), 2)
        group_ids = {str(r[1]) for r in rows}
        check("Shared non-null booking_group_id", len(group_ids), 1)
        check("booking_group_id is valid UUID string", len(list(group_ids)[0]), 36)
    conn.close()

    # ── Test 4: Multi-Seat Release ──
    print("\n── Test Group 4: Multi-Seat Release ──")
    hold_seats([602, 603], user_id=5)
    rel_cnt, fail_cnt = release_holds([602, 603], user_id=5)
    check("release_holds([602, 603], user=5) -> (2, 0)", (rel_cnt, fail_cnt), (2, 0))
    check("seat 602 status → FREE", get_seat_status(602), "FREE")
    check("seat 603 status → FREE", get_seat_status(603), "FREE")

    # ── Cleanup ──
    cleanup(seats)

    print("\n" + "=" * 60)
    print(f"  Results: {passed_count} passed, {failed_count} failed")
    print("=" * 60 + "\n")

    sys.exit(0 if failed_count == 0 else 1)


if __name__ == "__main__":
    run_tests()
