"""
SlotGuard — Distributed Systems Infrastructure Test Suite
──────────────────────────────────────────────────────────
Automated tests for:
1. Transactional Outbox Pattern & SKIP LOCKED CDC Worker
2. Redis Keyspace Expiry Event Worker (TTL Expiration Auto-Offer)
3. Prometheus Metrics Scraping Endpoint
"""

import sys
import time
from fastapi.testclient import TestClient

from app import app
from config import get_redis_client, get_pg_connection
from booking import hold_seat, confirm_booking, join_waitlist, get_seat_status
from outbox_worker import process_outbox_batch
from expiry_worker import handle_expiry_event, enable_redis_keyspace_events

client = TestClient(app)

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
        cur.execute("DELETE FROM outbox WHERE aggregate_id = ANY(%s)", ([str(s) for s in seat_ids],))
    conn.commit()
    conn.close()


def run_tests():
    test_slots = [800, 801, 802]
    cleanup(test_slots)

    print("\n" + "=" * 68)
    print("  SlotGuard Distributed Infrastructure — Verification Suite")
    print("=" * 68)

    # ── Test 1: Transactional Outbox & CDC Worker ──
    print("\n── Test Group 1: Transactional Outbox & CDC Poller ──")
    hold_seat(800, user_id=500)
    ok, msg, _ = confirm_booking(800, user_id=500)
    check("Booking confirmation succeeded", ok, True)

    # Check pending row in outbox
    conn = get_pg_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT status FROM outbox WHERE aggregate_id = '800'")
        row = cur.fetchone()
    conn.close()

    check("Outbox record created atomically in DB (status=PENDING)", row[0] if row else None, "PENDING")

    # Run CDC Worker batch
    processed_count = process_outbox_batch(batch_size=10)
    check("CDC Outbox Worker processed batch count > 0", processed_count > 0, True)

    # Verify updated status
    conn = get_pg_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT status FROM outbox WHERE aggregate_id = '800'")
        row_after = cur.fetchone()
    conn.close()

    check("Outbox record updated to PROCESSED by CDC worker", row_after[0] if row_after else None, "PROCESSED")

    # ── Test 2: Event-Driven Keyspace TTL Expiry Auto-Offer ──
    print("\n── Test Group 2: Event-Driven Keyspace TTL Expiry Auto-Offer ──")
    # Hold slot 801 for citizen 600 (ttl=120), waitlisted by citizen 601
    hold_ok = hold_seat(801, user_id=600, ttl=120)
    check("Initial hold on slot 801 granted to citizen 600", hold_ok, True)

    wl_ok, wl_msg = join_waitlist(801, user_id=601)
    check("Citizen 601 join waitlist succeeded", wl_ok, True)

    # Simulate TTL key expiration (Redis deletes key 'seat:801' and publishes expired event)
    r = get_redis_client()
    r.delete("seat:801")

    # Trigger Keyspace Expiry Handler for 'seat:801'
    triggered = handle_expiry_event("seat:801")
    check("Keyspace expiry event handler triggered auto-offer", triggered, True)

    # Verify slot 801 is now held by waitlisted citizen 601
    new_status = get_seat_status(801)
    check("Slot 801 auto-offered to waitlisted citizen 601", new_status, "HELD:601")



    # ── Test 3: Prometheus Metrics Endpoint ──
    print("\n── Test Group 3: Prometheus Observability Endpoint ──")
    res_m = client.get("/metrics")
    check("GET /metrics status code -> 200", res_m.status_code, 200)
    metrics_text = res_m.text
    check("Metrics contains 'slotguard_holds_total'", "slotguard_holds_total" in metrics_text, True)
    check("Metrics contains 'slotguard_confirms_total'", "slotguard_confirms_total" in metrics_text, True)
    check("Metrics contains 'slotguard_outbox_events_total'", "slotguard_outbox_events_total" in metrics_text, True)

    cleanup(test_slots)

    print("\n" + "=" * 68)
    print(f"  Results: {passed_count} passed, {failed_count} failed")
    print("=" * 68 + "\n")

    sys.exit(0 if failed_count == 0 else 1)


if __name__ == "__main__":
    run_tests()
