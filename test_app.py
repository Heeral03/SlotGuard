"""
SlotGuard FastAPI Web Service Test Suite
──────────────────────────────────────────
Comprehensive endpoint tests using FastAPI TestClient.
"""

import sys
from fastapi.testclient import TestClient

from app import app
from config import get_redis_client, get_pg_connection

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
    conn.commit()
    conn.close()


def run_tests():
    test_slots = [700, 701, 702]
    cleanup(test_slots)

    print("\n" + "=" * 60)
    print("  SlotGuard FastAPI — Integration Test Suite")
    print("=" * 60)

    # 1. Health check
    print("\n── Test Group 1: System Health Endpoint ──")
    res = client.get("/health")
    check("GET /health status code -> 200", res.status_code, 200)
    check("Health status -> HEALTHY", res.json().get("status"), "HEALTHY")

    # 2. Slot Status & Single Hold
    print("\n── Test Group 2: Single Slot Hold & Conflict ──")
    res_st = client.get("/api/v1/slots/700/status")
    check("GET slot 700 initial status -> FREE", res_st.json().get("status"), "FREE")

    res_h1 = client.post("/api/v1/slots/700/hold", json={"citizen_id": 100, "ttl_seconds": 120})
    check("Hold slot 700 (citizen 100) -> 200 OK", res_h1.status_code, 200)
    check("Response status -> HELD:100", res_h1.json().get("status"), "HELD:100")

    # Conflict
    res_h2 = client.post("/api/v1/slots/700/hold", json={"citizen_id": 101, "ttl_seconds": 120})
    check("Hold slot 700 (citizen 101) conflict -> 409", res_h2.status_code, 409)

    # 3. Fair Waitlist Queue
    print("\n── Test Group 3: Waitlist Queue Operations ──")
    res_w1 = client.post("/api/v1/slots/700/waitlist/join", json={"citizen_id": 101})
    check("Citizen 101 join waitlist -> 200 OK", res_w1.status_code, 200)

    res_wq = client.get("/api/v1/slots/700/waitlist")
    check("Waitlist query -> queue=[101]", res_wq.json().get("queue"), [101])

    # 4. Release Hold + Auto-Offer to Waitlisted Citizen
    print("\n── Test Group 4: Release & Auto-Offer ──")
    res_rel = client.post("/api/v1/slots/700/release", json={"citizen_id": 100, "offer_waitlist": True})
    check("Citizen 100 release hold -> 200 OK", res_rel.status_code, 200)

    # Status should now be HELD:101 (auto-offered to waitlisted citizen 101)
    res_st_after = client.get("/api/v1/slots/700/status")
    check("Slot 700 auto-offered status -> HELD:101", res_st_after.json().get("status"), "HELD:101")

    # Confirm by Citizen 101
    res_c101 = client.post("/api/v1/slots/700/confirm", json={"citizen_id": 101})
    check("Citizen 101 confirm slot 700 -> 200 OK", res_c101.status_code, 200)
    check("Slot 700 confirmed status -> CONFIRMED", res_c101.json().get("status"), "CONFIRMED")

    # 5. Multi-Slot Family Appointments
    print("\n── Test Group 5: Multi-Slot Family Appointments ──")
    res_mh = client.post("/api/v1/slots/hold-multi", json={"slot_ids": [701, 702], "citizen_id": 200, "ttl_seconds": 120})
    check("Multi-hold slots [701, 702] -> 200 OK", res_mh.status_code, 200)

    res_mc = client.post("/api/v1/slots/confirm-multi", json={"slot_ids": [701, 702], "citizen_id": 200})
    check("Multi-confirm slots [701, 702] -> 200 OK", res_mc.status_code, 200)

    cleanup(test_slots)

    print("\n" + "=" * 60)
    print(f"  Results: {passed_count} passed, {failed_count} failed")
    print("=" * 60 + "\n")

    sys.exit(0 if failed_count == 0 else 1)


if __name__ == "__main__":
    run_tests()
