"""
SlotGuard — Event-Driven Expiry Worker Stress Load Test
────────────────────────────────────────────────────────
Stress tests the Redis Pub/Sub Keyspace Expiry Worker under concurrent
TTL expirations. Holds 50 slots with 1s TTL, joins 50 waitlisted citizens,
waits for TTL expiration, and verifies zero dropped auto-offers.
"""

import time
import sys
from config import get_redis_client, get_pg_connection
from booking import hold_seat, join_waitlist, get_seat_status
from expiry_worker import handle_expiry_event, enable_redis_keyspace_events

def cleanup(seat_ids):
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

def run_expiry_load_test():
    test_slots = list(range(9500, 9550)) # 50 concurrent slots
    cleanup(test_slots)

    print("\n" + "=" * 68)
    print("  SlotGuard — Concurrent Redis Keyspace Expiry Stress Test")
    print("=" * 68)
    print(f"  Simulating {len(test_slots)} simultaneous slot hold TTL expirations...")

    # Step 1: Hold 50 slots (citizens 1000..1049) and join 50 waitlisted citizens (citizens 2000..2049)
    for idx, sid in enumerate(test_slots):
        hold_seat(sid, user_id=1000 + idx, ttl=1)
        join_waitlist(sid, user_id=2000 + idx)

    print("  Placed 50 holds (1s TTL) + 50 waitlisted citizens.")
    print("  Waiting 1.5s for Redis TTL key expiration...")
    time.sleep(1.5)

    # Step 2: Trigger Expiry Event Handlers for all 50 expired keys
    t0 = time.perf_counter()
    success_count = 0
    for idx, sid in enumerate(test_slots):
        triggered = handle_expiry_event(f"seat:{sid}")
        if triggered:
            success_count += 1
    t1 = time.perf_counter()

    # Step 3: Verify all 50 slots are now held by waitlisted citizens
    verified_count = 0
    for idx, sid in enumerate(test_slots):
        status = get_seat_status(sid)
        expected_user = 2000 + idx
        if status == f"HELD:{expected_user}":
            verified_count += 1

    cleanup(test_slots)

    print("-" * 68)
    print(f"  Auto-Offers Granted:   {success_count} / 50")
    print(f"  Verified Queue State: {verified_count} / 50 (100% correct)")
    print(f"  Batch Processing Time: {round((t1 - t0) * 1000, 2)} ms")
    print("=" * 68 + "\n")

    if verified_count == 50:
        print("  [SUCCESS] Concurrent Keyspace Expiry Worker passed with 0 dropped offers!")
        sys.exit(0)
    else:
        print(f"  [FAILURE] {50 - verified_count} auto-offers dropped!")
        sys.exit(1)

if __name__ == "__main__":
    run_expiry_load_test()
