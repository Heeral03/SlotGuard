#!/usr/bin/env python3
"""
SlotGuard — FastAPI Endpoint Load Test & Benchmark
──────────────────────────────────────────────────
Evaluates REST API endpoint throughput and latency percentiles under high concurrency.
"""

from __future__ import annotations

import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi.testclient import TestClient

from app import app
from config import get_redis_client, get_pg_connection

client = TestClient(app)


def percentiles(data: list[float]) -> dict:
    if not data:
        return {"count": 0}
    s = sorted(data)
    n = len(s)
    return {
        "count": n,
        "min_ms": round(s[0] * 1000, 2),
        "p50_ms": round(s[n // 2] * 1000, 2),
        "p90_ms": round(s[int(n * 0.90)] * 1000, 2),
        "p95_ms": round(s[int(n * 0.95)] * 1000, 2),
        "p99_ms": round(s[int(n * 0.99)] * 1000, 2),
        "max_ms": round(s[-1] * 1000, 2),
        "avg_ms": round(statistics.mean(s) * 1000, 2),
    }


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


def run_api_load_test(num_citizens: int = 500, num_slots: int = 50, workers: int = 50):
    print("=" * 72)
    print("  SLOTGUARD FASTAPI WEB API — HIGH CONCURRENCY LOAD BENCHMARK")
    print("=" * 72)

    slot_offset = 2000
    slot_ids = list(range(slot_offset, slot_offset + num_slots))
    cleanup(slot_ids)

    print(f"  • Target: {num_citizens} citizens competing for {num_slots} appointment slots ({slot_offset}..{slot_offset + num_slots - 1})")
    print(f"  • Concurrent Threads: {workers}")

    hold_latencies = []
    confirm_latencies = []
    holds_granted = 0
    holds_denied = 0
    confirms_ok = 0

    def citizen_worker(cid: int):
        nonlocal holds_granted, holds_denied, confirms_ok
        # Pick a target slot (creating high contention on slot_offset..slot_offset+10)
        target_slot = slot_offset + (cid % num_slots)

        # 1. API Hold Request
        t0 = time.perf_counter()
        res_h = client.post(f"/api/v1/slots/{target_slot}/hold", json={"citizen_id": cid, "ttl_seconds": 120})
        t1 = time.perf_counter()
        hold_latencies.append(t1 - t0)

        if res_h.status_code == 200:
            holds_granted += 1

            # 2. API Confirm Request
            t2 = time.perf_counter()
            res_c = client.post(f"/api/v1/slots/{target_slot}/confirm", json={"citizen_id": cid})
            t3 = time.perf_counter()
            confirm_latencies.append(t3 - t2)

            if res_c.status_code == 200:
                confirms_ok += 1
        else:
            holds_denied += 1
            # Optional: Join Waitlist
            client.post(f"/api/v1/slots/{target_slot}/waitlist/join", json={"citizen_id": cid})

    wall_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(citizen_worker, cid) for cid in range(1, num_citizens + 1)]
        for f in as_completed(futures):
            pass
    wall_end = time.perf_counter()

    wall_duration = wall_end - wall_start
    total_ops = len(hold_latencies) + len(confirm_latencies)

    print("\n" + "─" * 72)
    print("  API THROUGHPUT & LATENCY METRICS:")
    print("─" * 72)
    print(f"    Wall Clock Duration: {round(wall_duration, 3)}s")
    print(f"    Total REST Operations: {total_ops}")
    print(f"    API Throughput:       {round(total_ops / wall_duration, 1)} ops/sec")
    print(f"    Holds Granted:        {holds_granted}")
    print(f"    Holds Denied:         {holds_denied}")
    print(f"    Confirms Succeeded:   {confirms_ok}")

    h_stats = percentiles(hold_latencies)
    print("\n    POST /api/v1/slots/{id}/hold Latency (ms):")
    print(f"      min={h_stats['min_ms']}  p50={h_stats['p50_ms']}  p90={h_stats['p90_ms']}  p95={h_stats['p95_ms']}  p99={h_stats['p99_ms']}  max={h_stats['max_ms']}")

    if confirm_latencies:
        c_stats = percentiles(confirm_latencies)
        print("\n    POST /api/v1/slots/{id}/confirm Latency (ms):")
        print(f"      min={c_stats['min_ms']}  p50={c_stats['p50_ms']}  p90={c_stats['p90_ms']}  p95={c_stats['p95_ms']}  p99={c_stats['p99_ms']}  max={c_stats['max_ms']}")

    # Verification: DB Double Booking Check
    conn = get_pg_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT seat_id, COUNT(*)
            FROM bookings
            WHERE seat_id = ANY(%s) AND status = 'CONFIRMED'
            GROUP BY seat_id HAVING COUNT(*) > 1
        """, (slot_ids,))
        double_bookings = cur.fetchall()
    conn.close()

    print(f"\n    Double-Booked Slots in Database: {len(double_bookings)}")

    cleanup(slot_ids)

    print("\n" + "=" * 72)
    if len(double_bookings) == 0:
        print("  ✅ FASTAPI BENCHMARK COMPLETE — ZERO DOUBLE BOOKINGS DEMONSTRATED")
    else:
        print("  ❌ FAIL: Double bookings detected in database audit")
    print("=" * 72 + "\n")


if __name__ == "__main__":
    run_api_load_test()
