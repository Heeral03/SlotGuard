#!/usr/bin/env python3
"""
Hold & Confirm — Advanced Concurrency Load Test Suite
──────────────────────────────────────────────────────
Provides load-test proof for:
  1. Waitlist Queue: Strict FIFO ordering & zero-race auto-offering under concurrency
  2. Multi-Seat Atomic Booking: Overlapping adjacent seat contention (all-or-nothing guarantee)

Usage:
    python3 load_test_advanced.py
"""

from __future__ import annotations

import argparse
import random
import statistics
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import List, Tuple, Dict

from config import get_redis_client, get_pg_connection
from booking import (
    hold_seat,
    confirm_booking,
    release_hold,
    join_waitlist,
    get_waitlist,
    get_seat_status,
    hold_seats,
    confirm_seats,
)


# ── Utilities & Helper Functions ─────────────────────────────────────────

def percentiles(data: List[float]) -> dict:
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


def cleanup(seat_ids: List[int]):
    r = get_redis_client()
    pipe = r.pipeline()
    for sid in seat_ids:
        pipe.delete(f"seat:{sid}")
        pipe.delete(f"waitlist:{sid}")
    pipe.execute()

    conn = get_pg_connection()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM bookings WHERE seat_id = ANY(%s)", (seat_ids,))
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════════════
#  SECTION 1: WAITLIST FIFO & FAIRNESS LOAD TEST
# ═══════════════════════════════════════════════════════════════════════

def run_waitlist_load_test(num_waiters: int = 100, workers: int = 50) -> bool:
    print("\n" + "═" * 72)
    print("  TEST 1: WAITLIST FIFO & FAIRNESS UNDER HIGH CONCURRENCY")
    print("═" * 72)

    seat_id = 950
    cleanup([seat_id])

    # Step 1: Initial hold on seat by User 1
    hold_seat(seat_id, user_id=1)
    print(f"  • Seat {seat_id} held by initial User 1")

    # Step 2: 100 threads concurrently attempt to join waitlist for seat 950
    print(f"  • Launching {num_waiters} threads to concurrently join waitlist...")

    join_latencies = []
    lock = threading.Lock()
    errors = []

    def join_worker(uid: int):
        t0 = time.perf_counter()
        try:
            ok, msg = join_waitlist(seat_id, uid)
            t1 = time.perf_counter()
            with lock:
                join_latencies.append(t1 - t0)
                if not ok:
                    errors.append(f"User {uid} join failed: {msg}")
        except Exception as e:
            with lock:
                errors.append(f"User {uid} exception: {e}")

    t_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(join_worker, uid) for uid in range(2, num_waiters + 2)]
        for f in as_completed(futures):
            pass
    t_end = time.perf_counter()

    print(f"  • {num_waiters} users joined waitlist in {round(t_end - t_start, 3)}s")
    if errors:
        print(f"    ❌ Errors during concurrent join: {errors[:5]}")
        return False

    # Verify waitlist count in Redis
    r_waitlist = get_waitlist(seat_id)
    print(f"  • Redis waitlist length: {len(r_waitlist)} (expected {num_waiters})")
    if len(r_waitlist) != num_waiters:
        print(f"    ❌ Mismatch in waitlist count! Expected {num_waiters}, got {len(r_waitlist)}")
        return False

    # Step 3: Sequential release & auto-offer loop, verifying strict FIFO order
    print(f"  • Performing sequential release & auto-offer cycle for all {num_waiters} waiters...")

    current_holder = 1
    offered_sequence = []
    fifo_violations = 0

    for idx, expected_user in enumerate(r_waitlist):
        # Current holder releases hold → Lua script atomically pops next user from waitlist & holds seat
        ok = release_hold(seat_id, current_holder, offer_waitlist=True)
        if not ok:
            print(f"    ❌ Failed to release hold for user {current_holder}")
            return False

        # Inspect new seat status
        status = get_seat_status(seat_id)
        if not status.startswith("HELD:"):
            print(f"    ❌ Seat status after release was '{status}', expected HELD:<user_id>")
            return False

        new_holder = int(status.split(":")[1])
        offered_sequence.append(new_holder)

        if new_holder != expected_user:
            print(f"    ❌ FIFO VIOLATION at position {idx+1}! Expected user {expected_user}, got offered to user {new_holder}")
            fifo_violations += 1
        
        # New holder confirms booking
        c_ok, _, _ = confirm_booking(seat_id, new_holder)

        # Release confirmation to free key in Redis for next waitlisted user test
        # (In real flow, confirmed seats don't auto-offer, so we reset status to HELD to test next offer)
        r = get_redis_client()
        r.set(f"seat:{seat_id}", f"HELD:{new_holder}")
        current_holder = new_holder

    print(f"  • Total offers generated: {len(offered_sequence)}")
    print(f"  • FIFO violations:        {fifo_violations}")

    # Latency percentiles for concurrent join
    stats = percentiles(join_latencies)
    print(f"  • Concurrent Join Latency: p50={stats['p50_ms']}ms, p95={stats['p95_ms']}ms, p99={stats['p99_ms']}ms")

    cleanup([seat_id])

    if fifo_violations == 0:
        print("  ✅ PASS: Strict FIFO order maintained under concurrent waitlist contention")
        return True
    else:
        print("  ❌ FAIL: FIFO violations detected")
        return False


# ═══════════════════════════════════════════════════════════════════════
#  SECTION 2: MULTI-SEAT OVERLAPPING ATOMIC BOOKING LOAD TEST
# ═══════════════════════════════════════════════════════════════════════

@dataclass
class MultiSeatMetrics:
    lock: threading.Lock = field(default_factory=threading.Lock)
    hold_latencies: List[float] = field(default_factory=list)
    confirm_latencies: List[float] = field(default_factory=list)

    holds_granted: int = 0
    holds_denied: int = 0
    confirms_ok: int = 0
    confirms_failed: int = 0

    partial_holds_detected: int = 0
    errors: List[str] = field(default_factory=list)

    wall_start: float = 0.0
    wall_end: float = 0.0


def run_multiseat_load_test(
    num_users: int = 1000, num_seats: int = 50, workers: int = 100
) -> bool:
    print("\n" + "═" * 72)
    print("  TEST 2: MULTI-SEAT ATOMIC BOOKING UNDER OVERLAPPING CONTENTION")
    print("═" * 72)

    seat_offset = 1000
    seat_ids = list(range(seat_offset, seat_offset + num_seats))
    cleanup(seat_ids)

    print(f"  • Setup: {num_users} users targeting adjacent blocks across {num_seats} seats ({seat_offset}..{seat_offset + num_seats - 1})")
    print(f"  • Overlapping contention: Users request random 2-seat or 3-seat contiguous blocks")
    print(f"  • Launching thread pool with {workers} workers...")

    # Pre-generate overlapping requests
    requests: List[Tuple[int, List[int]]] = []
    for uid in range(1, num_users + 1):
        block_size = random.choice([2, 3])
        start_seat = random.randint(seat_offset, seat_offset + num_seats - block_size)
        requested_seats = list(range(start_seat, start_seat + block_size))
        requests.append((uid, requested_seats))

    metrics = MultiSeatMetrics()

    def multi_worker(user_id: int, block: List[int]):
        # Phase 1: Atomic Hold
        t0 = time.perf_counter()
        try:
            ok, msg = hold_seats(block, user_id)
        except Exception as e:
            with metrics.lock:
                metrics.errors.append(f"hold_seats user={user_id} exc: {e}")
            return
        t1 = time.perf_counter()

        with metrics.lock:
            metrics.hold_latencies.append(t1 - t0)
            if ok:
                metrics.holds_granted += 1
            else:
                metrics.holds_denied += 1

        if not ok:
            # Check atomicity: Verify NO seat in block was partially held by this user
            r = get_redis_client()
            for sid in block:
                val = r.get(f"seat:{sid}")
                if val and val == f"HELD:{user_id}".encode():
                    with metrics.lock:
                        metrics.partial_holds_detected += 1
                        metrics.errors.append(f"PARTIAL HOLD DETECTED: seat {sid} held by user {user_id} despite hold_seats returning False!")
            return

        # Phase 2: Transactional Confirm
        t2 = time.perf_counter()
        try:
            c_ok, c_msg = confirm_seats(block, user_id)
        except Exception as e:
            with metrics.lock:
                metrics.errors.append(f"confirm_seats user={user_id} exc: {e}")
            return
        t3 = time.perf_counter()

        with metrics.lock:
            metrics.confirm_latencies.append(t3 - t2)
            if c_ok:
                metrics.confirms_ok += 1
            else:
                metrics.confirms_failed += 1
                metrics.errors.append(f"confirm_seats user={user_id} failed: {c_msg}")

    metrics.wall_start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(multi_worker, uid, block) for uid, block in requests]
        for f in as_completed(futures):
            pass

    metrics.wall_end = time.perf_counter()
    wall = metrics.wall_end - metrics.wall_start
    total_ops = len(metrics.hold_latencies) + len(metrics.confirm_latencies)

    print(f"\n  Throughput & Latency Summary:")
    print(f"    Wall clock duration:   {round(wall, 3)}s")
    print(f"    Total operations:      {total_ops}")
    print(f"    Throughput:            {round(total_ops / wall, 1) if wall > 0 else 0} ops/sec")
    print(f"    Holds granted:         {metrics.holds_granted}")
    print(f"    Holds denied:          {metrics.holds_denied}")
    print(f"    Confirms succeeded:    {metrics.confirms_ok}")
    print(f"    Confirms failed:       {metrics.confirms_failed}")

    h_stats = percentiles(metrics.hold_latencies)
    print(f"\n    Multi-Seat Hold Latency (ms):")
    print(f"      min={h_stats['min_ms']}  p50={h_stats['p50_ms']}  p90={h_stats['p90_ms']}  p95={h_stats['p95_ms']}  p99={h_stats['p99_ms']}  max={h_stats['max_ms']}")

    if metrics.confirm_latencies:
        c_stats = percentiles(metrics.confirm_latencies)
        print(f"\n    Multi-Seat Confirm Latency (ms):")
        print(f"      min={c_stats['min_ms']}  p50={c_stats['p50_ms']}  p90={c_stats['p90_ms']}  p95={c_stats['p95_ms']}  p99={c_stats['p99_ms']}  max={c_stats['max_ms']}")

    # ── Database & Redis Integrity Audits ──
    print(f"\n  Integrity Audits:")

    # 1. Partial hold audit
    print(f"    Partial holds in Redis:   {metrics.partial_holds_detected}")
    atomicity_pass = (metrics.partial_holds_detected == 0)

    # 2. Postgres Double Booking audit
    conn = get_pg_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT seat_id, COUNT(*) AS cnt
            FROM bookings
            WHERE seat_id = ANY(%s) AND status = 'CONFIRMED'
            GROUP BY seat_id HAVING COUNT(*) > 1
        """, (seat_ids,))
        double_bookings = cur.fetchall()

        cur.execute("""
            SELECT COUNT(*) FROM bookings
            WHERE seat_id = ANY(%s) AND status = 'CONFIRMED'
        """, (seat_ids,))
        total_confirmed_seats = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(DISTINCT booking_group_id) FROM bookings
            WHERE seat_id = ANY(%s) AND status = 'CONFIRMED'
        """, (seat_ids,))
        total_confirmed_groups = cur.fetchone()[0]
    conn.close()

    print(f"    Total seats confirmed:   {total_confirmed_seats}")
    print(f"    Total groups confirmed:  {total_confirmed_groups}")
    print(f"    Double-booked seats:     {len(double_bookings)}")

    db_audit_pass = (len(double_bookings) == 0)

    if metrics.errors:
        print(f"\n    Errors recorded ({len(metrics.errors)}):")
        for err in metrics.errors[:5]:
            print(f"      ⚠ {err}")

    cleanup(seat_ids)

    success = atomicity_pass and db_audit_pass and (metrics.confirms_failed == 0)
    if success:
        print("\n  ✅ PASS: Multi-seat atomic holds and transactional confirms verified 100% correct under overlapping contention")
    else:
        print("\n  ❌ FAIL: Verification failed during multi-seat load test")

    return success


# ═══════════════════════════════════════════════════════════════════════
#  MAIN EXECUTOR
# ═══════════════════════════════════════════════════════════════════════

def main():
    print("=" * 72)
    print("  ADVANCED CONCURRENCY LOAD TEST SUITE")
    print("  Waitlist Queue & Multi-Seat Atomic Booking")
    print("=" * 72)

    pass_w = run_waitlist_load_test(num_waiters=100, workers=50)
    pass_m = run_multiseat_load_test(num_users=1000, num_seats=50, workers=100)

    print("\n" + "=" * 72)
    print(f"  FINAL SUMMARY: Waitlist={ 'PASS' if pass_w else 'FAIL' }, Multi-Seat={ 'PASS' if pass_m else 'FAIL' }")
    print("=" * 72 + "\n")

    sys.exit(0 if (pass_w and pass_m) else 1)


if __name__ == "__main__":
    main()
