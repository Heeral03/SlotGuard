#!/usr/bin/env python3
"""
Hold & Confirm — Load Test with Full Benchmarking
──────────────────────────────────────────────────
• Runs the Lua-based hold-and-confirm system under concurrent load
• Profiles confirm_booking into per-phase breakdown (Redis check, PG conn, PG insert+commit, Redis promote)
• Runs a naive SELECT-then-INSERT baseline under identical load to prove overselling
• Audits Postgres for double-bookings on both paths

Usage:
    python3 load_test.py                                # defaults: 10k users, 100 seats
    python3 load_test.py --users 5000 --seats 50        # custom
    python3 load_test.py --json                         # also dump JSON results
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import List, Tuple

from config import get_redis_client, get_pg_connection
from booking import hold_seat, confirm_booking
from naive_baseline import (
    setup_naive_table, cleanup_naive, naive_book_seat, audit_naive_bookings,
)


# ── Thread-safe Metrics Collector ────────────────────────────────────────

@dataclass
class Metrics:
    """Accumulates raw timing data; computes stats at the end."""

    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    hold_latencies: List[float]    = field(default_factory=list)
    confirm_latencies: List[float] = field(default_factory=list)

    # Per-phase confirm profiling (seconds, one entry per successful confirm)
    confirm_phases: List[dict]     = field(default_factory=list)

    hold_granted: int   = 0
    hold_denied: int    = 0
    confirm_ok: int     = 0
    confirm_fail: int   = 0

    errors: List[str]   = field(default_factory=list)

    wall_start: float   = 0.0
    wall_end: float     = 0.0

    def record_hold(self, latency: float, granted: bool):
        with self._lock:
            self.hold_latencies.append(latency)
            if granted:
                self.hold_granted += 1
            else:
                self.hold_denied += 1

    def record_confirm(self, latency: float, ok: bool, phases: dict = None):
        with self._lock:
            self.confirm_latencies.append(latency)
            if ok:
                self.confirm_ok += 1
                if phases:
                    self.confirm_phases.append(phases)
            else:
                self.confirm_fail += 1

    def record_error(self, msg: str):
        with self._lock:
            self.errors.append(msg)

    def _percentiles(self, data: List[float], label: str = "") -> dict:
        if not data:
            return {"count": 0}
        s = sorted(data)
        n = len(s)
        result = {
            "count":  n,
            "min_ms": round(s[0] * 1000, 2),
            "p50_ms": round(s[n // 2] * 1000, 2),
            "p90_ms": round(s[int(n * 0.90)] * 1000, 2),
            "p95_ms": round(s[int(n * 0.95)] * 1000, 2),
            "p99_ms": round(s[int(n * 0.99)] * 1000, 2),
            "max_ms": round(s[-1] * 1000, 2),
            "avg_ms": round(statistics.mean(s) * 1000, 2),
            "stddev_ms": round(statistics.stdev(s) * 1000, 2) if n > 1 else 0,
        }
        if n <= 100:
            result["sample_size_note"] = (
                f"⚠ Small sample (n={n}): p95/p99 are statistically thin. "
                f"p99 is essentially the {n - int(n * 0.01)}th of {n} samples."
            )
        return result

    def phase_breakdown(self) -> dict:
        """Average per-phase timing for successful confirms (milliseconds)."""
        if not self.confirm_phases:
            return {}
        keys = self.confirm_phases[0].keys()
        breakdown = {}
        for k in keys:
            vals = [p[k] for p in self.confirm_phases if k in p]
            if vals:
                breakdown[k.replace("_s", "_ms")] = round(
                    statistics.mean(vals) * 1000, 2
                )
        return breakdown

    def summary(self, total_users: int, total_seats: int) -> dict:
        wall = self.wall_end - self.wall_start
        total_hold_attempts = len(self.hold_latencies)
        total_confirm_attempts = len(self.confirm_latencies)
        total_ops = total_hold_attempts + total_confirm_attempts
        return {
            "config": {
                "total_users": total_users,
                "total_seats": total_seats,
                "contention_ratio": f"{total_users / total_seats:.1f}:1",
            },
            "throughput": {
                "wall_clock_seconds": round(wall, 3),
                "total_operations": total_ops,
                "ops_per_second": round(total_ops / wall, 1) if wall > 0 else 0,
                "definition": (
                    f"{total_hold_attempts} hold attempts + "
                    f"{total_confirm_attempts} confirm attempts = "
                    f"{total_ops} total ops / {round(wall, 3)}s wall clock"
                ),
            },
            "hold": {
                "granted": self.hold_granted,
                "denied": self.hold_denied,
                "success_rate": f"{self.hold_granted / max(total_users, 1) * 100:.1f}%",
                "latency": self._percentiles(self.hold_latencies, "hold"),
            },
            "confirm": {
                "success": self.confirm_ok,
                "failed": self.confirm_fail,
                "latency": self._percentiles(self.confirm_latencies, "confirm"),
                "phase_breakdown_avg_ms": self.phase_breakdown(),
            },
            "errors": self.errors[:20],
        }


# ── Naive Baseline Metrics ───────────────────────────────────────────────

@dataclass
class NaiveMetrics:
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    latencies: List[float] = field(default_factory=list)
    booked: int = 0
    denied: int = 0
    errors: List[str] = field(default_factory=list)
    wall_start: float = 0.0
    wall_end: float = 0.0

    def record(self, latency: float, success: bool):
        with self._lock:
            self.latencies.append(latency)
            if success:
                self.booked += 1
            else:
                self.denied += 1

    def record_error(self, msg: str):
        with self._lock:
            self.errors.append(msg)


# ── Workers ──────────────────────────────────────────────────────────────

def worker(user_id: int, seat_id: int, metrics: Metrics):
    """One user attempts to hold then confirm a seat."""
    # ── HOLD ──
    t0 = time.perf_counter()
    try:
        held = hold_seat(seat_id, user_id, ttl=30)
    except Exception as e:
        metrics.record_error(f"hold error user={user_id} seat={seat_id}: {e}")
        return
    t1 = time.perf_counter()
    metrics.record_hold(t1 - t0, held)

    if not held:
        return

    # ── CONFIRM (with profiling) ──
    t2 = time.perf_counter()
    try:
        ok, msg, phases = confirm_booking(seat_id, user_id, profile=True)
    except Exception as e:
        metrics.record_error(f"confirm error user={user_id} seat={seat_id}: {e}")
        return
    t3 = time.perf_counter()
    metrics.record_confirm(t3 - t2, ok, phases)


def naive_worker(user_id: int, seat_id: int, metrics: NaiveMetrics):
    """One user attempts the naive SELECT-then-INSERT booking."""
    t0 = time.perf_counter()
    try:
        ok = naive_book_seat(seat_id, user_id)
    except Exception as e:
        metrics.record_error(f"naive error user={user_id} seat={seat_id}: {e}")
        return
    t1 = time.perf_counter()
    metrics.record(t1 - t0, ok)


# ── Double-Booking Audit (Hold & Confirm system) ────────────────────────

def audit_double_bookings(seat_ids: List[int]) -> Tuple[bool, str]:
    conn = get_pg_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT seat_id, COUNT(*) AS cnt
            FROM bookings WHERE seat_id = ANY(%s) AND status = 'CONFIRMED'
            GROUP BY seat_id HAVING COUNT(*) > 1
        """, (seat_ids,))
        violations = cur.fetchall()
        cur.execute("""
            SELECT COUNT(*) FROM bookings
            WHERE seat_id = ANY(%s) AND status = 'CONFIRMED'
        """, (seat_ids,))
        total_confirmed = cur.fetchone()[0]
    conn.close()

    if violations:
        return False, f"DOUBLE BOOKING DETECTED on seats: {violations}"
    return True, f"No double bookings. {total_confirmed} seats confirmed."


# ── Cleanup ──────────────────────────────────────────────────────────────

def cleanup(seat_ids: List[int]):
    r = get_redis_client()
    pipe = r.pipeline()
    for sid in seat_ids:
        pipe.delete(f"seat:{sid}")
    pipe.execute()

    conn = get_pg_connection()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM bookings WHERE seat_id = ANY(%s)", (seat_ids,))
    conn.commit()
    conn.close()


# ── Pretty Printer ───────────────────────────────────────────────────────

def print_report(summary: dict, audit_ok: bool, audit_msg: str,
                 naive_audit: dict = None, naive_metrics: NaiveMetrics = None):
    cfg = summary["config"]
    tp  = summary["throughput"]
    h   = summary["hold"]
    c   = summary["confirm"]

    print()
    print("=" * 72)
    print("  HOLD & CONFIRM — BENCHMARK REPORT")
    print("=" * 72)

    print(f"\n  Configuration")
    print(f"    Users:             {cfg['total_users']}")
    print(f"    Seats:             {cfg['total_seats']}")
    print(f"    Contention ratio:  {cfg['contention_ratio']}")

    print(f"\n  Throughput")
    print(f"    Wall clock:        {tp['wall_clock_seconds']}s")
    print(f"    Total operations:  {tp['total_operations']}")
    print(f"    Ops/second:        {tp['ops_per_second']}")
    print(f"    Definition:        {tp['definition']}")

    print(f"\n  Hold Operations  (n={h['latency'].get('count', 0)} samples)")
    print(f"    Granted:           {h['granted']}")
    print(f"    Denied:            {h['denied']}")
    print(f"    Success rate:      {h['success_rate']}")
    hl = h["latency"]
    if hl.get("count", 0) > 0:
        print(f"    Latency (ms):      min={hl['min_ms']}  p50={hl['p50_ms']}  "
              f"p90={hl['p90_ms']}  p95={hl['p95_ms']}  p99={hl['p99_ms']}  max={hl['max_ms']}")
        print(f"                       avg={hl['avg_ms']}  stddev={hl['stddev_ms']}")

    print(f"\n  Confirm Operations  (n={c['latency'].get('count', 0)} samples)")
    cl = c["latency"]
    print(f"    Success:           {c['success']}")
    print(f"    Failed:            {c['failed']}")
    if cl.get("count", 0) > 0:
        print(f"    Latency (ms):      min={cl['min_ms']}  p50={cl['p50_ms']}  "
              f"p90={cl['p90_ms']}  p95={cl['p95_ms']}  p99={cl['p99_ms']}  max={cl['max_ms']}")
        print(f"                       avg={cl['avg_ms']}  stddev={cl['stddev_ms']}")
        if cl.get("sample_size_note"):
            print(f"    Note:              {cl['sample_size_note']}")

    # Per-phase breakdown
    pb = c.get("phase_breakdown_avg_ms", {})
    if pb:
        print(f"\n    ── Confirm Latency Breakdown (avg over {c['success']} successful confirms) ──")
        total_accounted = 0
        for phase, ms in pb.items():
            total_accounted += ms
            label = phase.replace("_ms", "").replace("_", " ").title()
            bar = "█" * max(1, int(ms / 2))
            print(f"      {label:<22s}  {ms:>8.2f} ms  {bar}")
        print(f"      {'─' * 45}")
        print(f"      {'Sum accounted':<22s}  {total_accounted:>8.2f} ms")

    color = "\033[32m" if audit_ok else "\033[31m"
    reset = "\033[0m"
    print(f"\n  Double-Booking Audit")
    print(f"    {color}{'✅ PASS' if audit_ok else '❌ FAIL'}{reset}  {audit_msg}")

    if summary["errors"]:
        print(f"\n  Errors ({len(summary['errors'])} shown):")
        for e in summary["errors"]:
            print(f"    ⚠  {e}")

    # ── Naive Baseline Comparison ──
    if naive_audit and naive_metrics:
        na = naive_audit
        nm = naive_metrics
        wall = nm.wall_end - nm.wall_start
        print(f"\n{'─' * 72}")
        print(f"  NAIVE BASELINE COMPARISON  (SELECT-then-INSERT, no atomic hold)")
        print(f"{'─' * 72}")
        print(f"    Load:              {na.get('naive_users', '?')} users, {na.get('naive_seats', '?')} seats "
              f"({na.get('contention_per_seat', '?')}:1 per seat)")
        print(f"    Wall clock:        {round(wall, 3)}s")
        print(f"    Thought they booked: {nm.booked}")
        print(f"    Actual in DB:      {na['total_bookings']} rows")
        print(f"    Unique seats:      {na['unique_seats_booked']}")

        oversold = na["oversold_seats"]
        if oversold > 0:
            color_bad = "\033[31m"
            print(f"    {color_bad}❌ OVERSOLD SEATS: {oversold}{reset}")
            print(f"    Double-booked examples:")
            for sid, cnt in na["oversold_examples"][:10]:
                print(f"      seat {sid}: {cnt} bookings (should be 1)")
        else:
            print(f"    Oversold seats: 0 (got lucky this time — retry with higher load)")

    print("\n" + "=" * 72)


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Hold & Confirm load test")
    parser.add_argument("--users",   type=int, default=10000, help="Total concurrent users (default: 10000)")
    parser.add_argument("--seats",   type=int, default=100,   help="Available seats (default: 100)")
    parser.add_argument("--workers", type=int, default=200,   help="Thread pool size (default: 200)")
    parser.add_argument("--json",    action="store_true",     help="Dump JSON metrics to load_results.json")
    parser.add_argument("--skip-naive", action="store_true",  help="Skip the naive baseline comparison")
    args = parser.parse_args()

    seat_ids = list(range(1, args.seats + 1))

    # ── Phase 1: Hold & Confirm (the real system) ──
    print(f"\n{'═' * 72}")
    print(f"  PHASE 1: HOLD & CONFIRM (Lua + Postgres backstop)")
    print(f"{'═' * 72}")

    print(f"\n🔧 Cleaning up...")
    cleanup(seat_ids)

    metrics = Metrics()

    tasks = []
    for user_id in range(1, args.users + 1):
        seat_id = ((user_id - 1) % args.seats) + 1
        tasks.append((user_id, seat_id))

    print(f"🚀 Launching {args.users} users → {args.seats} seats "
          f"({args.users // args.seats}:1 contention), {args.workers} threads\n")

    metrics.wall_start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(worker, uid, sid, metrics) for uid, sid in tasks]
        for f in as_completed(futures):
            exc = f.exception()
            if exc:
                metrics.record_error(f"Thread exception: {exc}")

    metrics.wall_end = time.perf_counter()

    audit_ok, audit_msg = audit_double_bookings(seat_ids)

    # ── Phase 2: Naive Baseline ──
    naive_audit = None
    naive_met = None

    if not args.skip_naive:
        # Focused contention: many users per seat to reliably trigger the race
        naive_seats_count = min(10, args.seats)
        naive_users = min(500, args.users)
        naive_seat_ids = list(range(1, naive_seats_count + 1))
        users_per_seat = naive_users // naive_seats_count

        print(f"\n{'═' * 72}")
        print(f"  PHASE 2: NAIVE BASELINE (SELECT-then-INSERT, no lock)")
        print(f"{'═' * 72}")

        setup_naive_table()
        cleanup_naive(naive_seat_ids)

        naive_met = NaiveMetrics()

        # All users target the same small set of seats
        naive_tasks = []
        for uid in range(1, naive_users + 1):
            sid = ((uid - 1) % naive_seats_count) + 1
            naive_tasks.append((uid, sid))

        print(f"🚀 {naive_users} users → {naive_seats_count} seats "
              f"({users_per_seat}:1 contention per seat), {args.workers} threads\n")

        naive_met.wall_start = time.perf_counter()

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = [pool.submit(naive_worker, uid, sid, naive_met) for uid, sid in naive_tasks]
            for f in as_completed(futures):
                exc = f.exception()
                if exc:
                    naive_met.record_error(f"Thread exception: {exc}")

        naive_met.wall_end = time.perf_counter()

        naive_audit = audit_naive_bookings(naive_seat_ids)
        naive_audit["naive_users"] = naive_users
        naive_audit["naive_seats"] = naive_seats_count
        naive_audit["contention_per_seat"] = users_per_seat
        cleanup_naive(naive_seat_ids)

    # ── Report ──
    summary = metrics.summary(args.users, args.seats)
    summary["audit"] = {"passed": audit_ok, "message": audit_msg}
    if naive_audit:
        summary["naive_baseline"] = naive_audit

    print_report(summary, audit_ok, audit_msg, naive_audit, naive_met)

    if args.json:
        out_path = os.path.join(os.path.dirname(__file__), "load_results.json")
        with open(out_path, "w") as f:
            json.dump(summary, f, indent=2)
        print(f"\n  📄 JSON results saved to {out_path}")

    print(f"\n🧹 Cleaning up...")
    cleanup(seat_ids)

    sys.exit(0 if audit_ok else 1)


if __name__ == "__main__":
    main()
