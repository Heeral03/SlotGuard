"""
Comprehensive Pytest Test Suite for SlotGuard
"""

import pytest
from fastapi.testclient import TestClient

from app import app
from booking import (
    hold_seat,
    confirm_booking,
    get_seat_status,
    release_hold,
    join_waitlist,
    get_waitlist,
    hold_seats,
    confirm_seats,
)
from outbox_worker import process_outbox_batch
from expiry_worker import handle_expiry_event, run_zset_expiry_poller
from config import get_redis_client, get_pg_conn

client = TestClient(app)


# ── Core Engine Tests ───────────────────────────────────────────────────

def test_hold_and_confirm_single_seat():
    assert hold_seat(seat_id=1, user_id=101) is True
    assert get_seat_status(1) == "HELD:101"

    ok, msg, timings = confirm_booking(seat_id=1, user_id=101, profile=True)
    assert ok is True
    assert "confirmed" in msg.lower()
    assert get_seat_status(1) == "CONFIRMED"

    # Verify DB insertion
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, status FROM bookings WHERE seat_id = 1;")
            row = cur.fetchone()
            assert row == (101, "CONFIRMED")


def test_conflict_double_hold():
    assert hold_seat(seat_id=2, user_id=101) is True
    assert hold_seat(seat_id=2, user_id=102) is False
    assert get_seat_status(2) == "HELD:101"


def test_confirm_without_hold_fails():
    ok, msg, _ = confirm_booking(seat_id=3, user_id=101)
    assert ok is False
    assert "invalid" in msg.lower() or "expired" in msg.lower()


# ── Waitlist Queue Tests ─────────────────────────────────────────────────

def test_waitlist_fifo_queue():
    assert hold_seat(seat_id=10, user_id=101) is True

    ok1, msg1 = join_waitlist(seat_id=10, user_id=201)
    assert ok1 is True
    assert "position: 1" in msg1

    ok2, msg2 = join_waitlist(seat_id=10, user_id=202)
    assert ok2 is True
    assert "position: 2" in msg2

    assert get_waitlist(10) == [201, 202]

    # Release hold and auto-offer to next queued citizen (201)
    released = release_hold(seat_id=10, user_id=101, offer_waitlist=True)
    assert released is True
    assert get_seat_status(10) == "HELD:201"
    assert get_waitlist(10) == [202]


# ── Multi-Seat Atomic Holds & Confirmations ──────────────────────────────

def test_multi_seat_atomic_hold():
    ok, msg = hold_seats(seat_ids=[20, 21, 22], user_id=301)
    assert ok is True
    assert get_seat_status(20) == "HELD:301"
    assert get_seat_status(21) == "HELD:301"
    assert get_seat_status(22) == "HELD:301"

    ok_confirm, msg_confirm = confirm_seats(seat_ids=[20, 21, 22], user_id=301)
    assert ok_confirm is True

    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bookings WHERE user_id = 301 AND status = 'CONFIRMED';")
            count = cur.fetchone()[0]
            assert count == 3


def test_multi_seat_partial_conflict_rolls_back():
    assert hold_seat(seat_id=31, user_id=999) is True

    ok, msg = hold_seats(seat_ids=[30, 31, 32], user_id=302)
    assert ok is False
    assert "taken" in msg.lower()

    # Verify atomic all-or-nothing rollback (seat 30 and 32 remain free)
    assert get_seat_status(30) == "FREE"
    assert get_seat_status(32) == "FREE"


# ── REST API Tests ───────────────────────────────────────────────────────

def test_api_health_and_metrics():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "HEALTHY"

    metrics_res = client.get("/metrics")
    assert metrics_res.status_code == 200
    assert "slotguard_holds_total" in metrics_res.text


def test_api_hold_and_confirm_flow():
    hold_res = client.post("/api/v1/slots/100/hold", json={"citizen_id": 501, "ttl_seconds": 60})
    assert hold_res.status_code == 200
    assert hold_res.json()["success"] is True

    confirm_res = client.post("/api/v1/slots/100/confirm", json={"citizen_id": 501})
    assert confirm_res.status_code == 200
    assert confirm_res.json()["success"] is True


# ── CDC Outbox Worker Test ───────────────────────────────────────────────

def test_transactional_outbox_cdc():
    hold_seat(seat_id=50, user_id=601)
    confirm_booking(seat_id=50, user_id=601)

    processed = process_outbox_batch(batch_size=10)
    assert processed >= 1

    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM outbox WHERE aggregate_id = '50';")
            status = cur.fetchone()[0]
            assert status == "PROCESSED"
