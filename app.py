"""
SlotGuard — High-Concurrency Public Service Appointment Engine
─────────────────────────────────────────────────────────────
FastAPI REST Web Service providing race-condition-safe appointment slot
holds, confirmations, fair FIFO waitlist queues, and multi-slot family bookings.
"""

from __future__ import annotations

import time
from typing import List, Optional
from pydantic import BaseModel, Field
from fastapi import FastAPI, HTTPException, Path, Response, status
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from config import get_redis_client, get_pg_connection

from booking import (
    hold_seat as hold_slot,
    confirm_booking as confirm_slot,
    get_seat_status as get_slot_status,
    release_hold as release_slot_hold,
    join_waitlist,
    leave_waitlist,
    get_waitlist,
    hold_seats as hold_slots,
    confirm_seats as confirm_slots,
    release_holds as release_slot_holds,
)

app = FastAPI(
    title="SlotGuard API",
    description=(
        "High-Concurrency Public Service Appointment Engine (CoWIN/Passport-style). "
        "Guarantees race-condition safety, zero double-bookings, atomic multi-slot family holds, "
        "and fair FIFO waitlist queues under heavy contention."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS middleware for frontend / dashboard integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Pydantic Request / Response Schemas ──────────────────────────────────

class HoldSlotRequest(BaseModel):
    citizen_id: int = Field(..., description="ID of the citizen acquiring the slot hold", json_schema_extra={"example": 101})
    ttl_seconds: Optional[int] = Field(None, description="Optional TTL in seconds for the hold window", json_schema_extra={"example": 120})


class ConfirmSlotRequest(BaseModel):
    citizen_id: int = Field(..., description="ID of the citizen confirming the slot", json_schema_extra={"example": 101})


class MultiHoldRequest(BaseModel):
    slot_ids: List[int] = Field(..., description="List of contiguous appointment slot IDs to hold atomically", json_schema_extra={"example": [101, 102]})
    citizen_id: int = Field(..., description="ID of the citizen acquiring family slots", json_schema_extra={"example": 202})
    ttl_seconds: Optional[int] = Field(None, description="Optional TTL window in seconds", json_schema_extra={"example": 120})


class MultiConfirmRequest(BaseModel):
    slot_ids: List[int] = Field(..., description="List of held appointment slot IDs to confirm", json_schema_extra={"example": [101, 102]})
    citizen_id: int = Field(..., description="ID of the citizen confirming family slots", json_schema_extra={"example": 202})


class ReleaseSlotRequest(BaseModel):
    citizen_id: int = Field(..., description="ID of the citizen releasing the hold", json_schema_extra={"example": 101})
    offer_waitlist: bool = Field(True, description="Whether to auto-offer released slot to next waitlisted citizen")


class WaitlistRequest(BaseModel):
    citizen_id: int = Field(..., description="ID of the citizen joining/leaving the waitlist", json_schema_extra={"example": 303})



# ── API Endpoints ────────────────────────────────────────────────────────

@app.get("/health", tags=["Health"])
def health_check():
    """Health check validating Redis & PostgreSQL connectivity."""
    redis_ok = False
    pg_ok = False

    try:
        r = get_redis_client()
        redis_ok = r.ping()
    except Exception:
        pass

    try:
        from config import get_pg_conn
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        pg_ok = True
    except Exception:
        pass

    if redis_ok and pg_ok:
        return {"status": "HEALTHY", "redis": "CONNECTED", "postgres": "CONNECTED"}
    
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail={"status": "UNHEALTHY", "redis": "CONNECTED" if redis_ok else "DISCONNECTED", "postgres": "CONNECTED" if pg_ok else "DISCONNECTED"},
    )



@app.get("/metrics", tags=["Observability"])
def metrics_endpoint():
    """Prometheus scrapable metrics endpoint exposing lock latencies, outbox events, and waitlist metrics."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ── Single Slot Management ───────────────────────────────────────────────


@app.get("/api/v1/slots/{slot_id}/status", tags=["Single Slot"])
def query_slot_status(slot_id: int = Path(..., description="Target appointment slot ID")):
    """Query the real-time status of an appointment slot (FREE, HELD:<citizen_id>, or CONFIRMED)."""
    state = get_slot_status(slot_id)
    return {"slot_id": slot_id, "status": state}


@app.post("/api/v1/slots/{slot_id}/hold", tags=["Single Slot"])
def hold_appointment_slot(
    slot_id: int = Path(..., description="Target appointment slot ID"),
    body: HoldSlotRequest = ...,
):
    """Atomically attempt to hold an appointment slot for a citizen (check-and-set via Redis Lua script)."""
    ok = hold_slot(slot_id, body.citizen_id, ttl=body.ttl_seconds)
    if ok:
        return {
            "success": True,
            "message": f"Hold granted on slot {slot_id} for citizen {body.citizen_id}",
            "slot_id": slot_id,
            "citizen_id": body.citizen_id,
            "status": f"HELD:{body.citizen_id}",
        }
    
    current_status = get_slot_status(slot_id)
    raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail={
            "success": False,
            "message": f"Slot {slot_id} is currently unavailable ({current_status})",
            "current_status": current_status,
        },
    )


@app.post("/api/v1/slots/{slot_id}/confirm", tags=["Single Slot"])
def confirm_appointment_slot(
    slot_id: int = Path(..., description="Target appointment slot ID"),
    body: ConfirmSlotRequest = ...,
):
    """Confirm a previously held appointment slot (Redis verify -> Postgres ACID insert -> Redis promote)."""
    ok, msg, timings = confirm_slot(slot_id, body.citizen_id, profile=True)
    if ok:
        return {
            "success": True,
            "message": msg,
            "slot_id": slot_id,
            "citizen_id": body.citizen_id,
            "status": "CONFIRMED",
            "timings_ms": {k: round(v * 1000, 2) for k, v in timings.items()} if timings else None,
        }
    
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail={"success": False, "message": msg},
    )


@app.post("/api/v1/slots/{slot_id}/release", tags=["Single Slot"])
def release_appointment_slot(
    slot_id: int = Path(..., description="Target appointment slot ID"),
    body: ReleaseSlotRequest = ...,
):
    """Release a held slot. If offer_waitlist=True, auto-offers to next queued citizen via Lua ZPOPMIN."""
    ok = release_slot_hold(slot_id, body.citizen_id, offer_waitlist=body.offer_waitlist)
    if ok:
        new_status = get_slot_status(slot_id)
        return {
            "success": True,
            "message": f"Hold on slot {slot_id} released successfully",
            "slot_id": slot_id,
            "new_status": new_status,
        }
    
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail={"success": False, "message": f"Failed to release hold: slot {slot_id} is not held by citizen {body.citizen_id}"},
    )


# ── Fair Waitlist Queue ──────────────────────────────────────────────────

@app.post("/api/v1/slots/{slot_id}/waitlist/join", tags=["Waitlist Queue"])
def join_slot_waitlist(
    slot_id: int = Path(..., description="Target busy appointment slot ID"),
    body: WaitlistRequest = ...,
):
    """Join the fair FIFO waitlist queue for a busy or held appointment slot."""
    ok, msg = join_waitlist(slot_id, body.citizen_id)
    if ok:
        return {"success": True, "message": msg, "slot_id": slot_id, "citizen_id": body.citizen_id}
    
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail={"success": False, "message": msg},
    )


@app.post("/api/v1/slots/{slot_id}/waitlist/leave", tags=["Waitlist Queue"])
def leave_slot_waitlist(
    slot_id: int = Path(..., description="Target appointment slot ID"),
    body: WaitlistRequest = ...,
):
    """Leave the waitlist queue for an appointment slot."""
    ok = leave_waitlist(slot_id, body.citizen_id)
    if ok:
        return {"success": True, "message": f"Citizen {body.citizen_id} removed from waitlist", "slot_id": slot_id}
    
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail={"success": False, "message": f"Citizen {body.citizen_id} was not found on the waitlist for slot {slot_id}"},
    )


@app.get("/api/v1/slots/{slot_id}/waitlist", tags=["Waitlist Queue"])
def query_slot_waitlist(slot_id: int = Path(..., description="Target appointment slot ID")):
    """Retrieve the ordered list of citizen IDs on the waitlist queue (FIFO sequence)."""
    waiters = get_waitlist(slot_id)
    return {"slot_id": slot_id, "waitlist_count": len(waiters), "queue": waiters}


# ── Multi-Slot Family Appointments ───────────────────────────────────────

@app.post("/api/v1/slots/hold-multi", tags=["Multi-Slot Family Appointments"])
def hold_family_appointment_slots(body: MultiHoldRequest):
    """Atomically hold ALL requested appointment slots (all-or-nothing check-and-set via multi-key Redis Lua script)."""
    ok, msg = hold_slots(body.slot_ids, body.citizen_id, ttl=body.ttl_seconds)
    if ok:
        return {
            "success": True,
            "message": msg,
            "slot_ids": body.slot_ids,
            "citizen_id": body.citizen_id,
        }
    
    raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail={"success": False, "message": msg, "requested_slots": body.slot_ids},
    )


@app.post("/api/v1/slots/confirm-multi", tags=["Multi-Slot Family Appointments"])
def confirm_family_appointment_slots(body: MultiConfirmRequest):
    """Confirm ALL held family appointment slots inside a single transactional Postgres group commit."""
    ok, msg = confirm_slots(body.slot_ids, body.citizen_id)
    if ok:
        return {
            "success": True,
            "message": msg,
            "slot_ids": body.slot_ids,
            "citizen_id": body.citizen_id,
        }
    
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail={"success": False, "message": msg},
    )
