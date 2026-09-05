"""
SlotGuard — Transactional Outbox CDC Event Worker
─────────────────────────────────────────────────
Polling worker using non-blocking 'FOR UPDATE SKIP LOCKED' to process
pending Outbox events from PostgreSQL without lock contention across multiple workers.
Uses connection pooling for maximum throughput.
"""

from __future__ import annotations

import json
import time
import psycopg2
from typing import List, Dict, Any

from config import get_pg_conn
from metrics import SLOTGUARD_OUTBOX_EVENTS_TOTAL, SLOTGUARD_OUTBOX_LATENCY


def process_outbox_batch(batch_size: int = 50) -> int:
    """
    Process up to batch_size pending outbox events using SKIP LOCKED.
    Returns the number of events processed.
    """
    processed_count = 0

    try:
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                # Non-blocking skip locked query for multi-worker safety
                cur.execute("""
                    SELECT id, event_type, aggregate_id, payload, created_at
                    FROM outbox
                    WHERE status = 'PENDING'
                    ORDER BY id ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                """, (batch_size,))
                rows = cur.fetchall()

                if not rows:
                    conn.commit()
                    return 0

                processed_ids = []
                now_ts = time.time()

                for row in rows:
                    event_id, event_type, agg_id, payload, created_at = row
                    
                    # Calculate outbox processing latency
                    created_ts = created_at.timestamp() if hasattr(created_at, 'timestamp') else now_ts
                    SLOTGUARD_OUTBOX_LATENCY.observe(now_ts - created_ts)
                    SLOTGUARD_OUTBOX_EVENTS_TOTAL.labels(status="processed").inc()
                    
                    processed_ids.append(event_id)
                    processed_count += 1

                if processed_ids:
                    cur.execute("""
                        UPDATE outbox
                        SET status = 'PROCESSED', processed_at = NOW()
                        WHERE id = ANY(%s)
                    """, (processed_ids,))

            conn.commit()
    except Exception as e:
        SLOTGUARD_OUTBOX_EVENTS_TOTAL.labels(status="failed").inc()
        print(f"[OutboxWorker] Error processing batch: {e}")

    return processed_count


def start_outbox_poller(poll_interval: float = 1.0, stop_event=None):
    """Continuous polling loop for outbox events."""
    print("[OutboxWorker] Started CDC Outbox polling worker...")
    while True:
        if stop_event and stop_event.is_set():
            print("[OutboxWorker] Stopping CDC Outbox worker.")
            break

        count = process_outbox_batch(batch_size=50)
        if count == 0:
            time.sleep(poll_interval)
