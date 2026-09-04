"""
SlotGuard — Event-Driven Redis Keyspace Expiry Worker
─────────────────────────────────────────────────────
Listens to Redis Pub/Sub '__keyevent@0__:expired' events. When a slot hold TTL
expires, this daemon automatically pops the next waitlisted citizen via Lua (ZPOPMIN)
and grants a new hold — requiring zero human or API polling intervention.
"""

from __future__ import annotations

import time
import threading
import redis

from config import get_redis_client
from booking import _offer_to_next_in_waitlist
from metrics import SLOTGUARD_EXPIRY_AUTO_OFFERS_TOTAL


def enable_redis_keyspace_events(r: redis.Redis = None):
    """Enable Keyspace Notifications for expired events in Redis configuration."""
    r = r or get_redis_client()
    try:
        r.config_set("notify-keyspace-events", "Ex")
        print("[ExpiryWorker] Configured Redis notify-keyspace-events = 'Ex'")
    except Exception as e:
        print(f"[ExpiryWorker] Warning: Could not set notify-keyspace-events config: {e}")


def handle_expiry_event(key_str: str) -> bool:
    """
    Parse expired Redis key and trigger automatic waitlist offer if key is seat:<id>.
    Returns True if a slot offer was triggered.
    """
    if not key_str.startswith("seat:"):
        return False

    parts = key_str.split(":")
    if len(parts) != 2:
        return False

    try:
        slot_id = int(parts[1])
    except ValueError:
        return False

    print(f"[ExpiryWorker] Event: Hold TTL expired for seat {slot_id}. Triggering auto-offer...")
    offered_user = _offer_to_next_in_waitlist(slot_id)

    if offered_user is not None:
        print(f"[ExpiryWorker] Success: Auto-offered seat {slot_id} to next waitlisted citizen {offered_user}")
        SLOTGUARD_EXPIRY_AUTO_OFFERS_TOTAL.labels(result="offered").inc()
        return True
    else:
        print(f"[ExpiryWorker] Notice: Waitlist for seat {slot_id} was empty.")
        SLOTGUARD_EXPIRY_AUTO_OFFERS_TOTAL.labels(result="empty").inc()
        return False


def start_expiry_listener(stop_event=None):
    """Subscribe to Redis expired events and process them in real time."""
    r = get_redis_client()
    enable_redis_keyspace_events(r)

    pubsub = r.pubsub()
    pubsub.subscribe("__keyevent@0__:expired")

    print("[ExpiryWorker] Subscribed to '__keyevent@0__:expired'. Waiting for TTL expiration events...")

    for message in pubsub.listen():
        if stop_event and stop_event.is_set():
            print("[ExpiryWorker] Stopping Redis Expiry Worker daemon.")
            break

        if message["type"] == "message":
            key_data = message["data"]
            key_str = key_data.decode("utf-8") if isinstance(key_data, bytes) else str(key_data)
            handle_expiry_event(key_str)
