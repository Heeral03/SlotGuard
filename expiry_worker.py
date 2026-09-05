"""
SlotGuard — Event-Driven & Deterministic Redis Expiry Worker
─────────────────────────────────────────────────────────────
Dual-Layer Hybrid Expiry Resolution:
1. Fast-path: Listens to Redis Pub/Sub '__keyevent@0__:expired' events.
2. Deterministic Poller: Scans Redis 'holds:ttl' ZSET via Lua script (scan_expired_holds.lua)
   every 100ms — guaranteeing ZERO dropped expiries even on Pub/Sub drops or worker restarts.
"""

from __future__ import annotations

import time
import threading
import redis

from config import get_redis_client, HOLD_TTL_SECONDS
from booking import _offer_to_next_in_waitlist, _scan_expired_script
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


def run_zset_expiry_poller(poll_interval: float = 0.1, stop_event=None):
    """
    Deterministic ZSET TTL Scanner loop.
    Scans 'holds:ttl' ZSET every poll_interval seconds as a fail-safe backstop.
    """
    print(f"[ExpiryWorker] Started ZSET TTL Poller (interval={poll_interval}s)...")
    while True:
        if stop_event and stop_event.is_set():
            print("[ExpiryWorker] Stopping ZSET TTL Poller daemon.")
            break

        try:
            now_ts = int(time.time())
            results = _scan_expired_script(args=[now_ts, HOLD_TTL_SECONDS])
            if results:
                for item in results:
                    item_str = item.decode() if isinstance(item, bytes) else str(item)
                    print(f"[ExpiryWorker] ZSET Scanner auto-processed expired seat hold: {item_str}")
                    SLOTGUARD_EXPIRY_AUTO_OFFERS_TOTAL.labels(result="zset_processed").inc()
        except Exception as e:
            print(f"[ExpiryWorker] ZSET Poller error: {e}")

        time.sleep(poll_interval)


def start_expiry_listener(stop_event=None):
    """
    Start dual-layer expiry listener:
    1. Spawns ZSET polling thread for deterministic cleanup.
    2. Subscribes main thread to Pub/Sub events for real-time fast path.
    """
    r = get_redis_client()
    enable_redis_keyspace_events(r)

    # Spawn ZSET Poller thread
    poller_thread = threading.Thread(
        target=run_zset_expiry_poller,
        args=(0.1, stop_event),
        daemon=True
    )
    poller_thread.start()

    pubsub = r.pubsub()
    pubsub.subscribe("__keyevent@0__:expired")

    print("[ExpiryWorker] Subscribed to '__keyevent@0__:expired' + ZSET Poller active.")

    for message in pubsub.listen():
        if stop_event and stop_event.is_set():
            print("[ExpiryWorker] Stopping Redis Expiry Worker daemon.")
            break

        if message["type"] == "message":
            key_data = message["data"]
            key_str = key_data.decode("utf-8") if isinstance(key_data, bytes) else str(key_data)
            handle_expiry_event(key_str)
