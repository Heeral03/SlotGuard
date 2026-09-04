"""
SlotGuard — Prometheus Observability & Instrumentation Layer
─────────────────────────────────────────────────────────────
Exposes scrapable metrics for Redis hold locks, PostgreSQL confirmations,
waitlist queue depths, outbox CDC events, and Redis TTL auto-offers.
"""

from prometheus_client import Counter, Gauge, Histogram

# ── Counters ──────────────────────────────────────────────────────────────

SLOTGUARD_HOLDS_TOTAL = Counter(
    "slotguard_holds_total",
    "Total number of appointment slot hold attempts",
    ["status"],  # granted, denied
)

SLOTGUARD_CONFIRMS_TOTAL = Counter(
    "slotguard_confirms_total",
    "Total number of appointment slot confirmation attempts",
    ["status"],  # success, failed
)

SLOTGUARD_WAITLIST_JOIN_TOTAL = Counter(
    "slotguard_waitlist_join_total",
    "Total number of citizens joining slot waitlists",
)

SLOTGUARD_OUTBOX_EVENTS_TOTAL = Counter(
    "slotguard_outbox_events_total",
    "Total outbox events dispatched by CDC worker",
    ["status"],  # created, processed, failed
)

SLOTGUARD_EXPIRY_AUTO_OFFERS_TOTAL = Counter(
    "slotguard_expiry_auto_offers_total",
    "Total automatic waitlist slot offers triggered by Redis TTL key expiration",
    ["result"],  # offered, empty
)

# ── Gauges ────────────────────────────────────────────────────────────────

SLOTGUARD_WAITLIST_DEPTH = Gauge(
    "slotguard_waitlist_depth",
    "Current waitlist queue depth for a given appointment slot",
    ["slot_id"],
)

SLOTGUARD_ACTIVE_HOLDS = Gauge(
    "slotguard_active_holds",
    "Estimated count of active temporary slot holds in Redis",
)

# ── Histograms ────────────────────────────────────────────────────────────

SLOTGUARD_HOLD_LATENCY = Histogram(
    "slotguard_hold_latency_seconds",
    "Latency of Redis check-and-set hold Lua operations in seconds",
    buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)

SLOTGUARD_CONFIRM_LATENCY = Histogram(
    "slotguard_confirm_latency_seconds",
    "Latency of PostgreSQL transactional confirmation operations in seconds",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

SLOTGUARD_OUTBOX_LATENCY = Histogram(
    "slotguard_outbox_latency_seconds",
    "Latency between outbox event creation and CDC worker dispatch in seconds",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)
