-- Hold & Confirm Ticketing System — Postgres Schema
-- Durable record of confirmed (and cancelled) bookings.

CREATE TABLE IF NOT EXISTS bookings (
    id               SERIAL PRIMARY KEY,
    seat_id          INT NOT NULL,
    user_id          INT NOT NULL,
    status           TEXT NOT NULL CHECK (status IN ('CONFIRMED', 'CANCELLED')),
    booking_group_id UUID,            -- links seats in a multi-seat booking
    created_at       TIMESTAMP DEFAULT NOW()
);

-- Partial unique index: only one CONFIRMED row per seat_id.
-- This is the backstop that prevents double-booking even if Redis state is lost.
CREATE UNIQUE INDEX IF NOT EXISTS idx_no_double_booking
    ON bookings (seat_id)
    WHERE status = 'CONFIRMED';

-- Transactional Outbox Table for CDC / Async Event Streaming (Zero Dual-Write Loss)
CREATE TABLE IF NOT EXISTS outbox (
    id           BIGSERIAL PRIMARY KEY,
    event_type   VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(64) NOT NULL,
    payload      JSONB NOT NULL,
    status       VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    created_at   TIMESTAMP DEFAULT NOW(),
    processed_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox (status, created_at) WHERE status = 'PENDING';

