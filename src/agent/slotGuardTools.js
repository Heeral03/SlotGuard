// slotGuardTools.js
// LangChain tool definitions wrapping SlotGuard's high-performance backend primitives.

import { DynamicStructuredTool } from "@langchain/core/tools";
import { z } from "zod";
import { registerRedisCommands } from '../redisCommands.js';
import Redis from 'ioredis';
import { pool } from '../db.js';
import { slotQueue } from '../queue.js';
import { WAITING_QUEUE_KEY, admittedKey } from '../waitingRoom.js';

// Connection setup for tool execution
const redis = registerRedisCommands(
    new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
        maxRetriesPerRequest: null,
    })
);

/**
 * Tool 1: Query Available Clinic Slots
 */
export const getAvailableSlotsTool = new DynamicStructuredTool({
    name: "get_available_slots",
    description: "Returns current status of all 24 clinic seat pods (Available, Held, Confirmed, Waitlisted count).",
    schema: z.object({}),
    func: async () => {
        try {
            const result = [];
            const keys = await redis.keys('seat:*');
            const redisState = {};

            if (keys.length > 0) {
                const values = await redis.mget(keys);
                const pipeline = redis.pipeline();
                keys.forEach(k => pipeline.ttl(k));
                const ttls = await pipeline.exec();

                keys.forEach((key, index) => {
                    const seatId = key.replace('seat:', '');
                    const val = values[index];
                    const ttl = ttls[index] ? ttls[index][1] : -1;
                    redisState[seatId] = { val, ttl };
                });
            }

            const dbRes = await pool.query("SELECT seat_id, user_id, status FROM bookings WHERE status = 'CONFIRMED';");
            const dbConfirmed = {};
            dbRes.rows.forEach(row => {
                dbConfirmed[row.seat_id] = row.user_id;
            });

            for (let i = 1; i <= 24; i++) {
                const id = i.toString();
                const waitlistCount = await redis.zcard(`waitlist:${id}`);

                if (dbConfirmed[id]) {
                    result.push({ id, status: 'CONFIRMED', userId: dbConfirmed[id], waitlistCount });
                } else if (redisState[id]) {
                    const { val, ttl } = redisState[id];
                    if (val === 'CONFIRMED') {
                        result.push({ id, status: 'CONFIRMED', userId: 'Booked', waitlistCount });
                    } else if (val.startsWith('HELD:')) {
                        const heldBy = val.replace('HELD:', '');
                        result.push({ id, status: 'HELD', userId: heldBy, ttl: ttl > 0 ? ttl : 0, waitlistCount });
                    } else {
                        result.push({ id, status: 'AVAILABLE', waitlistCount });
                    }
                } else {
                    result.push({ id, status: 'AVAILABLE', waitlistCount });
                }
            }

            return JSON.stringify({ success: true, total: result.length, slots: result });
        } catch (err) {
            return JSON.stringify({ success: false, error: err.message });
        }
    }
});

/**
 * Tool 2: Hold a Clinic Slot (Atomic Redis Lua Lock)
 */
export const holdSlotTool = new DynamicStructuredTool({
    name: "hold_clinic_slot",
    description: "Executes microsecond atomic Lua lock to hold a clinic slot for 60 seconds.",
    schema: z.object({
        seatId: z.string().describe("Slot number ID to hold (e.g. '1', '5', '12')"),
        userId: z.string().describe("User ID requesting the hold")
    }),
    func: async ({ seatId, userId }) => {
        try {
            const ttlSeconds = 60;
            const res = await redis.holdSeat(`seat:${seatId}`, userId, ttlSeconds);
            if (res === 1) {
                // Schedule BullMQ hold expiry check
                await slotQueue.add('check-hold-expiry', { seatId }, { delay: ttlSeconds * 1000 });
                return JSON.stringify({
                    success: true,
                    message: `Slot #${seatId} held successfully for user '${userId}' for 60 seconds!`,
                    seatId,
                    userId,
                    ttl: ttlSeconds
                });
            } else {
                return JSON.stringify({
                    success: false,
                    error: `Slot #${seatId} is already held or booked by another user.`
                });
            }
        } catch (err) {
            return JSON.stringify({ success: false, error: err.message });
        }
    }
});

/**
 * Tool 3: Confirm Booking (PostgreSQL ACID Transaction)
 */
export const confirmBookingTool = new DynamicStructuredTool({
    name: "confirm_clinic_booking",
    description: "Confirms a held slot and commits permanent record to PostgreSQL database inside an ACID transaction.",
    schema: z.object({
        seatId: z.string().describe("Slot number ID to confirm"),
        userId: z.string().describe("User ID holding the slot")
    }),
    func: async ({ seatId, userId }) => {
        try {
            const seatKey = `seat:${seatId}`;
            const redisResult = await redis.confirmHold(seatKey, userId);

            if (redisResult === -1) {
                return JSON.stringify({ success: false, error: `Hold for Slot #${seatId} has expired or does not exist.` });
            }
            if (redisResult === 0) {
                return JSON.stringify({ success: false, error: `Slot #${seatId} is held by another user or already confirmed.` });
            }

            // PostgreSQL ACID transaction write
            const client = await pool.connect();
            try {
                await client.query('BEGIN');
                const query = `
                    INSERT INTO bookings (seat_id, user_id, status)
                    VALUES ($1, $2, 'CONFIRMED')
                    RETURNING id, created_at;
                `;
                const result = await client.query(query, [seatId, userId]);
                await client.query('COMMIT');

                return JSON.stringify({
                    success: true,
                    message: `Slot #${seatId} permanently confirmed in PostgreSQL!`,
                    bookingId: result.rows[0].id,
                    timestamp: result.rows[0].created_at
                });
            } catch (dbErr) {
                await client.query('ROLLBACK');
                if (dbErr.code === '23505') {
                    return JSON.stringify({ success: false, error: `Conflict: Slot #${seatId} was already confirmed.` });
                }
                throw dbErr;
            } finally {
                client.release();
            }
        } catch (err) {
            return JSON.stringify({ success: false, error: err.message });
        }
    }
});

/**
 * Tool 4: Join Seat Waitlist
 */
export const joinWaitlistTool = new DynamicStructuredTool({
    name: "join_slot_waitlist",
    description: "Adds a user to the FIFO waitlist for a currently held slot.",
    schema: z.object({
        seatId: z.string().describe("Slot number ID to join waitlist for"),
        userId: z.string().describe("User ID joining waitlist")
    }),
    func: async ({ seatId, userId }) => {
        try {
            const val = await redis.get(`seat:${seatId}`);
            if (val === 'CONFIRMED') {
                return JSON.stringify({ success: false, error: `Slot #${seatId} is already confirmed and cannot be waitlisted.` });
            }

            const waitlistKey = `waitlist:${seatId}`;
            const added = await redis.zadd(waitlistKey, 'NX', Date.now(), userId);
            if (added === 1) {
                const rank = await redis.zrank(waitlistKey, userId);
                return JSON.stringify({
                    success: true,
                    message: `User '${userId}' joined waitlist for Slot #${seatId} at position #${rank + 1}!`,
                    position: rank + 1
                });
            } else {
                return JSON.stringify({ success: false, error: `User '${userId}' is already on the waitlist for Slot #${seatId}.` });
            }
        } catch (err) {
            return JSON.stringify({ success: false, error: err.message });
        }
    }
});

/**
 * Tool 5: Virtual Waiting Room Admission Queue
 */
export const joinWaitingRoomTool = new DynamicStructuredTool({
    name: "join_waiting_room",
    description: "Places user in virtual waiting room queue during high-demand surges.",
    schema: z.object({
        userId: z.string().describe("User ID requesting admission")
    }),
    func: async ({ userId }) => {
        try {
            const admitted = await redis.get(admittedKey(userId));
            if (admitted) {
                return JSON.stringify({ success: true, status: 'admitted', message: `User '${userId}' is already admitted!` });
            }

            const added = await redis.zadd(WAITING_QUEUE_KEY, 'NX', Date.now(), userId);
            const rank = await redis.zrank(WAITING_QUEUE_KEY, userId);

            return JSON.stringify({
                success: true,
                status: 'queued',
                message: `User '${userId}' joined admission queue at position #${rank + 1}.`,
                position: rank + 1
            });
        } catch (err) {
            return JSON.stringify({ success: false, error: err.message });
        }
    }
});

// Export all agent tools
export const slotGuardTools = [
    getAvailableSlotsTool,
    holdSlotTool,
    confirmBookingTool,
    joinWaitlistTool,
    joinWaitingRoomTool
];
