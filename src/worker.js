import { Worker } from 'bullmq';
import Redis from 'ioredis';
import { pool } from './db.js';
import { slotQueue } from './queue.js';
import { setupGracefulShutdown } from './shutdown.js';
import { runAdmissionCycle, ADMISSION_INTERVAL_MS } from './waitingRoom.js';
const connection = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
    maxRetriesPerRequest: null,
});

connection.defineCommand('holdSeat', {
    numberOfKeys: 1,
    lua: `
        local current = redis.call('GET', KEYS[1])
        if current == false then
            redis.call('SET', KEYS[1], ARGV[1])
            redis.call('EXPIRE', KEYS[1], ARGV[2])
            return 1
        else
            return 0
        end
    `
});

const ttlSeconds = 60;

setInterval(async () => {
    try {
        const admitted = await runAdmissionCycle(connection);
        if (admitted.length > 0) {
            console.log(`[waiting-room] Admitted batch: ${admitted.join(', ')}`);
        }
    } catch (err) {
        console.error('[waiting-room] Admission cycle error:', err);
    }
}, ADMISSION_INTERVAL_MS);

const worker = new Worker(
    'hold-expiry',
    async (job) => {
        const { seatId } = job.data;

        try {
            const dbCheck = await pool.query(
                "SELECT 1 FROM bookings WHERE seat_id = $1 AND status = 'CONFIRMED' LIMIT 1;",
                [seatId]
            );

            if (dbCheck.rows.length > 0) {
                console.log(`[hold-expiry] Seat ${seatId} was confirmed in time. No action needed.`);
                return;
            }

            console.log(`[hold-expiry] Seat ${seatId} expired unclaimed. Checking waitlist.`);
            const waitlistKey = `waitlist:${seatId}`;
            const popped = await connection.zpopmin(waitlistKey);

            if (popped.length === 0) {
                console.log(`[hold-expiry] No one on waitlist for seat ${seatId}.`);
            } else {
                const nextUser = popped[0];
                const result = await connection.holdSeat(`seat:${seatId}`, nextUser, ttlSeconds);

                if (result === 1) {
                    console.log(`[hold-expiry] Reassigned seat ${seatId} to waitlisted user ${nextUser}.`);
                    await slotQueue.add('check-hold-expiry', { seatId }, { delay: ttlSeconds * 1000 });
                } else {
                    console.log(`[hold-expiry] Failed to reassign seat ${seatId} to ${nextUser} — seat was already held.`);
                }
            }

        } catch (err) {
            console.error(`[hold-expiry] Error processing seatId ${seatId}:`, err);
        }
    },
    { connection }
);

worker.on('failed', (job, err) => {
    console.error(`[hold-expiry] Job ${job.id} failed:`, err);
});

setupGracefulShutdown({
    worker,
    redis: connection,
    pool,
    queue: slotQueue,
    name: 'Worker'
});