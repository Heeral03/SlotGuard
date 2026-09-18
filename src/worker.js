import { Worker } from 'bullmq';
import Redis from 'ioredis';
import { pool } from './db.js';
import { slotQueue } from './queue.js';
import { setupGracefulShutdown } from './shutdown.js';
import { runAdmissionCycle, ADMISSION_INTERVAL_MS } from './waitingRoom.js';
import { registerRedisCommands } from './redisCommands.js';
import { ADMISSION_CHANNEL, REASSIGNMENT_CHANNEL } from './waitingRoom.js';
const connection = registerRedisCommands(
    new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
        maxRetriesPerRequest: null,
    })
);

const WORKER_ID = process.env.WORKER_ID || process.env.HOSTNAME || 'worker-1';
const ttlSeconds = 60;

const admissionInterval = setInterval(async () => {
    try {
        const admitted = await runAdmissionCycle(connection);
        if (admitted.length > 0) {
            console.log(`[${WORKER_ID}] [waiting-room] Admitted batch: ${admitted.join(', ')}`);

            for(let i=0;i<admitted.length;i++){
                await connection.publish(ADMISSION_CHANNEL,admitted[i]);
            }




        }
    } catch (err) {
        console.error(`[${WORKER_ID}] [waiting-room] Admission cycle error:`, err);
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
                console.log(`[${WORKER_ID}] [hold-expiry] Seat ${seatId} was confirmed in time. No action needed.`);
                return;
            }

            console.log(`[${WORKER_ID}] [hold-expiry] Seat ${seatId} expired unclaimed. Checking waitlist.`);
            const waitlistKey = `waitlist:${seatId}`;
            const popped = await connection.zpopmin(waitlistKey);

            if (popped.length === 0) {
                console.log(`[${WORKER_ID}] [hold-expiry] No one on waitlist for seat ${seatId}.`);
            } else {
                const nextUser = popped[0];
                const result = await connection.holdSeat(`seat:${seatId}`, nextUser, ttlSeconds);

                if (result === 1) {
                    console.log(`[${WORKER_ID}] [hold-expiry] Reassigned seat ${seatId} to waitlisted user ${nextUser}.`);
                    await slotQueue.add('check-hold-expiry', { seatId }, { delay: ttlSeconds * 1000 });
                    await connection.publish(REASSIGNMENT_CHANNEL, JSON.stringify({ userId: nextUser, seatId }));
                }else {
                    console.log(`[${WORKER_ID}] [hold-expiry] Failed to reassign seat ${seatId} to ${nextUser} — seat was already held.`);
                }
            }

        } catch (err) {
            console.error(`[${WORKER_ID}] [hold-expiry] Error processing seatId ${seatId}:`, err);
        }
    },
    { connection }
);

worker.on('failed', (job, err) => {
    console.error(`[${WORKER_ID}] [hold-expiry] Job ${job.id} failed:`, err);
});

setupGracefulShutdown({
    worker,
    redis: connection,
    pool,
    queue: slotQueue,
    intervals: [admissionInterval],
    name: 'Worker'
});