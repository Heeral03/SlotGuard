import path from 'path';
import { fileURLToPath } from 'url';
import jwt from 'jsonwebtoken';
import express from 'express';
import Redis from 'ioredis';
import { authMiddleware } from './middleware/auth.js';
import { createRateLimiter } from './middleware/rateLimit.js';
import { createIdempotencyMiddleware } from './middleware/idempotency.js';
import { pool } from './db.js';
import { slotQueue } from './queue.js';
import { setupBullBoard } from './dashboard.js';
import { createHealthRouter } from './health.js';
import { setupGracefulShutdown } from './shutdown.js';
import { createAdmissionMiddleware, WAITING_QUEUE_KEY, admittedKey } from './waitingRoom.js';
import { registerRedisCommands } from './redisCommands.js';
import { ADMISSION_CHANNEL } from './waitingRoom.js';
import { addClient, removeClient, sseClients } from './sseConnections.js';
import { REASSIGNMENT_CHANNEL } from './waitingRoom.js';
import { runClinicAgent } from './agent/langgraphAgent.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const JWT_SECRET = process.env.JWT_SECRET || 'super_secret_dev_key';

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, '../public')));

const redis = registerRedisCommands(new Redis(process.env.REDIS_URL || 'redis://localhost:6379'));
const subscriber = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');


subscriber.subscribe(ADMISSION_CHANNEL, REASSIGNMENT_CHANNEL, (err) => {
    if (err) {
        console.error('Failed to subscribe to channels:', err);
    } else {
        console.log(`Subscribed to ${ADMISSION_CHANNEL} and ${REASSIGNMENT_CHANNEL}`);
    }
});

subscriber.on('message', (channel, message) => {
    if (channel === ADMISSION_CHANNEL) {
        const userId = message;
        const clientRes = sseClients.get(userId);

        if (clientRes) {
            clientRes.write(`data: ${JSON.stringify({ status: 'admitted' })}\n\n`);
            console.log(`[SSE] Pushed admission notice to ${userId}`);
        } else {
            console.log(`[SSE] ${userId} was admitted but has no open stream connection.`);
        }

    } else if (channel === REASSIGNMENT_CHANNEL) {
        const { userId, seatId } = JSON.parse(message);
        const clientRes = sseClients.get(userId);

        if (clientRes) {
            clientRes.write(`data: ${JSON.stringify({ status: 'seat_reassigned', seatId })}\n\n`);
            console.log(`[SSE] Pushed reassignment notice for seat ${seatId} to ${userId}`);
        } else {
            console.log(`[SSE] ${userId} was reassigned seat ${seatId} but has no open stream connection.`);
        }
    }
});

// Mount Bull Board dashboard and Health Check endpoint
setupBullBoard(app, '/admin/queues');
app.use(createHealthRouter(redis));

const rateLimiterMiddleware = createRateLimiter(redis);
const idempotencyMiddleware = createIdempotencyMiddleware(redis);
const admissionMiddleware = createAdmissionMiddleware(redis);

// Secure Route: Auth -> Admission -> Rate Limit -> Hold Logic
app.post('/api/v1/slots/:id/hold', authMiddleware, admissionMiddleware, rateLimiterMiddleware, async (req, res) => {
    const seatId = req.params.id;
    const userId = req.user.id; // Extracted safely from verified JWT token
    const ttlSeconds = 60;

    try {
        const result = await redis.holdSeat(`seat:${seatId}`, userId, ttlSeconds);
        if (result === 1) {
            // Check PostgreSQL fallback in case Redis key was flushed/expired but seat is confirmed
            const dbCheck = await pool.query("SELECT 1 FROM bookings WHERE seat_id = $1 AND status = 'CONFIRMED' LIMIT 1;", [seatId]);
            if (dbCheck.rows.length > 0) {
                await redis.set(`seat:${seatId}`, 'CONFIRMED');
                return res.status(409).json({ error: `Seat ${seatId} is already confirmed` });
            }

            await slotQueue.add('check-hold-expiry', { seatId }, { delay: ttlSeconds * 1000 });
            
            return res.json({ message: `Seat ${seatId} held successfully for user ${userId}` });
        } else {
            return res.status(409).json({ error: `Seat ${seatId} is already held or confirmed` });
        }
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: "Internal server error" });
    }
});

app.post('/api/v1/slots/:id/confirm', authMiddleware, idempotencyMiddleware, rateLimiterMiddleware, async (req, res) => {
    const seatId = req.params.id;
    const userId = req.user.id;
    const seatKey = `seat:${seatId}`;

    try {
        // 1. Verify and update the hold in Redis atomically to 'CONFIRMED'
        const redisResult = await redis.confirmHold(seatKey, userId);

        if (redisResult === -1) {
            const body = { error: 'Hold has expired or does not exist.' };
            await req.cacheIdempotentResponse(400, body);
            return res.status(400).json(body);
        }
        if (redisResult === 0) {
            const body = { error: 'Hold belongs to another user or seat is already confirmed.' };
            await req.cacheIdempotentResponse(403, body);
            return res.status(403).json(body);           
        }

        // 2. Redis hold confirmed! Now write permanently to PostgreSQL inside an ACID transaction
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

            const body = {
                success: true,
                message: `Seat ${seatId} successfully booked!`,
                bookingId: result.rows[0].id,
                timestamp: result.rows[0].created_at
            };

            await req.cacheIdempotentResponse(201, body);
            return res.status(201).json(body);

        } catch (dbErr) {
            await client.query('ROLLBACK');

            // If DB write failed, revert Redis state or handle unique constraint gracefully
            if (dbErr.code === '23505') {
                const body = { error: 'Conflict: Seat was already confirmed.' };
                await req.cacheIdempotentResponse(409, body);
                return res.status(409).json(body);
            }
            throw dbErr;
        } finally {
            client.release();
        }

    } catch (err) {
        console.error('Confirmation error:', err);
        return res.status(500).json({ error: 'Internal server error' });
    }
});

app.post('/api/v1/slots/:id/waitlist', authMiddleware, async (req, res) => {
    const seatId = req.params.id;
    const userId = req.user.id;

    const waitlistKey = `waitlist:${seatId}`;
    try {
        await redis.zadd(waitlistKey, Date.now(), userId);
        return res.status(201).json({ message: `User ${userId} joined the waitlist for seat ${seatId}` });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: "Internal server error" });
    }
});

app.post('/api/v1/queue/join', authMiddleware, async (req, res) => {
    const userId = req.user.id;
    try {
        // NX = only add if not already present, so re-joining doesn't reset their position
        await redis.zadd(WAITING_QUEUE_KEY, 'NX', Date.now(), userId);
        return res.status(201).json({ message: 'Joined the queue.' });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/api/v1/queue/status', authMiddleware, async (req, res) => {
    const userId = req.user.id;
    try {
        const admitted = await redis.get(admittedKey(userId));
        if (admitted) {
            return res.json({ status: 'admitted' });
        }

        const rank = await redis.zrank(WAITING_QUEUE_KEY, userId);
        if (rank === null) {
            return res.status(404).json({ error: 'You have not joined the queue yet.' });
        }

        return res.json({ status: 'waiting', position: rank + 1 });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/api/v1/queue/stream', authMiddleware, (req, res) => {
    const userId = req.user.id;

    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
    });

    addClient(userId, res);

    // Send an initial event immediately, so the client knows the
    // connection is genuinely open (useful for debugging/confirming).
    res.write(`data: ${JSON.stringify({ status: 'connected' })}\n\n`);

    // Cleanup: when the client disconnects (closes tab, loses network,
    // or the connection is otherwise terminated), Express/Node fires
    // this event. Without this, sseClients would accumulate dead entries.
    req.on('close', () => {
        removeClient(userId);
        console.log(`SSE connection closed for ${userId}`);
    });
});

app.post('/api/v1/auth/login', (req, res) => {
    const { userId } = req.body;
    if (!userId) {
        return res.status(400).json({ error: 'userId is required' });
    }
    const token = jwt.sign({ id: userId }, JWT_SECRET, { expiresIn: '24h' });
    return res.json({ token, userId });
});

app.post('/api/v1/agent/chat', async (req, res) => {
    const { message, userId } = req.body;
    if (!message) {
        return res.status(400).json({ error: 'message string is required' });
    }
    try {
        const result = await runClinicAgent(message, userId || 'user_1');
        return res.json(result);
    } catch (err) {
        console.error('Agent execution error:', err);
        return res.status(500).json({ error: err.message });
    }
});

app.get('/api/v1/slots', async (req, res) => {
    try {
        const numSlots = 24;
        const slots = [];
        
        let dbRows = [];
        try {
            const dbRes = await pool.query("SELECT seat_id, user_id FROM bookings WHERE status = 'CONFIRMED'");
            dbRows = dbRes.rows;
        } catch (dbErr) {
            console.warn('DB query error on slots fetch:', dbErr.message);
        }
        const confirmedMap = new Map();
        dbRows.forEach(r => confirmedMap.set(String(r.seat_id), r.user_id));

        for (let i = 1; i <= numSlots; i++) {
            const seatId = String(i);
            const seatKey = `seat:${seatId}`;
            
            if (confirmedMap.has(seatId)) {
                slots.push({
                    id: seatId,
                    status: 'CONFIRMED',
                    userId: confirmedMap.get(seatId)
                });
            } else {
                const heldUser = await redis.get(seatKey);
                const ttl = await redis.ttl(seatKey);
                if (heldUser) {
                    if (heldUser === 'CONFIRMED') {
                        slots.push({
                            id: seatId,
                            status: 'CONFIRMED'
                        });
                    } else {
                        slots.push({
                            id: seatId,
                            status: 'HELD',
                            userId: heldUser,
                            ttl: ttl > 0 ? ttl : 0
                        });
                    }
                } else {
                    const waitlistCount = await redis.zcard(`waitlist:${seatId}`);
                    slots.push({
                        id: seatId,
                        status: 'AVAILABLE',
                        waitlistCount
                    });
                }
            }
        }
        return res.json({ slots });
    } catch (err) {
        console.error('Error fetching slots:', err);
        return res.status(500).json({ error: 'Failed to fetch slots' });
    }
});



const PORT = process.env.PORT || 3000;
const server = app.listen(PORT, () => {
    console.log(`SlotGuard engine running on port ${PORT}`);
});

setupGracefulShutdown({
    server,
    redis,
    pool,
    queue: slotQueue,
    name: 'Server'
});