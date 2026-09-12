import express from 'express';
import Redis from 'ioredis';
import { authMiddleware } from './middleware/auth.js';
import { createRateLimiter } from './middleware/rateLimit.js';
import { pool } from './db.js';

const app = express();
app.use(express.json());

const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

// Define Redis Lua Commands
redis.defineCommand('holdSeat', {
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

redis.defineCommand('checkRateLimit', {
    numberOfKeys: 1,
    lua: `
        local key = KEYS[1]
        local limit = tonumber(ARGV[1])
        local window = tonumber(ARGV[2])
        local now = tonumber(ARGV[3])
        local bucket = redis.call('HMGET', key, 'tokens', 'last_updated')
        local tokens = tonumber(bucket[1])
        local last_updated = tonumber(bucket[2])

        if not tokens then
            tokens = limit - 1
            last_updated = now
            redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
            redis.call('EXPIRE', key, math.ceil(window * 2))
            return 1
        else
            local elapsed = now - last_updated
            if elapsed < 0 then elapsed = 0 end
            local refill = elapsed * (limit / window)
            tokens = math.min(limit, tokens + refill)
            last_updated = now

            if tokens >= 1 then
                tokens = tokens - 1
                redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
                redis.call('EXPIRE', key, math.ceil(window * 2))
                return 1
            else
                redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
                redis.call('EXPIRE', key, math.ceil(window * 2))
                return 0
            end
        end
    `
});

redis.defineCommand('confirmHold', {
    numberOfKeys: 1,
    lua: `
        local current = redis.call('GET', KEYS[1])
        if current == ARGV[1] then
            redis.call('SET', KEYS[1], 'CONFIRMED')
            return 1
        elseif not current then
            return -1 -- Expired or doesn't exist
        else
            return 0  -- Held by someone else or already confirmed
        end
    `
});

const rateLimiterMiddleware = createRateLimiter(redis);

// Secure Route: Auth -> Rate Limit -> Hold Logic
app.post('/api/v1/slots/:id/hold', authMiddleware, rateLimiterMiddleware, async (req, res) => {
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
            return res.json({ message: `Seat ${seatId} held successfully for user ${userId}` });
        } else {
            return res.status(409).json({ error: `Seat ${seatId} is already held or confirmed` });
        }
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: "Internal server error" });
    }
});

app.post('/api/v1/slots/:id/confirm', authMiddleware, rateLimiterMiddleware, async (req, res) => {
    const seatId = req.params.id;
    const userId = req.user.id;
    const seatKey = `seat:${seatId}`;

    try {
        // 1. Verify and update the hold in Redis atomically to 'CONFIRMED'
        const redisResult = await redis.confirmHold(seatKey, userId);

        if (redisResult === -1) {
            return res.status(400).json({ error: 'Hold has expired or does not exist.' });
        }
        if (redisResult === 0) {
            return res.status(403).json({ error: 'Hold belongs to another user or seat is already confirmed.' });
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

            return res.status(201).json({
                success: true,
                message: `Seat ${seatId} successfully booked!`,
                bookingId: result.rows[0].id,
                timestamp: result.rows[0].created_at
            });

        } catch (dbErr) {
            await client.query('ROLLBACK');

            // If DB write failed, revert Redis state or handle unique constraint gracefully
            if (dbErr.code === '23505') {
                return res.status(409).json({ error: 'Conflict: Seat was already confirmed.' });
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


app.listen(3000, () => {
    console.log("SlotGuard engine running on port 3000");
});