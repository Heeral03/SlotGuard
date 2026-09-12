import jwt from 'jsonwebtoken';
import { pool } from './db.js';
import Redis from 'ioredis';

const JWT_SECRET = 'super_secret_dev_key';
const BASE_URL = 'http://localhost:3000';

function makeToken(userId) {
    return jwt.sign({ id: userId }, JWT_SECRET, { expiresIn: '1h' });
}

async function testFlow() {
    console.log('=== STARTING CONNECTIONS & FLOW TEST ===\n');

    // 1. Test PostgreSQL Connection
    try {
        const pgRes = await pool.query('SELECT NOW(), current_database(), current_user;');
        console.log('✅ PostgreSQL Connection: SUCCESS');
        console.log(`   Database: ${pgRes.rows[0].current_database} | User: ${pgRes.rows[0].current_user} | Time: ${pgRes.rows[0].now}`);
    } catch (err) {
        console.error('❌ PostgreSQL Connection: FAILED', err);
        process.exit(1);
    }

    // 2. Test Redis Connection
    const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');
    try {
        const redisPing = await redis.ping();
        console.log(`✅ Redis Connection: SUCCESS (Ping: ${redisPing})\n`);
    } catch (err) {
        console.error('❌ Redis Connection: FAILED', err);
        process.exit(1);
    }

    // 3. Test HTTP Endpoints against running server
    const testSeatId = `seat_test_${Date.now()}`;
    const user1Token = makeToken('user_alpha');
    const user2Token = makeToken('user_beta');

    console.log(`--- Testing Seat Hold for ${testSeatId} ---`);
    const holdRes = await fetch(`${BASE_URL}/api/v1/slots/${testSeatId}/hold`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${user1Token}`, 'Content-Type': 'application/json' }
    });
    const holdData = await holdRes.json();
    console.log(`Hold Status: ${holdRes.status}`, holdData);

    if (holdRes.status !== 200) {
        console.error('❌ Hold Failed!');
        process.exit(1);
    }

    console.log(`\n--- Testing Seat Confirmation for ${testSeatId} ---`);
    const confirmRes = await fetch(`${BASE_URL}/api/v1/slots/${testSeatId}/confirm`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${user1Token}`, 'Content-Type': 'application/json' }
    });
    const confirmData = await confirmRes.json();
    console.log(`Confirm Status: ${confirmRes.status}`, confirmData);

    if (confirmRes.status !== 201) {
        console.error('❌ Confirmation Failed!');
        process.exit(1);
    }

    // 4. Verify Record in PostgreSQL Database
    console.log(`\n--- Verifying PostgreSQL Record for ${testSeatId} ---`);
    const dbRecord = await pool.query('SELECT * FROM bookings WHERE seat_id = $1;', [testSeatId]);
    console.log('PostgreSQL Row Found:', dbRecord.rows[0]);

    if (dbRecord.rows.length === 1 && dbRecord.rows[0].status === 'CONFIRMED') {
        console.log('✅ PostgreSQL Persistence: VERIFIED CORRECT');
    } else {
        console.error('❌ PostgreSQL Persistence Check Failed!');
        process.exit(1);
    }

    // 5. Test Partial Unique Index Safety Net (Attempt double booking in DB)
    console.log(`\n--- Testing Partial Unique Index Safety Net ---`);
    try {
        await pool.query(
            "INSERT INTO bookings (seat_id, user_id, status) VALUES ($1, $2, 'CONFIRMED');",
            [testSeatId, 'user_attacker']
        );
        console.error('❌ Partial Unique Index FAILED (Allowed duplicate CONFIRMED seat!)');
        process.exit(1);
    } catch (err) {
        if (err.code === '23505') {
            console.log('✅ Partial Unique Index Safety Net: SUCCESS (Blocked duplicate CONFIRMED seat with 23505 unique_violation)');
        } else {
            console.error('Unexpected DB error during duplicate test:', err);
            process.exit(1);
        }
    }

    console.log('\n=== ALL CONNECTION & FLOW TESTS PASSED SUCCESSFULLY! ✅ ===');
    await redis.quit();
    await pool.end();
    process.exit(0);
}

testFlow().catch(err => {
    console.error('Test runner error:', err);
    process.exit(1);
});
