import jwt from 'jsonwebtoken';
import { pool } from '../src/db.js';
import Redis from 'ioredis';

const JWT_SECRET = 'super_secret_dev_key';
const BASE_URL = 'http://localhost:3000';

function makeToken(userId) {
    return jwt.sign({ id: userId }, JWT_SECRET, { expiresIn: '1h' });
}

async function requestHold(seatId, token) {
    const start = performance.now();
    try {
        const res = await fetch(`${BASE_URL}/api/v1/slots/${seatId}/hold`, {
            method: 'POST',
            headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }
        });
        const data = await res.json().catch(() => ({}));
        return { status: res.status, data, latency: performance.now() - start };
    } catch (err) {
        console.error(`fetch error on hold ${seatId}:`, err.message);
        return { status: 0, error: err.message, latency: performance.now() - start };
    }
}

async function requestConfirm(seatId, token) {
    const start = performance.now();
    try {
        const res = await fetch(`${BASE_URL}/api/v1/slots/${seatId}/confirm`, {
            method: 'POST',
            headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }
        });
        const data = await res.json().catch(() => ({}));
        return { status: res.status, data, latency: performance.now() - start };
    } catch (err) {
        return { status: 0, error: err.message, latency: performance.now() - start };
    }
}

function printSection(title) {
    const msg = `\n==================================================\n  ${title}\n==================================================\n`;
    process.stdout.write(msg);
}

async function runTestSuite() {
    console.log('🚀 Starting SlotGuard Comprehensive Test Suite...\n');

    // Wait for server readiness
    for (let i = 0; i < 20; i++) {
        try {
            await fetch(`${BASE_URL}/api/v1/slots/health_check/hold`, { method: 'POST' });
            break;
        } catch {
            await new Promise(r => setTimeout(r, 200));
        }
    }

    let totalPassed = 0;
    let totalFailed = 0;

    const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

    // -------------------------------------------------------------
    // TEST 1: Rate Limiting - Concurrency Burst (50 Concurrent Reqs)
    // -------------------------------------------------------------
    printSection('TEST 1: Rate Limiting - Concurrency Burst (50 Concurrent Reqs)');
    {
        const userId = `rl_burst_user_${Date.now()}`;
        const token = makeToken(userId);
        const burstSize = 50;
        const promises = [];

        for (let i = 0; i < burstSize; i++) {
            promises.push(requestHold(`seat_burst_${Date.now()}_${i}`, token));
        }

        const results = await Promise.all(promises);
        const allowed = results.filter(r => r.status === 200).length;
        const rateLimited = results.filter(r => r.status === 429).length;
        const errors = results.filter(r => r.status !== 200 && r.status !== 429).length;

        console.log(`- Allowed (200): ${allowed} (Expected: 5)`);
        console.log(`- Rate Limited (429): ${rateLimited} (Expected: 45)`);
        console.log(`- Other/Errors: ${errors}`);

        if (allowed === 5 && rateLimited === 45 && errors === 0) {
            console.log('✅ TEST 1 PASSED: Burst rate limiting enforced exactly at 5 requests.');
            totalPassed++;
        } else {
            console.log('❌ TEST 1 FAILED: Burst rate limiting failed threshold check.');
            totalFailed++;
        }
    }

    // -------------------------------------------------------------
    // TEST 2: Rate Limiting - Continuous Polling Starvation Check
    // -------------------------------------------------------------
    printSection('TEST 2: Rate Limiting - Refill & Starvation Prevention');
    {
        const userId = `rl_refill_user_${Date.now()}`;
        const token = makeToken(userId);

        // Exhaust all 5 tokens
        for (let i = 0; i < 5; i++) {
            await requestHold(`seat_refill_init_${Date.now()}_${i}`, token);
        }

        // Verify exhausted
        const exhaustedCheck = await requestHold(`seat_refill_exhausted_${Date.now()}`, token);
        console.log(`- Initial exhausted status: ${exhaustedCheck.status} (Expected: 429)`);

        // Send requests every 4 seconds for 20 seconds (t=4, 8, 12, 16, 20)
        // With limit=5, window=60, 1 token SHOULD refill every 12 seconds.
        // In 20 seconds, at least 1 request MUST succeed (200 OK).
        console.log('- Sending requests every 4s for 20s total...');
        let got200 = false;
        const statuses = [];
        const testRunId = Date.now();
        for (let step = 1; step <= 5; step++) {
            await new Promise(r => setTimeout(r, 4000));
            const res = await requestHold(`seat_refill_step_${testRunId}_${step}`, token);
            statuses.push(res.status);
            if (res.status === 200) got200 = true;
        }

        console.log(`- Polling statuses over 20s: [${statuses.join(', ')}]`);
        console.log(`- At least one token refilled within 20s? ${got200 ? 'YES' : 'NO'}`);

        if (exhaustedCheck.status === 429 && got200) {
            console.log('✅ TEST 2 PASSED: Token refill worked properly without starvation.');
            totalPassed++;
        } else {
            console.log('❌ TEST 2 FAILED: Token bucket starvation detected! Frequent polling prevented token refill.');
            totalFailed++;
        }
    }

    // -------------------------------------------------------------
    // TEST 3: Rate Limiting - Multi-User Isolation
    // -------------------------------------------------------------
    printSection('TEST 3: Rate Limiting - Multi-User Isolation');
    {
        const testId = Date.now();
        const tokenUserA = makeToken(`iso_user_A_${testId}`);
        const tokenUserB = makeToken(`iso_user_B_${testId}`);

        // User A uses all 5 tokens
        for (let i = 0; i < 5; i++) {
            await requestHold(`seat_iso_a_${testId}_${i}`, tokenUserA);
        }
        const userABlocked = await requestHold(`seat_iso_a_block_${testId}`, tokenUserA);

        // User B sends 1 request
        const userBAllowed = await requestHold(`seat_iso_b_allow_${testId}`, tokenUserB);

        console.log(`- User A 6th request status: ${userABlocked.status} (Expected: 429)`);
        console.log(`- User B 1st request status: ${userBAllowed.status} (Expected: 200)`);

        if (userABlocked.status === 429 && userBAllowed.status === 200) {
            console.log('✅ TEST 3 PASSED: Multi-user rate limiting is strictly isolated.');
            totalPassed++;
        } else {
            console.log('❌ TEST 3 FAILED: Rate limit state leaked across users.');
            totalFailed++;
        }
    }

    // -------------------------------------------------------------
    // TEST 4: Seat Lock - 20 Concurrent Hold Attempts on Single Seat
    // -------------------------------------------------------------
    printSection('TEST 4: Seat Lock - 20 Concurrent Hold Attempts on Single Seat');
    {
        const targetSeat = `seat_concurrent_hold_${Date.now()}`;
        const userCount = 20;
        const promises = [];

        for (let i = 0; i < userCount; i++) {
            const token = makeToken(`hold_user_${i}_${Date.now()}`);
            promises.push(requestHold(targetSeat, token));
        }

        const results = await Promise.all(promises);
        const successCount = results.filter(r => r.status === 200).length;
        const conflictCount = results.filter(r => r.status === 409).length;

        console.log(`- Holds Succeeded (200): ${successCount} (Expected: 1)`);
        console.log(`- Holds Conflicts (409): ${conflictCount} (Expected: 19)`);

        if (successCount === 1 && conflictCount === 19) {
            console.log('✅ TEST 4 PASSED: Exactly 1 user held the seat atomically.');
            totalPassed++;
        } else {
            console.log('❌ TEST 4 FAILED: Race condition detected on concurrent seat hold.');
            totalFailed++;
        }
    }

    // -------------------------------------------------------------
    // TEST 5: Seat Lock - Post-Confirmation Hold Protection
    // -------------------------------------------------------------
    printSection('TEST 5: Seat Lock - Post-Confirmation Hold Protection');
    {
        const seatId = `seat_confirmed_protection_${Date.now()}`;
        const user1Token = makeToken(`confirm_owner_${Date.now()}`);
        const user2Token = makeToken(`attacker_${Date.now()}`);

        // User 1 holds and confirms the seat
        const hold1 = await requestHold(seatId, user1Token);
        const confirm1 = await requestConfirm(seatId, user1Token);

        console.log(`- User 1 Hold Status: ${hold1.status} (Expected: 200)`);
        console.log(`- User 1 Confirm Status: ${confirm1.status} (Expected: 201)`);

        // User 2 attempts to hold the confirmed seat
        const hold2 = await requestHold(seatId, user2Token);
        console.log(`- User 2 Hold Status on Confirmed Seat: ${hold2.status} (Expected: 409)`);

        if (confirm1.status === 201 && hold2.status === 409) {
            console.log('✅ TEST 5 PASSED: Post-confirmation hold protected.');
            totalPassed++;
        } else {
            console.log('❌ TEST 5 FAILED: Allowed user to hold an already confirmed seat!');
            totalFailed++;
        }
    }

    // -------------------------------------------------------------
    // TEST 6: Seat Lock - Double Confirmation Race Condition
    // -------------------------------------------------------------
    printSection('TEST 6: Seat Lock - Concurrent Confirmations on Single Hold');
    {
        const seatId = `seat_double_confirm_${Date.now()}`;
        const token = makeToken(`double_confirm_user_${Date.now()}`);

        // Hold seat
        await requestHold(seatId, token);

        // Fire 10 concurrent confirm requests
        const promises = [];
        for (let i = 0; i < 10; i++) {
            promises.push(requestConfirm(seatId, token));
        }

        const results = await Promise.all(promises);
        const confirmSuccess = results.filter(r => r.status === 201).length;
        const confirmBlocked = results.filter(r => r.status !== 201).length;

        // Verify DB count
        const dbRes = await pool.query('SELECT COUNT(*) FROM bookings WHERE seat_id = $1;', [seatId]);
        const dbCount = parseInt(dbRes.rows[0].count, 10);

        console.log(`- Confirmations Succeeded (201): ${confirmSuccess} (Expected: 1)`);
        console.log(`- Confirmations Blocked (Non-201): ${confirmBlocked} (Expected: 9)`);
        console.log(`- PostgreSQL Rows Inserted: ${dbCount} (Expected: 1)`);

        if (confirmSuccess === 1 && confirmBlocked === 9 && dbCount === 1) {
            console.log('✅ TEST 6 PASSED: Double confirmation prevented cleanly.');
            totalPassed++;
        } else {
            console.log('❌ TEST 6 FAILED: Race condition in confirmation logic.');
            totalFailed++;
        }
    }

    // Summary
    printSection('TEST SUITE SUMMARY');
    console.log(`Total Tests Run: ${totalPassed + totalFailed}`);
    console.log(`Passed: ${totalPassed} ✅`);
    console.log(`Failed: ${totalFailed} ❌`);

    await redis.quit();
    await pool.end();
    process.exit(totalFailed > 0 ? 1 : 0);
}

runTestSuite().catch(err => {
    console.error('Test suite runner crashed:', err);
    process.exit(1);
});
