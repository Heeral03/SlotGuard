// Test.js
import jwt from 'jsonwebtoken';

const JWT_SECRET = 'super_secret_dev_key';
const BASE_URL = 'http://localhost:3000';

// ---------- Helpers ----------

function makeToken(userId) {
    return jwt.sign({ id: userId }, JWT_SECRET, { expiresIn: '1h' });
}

async function hitEndpoint(seatId, token) {
    const start = performance.now();
    let status;
    try {
        const res = await fetch(`${BASE_URL}/api/v1/slots/${seatId}/hold`, {
            method: 'POST',
            headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }
        });
        status = res.status;
    } catch {
        status = 0; // network-level failure
    }
    return { status, latency: performance.now() - start };
}

function percentile(sortedArr, p) {
    if (sortedArr.length === 0) return 0;
    const idx = Math.ceil((p / 100) * sortedArr.length) - 1;
    return sortedArr[Math.max(0, idx)];
}

// ---------- CORE PROOF: fire N requests from ONE user, EXACTLY simultaneously ----------

async function proveRateLimitHoldsUnderConcurrency(userId, burstSize, limit, seatOffset) {
    const token = makeToken(userId);

    // Each test call gets its OWN non-overlapping seat range via seatOffset,
    // so leftover holds from a previous test never contaminate this one.
    const tasks = Array.from({ length: burstSize }, (_, i) =>
        hitEndpoint(seatOffset + i, token)
    );

    const start = performance.now();
    const results = await Promise.all(tasks);
    const wallTime = (performance.now() - start) / 1000;

    const allowed = results.filter(r => r.status === 200).length;
    const limited = results.filter(r => r.status === 429).length;

    const otherResults = results.filter(r => r.status !== 200 && r.status !== 429);
    const other = otherResults.length; // count kept SEPARATE from the array itself

    if (other > 0) {
        console.log('Non-200/429 statuses found:', otherResults.map(r => r.status));
    }

    const latencies = results.map(r => r.latency).sort((a, b) => a - b);

    console.log(`\n===== Concurrency Proof: ${burstSize} simultaneous requests, limit=${limit} =====`);
    console.log(`Allowed (200):        ${allowed}`);
    console.log(`Rate-limited (429):   ${limited}`);
    console.log(`Other/errors:         ${other}`);
    console.log(`CORRECTNESS CHECK -> allowed === limit? ${allowed === limit ? 'PASS ✅' : 'FAIL ❌ (bucket over/under-allowed!)'}`);
    console.log(`Wall clock: ${wallTime.toFixed(3)}s | Throughput: ${(burstSize / wallTime).toFixed(1)} req/sec`);
    console.log(`Latency (ms) -> min: ${latencies[0]?.toFixed(2)}, p50: ${percentile(latencies, 50).toFixed(2)}, ` +
        `p95: ${percentile(latencies, 95).toFixed(2)}, p99: ${percentile(latencies, 99).toFixed(2)}, max: ${latencies.at(-1)?.toFixed(2)}`);

    return { allowed, limited, other, burstSize };
}

// ---------- FAIRNESS PROOF: many DIFFERENT users, hammering concurrently ----------

async function proveFairnessAcrossUsers(userCount, requestsPerUser, limit, seatOffset) {
    const tasks = [];
    let seatCounter = seatOffset; // also given its own dedicated range

    for (let u = 0; u < userCount; u++) {
        const token = makeToken(`fairness_user_${u}`);
        for (let r = 0; r < requestsPerUser; r++) {
            tasks.push(hitEndpoint(seatCounter++, token));
        }
    }

    const start = performance.now();
    const results = await Promise.all(tasks);
    const wallTime = (performance.now() - start) / 1000;

    // Group results back by user to verify EACH user independently got exactly `limit` allowed
    const perUserAllowed = {};
    let idx = 0;
    for (let u = 0; u < userCount; u++) {
        let allowedForThisUser = 0;
        for (let r = 0; r < requestsPerUser; r++) {
            if (results[idx].status === 200) allowedForThisUser++;
            idx++;
        }
        perUserAllowed[`user_${u}`] = allowedForThisUser;
    }

    const expectedPerUser = Math.min(limit, requestsPerUser);
    const allUsersCorrect = Object.values(perUserAllowed).every(v => v === expectedPerUser);

    console.log(`\n===== Fairness Proof: ${userCount} concurrent users x ${requestsPerUser} req each =====`);
    console.log(`Expected allowed per user: ${expectedPerUser}`);
    console.log(`Sample of per-user results:`, Object.fromEntries(Object.entries(perUserAllowed).slice(0, 5)));
    console.log(`ALL users correctly isolated (no cross-contamination)? ${allUsersCorrect ? 'PASS ✅' : 'FAIL ❌'}`);
    console.log(`Total requests: ${results.length} | Wall clock: ${wallTime.toFixed(3)}s | Throughput: ${(results.length / wallTime).toFixed(1)} req/sec`);
}

// ---------- Main ----------

async function main() {
    // Test 1: 50 simultaneous requests, seats 90000-90049
    await proveRateLimitHoldsUnderConcurrency('burst_user_1', 50, 5, 90000);

    // Test 2: 500 simultaneous requests, seats 100000-100499 -- NO overlap with Test 1
    await proveRateLimitHoldsUnderConcurrency('burst_user_2', 500, 5, 100000);

    // Test 3: fairness across users, seats 200000+ -- own dedicated range too
    await proveFairnessAcrossUsers(50, 10, 5, 200000);
}

main().catch(err => {
    console.error('Test crashed:', err);
    process.exit(1);
});