import http from 'k6/http';
import { check } from 'k6';
import { SharedArray } from 'k6/data';

const tokens = new SharedArray('tokens', function () {
  return JSON.parse(open('./tokens.json'));
});

// Change this number to scale the test up/down.
// Start at 500, and if it passes cleanly, try 1000, then higher,
// until you find the point where it genuinely starts to struggle --
// THAT number is your real, honest "handles X concurrent users" claim.
const CONCURRENT_USERS = 1000;

export const options = {
  scenarios: {
    capacity: {
      executor: 'shared-iterations',
      vus: CONCURRENT_USERS,
      iterations: CONCURRENT_USERS,
      maxDuration: '30s',
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<500'],
    http_req_failed: ['rate<0.01'],
  },
};

const BASE_URLS = ['http://localhost:8080'];

// setup() runs once per test invocation, giving us a fresh, unique
// run ID each time — so re-running this script never collides with
// seats still locked (60s TTL) from a previous run.
export function setup() {
  const runId = Date.now();
  console.log(`This run's unique ID: ${runId}`);
  return { runId };
}

export default function (data) {
  const user = tokens[__VU - 1];
  const targetUrl = BASE_URLS[(__VU - 1) % BASE_URLS.length];
  // __ITER distinguishes repeat iterations by the SAME VU
  const seatId = `capacity_seat_${data.runId}_${__VU}_${__ITER}`;

  const res = http.post(
    `${targetUrl}/api/v1/slots/${seatId}/hold`,
    null,
    {
      headers: {
        Authorization: `Bearer ${user.token}`,
        'Content-Type': 'application/json',
      },
    }
  );

  check(res, {
    'hold succeeded (200)': (r) => r.status === 200,
  });

  // Log the exact details of any unexpected failure, so we can
  // diagnose it instead of just knowing "something" failed.
  if (res.status !== 200) {
    console.error(`VU ${__VU} FAILED: status=${res.status} body=${res.body}`);
  }
}