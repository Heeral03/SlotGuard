import http from 'k6/http';
import { check } from 'k6';
import { SharedArray } from 'k6/data';

// SharedArray loads the file ONCE and shares it read-only across all VUs,
// instead of every virtual user re-reading the file from disk.
const tokens = new SharedArray('tokens', function () {
  return JSON.parse(open('./tokens.json'));
});

export const options = {
  vus: 1,        // just ONE virtual user for this smoke test
  iterations: 1, // run the script body exactly once
};

const BASE_URL = 'http://localhost:3000';

export default function () {
  const user = tokens[0]; // just use the very first generated token
  const seatId = `smoke_test_seat_${Date.now()}`;

  const holdRes = http.post(
    `${BASE_URL}/api/v1/slots/${seatId}/hold`,
    null, // no body needed, your /hold route doesn't read req.body
    {
      headers: {
        Authorization: `Bearer ${user.token}`,
        'Content-Type': 'application/json',
      },
    }
  );

  console.log(`Hold response: ${holdRes.status} - ${holdRes.body}`);

  check(holdRes, {
    'hold succeeded (200)': (r) => r.status === 200,
  });
}