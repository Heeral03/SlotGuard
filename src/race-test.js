import http from 'k6/http';
import { check } from 'k6';
import { SharedArray } from 'k6/data';

const tokens = new SharedArray('tokens', function () {
  return JSON.parse(open('./tokens.json'));
});

export const options = {
  scenarios: {
    seat_race: {
      executor: 'shared-iterations',
      vus: 50,
      iterations: 50,
      maxDuration: '10s',
    },
  },
};

const BASE_URL = 'http://localhost:3000';

// setup() runs EXACTLY ONCE, globally, before any VU starts.
// Its return value is passed into every single VU's default function call
// as the `data` argument below -- this is the correct way to share
// one value across all VUs in k6 (unlike top-level module code,
// which runs once PER VU, not once total).
export function setup() {
  const seatId = `race_test_seat_${Date.now()}`;
  console.log(`All VUs will race for: ${seatId}`);
  return { seatId };
}

export default function (data) {
  const user = tokens[__VU - 1];

  const res = http.post(
    `${BASE_URL}/api/v1/slots/${data.seatId}/hold`,
    null,
    {
      headers: {
        Authorization: `Bearer ${user.token}`,
        'Content-Type': 'application/json',
      },
    }
  );

  check(res, {
    'got a response (200 or 409)': (r) => r.status === 200 || r.status === 409,
  });

  if (res.status === 200) {
    console.log(`VU ${__VU}: WON the seat`);
  }
}