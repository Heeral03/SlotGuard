
import http from 'k6/http';
import { SharedArray } from 'k6/data';

const tokens = new SharedArray('tokens', function () {
  return JSON.parse(open('./tokens.json'));
});

export const options = {
  scenarios: {
    sustained: {
      executor: 'constant-vus',
      vus: 300,
      duration: '10s',
    },
  },
};

const BASE_URLS = ['http://localhost:3000', 'http://localhost:3001', 'http://localhost:3002'];

export default function () {
  const user = tokens[__VU - 1];
  const targetUrl = BASE_URLS[(__VU - 1) % BASE_URLS.length];
  const seatId = 'sustained_seat_' + Date.now() + '_' + __VU + '_' + __ITER;

  http.post(
    targetUrl + '/api/v1/slots/' + seatId + '/hold',
    null,
    {
      headers: {
        Authorization: 'Bearer ' + user.token,
        'Content-Type': 'application/json',
      },
    }
  );
}
