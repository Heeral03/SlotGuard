import jwt from 'jsonwebtoken';
import fs from 'fs';
const JWT_SECRET = 'super_secret_dev_key';

const NUM_USERS = 2000

const tokens = []

for (let i = 0; i < NUM_USERS; i++) {
   const userId = `load_test_user_${i}`;
   const token = jwt.sign({ id: userId }, JWT_SECRET, { expiresIn: '1h' });
   tokens.push({ userId, token });
}

fs.writeFileSync('tokens.json', JSON.stringify(tokens, null, 2));

console.log(`Generated ${tokens.length} tokens -> tokens.json`);