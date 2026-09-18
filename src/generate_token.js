import jwt from 'jsonwebtoken';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const JWT_SECRET = process.env.JWT_SECRET || 'super_secret_dev_key';

const NUM_USERS = 1000;

const tokens = [];

for (let i = 0; i < NUM_USERS; i++) {
   const userId = `load_test_user_${i}`;
   const token = jwt.sign({ id: userId }, JWT_SECRET, { expiresIn: '1h' });
   tokens.push({ userId, token });
}

const outputPath = path.resolve(__dirname, 'tokens.json');
fs.writeFileSync(outputPath, JSON.stringify(tokens, null, 2));

console.log(`Generated ${tokens.length} tokens -> ${outputPath}`);