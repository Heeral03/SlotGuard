import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';
import pkg from 'pg';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

dotenv.config({ path: path.resolve(__dirname, '../.env') });

const { Pool } = pkg;

export const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
});


export const db = pool;

// Test connection
pool.query('SELECT NOW()')
    .then(res => console.log('PostgreSQL Connected:', res.rows[0]))
    .catch(err => console.error('PostgreSQL Connection Error:', err));
