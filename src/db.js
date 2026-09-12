import 'dotenv/config';
import pkg from 'pg';
const { Pool } = pkg;

export const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
});

export const db = pool;

// Test connection
pool.query('SELECT NOW()')
    .then(res => console.log('PostgreSQL Connected:', res.rows[0]))
    .catch(err => console.error('PostgreSQL Connection Error:', err));
