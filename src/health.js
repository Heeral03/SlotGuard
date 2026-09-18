import express from 'express';
import { pool } from './db.js';

export function createHealthRouter(redisClient) {
    const router = express.Router();

    router.get('/health', async (req, res) => {
        const healthStatus = {
            status: 'ok',
            timestamp: new Date().toISOString(),
            services: {
                postgres: 'down',
                redis: 'down'
            }
        };

        let isHealthy = true;

        // Check PostgreSQL connectivity
        try {
            await pool.query('SELECT 1');
            healthStatus.services.postgres = 'up';
        } catch (err) {
            healthStatus.services.postgres = 'down';
            healthStatus.services.postgresError = err.message;
            isHealthy = false;
        }

        // Check Redis connectivity
        try {
            const pong = await redisClient.ping();
            if (pong === 'PONG') {
                healthStatus.services.redis = 'up';
            } else {
                healthStatus.services.redis = 'down';
                isHealthy = false;
            }
        } catch (err) {
            healthStatus.services.redis = 'down';
            healthStatus.services.redisError = err.message;
            isHealthy = false;
        }

        healthStatus.status = isHealthy ? 'ok' : 'degraded';
        const statusCode = isHealthy ? 200 : 503;

        return res.status(statusCode).json(healthStatus);
    });

    return router;
}
