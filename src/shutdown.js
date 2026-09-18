export function setupGracefulShutdown({ server, worker, redis, pool, queue, intervals = [], name = 'Service', timeoutMs = 10000 }) {
    let isShuttingDown = false;

    const shutdown = async (signal) => {
        if (isShuttingDown) {
            console.log(`[${name}] Shutdown already in progress. Ignoring duplicate signal ${signal}.`);
            return;
        }
        isShuttingDown = true;
        console.log(`\n[${name}] Received ${signal}. Starting graceful shutdown...`);

        // Hard fallback timeout limit (e.g. 10s) to force process exit if active operations hang
        const forceExitTimeout = setTimeout(() => {
            console.error(`[${name}] Graceful shutdown timed out after ${timeoutMs}ms. Forcing process exit.`);
            process.exit(1);
        }, timeoutMs);

        // Prevent timeout from holding event loop active if all cleanups finish early
        if (forceExitTimeout.unref) {
            forceExitTimeout.unref();
        }

        try {
            // 0. Clear active intervals (e.g. background worker intervals)
            if (Array.isArray(intervals)) {
                for (const interval of intervals) {
                    if (interval) {
                        clearInterval(interval);
                    }
                }
                if (intervals.length > 0) {
                    console.log(`[${name}] Cleared ${intervals.length} active interval(s).`);
                }
            }

            // 1. Stop HTTP server from receiving new requests
            if (server) {
                console.log(`[${name}] Closing HTTP server...`);
                await new Promise((resolve) => server.close(resolve));
                console.log(`[${name}] HTTP server closed.`);
            }

            // 2. Stop BullMQ worker (waits for active job processing to finish)
            if (worker) {
                console.log(`[${name}] Closing BullMQ worker (draining active jobs)...`);
                await worker.close();
                console.log(`[${name}] BullMQ worker closed.`);
            }

            // 3. Close BullMQ Queue connection
            if (queue) {
                console.log(`[${name}] Closing BullMQ queue connection...`);
                await queue.close();
                console.log(`[${name}] BullMQ queue connection closed.`);
            }

            // 4. Close Redis client connection
            if (redis) {
                console.log(`[${name}] Closing Redis connection...`);
                await redis.quit();
                console.log(`[${name}] Redis connection closed.`);
            }

            // 5. Close PostgreSQL database connection pool
            if (pool) {
                console.log(`[${name}] Closing PostgreSQL pool...`);
                await pool.end();
                console.log(`[${name}] PostgreSQL pool closed.`);
            }

            clearTimeout(forceExitTimeout);
            console.log(`[${name}] Graceful shutdown completed cleanly.`);
            process.exit(0);
        } catch (err) {
            console.error(`[${name}] Error during graceful shutdown:`, err);
            clearTimeout(forceExitTimeout);
            process.exit(1);
        }
    };

    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));
}
