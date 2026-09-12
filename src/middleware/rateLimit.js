export function createRateLimiter(redisClient) {
    return async (req, res, next) => {
        // SECURITY: Prioritize authenticated user ID, fallback to IP address
        const identifier = req.user?.id || req.ip;
        const rateLimitKey = `ratelimit:${identifier}`;
        
        const maxCapacity = 5;  
        const windowSeconds = 60; 
        const now = Date.now() / 1000; 

        try {

            const allowed = await redisClient.checkRateLimit(rateLimitKey, maxCapacity, windowSeconds, now);

            if (allowed === 1) {
                return next();
            } else {
                res.setHeader('Retry-After', windowSeconds);
                return res.status(429).json({
                    error: 'Too Many Requests',
                    message: 'Rate limit exceeded. Please slow down.'
                });
            }
        } catch (err) {
            console.error('Rate limiter error, failing open:', err);
            return next(); // Fail-open pattern
        }
    };
}