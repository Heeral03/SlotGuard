export function registerRedisCommands(redis) {
    redis.defineCommand('holdSeat', {
        numberOfKeys: 1,
        lua: `
            local current = redis.call('GET', KEYS[1])
            if current == false then
                redis.call('SET', KEYS[1], ARGV[1])
                redis.call('EXPIRE', KEYS[1], ARGV[2])
                return 1
            else
                return 0
            end
        `
    });

    redis.defineCommand('checkRateLimit', {
        numberOfKeys: 1,
        lua: `
            local key = KEYS[1]
            local limit = tonumber(ARGV[1])
            local window = tonumber(ARGV[2])
            local now = tonumber(ARGV[3])
            local bucket = redis.call('HMGET', key, 'tokens', 'last_updated')
            local tokens = tonumber(bucket[1])
            local last_updated = tonumber(bucket[2])

            if not tokens then
                tokens = limit - 1
                last_updated = now
                redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
                redis.call('EXPIRE', key, math.ceil(window * 2))
                return 1
            else
                local elapsed = now - last_updated
                if elapsed < 0 then elapsed = 0 end
                local refill = elapsed * (limit / window)
                tokens = math.min(limit, tokens + refill)
                last_updated = now

                if tokens >= 1 then
                    tokens = tokens - 1
                    redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
                    redis.call('EXPIRE', key, math.ceil(window * 2))
                    return 1
                else
                    redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
                    redis.call('EXPIRE', key, math.ceil(window * 2))
                    return 0
                end
            end
        `
    });

    redis.defineCommand('confirmHold', {
        numberOfKeys: 1,
        lua: `
            local current = redis.call('GET', KEYS[1])
            if current == ARGV[1] then
                redis.call('SET', KEYS[1], 'CONFIRMED')
                return 1
            elseif not current then
                return -1 -- Expired or doesn't exist
            else
                return 0  -- Held by someone else or already confirmed
            end
        `
    });

    return redis;
}
