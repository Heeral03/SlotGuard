-- Atomic seat hold via Redis Lua script.
-- Guarantees that only ONE user can hold a given seat at a time.
--
-- KEYS[1] = seat:{seat_id}
-- ARGV[1] = user_id
-- ARGV[2] = TTL in seconds (e.g. 120)
--
-- Returns:
--   1  = hold granted
--   0  = seat already held or confirmed

local current = redis.call('GET', KEYS[1])

if current == false then
    local ttl = tonumber(ARGV[2])
    redis.call('SET', KEYS[1], 'HELD:' .. ARGV[1], 'EX', ttl)
    local seat_id = string.sub(KEYS[1], 6)
    local now = tonumber(redis.call('TIME')[1])
    redis.call('ZADD', 'holds:ttl', now + ttl, seat_id)
    return 1
else
    return 0
end

