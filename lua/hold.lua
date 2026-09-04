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
    redis.call('SET', KEYS[1], 'HELD:' .. ARGV[1])
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
    return 1
else
    return 0
end
