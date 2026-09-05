-- Atomic confirmation check via Redis Lua script.
-- Validates hold state and removes key from holds:ttl ZSET.
--
-- KEYS[1] = seat:{seat_id}
-- ARGV[1] = user_id
--
-- Returns:
--   1 = hold valid, state marked as CONFIRMING
--   0 = hold invalid or expired

local current = redis.call('GET', KEYS[1])
local expected = 'HELD:' .. ARGV[1]

if current == expected then
    -- Mark state as CONFIRMING and remove from TTL sorted set so it won't be auto-expired during DB transaction
    redis.call('SET', KEYS[1], 'CONFIRMING:' .. ARGV[1], 'EX', 30)
    local seat_id = string.sub(KEYS[1], 6)
    redis.call('ZREM', 'holds:ttl', seat_id)
    return 1
else
    return 0
end
