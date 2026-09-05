-- Deterministic TTL expiry scanner Lua script.
-- Scans holds:ttl ZSET for expired seat holds (score <= now),
-- removes expired seats, deletes seat key if it matches HELD:*, and triggers waitlist auto-offer.
--
-- ARGV[1] = now_timestamp
-- ARGV[2] = hold_ttl (e.g. 120)
--
-- Returns list of seat_ids that were processed for waitlist auto-offers.

local now = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])

local expired = redis.call('ZRANGEBYSCORE', 'holds:ttl', 0, now)
local processed = {}

for i = 1, #expired do
    local seat_id = expired[i]
    redis.call('ZREM', 'holds:ttl', seat_id)
    local key = 'seat:' .. seat_id
    local current = redis.call('GET', key)
    if current ~= false and string.sub(current, 1, 5) == 'HELD:' then
        redis.call('DEL', key)
        -- Attempt to pop next waitlisted user
        local next_user = redis.call('ZPOPMIN', 'waitlist:' .. seat_id)
        if #next_user > 0 then
            local user_id = next_user[1]
            redis.call('SET', key, 'HELD:' .. user_id, 'EX', ttl)
            redis.call('ZADD', 'holds:ttl', now + ttl, seat_id)
            table.insert(processed, seat_id .. ':' .. user_id)
        else
            table.insert(processed, seat_id .. ':empty')
        end
    end
end

return processed
