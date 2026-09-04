-- Waitlist → Auto-Offer Lua script.
-- Atomically pops the next user from the waitlist sorted set
-- and places a hold on the seat for them.
--
-- KEYS[1] = seat:{seat_id}
-- KEYS[2] = waitlist:{seat_id}
-- ARGV[1] = TTL in seconds for the new hold
--
-- Returns:
--   offered_user_id (string)  = hold placed for this user
--   0                         = waitlist empty or seat not free

-- Only offer if the seat is actually free
local current = redis.call('GET', KEYS[1])
if current ~= false then
    return 0  -- seat is still held or confirmed, don't offer
end

-- Pop the earliest entry (lowest score = earliest timestamp)
local next_user = redis.call('ZPOPMIN', KEYS[2])

if #next_user == 0 then
    return 0  -- waitlist empty
end

local user_id = next_user[1]  -- member (user_id as string)

-- Place the hold for this user
redis.call('SET', KEYS[1], 'HELD:' .. user_id)
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[1]))

return user_id
