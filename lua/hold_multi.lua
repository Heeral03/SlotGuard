-- Multi-seat atomic hold via Redis Lua script.
-- All-or-nothing: checks ALL seats are free, then holds ALL — or holds NONE.
--
-- KEYS = [seat:45, seat:46, ...]   (one key per requested seat)
-- ARGV[1] = user_id
-- ARGV[2] = TTL in seconds
-- ARGV[3] = count (number of seats, i.e. #KEYS)
--
-- Returns:
--   1           = all seats held successfully
--   0           = at least one seat was taken; nothing was held
--   (If 0, the second return value is a comma-separated list of taken keys)

local count = tonumber(ARGV[3])
local user_id = ARGV[1]
local ttl = tonumber(ARGV[2])

-- Phase 1: CHECK all seats are free
local taken = {}
for i = 1, count do
    local current = redis.call('GET', KEYS[i])
    if current ~= false then
        table.insert(taken, KEYS[i] .. '=' .. current)
    end
end

if #taken > 0 then
    -- At least one seat is taken — hold NOTHING
    return {0, table.concat(taken, ',')}
end

-- Phase 2: HOLD all seats (all were free)
for i = 1, count do
    redis.call('SET', KEYS[i], 'HELD:' .. user_id)
    redis.call('EXPIRE', KEYS[i], ttl)
end

return {1, 'OK'}
