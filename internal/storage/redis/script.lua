local key = KEYS[1]
local requested_batch = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local refill_rate = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local ttl_seconds = tonumber(ARGV[5])

local data = redis.call("HMGET", key, "tokens", "last_updated")
local tokens = tonumber(data[1])
local last_updated = tonumber(data[2])

if tokens == nil then
    tokens = capacity
    last_updated = now
else
    local elapsed = math.max(0, now - last_updated)
    tokens = math.min(capacity, tokens + elapsed * refill_rate)
    last_updated = now
end

local granted = 0
if tokens >= requested_batch then
    granted = requested_batch
    tokens = tokens - requested_batch
else
    granted = math.floor(tokens)
    tokens = tokens - granted
end

redis.call("HMSET", key, "tokens", tokens, "last_updated", last_updated)
redis.call("EXPIRE", key, ttl_seconds)

local needed = requested_batch - granted
local reset_after = 0
if refill_rate > 0 and needed > 0 then
    reset_after = needed / refill_rate
end

return { granted, math.floor(tokens), math.ceil(reset_after) }
