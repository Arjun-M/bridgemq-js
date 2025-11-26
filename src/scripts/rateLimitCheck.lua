-- rateLimitCheck.lua
-- Atomically check and enforce rate limits using token bucket algorithm
-- 
-- PURPOSE: Prevent job execution from exceeding configured rate limits
-- 
-- INPUTS:
--   KEYS[1] = 'bridgemq:ratelimit:{key}'
--   KEYS[2] = 'bridgemq:ratelimitqueue:{key}' (optional queue for excess jobs)
--   ARGV[1] = rate limit key
--   ARGV[2] = max requests
--   ARGV[3] = window (seconds)
--   ARGV[4] = jobId (optional, to queue if limit exceeded)
--   ARGV[5] = current timestamp (ms)
--   ARGV[6] = namespace prefix (bridgemq)
-- 
-- RETURNS: { allowed: boolean, current: number, limit: number, reset: number }
-- 
-- LOGIC:
-- 1. Get current count for rate limit key
-- 2. If count < max: Increment and allow
-- 3. If count >= max: Queue job (if jobId provided) and deny
-- 4. Set expiry on rate limit key (sliding window)

local rateLimitKey = ARGV[1]
local maxRequests = tonumber(ARGV[2])
local windowSeconds = tonumber(ARGV[3])
local jobId = ARGV[4]
local now = tonumber(ARGV[5])
local ns = ARGV[6]

local key = KEYS[1]
local queueKey = KEYS[2]

-- Get current count
local current = tonumber(redis.call('GET', key) or 0)

-- Check if under limit
if current < maxRequests then
  -- Increment counter
  local newCount = redis.call('INCR', key)
  
  -- Set expiry if this is the first request in the window
  if newCount == 1 then
    redis.call('EXPIRE', key, windowSeconds)
  end
  
  -- Get TTL for reset time
  local ttl = redis.call('TTL', key)
  local resetAt = now + (ttl * 1000)
  
  return cjson.encode({
    allowed = true,
    current = newCount,
    limit = maxRequests,
    remaining = maxRequests - newCount,
    reset = resetAt,
    timestamp = now
  })
else
  -- Rate limit exceeded
  
  -- Queue job if jobId provided
  if jobId and jobId ~= '' then
    redis.call('RPUSH', queueKey, jobId)
    redis.call('EXPIRE', queueKey, windowSeconds)
  end
  
  -- Get TTL for reset time
  local ttl = redis.call('TTL', key)
  local resetAt = now + (ttl * 1000)
  
  -- Publish rate limit event
  local eventChannel = ns .. ':events:global'
  local eventData = cjson.encode({
    event = 'ratelimit.exceeded',
    key = rateLimitKey,
    jobId = jobId or nil,
    current = current,
    limit = maxRequests,
    reset = resetAt,
    timestamp = now
  })
  redis.call('PUBLISH', eventChannel, eventData)
  
  return cjson.encode({
    allowed = false,
    current = current,
    limit = maxRequests,
    remaining = 0,
    reset = resetAt,
    timestamp = now
  })
end
