-- retryJob.lua
-- Handle job retry with exponential backoff or move to DLQ
-- 
-- PURPOSE: Atomically retry failed jobs or move to Dead Letter Queue
-- 
-- INPUTS:
--   KEYS[1] = 'bridgemq:job:{jobId}:meta'
--   KEYS[2] = 'bridgemq:active:{serverId}'
--   KEYS[3] = 'bridgemq:job:{jobId}:errors'
--   ARGV[1] = jobId
--   ARGV[2] = serverId
--   ARGV[3] = error object (JSON string)
--   ARGV[4] = current timestamp (ms)
--   ARGV[5] = namespace prefix (bridgemq)
-- 
-- RETURNS: { willRetry: boolean, nextRun: number, movedToDLQ: boolean }
-- 
-- LOGIC:
-- 1. Get job metadata and config
-- 2. Increment attempt counter
-- 3. Append error to errors list
-- 4. Check if retry attempts remaining
-- 5. If yes: Calculate backoff delay, add to delayed queue
-- 6. If no: Move to DLQ
-- 7. Publish retry/failed event

local jobId = ARGV[1]
local serverId = ARGV[2]
local errorJson = ARGV[3]
local now = tonumber(ARGV[4])
local ns = ARGV[5]

-- 1. Get job metadata
local metaKey = KEYS[1]
local meta = redis.call('HGETALL', metaKey)

if #meta == 0 then
  return cjson.encode({
    success = false,
    error = 'Job not found'
  })
end

-- Convert to table
local metaData = {}
for i = 1, #meta, 2 do
  metaData[meta[i]] = meta[i + 1]
end

-- 2. Verify ownership
if metaData.processedBy ~= serverId then
  return cjson.encode({
    success = false,
    error = 'Job not owned by this server'
  })
end

-- 3. Get config for retry settings
local configKey = ns .. ':job:' .. jobId .. ':config'
local configJson = redis.call('GET', configKey)

if not configJson then
  return cjson.encode({
    success = false,
    error = 'Job config not found'
  })
end

local config = cjson.decode(configJson)
local maxAttempts = (config.retry and config.retry.maxAttempts) or 3
local backoffType = (config.retry and config.retry.backoff) or 'exponential'
local baseDelay = (config.retry and config.retry.baseDelayMs) or 1000
local maxDelay = (config.retry and config.retry.maxDelayMs) or 60000

-- 4. Increment attempt counter
local currentAttempt = tonumber(metaData.attempt) or 0
local newAttempt = currentAttempt + 1

-- 5. Append error to errors list
local errorEntry = cjson.encode({
  attempt = newAttempt,
  error = errorJson,
  timestamp = now,
  serverId = serverId
})
redis.call('RPUSH', KEYS[3], errorEntry)
redis.call('LTRIM', KEYS[3], -10, -1) -- Keep only last 10 errors

-- 6. Remove from active set
redis.call('HDEL', KEYS[2], jobId)

-- 7. Check if retry attempts remaining
if newAttempt >= maxAttempts then
  -- Move to DLQ
  local dlqKey = ns .. ':dlq:' .. metaData.meshId
  redis.call('RPUSH', dlqKey, jobId)
  
  -- Update status
  redis.call('HMSET', metaKey,
    'status', 'failed',
    'attempt', newAttempt,
    'completedAt', now,
    'updatedAt', now
  )
  
  -- Update metrics
  local statsKey = ns .. ':stats:' .. metaData.meshId .. ':counters'
  redis.call('HINCRBY', statsKey, 'total:failed', 1)
  
  -- Publish failed event
  local eventChannel = ns .. ':events:global'
  local eventData = cjson.encode({
    event = 'job.failed',
    jobId = jobId,
    serverId = serverId,
    attempt = newAttempt,
    reason = 'retry_limit_exceeded',
    timestamp = now
  })
  redis.call('PUBLISH', eventChannel, eventData)
  
  return cjson.encode({
    willRetry = false,
    movedToDLQ = true,
    attempt = newAttempt,
    maxAttempts = maxAttempts
  })
end

-- 8. Calculate retry delay
local delay = 0

if backoffType == 'exponential' then
  -- Exponential: min(baseDelay * 2^(attempt-1), maxDelay)
  delay = math.min(baseDelay * math.pow(2, newAttempt - 1), maxDelay)
elseif backoffType == 'linear' then
  -- Linear: min(baseDelay * attempt, maxDelay)
  delay = math.min(baseDelay * newAttempt, maxDelay)
elseif backoffType == 'fixed' then
  -- Fixed: baseDelay
  delay = baseDelay
else
  -- Default to exponential
  delay = math.min(baseDelay * math.pow(2, newAttempt - 1), maxDelay)
end

-- Add jitter (±20% randomness) to prevent thundering herd
local jitter = delay * 0.2 * (math.random() - 0.5) * 2
delay = math.floor(delay + jitter)

local nextRun = now + delay

-- 9. Update job metadata
redis.call('HMSET', metaKey,
  'status', 'scheduled',
  'attempt', newAttempt,
  'scheduledFor', nextRun,
  'updatedAt', now,
  'processedBy', ''
)

-- 10. Add to delayed queue
local delayedKey = ns .. ':delayed'
redis.call('ZADD', delayedKey, nextRun, jobId)

-- 11. Publish retry event
local eventChannel = ns .. ':events:global'
local eventData = cjson.encode({
  event = 'job.retry',
  jobId = jobId,
  serverId = serverId,
  attempt = newAttempt,
  maxAttempts = maxAttempts,
  nextRun = nextRun,
  delay = delay,
  timestamp = now
})
redis.call('PUBLISH', eventChannel, eventData)

-- 12. Return success
return cjson.encode({
  willRetry = true,
  movedToDLQ = false,
  attempt = newAttempt,
  maxAttempts = maxAttempts,
  nextRun = nextRun,
  delay = delay
})
