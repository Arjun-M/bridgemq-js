-- createJob.lua
-- Atomically create a new job with idempotency and deduplication
-- 
-- PURPOSE: Prevent race conditions, duplicate jobs, and ensure atomic job creation
-- 
-- INPUTS:
--   KEYS[1] = 'bridgemq:job:{jobId}:meta'
--   KEYS[2] = 'bridgemq:job:{jobId}:config'
--   KEYS[3] = 'bridgemq:job:{jobId}:payload'
--   KEYS[4] = 'bridgemq:queue:{meshId}:{type}:p{priority}' (or delayed)
--   KEYS[5] = 'bridgemq:pending:{meshId}' (if not delayed)
--   ARGV[1] = jobId
--   ARGV[2] = JSON metadata object
--   ARGV[3] = JSON config object
--   ARGV[4] = payload (JSON/MessagePack string)
--   ARGV[5] = idempotency key (optional)
--   ARGV[6] = fingerprint hash (optional)
--   ARGV[7] = current timestamp (ms)
--   ARGV[8] = namespace prefix (bridgemq)
-- 
-- RETURNS: { jobId: string, created: boolean, existing: boolean }
-- 
-- LOGIC:
-- 1. Check idempotency key - return existing job if found
-- 2. Check fingerprint - return existing job if duplicate
-- 3. Create job metadata, config, payload
-- 4. Add to delayed queue OR pending queue based on scheduledFor
-- 5. Update indexes (capability, mesh, type)
-- 6. Set idempotency and fingerprint keys with TTL
-- 7. Publish job-created event
-- 8. Return jobId

local jobId = ARGV[1]
local metaJson = ARGV[2]
local configJson = ARGV[3]
local payload = ARGV[4]
local idempotencyKey = ARGV[5]
local fingerprintHash = ARGV[6]
local now = tonumber(ARGV[7])
local ns = ARGV[8]

-- Parse metadata and config
local meta = cjson.decode(metaJson)
local config = cjson.decode(configJson)

-- 1. Check idempotency key
if idempotencyKey and idempotencyKey ~= '' then
  local idempKey = ns .. ':idempotency:' .. idempotencyKey
  local existingJobId = redis.call('GET', idempKey)
  
  if existingJobId then
    return cjson.encode({
      jobId = existingJobId,
      created = false,
      existing = true,
      reason = 'idempotency'
    })
  end
end

-- 2. Check fingerprint for auto-deduplication
if fingerprintHash and fingerprintHash ~= '' then
  local fingerprintKey = ns .. ':fingerprint:' .. fingerprintHash
  local existingJobId = redis.call('GET', fingerprintKey)
  
  if existingJobId then
    return cjson.encode({
      jobId = existingJobId,
      created = false,
      existing = true,
      reason = 'fingerprint'
    })
  end
end

-- 3. Create job metadata
redis.call('HMSET', KEYS[1],
  'jobId', meta.jobId,
  'type', meta.type,
  'version', meta.version or '1.0',
  'priority', meta.priority or 5,
  'status', meta.status,
  'attempt', meta.attempt or 0,
  'createdAt', now,
  'scheduledFor', meta.scheduledFor or now,
  'updatedAt', now,
  'progress', 0,
  'stalledCount', 0
)

-- Set TTL if specified
if config.lifecycle and config.lifecycle.ttl then
  redis.call('EXPIRE', KEYS[1], config.lifecycle.ttl)
end

-- 4. Store config
redis.call('SET', KEYS[2], configJson)
if config.lifecycle and config.lifecycle.ttl then
  redis.call('EXPIRE', KEYS[2], config.lifecycle.ttl)
end

-- 5. Store payload
redis.call('SET', KEYS[3], payload)
if config.lifecycle and config.lifecycle.ttl then
  redis.call('EXPIRE', KEYS[3], config.lifecycle.ttl)
end

-- 6. Initialize empty errors list
local errorsKey = ns .. ':job:' .. jobId .. ':errors'
redis.call('DEL', errorsKey)

-- 7. Add to appropriate queue
local scheduledFor = tonumber(meta.scheduledFor) or now
local isDelayed = scheduledFor > now

if isDelayed then
  -- Add to delayed queue
  local delayedKey = ns .. ':delayed'
  redis.call('ZADD', delayedKey, scheduledFor, jobId)
else
  -- Add to priority queue
  local queueKey = ns .. ':queue:' .. meta.meshId .. ':' .. meta.type .. ':p' .. (meta.priority or 5)
  redis.call('ZADD', queueKey, scheduledFor, jobId)
  
  -- Add to pending index
  redis.call('ZADD', KEYS[5], meta.priority or 5, jobId)
end

-- 8. Update capability index if specified
if config.target and config.target.capabilities then
  for _, capability in ipairs(config.target.capabilities) do
    local capKey = ns .. ':capability:' .. capability
    redis.call('SADD', capKey, jobId)
  end
end

-- 9. Set idempotency key with TTL
if idempotencyKey and idempotencyKey ~= '' then
  local idempKey = ns .. ':idempotency:' .. idempotencyKey
  local ttl = (config.idempotency and config.idempotency.window) or 3600
  redis.call('SETEX', idempKey, ttl, jobId)
end

-- 10. Set fingerprint key with TTL
if fingerprintHash and fingerprintHash ~= '' then
  local fingerprintKey = ns .. ':fingerprint:' .. fingerprintHash
  local ttl = 3600 -- Default 1 hour dedup window
  redis.call('SETEX', fingerprintKey, ttl, jobId)
end

-- 11. Handle dependencies (waitFor)
if config.dependencies and config.dependencies.waitFor then
  local dependsKey = ns .. ':job:' .. jobId .. ':depends'
  for _, parentId in ipairs(config.dependencies.waitFor) do
    redis.call('SADD', dependsKey, parentId)
    
    -- Add to parent's waiters
    local waitersKey = ns .. ':job:' .. parentId .. ':waiters'
    redis.call('SADD', waitersKey, jobId)
  end
end

-- 12. Publish event
local eventChannel = ns .. ':events:global'
local meshEventChannel = ns .. ':events:mesh:' .. meta.meshId
local eventData = cjson.encode({
  event = 'job.created',
  jobId = jobId,
  type = meta.type,
  meshId = meta.meshId,
  priority = meta.priority or 5,
  status = meta.status,
  timestamp = now
})
redis.call('PUBLISH', eventChannel, eventData)
redis.call('PUBLISH', meshEventChannel, eventData)

-- 13. Return success
return cjson.encode({
  jobId = jobId,
  created = true,
  existing = false
})
