-- completeJob.lua
-- Atomically complete a job and trigger dependent jobs/chains
-- 
-- PURPOSE: Ensure job completion is atomic and triggers cascading actions
-- 
-- INPUTS:
--   KEYS[1] = 'bridgemq:job:{jobId}:meta'
--   KEYS[2] = 'bridgemq:active:{serverId}'
--   KEYS[3] = 'bridgemq:job:{jobId}:result'
--   ARGV[1] = jobId
--   ARGV[2] = serverId
--   ARGV[3] = result (JSON string)
--   ARGV[4] = status (completed|failed|cancelled)
--   ARGV[5] = current timestamp (ms)
--   ARGV[6] = namespace prefix (bridgemq)
-- 
-- RETURNS: { success: boolean, triggered: [jobIds] }
-- 
-- LOGIC:
-- 1. Verify job ownership (processedBy == serverId)
-- 2. Update job status and result
-- 3. Remove from active set
-- 4. Update metrics/counters
-- 5. Trigger dependent jobs (waiters)
-- 6. Execute job chains (onSuccess/onFailure)
-- 7. Clean up if removeOnComplete
-- 8. Publish job-completed event

local jobId = ARGV[1]
local serverId = ARGV[2]
local resultJson = ARGV[3]
local finalStatus = ARGV[4]
local now = tonumber(ARGV[5])
local ns = ARGV[6]

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
    error = 'Job not owned by this server',
    owner = metaData.processedBy
  })
end

-- 3. Verify job is active
if metaData.status ~= 'active' then
  return cjson.encode({
    success = false,
    error = 'Job is not active',
    status = metaData.status
  })
end

-- 4. Update job metadata
redis.call('HMSET', metaKey,
  'status', finalStatus,
  'completedAt', now,
  'updatedAt', now
)

-- 5. Store result
redis.call('SET', KEYS[3], resultJson)

-- Get config for TTL
local configKey = ns .. ':job:' .. jobId .. ':config'
local configJson = redis.call('GET', configKey)
if configJson then
  local config = cjson.decode(configJson)
  if config.lifecycle and config.lifecycle.ttl then
    redis.call('EXPIRE', KEYS[3], config.lifecycle.ttl)
  end
end

-- 6. Remove from active set
redis.call('HDEL', KEYS[2], jobId)

-- 7. Update metrics
local meshId = metaData.meshId or 'default'
local statsKey = ns .. ':stats:' .. meshId .. ':counters'

if finalStatus == 'completed' then
  redis.call('HINCRBY', statsKey, 'total:completed', 1)
elseif finalStatus == 'failed' then
  redis.call('HINCRBY', statsKey, 'total:failed', 1)
elseif finalStatus == 'cancelled' then
  redis.call('HINCRBY', statsKey, 'total:cancelled', 1)
end

-- 8. Calculate processing time
local claimedAt = tonumber(metaData.claimedAt) or now
local processingTime = now - claimedAt

-- 9. Trigger dependent jobs (waiters)
local triggered = {}
local waitersKey = ns .. ':job:' .. jobId .. ':waiters'
local waiters = redis.call('SMEMBERS', waitersKey)

for _, waiterId in ipairs(waiters) do
  local waiterDependsKey = ns .. ':job:' .. waiterId .. ':depends'
  
  -- Remove this job from waiter's dependencies
  redis.call('SREM', waiterDependsKey, jobId)
  
  -- Check if all dependencies are resolved
  local remainingDeps = redis.call('SCARD', waiterDependsKey)
  
  if remainingDeps == 0 then
    -- All dependencies resolved, move to pending
    local waiterMetaKey = ns .. ':job:' .. waiterId .. ':meta'
    local waiterMeta = redis.call('HGETALL', waiterMetaKey)
    
    if #waiterMeta > 0 then
      local waiterData = {}
      for i = 1, #waiterMeta, 2 do
        waiterData[waiterMeta[i]] = waiterMeta[i + 1]
      end
      
      -- Update status to pending
      redis.call('HSET', waiterMetaKey, 'status', 'pending')
      
      -- Add to pending queue
      local queueKey = ns .. ':queue:' .. waiterData.meshId .. ':' .. waiterData.type .. ':p' .. (waiterData.priority or 5)
      redis.call('ZADD', queueKey, now, waiterId)
      
      -- Add to pending index
      local pendingKey = ns .. ':pending:' .. waiterData.meshId
      redis.call('ZADD', pendingKey, waiterData.priority or 5, waiterId)
      
      table.insert(triggered, waiterId)
    end
  end
end

-- 10. Execute job chains (onSuccess/onFailure)
if configJson then
  local config = cjson.decode(configJson)
  local chainJobs = {}
  
  if finalStatus == 'completed' and config.chain and config.chain.onSuccess then
    chainJobs = config.chain.onSuccess
  elseif finalStatus == 'failed' and config.chain and config.chain.onFailure then
    chainJobs = config.chain.onFailure
  end
  
  -- Create chain jobs (simplified - actual creation would need full job data)
  for _, chainJob in ipairs(chainJobs) do
    -- Mark that chain jobs should be created
    -- This is typically handled by the application layer
    local chainKey = ns .. ':chain:' .. jobId
    redis.call('RPUSH', chainKey, cjson.encode(chainJob))
    redis.call('EXPIRE', chainKey, 300) -- 5 minutes TTL
  end
end

-- 11. Clean up if removeOnComplete
if configJson then
  local config = cjson.decode(configJson)
  if config.behavior and config.behavior.removeOnComplete and finalStatus == 'completed' then
    -- Schedule for cleanup (immediate or delayed)
    redis.call('DEL', metaKey)
    redis.call('DEL', configKey)
    redis.call('DEL', ns .. ':job:' .. jobId .. ':payload')
    redis.call('DEL', KEYS[3])
    redis.call('DEL', waitersKey)
    redis.call('DEL', ns .. ':job:' .. jobId .. ':depends')
    redis.call('DEL', ns .. ':job:' .. jobId .. ':errors')
  end
end

-- 12. Publish event
local eventChannel = ns .. ':events:global'
local jobEventChannel = ns .. ':events:job:' .. jobId
local eventData = cjson.encode({
  event = 'job.' .. finalStatus,
  jobId = jobId,
  serverId = serverId,
  status = finalStatus,
  processingTime = processingTime,
  triggered = triggered,
  timestamp = now
})
redis.call('PUBLISH', eventChannel, eventData)
redis.call('PUBLISH', jobEventChannel, eventData)

-- 13. Return success
return cjson.encode({
  success = true,
  status = finalStatus,
  triggered = triggered,
  processingTime = processingTime
})
