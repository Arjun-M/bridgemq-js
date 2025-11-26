-- claimJob.lua
-- Atomically claim a job from priority queues for worker processing
-- 
-- PURPOSE: Ensures only ONE server claims a job, preventing race conditions
-- 
-- INPUTS:
--   KEYS[1] = 'bridgemq:pending:{meshId}' - Pending jobs index
--   KEYS[2] = 'bridgemq:active:{serverId}' - Server's active jobs
--   KEYS[3] = 'bridgemq:queue:{meshId}:{type}:p{priority}' - Queue key pattern (constructed)
--   ARGV[1] = serverId
--   ARGV[2] = meshId
--   ARGV[3] = JSON array of capabilities
--   ARGV[4] = current timestamp (ms)
--   ARGV[5] = namespace prefix (bridgemq)
-- 
-- RETURNS: jobId or nil if no eligible job found
-- 
-- LOGIC:
-- 1. Scan priority queues from highest (10) to lowest (1)
-- 2. For each queue, get jobs with scheduledFor <= now
-- 3. Check if job requires specific capabilities
-- 4. Check if job is rate-limited
-- 5. If eligible, atomically:
--    - Remove from pending queue
--    - Add to server's active set
--    - Update job metadata (status=active, claimedAt, processedBy)
--    - Publish job-claimed event
-- 6. Return jobId

local serverId = ARGV[1]
local meshId = ARGV[2]
local capabilities = cjson.decode(ARGV[3])
local now = tonumber(ARGV[4])
local ns = ARGV[5]

-- Helper: Check if server has required capability
local function hasCapability(required, available)
  if not required or required == '' then
    return true
  end
  for _, cap in ipairs(available) do
    if cap == required then
      return true
    end
  end
  return false
end

-- Helper: Check if job is rate-limited
local function isRateLimited(jobId)
  local configKey = ns .. ':job:' .. jobId .. ':config'
  local configJson = redis.call('GET', configKey)
  if not configJson then
    return false
  end
  
  local config = cjson.decode(configJson)
  if not config.rateLimit then
    return false
  end
  
  local rateLimitKey = ns .. ':ratelimit:' .. config.rateLimit.key
  local count = tonumber(redis.call('GET', rateLimitKey) or 0)
  
  return count >= config.rateLimit.max
end

-- Scan priority queues from highest to lowest
for priority = 10, 1, -1 do
  -- Get all job types for this mesh and priority
  local queuePattern = ns .. ':queue:' .. meshId .. ':*:p' .. priority
  local queueKeys = redis.call('KEYS', queuePattern)
  
  for _, queueKey in ipairs(queueKeys) do
    -- Get jobs with scheduledFor <= now
    local jobs = redis.call('ZRANGEBYSCORE', queueKey, 0, now, 'LIMIT', 0, 1)
    
    for _, jobId in ipairs(jobs) do
      -- Check job metadata
      local metaKey = ns .. ':job:' .. jobId .. ':meta'
      local meta = redis.call('HGETALL', metaKey)
      
      if #meta > 0 then
        -- Convert to table
        local metaData = {}
        for i = 1, #meta, 2 do
          metaData[meta[i]] = meta[i + 1]
        end
        
        -- Check if job is already claimed
        if metaData.status == 'pending' or metaData.status == 'scheduled' then
          -- Get job config to check capability requirements
          local configKey = ns .. ':job:' .. jobId .. ':config'
          local configJson = redis.call('GET', configKey)
          local requiredCapability = nil
          
          if configJson then
            local config = cjson.decode(configJson)
            if config.target and config.target.capabilities then
              requiredCapability = config.target.capabilities[1]
            end
          end
          
          -- Check capability match
          if hasCapability(requiredCapability, capabilities) then
            -- Check rate limit
            if not isRateLimited(jobId) then
              -- CLAIM THE JOB
              -- 1. Remove from queue
              redis.call('ZREM', queueKey, jobId)
              
              -- 2. Remove from pending index
              redis.call('ZREM', KEYS[1], jobId)
              
              -- 3. Add to server's active set
              redis.call('HSET', KEYS[2], jobId, now)
              
              -- 4. Update job metadata
              redis.call('HMSET', metaKey,
                'status', 'active',
                'claimedAt', now,
                'processedBy', serverId,
                'updatedAt', now
              )
              
              -- 5. Publish event
              local eventChannel = ns .. ':events:global'
              local eventData = cjson.encode({
                event = 'job.claimed',
                jobId = jobId,
                serverId = serverId,
                timestamp = now
              })
              redis.call('PUBLISH', eventChannel, eventData)
              
              -- Return claimed job ID
              return jobId
            end
          end
        end
      end
    end
  end
end

-- No eligible job found
return nil
