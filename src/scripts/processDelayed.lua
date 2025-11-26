-- processDelayed.lua
-- Move delayed jobs to pending queues when their scheduled time arrives
-- 
-- PURPOSE: Background scheduler that processes delayed jobs
-- 
-- INPUTS:
--   KEYS[1] = 'bridgemq:delayed'
--   ARGV[1] = current timestamp (ms)
--   ARGV[2] = namespace prefix (bridgemq)
--   ARGV[3] = limit (max jobs to process in one run, default 100)
-- 
-- RETURNS: { processed: number, jobIds: [jobIds] }
-- 
-- LOGIC:
-- 1. Get all jobs from delayed queue where scheduledFor <= now
-- 2. For each job:
--    - Get job metadata
--    - Move to appropriate pending queue based on mesh/type/priority
--    - Update status to 'pending'
--    - Remove from delayed queue
--    - Publish job-scheduled event
-- 3. Return count of processed jobs

local now = tonumber(ARGV[1])
local ns = ARGV[2]
local limit = tonumber(ARGV[3]) or 100

local delayedKey = KEYS[1]
local processedJobs = {}
local processedCount = 0

-- Get jobs with scheduledFor <= now
local jobs = redis.call('ZRANGEBYSCORE', delayedKey, 0, now, 'LIMIT', 0, limit)

for _, jobId in ipairs(jobs) do
  -- Get job metadata
  local metaKey = ns .. ':job:' .. jobId .. ':meta'
  local meta = redis.call('HGETALL', metaKey)
  
  if #meta > 0 then
    -- Convert to table
    local metaData = {}
    for i = 1, #meta, 2 do
      metaData[meta[i]] = meta[i + 1]
    end
    
    -- Only process if status is 'scheduled'
    if metaData.status == 'scheduled' then
      local meshId = metaData.meshId or 'default'
      local jobType = metaData.type or 'default'
      local priority = tonumber(metaData.priority) or 5
      
      -- Add to pending queue
      local queueKey = ns .. ':queue:' .. meshId .. ':' .. jobType .. ':p' .. priority
      redis.call('ZADD', queueKey, now, jobId)
      
      -- Add to pending index
      local pendingKey = ns .. ':pending:' .. meshId
      redis.call('ZADD', pendingKey, priority, jobId)
      
      -- Update job status
      redis.call('HMSET', metaKey,
        'status', 'pending',
        'updatedAt', now
      )
      
      -- Remove from delayed queue
      redis.call('ZREM', delayedKey, jobId)
      
      -- Publish event
      local eventChannel = ns .. ':events:global'
      local meshEventChannel = ns .. ':events:mesh:' .. meshId
      local eventData = cjson.encode({
        event = 'job.scheduled',
        jobId = jobId,
        type = jobType,
        meshId = meshId,
        priority = priority,
        timestamp = now
      })
      redis.call('PUBLISH', eventChannel, eventData)
      redis.call('PUBLISH', meshEventChannel, eventData)
      
      table.insert(processedJobs, jobId)
      processedCount = processedCount + 1
    end
  end
end

return cjson.encode({
  processed = processedCount,
  jobIds = processedJobs,
  timestamp = now
})
