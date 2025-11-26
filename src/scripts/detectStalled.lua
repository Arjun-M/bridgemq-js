-- detectStalled.lua
-- Detect and recover stalled jobs (jobs claimed but not completed)
-- 
-- PURPOSE: Prevent jobs from being stuck forever if a worker crashes
-- 
-- INPUTS:
--   ARGV[1] = current timestamp (ms)
--   ARGV[2] = stall timeout (ms, default 300000 = 5 minutes)
--   ARGV[3] = max stall count (default 3)
--   ARGV[4] = namespace prefix (bridgemq)
-- 
-- RETURNS: { detected: number, recovered: number, movedToDLQ: number }
-- 
-- LOGIC:
-- 1. Scan all active job sets (active:{serverId})
-- 2. For each active job:
--    - Check if (now - claimedAt) > stallTimeout
--    - If stalled:
--      - Increment stalledCount
--      - If stalledCount < maxStallCount: Move back to pending
--      - If stalledCount >= maxStallCount: Move to DLQ
--      - Publish job-stalled event

local now = tonumber(ARGV[1])
local stallTimeout = tonumber(ARGV[2]) or 300000 -- 5 minutes
local maxStallCount = tonumber(ARGV[3]) or 3
local ns = ARGV[4]

local detectedCount = 0
local recoveredCount = 0
local dlqCount = 0

-- Get all active job sets
local activePattern = ns .. ':active:*'
local activeKeys = redis.call('KEYS', activePattern)

for _, activeKey in ipairs(activeKeys) do
  -- Extract serverId from key
  local serverId = string.match(activeKey, ns .. ':active:(.+)')
  
  -- Get all active jobs for this server
  local activeJobs = redis.call('HGETALL', activeKey)
  
  for i = 1, #activeJobs, 2 do
    local jobId = activeJobs[i]
    local claimedAt = tonumber(activeJobs[i + 1])
    
    -- Check if stalled
    local elapsed = now - claimedAt
    
    if elapsed > stallTimeout then
      detectedCount = detectedCount + 1
      
      -- Get job metadata
      local metaKey = ns .. ':job:' .. jobId .. ':meta'
      local meta = redis.call('HGETALL', metaKey)
      
      if #meta > 0 then
        local metaData = {}
        for j = 1, #meta, 2 do
          metaData[meta[j]] = meta[j + 1]
        end
        
        -- Increment stall count
        local stalledCount = tonumber(metaData.stalledCount) or 0
        stalledCount = stalledCount + 1
        
        -- Remove from active set
        redis.call('HDEL', activeKey, jobId)
        
        if stalledCount >= maxStallCount then
          -- Move to DLQ
          local dlqKey = ns .. ':dlq:' .. metaData.meshId
          redis.call('RPUSH', dlqKey, jobId)
          
          -- Update status
          redis.call('HMSET', metaKey,
            'status', 'failed',
            'stalledCount', stalledCount,
            'updatedAt', now,
            'processedBy', ''
          )
          
          dlqCount = dlqCount + 1
          
          -- Publish failed event
          local eventChannel = ns .. ':events:global'
          local eventData = cjson.encode({
            event = 'job.failed',
            jobId = jobId,
            serverId = serverId,
            reason = 'stall_limit_exceeded',
            stalledCount = stalledCount,
            timestamp = now
          })
          redis.call('PUBLISH', eventChannel, eventData)
        else
          -- Move back to pending for retry
          local meshId = metaData.meshId or 'default'
          local jobType = metaData.type or 'default'
          local priority = tonumber(metaData.priority) or 5
          
          -- Add to pending queue
          local queueKey = ns .. ':queue:' .. meshId .. ':' .. jobType .. ':p' .. priority
          redis.call('ZADD', queueKey, now, jobId)
          
          -- Add to pending index
          local pendingKey = ns .. ':pending:' .. meshId
          redis.call('ZADD', pendingKey, priority, jobId)
          
          -- Update status
          redis.call('HMSET', metaKey,
            'status', 'pending',
            'stalledCount', stalledCount,
            'updatedAt', now,
            'processedBy', ''
          )
          
          recoveredCount = recoveredCount + 1
          
          -- Publish stalled event
          local eventChannel = ns .. ':events:global'
          local eventData = cjson.encode({
            event = 'job.stalled',
            jobId = jobId,
            serverId = serverId,
            stalledCount = stalledCount,
            elapsed = elapsed,
            timestamp = now
          })
          redis.call('PUBLISH', eventChannel, eventData)
        end
      end
    end
  end
end

return cjson.encode({
  detected = detectedCount,
  recovered = recoveredCount,
  movedToDLQ = dlqCount,
  timestamp = now
})
