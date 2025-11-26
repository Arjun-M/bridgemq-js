-- batchJobs.lua
-- Finalize a batch by aggregating individual jobs into a single batch job
-- 
-- PURPOSE: Implement smart batching for efficient processing of grouped jobs
-- 
-- INPUTS:
--   KEYS[1] = 'bridgemq:batch:{batchKey}'
--   KEYS[2] = 'bridgemq:batch:{batchId}:meta'
--   ARGV[1] = batchKey
--   ARGV[2] = batchId
--   ARGV[3] = meshId
--   ARGV[4] = jobType
--   ARGV[5] = priority
--   ARGV[6] = current timestamp (ms)
--   ARGV[7] = namespace prefix (bridgemq)
-- 
-- RETURNS: { batchId: string, jobIds: [jobIds], count: number }
-- 
-- LOGIC:
-- 1. Get all jobs in batch group
-- 2. Create batch metadata
-- 3. Move individual jobs from pending to batch
-- 4. Clear batch accumulation key
-- 5. Publish batch-created event

local batchKey = ARGV[1]
local batchId = ARGV[2]
local meshId = ARGV[3]
local jobType = ARGV[4]
local priority = tonumber(ARGV[5]) or 5
local now = tonumber(ARGV[6])
local ns = ARGV[7]

local batchListKey = KEYS[1]
local batchMetaKey = KEYS[2]

-- 1. Get all jobs in batch
local jobIds = redis.call('LRANGE', batchListKey, 0, -1)
local jobCount = #jobIds

if jobCount == 0 then
  return cjson.encode({
    success = false,
    error = 'No jobs in batch'
  })
end

-- 2. Create batch metadata
redis.call('HMSET', batchMetaKey,
  'batchId', batchId,
  'batchKey', batchKey,
  'meshId', meshId,
  'type', jobType,
  'priority', priority,
  'size', jobCount,
  'status', 'pending',
  'createdAt', now,
  'executedAt', 0
)

-- Set TTL
redis.call('EXPIRE', batchMetaKey, 86400) -- 24 hours

-- 3. Store job IDs in batch
local batchJobsKey = ns .. ':batch:' .. batchId .. ':jobs'
for _, jobId in ipairs(jobIds) do
  redis.call('RPUSH', batchJobsKey, jobId)
end
redis.call('EXPIRE', batchJobsKey, 86400)

-- 4. Remove individual jobs from their pending queues
for _, jobId in ipairs(jobIds) do
  -- Get job metadata
  local jobMetaKey = ns .. ':job:' .. jobId .. ':meta'
  local meta = redis.call('HGETALL', jobMetaKey)
  
  if #meta > 0 then
    local metaData = {}
    for i = 1, #meta, 2 do
      metaData[meta[i]] = meta[i + 1]
    end
    
    -- Remove from queue
    local queueKey = ns .. ':queue:' .. meshId .. ':' .. jobType .. ':p' .. priority
    redis.call('ZREM', queueKey, jobId)
    
    -- Update job metadata to reference batch
    redis.call('HMSET', jobMetaKey,
      'batchId', batchId,
      'status', 'batched',
      'updatedAt', now
    )
  end
end

-- 5. Clear batch accumulation key
redis.call('DEL', batchListKey)

-- 6. Create the batch job itself in pending queue
local batchQueueKey = ns .. ':queue:' .. meshId .. ':' .. jobType .. ':p' .. priority
redis.call('ZADD', batchQueueKey, now, batchId)

-- Add to pending index
local pendingKey = ns .. ':pending:' .. meshId
redis.call('ZADD', pendingKey, priority, batchId)

-- 7. Publish batch-created event
local eventChannel = ns .. ':events:global'
local eventData = cjson.encode({
  event = 'batch.created',
  batchId = batchId,
  batchKey = batchKey,
  meshId = meshId,
  type = jobType,
  size = jobCount,
  jobIds = jobIds,
  timestamp = now
})
redis.call('PUBLISH', eventChannel, eventData)

return cjson.encode({
  success = true,
  batchId = batchId,
  jobIds = jobIds,
  count = jobCount,
  timestamp = now
})
