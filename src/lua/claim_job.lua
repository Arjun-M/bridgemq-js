-- claim_job.lua
-- Atomically claims a job matching worker criteria
-- 
-- KEYS[1] = waiting queue key
-- KEYS[2] = active set key
-- KEYS[3] = paused queues set key
-- 
-- ARGV[1] = worker serverId
-- ARGV[2] = worker stack (JSON array string)
-- ARGV[3] = worker capabilities (JSON array string)
-- ARGV[4] = worker region (JSON array string)
-- ARGV[5] = current timestamp
-- ARGV[6] = lock TTL (seconds)
-- ARGV[7] = job data key prefix
-- ARGV[8] = job lock key prefix
-- ARGV[9] = rate limit concurrent key prefix
-- 
-- Returns: job JSON string or nil

local waitingQueue = KEYS[1]
local activeSet = KEYS[2]
local pausedQueuesSet = KEYS[3]

local workerId = ARGV[1]
local workerStack = cjson.decode(ARGV[2])
local workerCapabilities = cjson.decode(ARGV[3])
local workerRegion = cjson.decode(ARGV[4])
local currentTime = tonumber(ARGV[5])
local lockTTL = tonumber(ARGV[6])
local jobDataPrefix = ARGV[7]
local jobLockPrefix = ARGV[8]
local rateLimitPrefix = ARGV[9]

-- Helper: Check if array contains value
local function arrayContains(arr, value)
  for _, v in ipairs(arr) do
    if v == value then return true end
  end
  return false
end

-- Helper: Check if any element of arr1 is in arr2
local function arrayIntersects(arr1, arr2)
  for _, v1 in ipairs(arr1) do
    for _, v2 in ipairs(arr2) do
      if v1 == v2 then return true end
    end
  end
  return false
end

-- Helper: Check if all elements of arr1 are in arr2
local function arrayContainsAll(arr1, arr2)
  for _, v1 in ipairs(arr1) do
    local found = false
    for _, v2 in ipairs(arr2) do
      if v1 == v2 then
        found = true
        break
      end
    end
    if not found then return false end
  end
  return true
end

-- Helper: Match worker against job target
local function matchesTarget(job, worker)
  local target = job.target
  if not target then return true end

  -- Priority 1: Server-specific routing (exact match)
  if target.server and target.server ~= "" then
    return target.server == worker.serverId
  end

  -- Priority 2: Stack/capability/region matching
  local mode = target.mode or "any"
  local matches = {}

  -- Check stack
  if target.stack and #target.stack > 0 then
    if mode == "any" then
      table.insert(matches, arrayIntersects(target.stack, worker.stack))
    else
      table.insert(matches, arrayContains(target.stack, worker.stack[1]))
    end
  end

  -- Check capabilities
  if target.capabilities and #target.capabilities > 0 then
    if mode == "any" then
      table.insert(matches, arrayIntersects(target.capabilities, worker.capabilities))
    else
      table.insert(matches, arrayContainsAll(target.capabilities, worker.capabilities))
    end
  end

  -- Check region
  if target.region and #target.region > 0 then
    if mode == "any" then
      table.insert(matches, arrayIntersects(target.region, worker.region))
    else
      table.insert(matches, arrayContains(target.region, worker.region[1]))
    end
  end

  -- If no criteria specified, match
  if #matches == 0 then return true end

  -- All criteria must match
  for _, match in ipairs(matches) do
    if not match then return false end
  end

  return true
end

-- Main logic: Scan waiting queue for matching job
local jobIds = redis.call('LRANGE', waitingQueue, 0, 99) -- Scan first 100 jobs

for _, jobId in ipairs(jobIds) do
  local jobDataKey = jobDataPrefix .. jobId
  local jobLockKey = jobLockPrefix .. jobId

  -- Try to acquire lock first
  local lockAcquired = redis.call('SET', jobLockKey, workerId, 'NX', 'EX', lockTTL)

  if lockAcquired then
    -- Load job data
    local jobJson = redis.call('GET', jobDataKey)

    if jobJson then
      local job = cjson.decode(jobJson)

      -- Check if job type queue is paused
      if redis.call('SISMEMBER', pausedQueuesSet, job.type) == 1 then
        -- Queue paused, release lock and skip
        redis.call('DEL', jobLockKey)
      else
        -- Check rate limits
        local rateLimitKey = rateLimitPrefix .. job.type
        local currentConcurrent = redis.call('GET', rateLimitKey)

        if job.rateLimit and job.rateLimit.maxConcurrent then
          if currentConcurrent and tonumber(currentConcurrent) >= job.rateLimit.maxConcurrent then
            -- Rate limit exceeded, release lock and skip
            redis.call('DEL', jobLockKey)
          else
            -- Check if worker matches
            local worker = {
              serverId = workerId,
              stack = workerStack,
              capabilities = workerCapabilities,
              region = workerRegion
            }

            if matchesTarget(job, worker) then
              -- Match found! Move job from waiting to active
              redis.call('LREM', waitingQueue, 1, jobId)
              redis.call('SADD', activeSet, jobId)

              -- Increment concurrent counter
              redis.call('INCR', rateLimitKey)

              -- Return job JSON
              return jobJson
            else
              -- No match, release lock
              redis.call('DEL', jobLockKey)
            end
          end
        else
          -- No rate limit, just check match
          local worker = {
            serverId = workerId,
            stack = workerStack,
            capabilities = workerCapabilities,
            region = workerRegion
          }

          if matchesTarget(job, worker) then
            redis.call('LREM', waitingQueue, 1, jobId)
            redis.call('SADD', activeSet, jobId)
            redis.call('INCR', rateLimitKey)
            return jobJson
          else
            redis.call('DEL', jobLockKey)
          end
        end
      end
    else
      -- Job data not found, clean up
      redis.call('LREM', waitingQueue, 1, jobId)
      redis.call('DEL', jobLockKey)
    end
  end
end

-- No matching job found
return nil
