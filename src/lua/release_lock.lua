-- release_lock.lua
-- Safely releases a job lock only if owned by the worker
--
-- KEYS[1] = lock key
-- KEYS[2] = active set key
-- KEYS[3] = rate limit concurrent key
--
-- ARGV[1] = worker ID
-- ARGV[2] = job ID
--
-- Returns: 1 if released, 0 if not owned

local lockKey = KEYS[1]
local activeSet = KEYS[2]
local rateLimitKey = KEYS[3]
local workerId = ARGV[1]
local jobId = ARGV[2]

-- Check if lock is owned by this worker
local lockOwner = redis.call('GET', lockKey)

if lockOwner == workerId then
  -- Release lock
  redis.call('DEL', lockKey)

  -- Remove from active set
  redis.call('SREM', activeSet, jobId)

  -- Decrement concurrent counter
  local current = redis.call('GET', rateLimitKey)
  if current and tonumber(current) > 0 then
    redis.call('DECR', rateLimitKey)
  end

  return 1
else
  -- Lock not owned by this worker
  return 0
end
