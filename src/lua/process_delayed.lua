-- process_delayed.lua
-- Moves delayed jobs to waiting queue when their delay has expired
--
-- KEYS[1] = delayed sorted set key
-- KEYS[2] = waiting queue key
--
-- ARGV[1] = current timestamp
-- ARGV[2] = batch size (max jobs to process)
--
-- Returns: number of jobs moved

local delayedSet = KEYS[1]
local waitingQueue = KEYS[2]
local currentTime = tonumber(ARGV[1])
local batchSize = tonumber(ARGV[2])

-- Get all jobs that should be run now (score <= currentTime)
local jobs = redis.call('ZRANGEBYSCORE', delayedSet, 0, currentTime, 'LIMIT', 0, batchSize)

local movedCount = 0

for _, jobId in ipairs(jobs) do
  -- Remove from delayed set
  redis.call('ZREM', delayedSet, jobId)

  -- Add to waiting queue (right push for FIFO)
  redis.call('RPUSH', waitingQueue, jobId)

  movedCount = movedCount + 1
end

return movedCount
