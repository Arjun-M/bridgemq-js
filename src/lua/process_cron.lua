-- process_cron.lua
-- Processes cron jobs, creates new instances, and reschedules
--
-- KEYS[1] = cron sorted set key
-- KEYS[2] = waiting queue key
-- KEYS[3] = job data key prefix
--
-- ARGV[1] = current timestamp
-- ARGV[2] = batch size
--
-- Returns: JSON array of jobs that need rescheduling (client calculates next run)

local cronSet = KEYS[1]
local waitingQueue = KEYS[2]
local jobDataPrefix = KEYS[3]
local currentTime = tonumber(ARGV[1])
local batchSize = tonumber(ARGV[2])

-- Get cron jobs ready to run
local cronJobs = redis.call('ZRANGEBYSCORE', cronSet, 0, currentTime, 'LIMIT', 0, batchSize)

local results = {}

for _, jobId in ipairs(cronJobs) do
  local jobDataKey = jobDataPrefix .. jobId
  local jobJson = redis.call('GET', jobDataKey)

  if jobJson then
    local job = cjson.decode(jobJson)

    -- Add to waiting queue for execution
    redis.call('RPUSH', waitingQueue, jobId)

    -- Return job info for rescheduling (client will calculate next timestamp)
    table.insert(results, {
      jobId = jobId,
      cronExpression = job.schedule.cron,
      timezone = job.schedule.timezone
    })
  else
    -- Job data missing, remove from cron set
    redis.call('ZREM', cronSet, jobId)
  end
end

return cjson.encode(results)
