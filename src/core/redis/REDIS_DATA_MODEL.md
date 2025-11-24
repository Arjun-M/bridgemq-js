# Redis Data Model - BridgeMQ v1.x

This document describes the complete Redis data model used by BridgeMQ, including all keys, data structures, and Lua scripts.

## Key Namespace

All keys are prefixed with a namespace (default: `bridge`) to support multi-tenancy.

Pattern: `{namespace}:{category}:{subcategory}:{identifier}`

## Data Structures

### 1. Worker Registry

#### Worker Metadata
**Key**: `bridge:workers:{serverId}`  
**Type**: String (JSON)  
**TTL**: None (persistent)  
**Purpose**: Store worker metadata

**Schema**:
```json
{
  "serverId": "worker-1",
  "stack": "node",
  "region": "us-east",
  "capabilities": ["email", "sms", "gpu"],
  "lastSeen": 1732376323000,
  "pid": 12345,
  "hostname": "server-01"
}
```

#### Worker Heartbeat
**Key**: `bridge:workers:heartbeat:{serverId}`  
**Type**: String (timestamp)  
**TTL**: 30 seconds  
**Purpose**: Track worker liveness

Worker sends heartbeat every 10 seconds. If key expires, worker is considered dead.

#### Active Workers Set
**Key**: `bridge:workers:active`  
**Type**: Set  
**Purpose**: Track all registered worker IDs

---

### 2. Job Data

#### Job Details
**Key**: `bridge:jobs:data:{jobId}`  
**Type**: String (JSON)  
**TTL**: None (cleaned up on completion)  
**Purpose**: Store complete job data

**Schema**: See `Job` type in `src/types.ts`

#### Job Status
**Key**: `bridge:jobs:status:{jobId}`  
**Type**: String (enum)  
**Values**: `waiting | active | completed | failed | delayed | cron | dead`  
**Purpose**: Track current job state

#### Job Result
**Key**: `bridge:jobs:result:{jobId}`  
**Type**: String (JSON)  
**TTL**: 24 hours  
**Purpose**: Store job execution result

**Schema**:
```json
{
  "success": true,
  "data": {...},
  "error": {
    "message": "...",
    "stack": "...",
    "name": "..."
  },
  "completedAt": 1732376323000,
  "processingTime": 234
}
```

#### Job Lock
**Key**: `bridge:jobs:lock:{jobId}`  
**Type**: String (worker ID)  
**TTL**: 30 seconds (configurable)  
**Purpose**: Distributed lock for job processing

**Value**: `{workerId}` - ID of worker holding the lock

**Lock Renewal**: Worker can renew lock during long-running jobs

#### Job Progress
**Key**: `bridge:jobs:progress:{jobId}`  
**Type**: String (JSON)  
**Purpose**: Track real-time job progress

**Schema**:
```json
{
  "percent": 75,
  "message": "Processing step 3 of 4",
  "data": {...},
  "updatedAt": 1732376323000
}
```

---

### 3. Job Queues

#### Waiting Queue (Ready Jobs)
**Key**: `bridge:jobs:waiting`  
**Type**: List  
**Purpose**: FIFO queue of jobs ready for processing

**Operations**:
- `RPUSH` - Add job to back (normal priority)
- `LPUSH` - Add job to front (high priority >= 7)
- `LPOP` - Claim next job (via Lua script)

#### Active Set (In-Progress Jobs)
**Key**: `bridge:jobs:active`  
**Type**: Set  
**Purpose**: Track currently processing jobs

**Operations**:
- `SADD` - Mark job as active (during claim)
- `SREM` - Remove job (on completion/failure)

#### Delayed Queue (Scheduled Jobs)
**Key**: `bridge:jobs:delayed`  
**Type**: Sorted Set  
**Score**: Timestamp (ms) when job should run  
**Purpose**: Store jobs with future execution time

**Scheduler Process**: Periodically moves jobs with `score <= now()` to waiting queue

#### Cron Queue (Recurring Jobs)
**Key**: `bridge:jobs:cron`  
**Type**: Sorted Set  
**Score**: Next execution timestamp  
**Purpose**: Store recurring jobs

**Scheduler Process**: 
1. Finds jobs with `score <= now()`
2. Adds job to waiting queue
3. Calculates next run time (client-side)
4. Updates score in cron set

#### Dead Letter Queue (Failed Jobs)
**Key**: `bridge:jobs:deadletter`  
**Type**: List  
**Purpose**: Store permanently failed jobs for analysis

Jobs moved here when:
- Retry attempts exhausted
- Unrecoverable errors
- Manual intervention needed

---

### 4. Rate Limiting

#### Concurrent Jobs Limit
**Key**: `bridge:ratelimit:concurrent:{jobType}`  
**Type**: String (counter)  
**Purpose**: Track concurrent jobs of a specific type

**Operations**:
- `INCR` - Increment on job claim
- `DECR` - Decrement on job completion
- Lua script checks limit before claiming

#### Per-Minute Rate Limit
**Key**: `bridge:ratelimit:minute:{jobType}`  
**Type**: String (counter)  
**TTL**: 60 seconds  
**Purpose**: Enforce max jobs per minute per type

---

### 5. Queue Management

#### Paused Queues
**Key**: `bridge:queues:paused`  
**Type**: Set  
**Purpose**: Store names of paused queues

**Operations**:
- `SADD {queueName}` - Pause queue
- `SREM {queueName}` - Resume queue
- Lua script checks membership before claiming

---

### 6. Scheduler Coordination

#### Scheduler Lock
**Key**: `bridge:scheduler:lock`  
**Type**: String (scheduler ID)  
**TTL**: 30 seconds  
**Purpose**: Leader election for scheduler (only one scheduler runs)

**Algorithm**:
```
SET bridge:scheduler:lock {schedulerId} NX EX 30
```
If successful, this scheduler is the leader.

---

## Lua Scripts

All Lua scripts are located in `src/core/lua/` and shipped in `lua/` directory.

### 1. claim_job.lua

**Purpose**: Atomically claim a job matching worker criteria

**Keys**:
1. `waiting` - Waiting queue key
2. `active` - Active set key
3. `pausedQueues` - Paused queues set key

**Arguments**:
1. `workerId` - Worker server ID
2. `workerStack` - JSON array `["node"]`
3. `workerCapabilities` - JSON array `["gpu", "email"]`
4. `workerRegion` - JSON array `["us-east"]`
5. `currentTime` - Current timestamp
6. `lockTTL` - Lock timeout in seconds
7. `jobDataPrefix` - `bridge:jobs:data:`
8. `jobLockPrefix` - `bridge:jobs:lock:`
9. `rateLimitPrefix` - `bridge:ratelimit:concurrent:`

**Returns**: Job JSON string or `nil`

**Algorithm**:
1. Scan first 100 jobs in waiting queue
2. For each job:
   - Acquire lock (`SET NX`)
   - Load job data
   - Check if queue paused
   - Check rate limits
   - Match worker criteria:
     - **Priority 1**: Exact server match (`target.server`)
     - **Priority 2**: Stack/capabilities/region with mode
   - If match:
     - Move from waiting → active
     - Increment rate limit counter
     - Return job JSON
   - If no match: release lock, continue
3. Return `nil` if no match found

**Routing Logic**:
```lua
-- mode: "any" - Worker matches ANY criterion
-- mode: "all" - Worker matches ALL criteria

if target.server then
  return target.server == worker.serverId
end

-- Check stack, capabilities, region arrays
-- Return true if all checks pass
```

**Atomicity Guarantees**:
- ✅ Job claimed by exactly one worker
- ✅ No race conditions
- ✅ Lock + queue movement atomic
- ✅ Rate limits enforced atomically

---

### 2. process_delayed.lua

**Purpose**: Move delayed jobs to waiting queue when delay expires

**Keys**:
1. `delayed` - Delayed sorted set key
2. `waiting` - Waiting queue key

**Arguments**:
1. `currentTime` - Current timestamp
2. `batchSize` - Max jobs to process (default 100)

**Returns**: Number of jobs moved

**Algorithm**:
```lua
-- Get jobs with score <= currentTime
local jobs = ZRANGEBYSCORE delayed 0 currentTime LIMIT 0 batchSize

for each job:
  ZREM delayed jobId
  RPUSH waiting jobId

return count
```

**Scheduler Invocation**: Every 1 second (configurable)

---

### 3. process_cron.lua

**Purpose**: Execute cron jobs and prepare for rescheduling

**Keys**:
1. `cron` - Cron sorted set key
2. `waiting` - Waiting queue key
3. `jobDataPrefix` - Job data key prefix

**Arguments**:
1. `currentTime` - Current timestamp
2. `batchSize` - Max jobs to process

**Returns**: JSON array of jobs needing reschedule

**Algorithm**:
```lua
local jobs = ZRANGEBYSCORE cron 0 currentTime LIMIT 0 batchSize

results = []
for each job:
  Load job data
  RPUSH waiting jobId
  Add to results: {jobId, cronExpression, timezone}

return JSON(results)
```

**Client Responsibility**: 
- Parse cron expression
- Calculate next run time (handles timezones, DST)
- Update cron set: `ZADD cron nextTimestamp jobId`

---

### 4. release_lock.lua

**Purpose**: Safely release job lock only if owned

**Keys**:
1. `lock` - Lock key
2. `active` - Active set key
3. `rateLimitKey` - Rate limit counter key

**Arguments**:
1. `workerId` - Worker ID
2. `jobId` - Job ID

**Returns**: `1` if released, `0` if not owned

**Algorithm**:
```lua
if GET lock == workerId then
  DEL lock
  SREM active jobId
  DECR rateLimitKey
  return 1
else
  return 0  -- Lock not owned
end
```

**Safety**: Prevents accidental release of another worker's lock

---

## Data Flow

### Job Lifecycle

```
1. Producer: dispatch(job)
   ↓
2. Save job data: SET bridge:jobs:data:{id} {json}
   ↓
3. Route by schedule:
   - Immediate → RPUSH bridge:jobs:waiting {id}
   - Delayed   → ZADD bridge:jobs:delayed {timestamp} {id}
   - Cron      → ZADD bridge:jobs:cron {nextRun} {id}
   ↓
4. Scheduler (background):
   - process_delayed.lua → moves ready jobs to waiting
   - process_cron.lua → creates cron instances
   ↓
5. Worker: claim_job.lua
   - Atomic: lock + match + move to active
   ↓
6. Worker: process job
   - Update progress: SET bridge:jobs:progress:{id} {json}
   ↓
7. Worker: complete
   - Success: SET bridge:jobs:result:{id} {json}
   - Failure: retry or LPUSH bridge:jobs:deadletter {id}
   ↓
8. Worker: release_lock.lua
   - Remove lock, update status, cleanup
```

---

## Consistency Guarantees

### Exactly-Once Job Claiming
- **Lua scripts execute atomically**
- **SET NX ensures single lock holder**
- **LREM + SADD move job between queues atomically**

### Worker Recovery
- Heartbeat expires → scheduler detects
- Find active jobs by dead worker
- Release locks
- Move jobs back to waiting queue

### Retry Safety
- Lock prevents duplicate processing during retry
- Attempt counter in job data prevents infinite retries
- Dead letter queue captures permanently failed jobs

---

## Performance Considerations

### Batch Sizes
- `claim_job.lua` scans max 100 jobs (configurable)
- `process_delayed.lua` processes max 100 jobs per run
- Prevents Redis blocking on large queues

### Key Expiration
- Heartbeat keys: 30s TTL
- Lock keys: 30s TTL (renewable)
- Result keys: 24h TTL (cleanup old results)

### Index Optimization
- Sorted sets use timestamps for efficient range queries
- Sets used for O(1) membership checks (active jobs, paused queues)
- Lists for FIFO ordering

---

## Multi-Language Compatibility

**This Redis data model is language-agnostic.** Any language can:

1. **Read/Write** - Standard Redis commands work from any client
2. **Execute Lua** - Load scripts once, execute via EVALSHA
3. **Parse JSON** - Job data is standard JSON

**v2.x Implementation** (Python, Go, Rust):
- Reuse same Lua scripts (load via SCRIPT LOAD)
- Implement same key naming conventions
- Parse/serialize JSON job data
- No changes to Redis data structures needed

---

## Monitoring Keys

Recommended metrics to track:

```bash
# Queue sizes
LLEN bridge:jobs:waiting
SCARD bridge:jobs:active
ZCARD bridge:jobs:delayed
ZCARD bridge:jobs:cron
LLEN bridge:jobs:deadletter

# Worker health
SMEMBERS bridge:workers:active
# For each: EXISTS bridge:workers:heartbeat:{id}

# Rate limits
GET bridge:ratelimit:concurrent:{type}
```

---

For Lua script source code and detailed comments, see:
- `src/core/lua/*.lua` (source)
- `lua/*.lua` (shipped copies)
- `src/core/lua/README_LUA.md` (detailed docs)
