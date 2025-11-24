# BridgeMQ Lua Scripts

This directory contains Lua scripts that run atomically inside Redis for critical operations that require consistency guarantees.

## Why Lua?

Lua scripts in Redis execute atomically in a single-threaded context, providing:
- **Atomicity**: No other commands can execute during script execution
- **Consistency**: Complex multi-step operations happen as a single unit
- **Performance**: No network round-trips for multi-command sequences
- **Safety**: Prevents race conditions in distributed environments

## Scripts Overview

### 1. claim_job.lua

**Purpose**: Atomically claim a job that matches worker criteria

**Guarantees**:
- Exactly-once job claiming across multiple workers
- Atomic routing, locking, and queue movement
- No race conditions or duplicate execution

**Algorithm**:
1. Scan waiting queue (first 100 jobs)
2. For each job:
   - Try to acquire lock (SET NX)
   - If lock acquired, load job data
   - Check if queue is paused
   - Check rate limits
   - Match worker criteria against job target
   - If match: move from waiting → active, return job
   - If no match: release lock, continue
3. Return nil if no match found

**Routing Logic**:
- **Priority 1**: Server-specific (exact serverId match)
- **Priority 2**: Stack/capabilities/region with mode (any/all)
- **Mode "any"**: Worker matches ANY specified criterion
- **Mode "all"**: Worker matches ALL specified criteria

### 2. process_delayed.lua

**Purpose**: Move delayed jobs to waiting queue when delay expires

**Algorithm**:
1. Query delayed sorted set for jobs with score ≤ current timestamp
2. For each job:
   - Remove from delayed ZSET
   - Add to waiting LIST (RPUSH for FIFO)
3. Return count of moved jobs

**Batch Processing**: Processes up to N jobs per execution (default: 100)

### 3. process_cron.lua

**Purpose**: Execute cron jobs and prepare for rescheduling

**Algorithm**:
1. Query cron sorted set for jobs with score ≤ current timestamp
2. For each job:
   - Load job data
   - Add to waiting queue for execution
   - Return job info for client to calculate next timestamp
3. Client recalculates next cron time and updates ZSET

**Why Client Recalculation?**
- Lua has limited timezone and cron parsing capabilities
- Node.js cron-parser handles timezones, DST, complex expressions
- Keeps Lua scripts simple and maintainable

### 4. release_lock.lua

**Purpose**: Safely release job lock after processing

**Safety**:
- Only releases lock if owned by the requesting worker
- Prevents accidental release of another worker's lock
- Cleans up active set and rate limit counters

**Algorithm**:
1. Check if lock key value == worker ID
2. If owned:
   - Delete lock key
   - Remove from active set
   - Decrement rate limit counter
3. If not owned: return 0 (no-op)

## Script Loading

Scripts are loaded at startup using Redis SCRIPT LOAD command, which returns a SHA hash. Subsequent executions use EVALSHA for efficiency.

**Loading Pattern**:
```typescript
const sha = await redis.script('LOAD', scriptContent);
// Later executions
await redis.evalsha(sha, keys.length, ...keys, ...args);
```

## Error Handling

- Scripts return `nil` when no action is taken (e.g., no matching job)
- Scripts return structured data (JSON) for complex results
- Scripts clean up inconsistent state (e.g., missing job data)

## Testing

Lua scripts should be tested via integration tests that:
1. Set up specific Redis state
2. Execute script via EVALSHA
3. Verify resulting Redis state and return values
4. Test concurrent execution scenarios

## Multi-Language Compatibility

These Lua scripts are **language-agnostic**. Any language can:
1. Load the scripts using SCRIPT LOAD
2. Execute them using EVALSHA
3. Parse the results (JSON or primitive types)

This enables the v2.x multi-language vision where Python, Go, Rust, etc. can all use the same Redis core logic.

## Performance Considerations

- **Batch Size**: Scripts process limited batches to prevent blocking Redis
- **Scan Depth**: claim_job scans max 100 jobs to bound execution time
- **Minimal Logic**: Complex calculations (cron next time) done in client
- **No Loops**: Scripts use Redis bulk operations where possible

## Debugging

To debug Lua scripts:
1. Use `redis-cli` to manually execute with EVAL
2. Add temporary logging via `redis.log(redis.LOG_WARNING, "message")`
3. Check Redis logs for script errors
4. Use integration tests with detailed assertions

## Security

- Scripts validate all inputs (ARGV) before use
- No dynamic script generation (prevents injection)
- Scripts are immutable once loaded (SHA verification)
- All data access is namespaced

---

For implementation details, see the inline comments in each .lua file.
