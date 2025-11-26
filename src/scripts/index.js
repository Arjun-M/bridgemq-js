const fs = require('fs');
const path = require('path');

/**
 * Lua Script Loader and Manager
 * 
 * PURPOSE: Load all Lua scripts into Redis and provide JavaScript interface
 * 
 * FEATURES:
 * - Lazy loading of Lua scripts
 * - SHA caching for performance
 * - Script reload capability
 * - Type-safe function wrappers
 */

class LuaScripts {
  constructor() {
    this.scripts = {};
    this.loaded = false;
  }

  /**
   * Load all Lua scripts into Redis
   * @param {Redis} redis - ioredis client instance
   * @returns {Promise<void>}
   */
  async load(redis) {
    if (this.loaded) {
      return;
    }

    const scriptsDir = __dirname;
    const scriptFiles = {
      claimJob: 'claimJob.lua',
      createJob: 'createJob.lua',
      completeJob: 'completeJob.lua',
      retryJob: 'retryJob.lua',
      processDelayed: 'processDelayed.lua',
      detectStalled: 'detectStalled.lua',
      rateLimitCheck: 'rateLimitCheck.lua',
      batchJobs: 'batchJobs.lua',
    };

    for (const [name, filename] of Object.entries(scriptFiles)) {
      const scriptPath = path.join(scriptsDir, filename);
      const scriptContent = fs.readFileSync(scriptPath, 'utf8');
      
      // Load script and get SHA
      const sha = await redis.script('LOAD', scriptContent);
      
      this.scripts[name] = {
        content: scriptContent,
        sha,
      };
    }

    this.loaded = true;
  }

  /**
   * Claim a job from pending queues
   * @param {Redis} redis - Redis client
   * @param {string} serverId - Server ID claiming the job
   * @param {string} meshId - Mesh ID to claim from
   * @param {string[]} capabilities - Server capabilities
   * @returns {Promise<string|null>} Job ID or null
   */
  async claimJob(redis, serverId, meshId, capabilities) {
    const now = Date.now();
    const ns = 'bridgemq';
    
    const keys = [
      `${ns}:pending:${meshId}`,
      `${ns}:active:${serverId}`,
      '', // Queue key pattern (constructed in Lua)
    ];
    
    const args = [
      serverId,
      meshId,
      JSON.stringify(capabilities),
      now.toString(),
      ns,
    ];

    return redis.evalsha(this.scripts.claimJob.sha, keys.length, ...keys, ...args);
  }

  /**
   * Create a new job
   * @param {Redis} redis - Redis client
   * @param {Object} jobData - Job creation data
   * @returns {Promise<Object>} Creation result
   */
  async createJob(redis, jobData) {
    const {
      jobId,
      meta,
      config,
      payload,
      idempotencyKey,
      fingerprintHash,
    } = jobData;

    const now = Date.now();
    const ns = 'bridgemq';
    
    const keys = [
      `${ns}:job:${jobId}:meta`,
      `${ns}:job:${jobId}:config`,
      `${ns}:job:${jobId}:payload`,
      '', // Queue key (constructed in Lua)
      `${ns}:pending:${meta.meshId}`,
    ];
    
    const args = [
      jobId,
      JSON.stringify(meta),
      JSON.stringify(config),
      payload,
      idempotencyKey || '',
      fingerprintHash || '',
      now.toString(),
      ns,
    ];

    const result = await redis.evalsha(
      this.scripts.createJob.sha,
      keys.length,
      ...keys,
      ...args,
    );

    return JSON.parse(result);
  }

  /**
   * Complete a job
   * @param {Redis} redis - Redis client
   * @param {string} jobId - Job ID
   * @param {string} serverId - Server ID that processed the job
   * @param {Object} result - Job result
   * @param {string} status - Final status (completed|failed|cancelled)
   * @returns {Promise<Object>} Completion result
   */
  async completeJob(redis, jobId, serverId, result, status = 'completed') {
    const now = Date.now();
    const ns = 'bridgemq';
    
    const keys = [
      `${ns}:job:${jobId}:meta`,
      `${ns}:active:${serverId}`,
      `${ns}:job:${jobId}:result`,
    ];
    
    const args = [
      jobId,
      serverId,
      JSON.stringify(result),
      status,
      now.toString(),
      ns,
    ];

    const resultJson = await redis.evalsha(
      this.scripts.completeJob.sha,
      keys.length,
      ...keys,
      ...args,
    );

    return JSON.parse(resultJson);
  }

  /**
   * Retry a failed job
   * @param {Redis} redis - Redis client
   * @param {string} jobId - Job ID
   * @param {string} serverId - Server ID
   * @param {Object} error - Error object
   * @returns {Promise<Object>} Retry result
   */
  async retryJob(redis, jobId, serverId, error) {
    const now = Date.now();
    const ns = 'bridgemq';
    
    const keys = [
      `${ns}:job:${jobId}:meta`,
      `${ns}:active:${serverId}`,
      `${ns}:job:${jobId}:errors`,
    ];
    
    const args = [
      jobId,
      serverId,
      JSON.stringify(error),
      now.toString(),
      ns,
    ];

    const result = await redis.evalsha(
      this.scripts.retryJob.sha,
      keys.length,
      ...keys,
      ...args,
    );

    return JSON.parse(result);
  }

  /**
   * Process delayed jobs (move to pending when ready)
   * @param {Redis} redis - Redis client
   * @param {number} limit - Max jobs to process (default 100)
   * @returns {Promise<Object>} Processing result
   */
  async processDelayed(redis, limit = 100) {
    const now = Date.now();
    const ns = 'bridgemq';
    
    const keys = [`${ns}:delayed`];
    const args = [now.toString(), ns, limit.toString()];

    const result = await redis.evalsha(
      this.scripts.processDelayed.sha,
      keys.length,
      ...keys,
      ...args,
    );

    return JSON.parse(result);
  }

  /**
   * Detect stalled jobs
   * @param {Redis} redis - Redis client
   * @param {number} stallTimeoutMs - Stall timeout in milliseconds (default 300000 = 5 min)
   * @param {number} maxStallCount - Max stall count before DLQ (default 3)
   * @returns {Promise<Object>} Detection result
   */
  async detectStalled(redis, stallTimeoutMs = 300000, maxStallCount = 3) {
    const now = Date.now();
    const ns = 'bridgemq';
    
    const args = [
      now.toString(),
      stallTimeoutMs.toString(),
      maxStallCount.toString(),
      ns,
    ];

    const result = await redis.evalsha(
      this.scripts.detectStalled.sha,
      0, // No keys
      ...args,
    );

    return JSON.parse(result);
  }

  /**
   * Check rate limit
   * @param {Redis} redis - Redis client
   * @param {string} key - Rate limit key
   * @param {number} max - Max requests
   * @param {number} windowSeconds - Time window in seconds
   * @param {string} jobId - Job ID to queue if limit exceeded (optional)
   * @returns {Promise<Object>} Rate limit check result
   */
  async rateLimitCheck(redis, key, max, windowSeconds, jobId = '') {
    const now = Date.now();
    const ns = 'bridgemq';
    
    const keys = [
      `${ns}:ratelimit:${key}`,
      `${ns}:ratelimitqueue:${key}`,
    ];
    
    const args = [
      key,
      max.toString(),
      windowSeconds.toString(),
      jobId,
      now.toString(),
      ns,
    ];

    const result = await redis.evalsha(
      this.scripts.rateLimitCheck.sha,
      keys.length,
      ...keys,
      ...args,
    );

    return JSON.parse(result);
  }

  /**
   * Finalize a batch
   * @param {Redis} redis - Redis client
   * @param {string} batchKey - Batch key
   * @param {string} batchId - Batch ID
   * @param {string} meshId - Mesh ID
   * @param {string} jobType - Job type
   * @param {number} priority - Priority (1-10)
   * @returns {Promise<Object>} Batch finalization result
   */
  async finalizeBatch(redis, batchKey, batchId, meshId, jobType, priority = 5) {
    const now = Date.now();
    const ns = 'bridgemq';
    
    const keys = [
      `${ns}:batch:${batchKey}`,
      `${ns}:batch:${batchId}:meta`,
    ];
    
    const args = [
      batchKey,
      batchId,
      meshId,
      jobType,
      priority.toString(),
      now.toString(),
      ns,
    ];

    const result = await redis.evalsha(
      this.scripts.batchJobs.sha,
      keys.length,
      ...keys,
      ...args,
    );

    return JSON.parse(result);
  }

  /**
   * Reload all scripts (useful for development/updates)
   * @param {Redis} redis - Redis client
   * @returns {Promise<void>}
   */
  async reload(redis) {
    this.loaded = false;
    this.scripts = {};
    await this.load(redis);
  }
}

// Export singleton instance
module.exports = new LuaScripts();
