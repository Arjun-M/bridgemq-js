const scripts = require('../scripts');

/**
 * Batching - Aggregate multiple jobs into batches
 * 
 * PURPOSE: Process multiple related jobs together efficiently
 * 
 * FEATURES:
 * - Batch collection by key
 * - Size and time-based triggers
 * - Atomic batch creation
 * - Member tracking
 * 
 * TRIGGERS:
 * - maxSize: Create batch when N jobs collected
 * - maxWaitMs: Create batch after timeout
 * - manual: Create batch on demand
 * 
 * LOGIC:
 * 1. Jobs added to batch with key
 * 2. When trigger condition met, finalize batch
 * 3. Create aggregate batch job
 * 4. Remove individual jobs from queues
 * 5. Process batch as single unit
 */
class Batching {
  constructor(redis, options = {}) {
    this.redis = redis;
    this.options = {
      namespace: options.namespace || 'bridgemq',
      defaultMaxSize: options.defaultMaxSize || 10,
      defaultMaxWaitMs: options.defaultMaxWaitMs || 5000,
    };
    this.timers = new Map();
  }

  async addToBatch(batchKey, jobId) {
    const key = `${this.options.namespace}:batch:${batchKey}`;
    await this.redis.sadd(key, jobId);
    
    const size = await this.redis.scard(key);
    return size;
  }

  async finalizeBatch(batchKey, meshId, type) {
    return await scripts.batchJobs(
      this.redis,
      batchKey,
      meshId,
      type,
    );
  }

  async getBatchMembers(batchKey) {
    const key = `${this.options.namespace}:batch:${batchKey}`;
    return await this.redis.smembers(key);
  }

  async clearBatch(batchKey) {
    const key = `${this.options.namespace}:batch:${batchKey}`;
    await this.redis.del(key);
  }

  startAutoFinalize(batchKey, maxSize, maxWaitMs, meshId, type) {
    const timer = setInterval(async () => {
      const size = await this.redis.scard(
        `${this.options.namespace}:batch:${batchKey}`,
      );
      
      if (size >= maxSize) {
        await this.finalizeBatch(batchKey, meshId, type);
        clearInterval(timer);
      }
    }, 1000);

    setTimeout(async () => {
      await this.finalizeBatch(batchKey, meshId, type);
      clearInterval(timer);
    }, maxWaitMs);

    this.timers.set(batchKey, timer);
  }

  stopAutoFinalize(batchKey) {
    const timer = this.timers.get(batchKey);
    if (timer) {
      clearInterval(timer);
      this.timers.delete(batchKey);
    }
  }
}

module.exports = Batching;
