/**
 * Idempotency - Prevent duplicate job processing
 * 
 * PURPOSE: Ensure jobs are not processed multiple times
 * 
 * FEATURES:
 * - Idempotency key management
 * - TTL-based key expiry
 * - Conflict resolution
 * - Result caching
 * 
 * LOGIC:
 * 1. Client provides idempotency key
 * 2. Check if key exists in Redis
 * 3. If exists, return cached result
 * 4. If not, process and store result
 * 5. Set TTL on key (default 24 hours)
 * 
 * REDIS KEYS:
 * - idempotency:{key} -> jobId
 * - job:{jobId}:result -> result
 */
class Idempotency {
  /**
   * Create idempotency manager
   * @param {Redis} redis - Redis client
   * @param {Object} options - Manager options
   */
  constructor(redis, options = {}) {
    this.redis = redis;
    this.options = {
      ttlSeconds: options.ttlSeconds || 86400, // 24 hours
      namespace: options.namespace || 'bridgemq',
    };
  }

  /**
   * Check if idempotency key exists
   * @param {string} key - Idempotency key
   * @returns {Promise<string|null>} Job ID or null
   */
  async checkKey(key) {
    const redisKey = `${this.options.namespace}:idempotency:${key}`;
    return await this.redis.get(redisKey);
  }

  /**
   * Register idempotency key for job
   * @param {string} key - Idempotency key
   * @param {string} jobId - Job ID
   * @returns {Promise<boolean>} True if registered (false if already exists)
   */
  async registerKey(key, jobId) {
    const redisKey = `${this.options.namespace}:idempotency:${key}`;
    
    // Use SETNX to set only if not exists
    const result = await this.redis.set(
      redisKey,
      jobId,
      'EX',
      this.options.ttlSeconds,
      'NX',
    );

    return result === 'OK';
  }

  /**
   * Get job ID for idempotency key
   * @param {string} key - Idempotency key
   * @returns {Promise<string|null>} Job ID or null
   */
  async getJobId(key) {
    return await this.checkKey(key);
  }

  /**
   * Remove idempotency key
   * @param {string} key - Idempotency key
   * @returns {Promise<void>}
   */
  async removeKey(key) {
    const redisKey = `${this.options.namespace}:idempotency:${key}`;
    await this.redis.del(redisKey);
  }

  /**
   * Extend key TTL
   * @param {string} key - Idempotency key
   * @param {number} seconds - TTL in seconds
   * @returns {Promise<void>}
   */
  async extendTTL(key, seconds) {
    const redisKey = `${this.options.namespace}:idempotency:${key}`;
    await this.redis.expire(redisKey, seconds);
  }

  /**
   * Generate idempotency key from payload
   * @param {any} payload - Job payload
   * @returns {string} Idempotency key
   */
  generateKey(payload) {
    const crypto = require('crypto');
    const content = JSON.stringify(payload);
    return crypto.createHash('sha256').update(content).digest('hex');
  }
}

module.exports = Idempotency;
