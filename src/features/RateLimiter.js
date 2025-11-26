const scripts = require('../scripts');

/**
 * RateLimiter - Enforce rate limits across distributed system
 * 
 * PURPOSE: Prevent overwhelming external services or APIs
 * 
 * ALGORITHM: Token bucket with distributed counters
 * 
 * FEATURES:
 * - Per-key rate limiting
 * - Sliding window
 * - Queue excess jobs
 * - Distributed coordination via Redis
 * 
 * EXAMPLE:
 * limit: 100 requests per 60 seconds
 * - Bucket starts with 100 tokens
 * - Each request consumes 1 token
 * - Tokens refill over time
 * - Excess requests queued
 */
class RateLimiter {
  constructor(redis, options = {}) {
    this.redis = redis;
    this.options = {
      namespace: options.namespace || 'bridgemq',
    };
  }

  async checkLimit(key, max, windowSeconds) {
    return await scripts.rateLimitCheck(
      this.redis,
      key,
      max,
      windowSeconds,
      '',
    );
  }

  async checkAndQueue(key, max, windowSeconds, jobId) {
    return await scripts.rateLimitCheck(
      this.redis,
      key,
      max,
      windowSeconds,
      jobId,
    );
  }

  async getCount(key) {
    const redisKey = `${this.options.namespace}:ratelimit:${key}`;
    return parseInt(await this.redis.get(redisKey) || 0, 10);
  }

  async reset(key) {
    const redisKey = `${this.options.namespace}:ratelimit:${key}`;
    await this.redis.del(redisKey);
  }
}

module.exports = RateLimiter;
