const JobStorage = require('../storage/JobStorage');
const QueueStorage = require('../storage/QueueStorage');
const scripts = require('../scripts');
const ExponentialBackoff = require('./ExponentialBackoff');
const LinearBackoff = require('./LinearBackoff');

/**
 * RetryManager - Manage retry decisions and execution
 * 
 * PURPOSE: Centralized retry management with strategy support
 * 
 * FEATURES:
 * - Multiple retry strategies (exponential, linear, fixed)
 * - Retry eligibility checking
 * - Backoff delay calculation with jitter
 * - DLQ movement for exhausted retries
 * - Error categorization (retryable vs non-retryable)
 * 
 * LOGIC:
 * 1. Check if job is eligible for retry
 * 2. Calculate next retry delay using strategy
 * 3. Move job to delayed queue or DLQ
 * 4. Update job metadata (attempt, nextRetry)
 */
class RetryManager {
  /**
   * Create retry manager
   * @param {Redis} redis - Redis client
   * @param {Object} options - Manager options
   */
  constructor(redis, options = {}) {
    this.redis = redis;
    this.options = {
      defaultStrategy: options.defaultStrategy || 'exponential',
      maxAttempts: options.maxAttempts || 3,
      baseDelayMs: options.baseDelayMs || 1000,
      maxDelayMs: options.maxDelayMs || 60000,
      enableDLQ: options.enableDLQ !== false,
    };

    // Initialize default strategy
    this.defaultStrategy = this._createStrategy(this.options.defaultStrategy);
  }

  /**
   * Should job be retried?
   * @param {Object} job - Job metadata
   * @param {Object} error - Error object
   * @returns {boolean} True if should retry
   */
  shouldRetry(job, error) {
    // Check if job has retries configured
    const jobConfig = job.config || {};
    const retryConfig = jobConfig.retry || {};

    // Check if retries disabled
    if (retryConfig.enabled === false) {
      return false;
    }

    // Check attempt count
    const maxAttempts = retryConfig.maxAttempts || this.options.maxAttempts;
    const currentAttempt = job.attempt || 0;

    if (currentAttempt >= maxAttempts) {
      return false; // Exhausted retries
    }

    // Check if error is retryable
    if (error && !this.isRetryableError(error)) {
      return false;
    }

    return true;
  }

  /**
   * Check if error is retryable
   * @param {Object} error - Error object
   * @returns {boolean} True if retryable
   */
  isRetryableError(error) {
    // Non-retryable error codes (4xxx validation errors)
    const nonRetryableCodes = [
      1001, // INVALID_PAYLOAD
      1002, // INVALID_CONFIG
      3003, // WORKER_CAPABILITY_MISMATCH
    ];

    if (error.code && nonRetryableCodes.includes(error.code)) {
      return false;
    }

    // Check error type
    if (error.retryable === false) {
      return false;
    }

    // Default: transient errors are retryable
    return true;
  }

  /**
   * Calculate next retry delay
   * @param {Object} job - Job metadata
   * @returns {number} Delay in milliseconds
   */
  calculateRetryDelay(job) {
    const jobConfig = job.config || {};
    const retryConfig = jobConfig.retry || {};

    // Get strategy for job
    const strategyType = retryConfig.strategy || this.options.defaultStrategy;
    const strategy = this._createStrategy(strategyType, retryConfig);

    // Calculate delay with jitter
    const attempt = (job.attempt || 0) + 1;
    return strategy.calculateDelayWithJitter(attempt);
  }

  /**
   * Schedule retry for job
   * @param {string} jobId - Job ID
   * @param {number} delayMs - Delay in milliseconds
   * @returns {Promise<void>}
   */
  async scheduleRetry(jobId, delayMs) {
    const now = Date.now();
    const scheduledFor = now + delayMs;

    // Move job to delayed queue
    await QueueStorage.pushToDelayed(this.redis, jobId, scheduledFor);

    // Update job metadata
    const metaKey = `bridgemq:v1:job:${jobId}:meta`;
    await this.redis.hmset(metaKey, {
      status: 'scheduled',
      scheduledFor,
      updatedAt: now,
    });
  }

  /**
   * Move job to DLQ
   * @param {string} jobId - Job ID
   * @param {string} meshId - Mesh ID
   * @returns {Promise<void>}
   */
  async moveToDLQ(jobId, meshId) {
    await QueueStorage.pushToDLQ(this.redis, meshId, jobId);

    // Update job status
    await JobStorage.setJobStatus(this.redis, jobId, 'failed');
  }

  /**
   * Handle job failure with retry logic
   * @param {string} jobId - Job ID
   * @param {string} serverId - Server ID
   * @param {Object} error - Error object
   * @returns {Promise<Object>} Result { retried, dlq, delay }
   */
  async handleFailure(jobId, serverId, error) {
    // Get job metadata
    const job = await JobStorage.getJob(this.redis, jobId);

    if (!job) {
      return { retried: false, dlq: false };
    }

    // Check if should retry
    if (this.shouldRetry(job, error)) {
      // Calculate delay
      const delayMs = this.calculateRetryDelay(job);

      // Use retry script
      await scripts.retryJob(this.redis, jobId, serverId, error);

      return {
        retried: true,
        dlq: false,
        delay: delayMs,
        nextAttempt: (job.attempt || 0) + 1,
      };
    }

    // Move to DLQ if enabled
    if (this.options.enableDLQ) {
      await this.moveToDLQ(jobId, job.meshId);
      
      return {
        retried: false,
        dlq: true,
      };
    }

    // Just mark as failed
    await JobStorage.setJobStatus(this.redis, jobId, 'failed');

    return {
      retried: false,
      dlq: false,
    };
  }

  /**
   * Create retry strategy instance
   * @private
   * @param {string} type - Strategy type
   * @param {Object} config - Strategy config
   * @returns {RetryStrategy} Strategy instance
   */
  _createStrategy(type, config = {}) {
    const options = {
      baseDelayMs: config.baseDelayMs || this.options.baseDelayMs,
      maxDelayMs: config.maxDelayMs || this.options.maxDelayMs,
      maxAttempts: config.maxAttempts || this.options.maxAttempts,
      jitterFactor: config.jitterFactor || 0.2,
    };

    switch (type) {
      case 'exponential':
        return new ExponentialBackoff(options);
      
      case 'linear':
        return new LinearBackoff({
          ...options,
          stepMs: config.stepMs || 2000,
        });
      
      case 'fixed':
        // Fixed delay (no backoff)
        return new LinearBackoff({
          ...options,
          stepMs: 0, // No increase
        });
      
      default:
        return new ExponentialBackoff(options);
    }
  }

  /**
   * Get retry statistics for job
   * @param {Object} job - Job metadata
   * @returns {Object} Retry stats
   */
  getRetryStats(job) {
    const jobConfig = job.config || {};
    const retryConfig = jobConfig.retry || {};
    const maxAttempts = retryConfig.maxAttempts || this.options.maxAttempts;
    const currentAttempt = job.attempt || 0;

    return {
      currentAttempt,
      maxAttempts,
      remainingAttempts: Math.max(0, maxAttempts - currentAttempt),
      exhausted: currentAttempt >= maxAttempts,
    };
  }
}

module.exports = RetryManager;
