const JobStorage = require('../storage/JobStorage');

/**
 * Job - Job instance representation with helper methods
 * 
 * PURPOSE: Provide convenient interface to job data and operations
 * 
 * FEATURES:
 * - Access job metadata, config, payload, result
 * - Update progress
 * - Log messages
 * - Trigger retry
 * - Type-safe accessors
 */
class Job {
  /**
   * Create job instance
   * @param {Object} jobData - Job data from storage
   * @param {Redis} redis - Redis client
   */
  constructor(jobData, redis) {
    this.redis = redis;
    
    // Core metadata
    this.jobId = jobData.jobId;
    this.type = jobData.type;
    this.version = jobData.version;
    this.meshId = jobData.meshId;
    this.status = jobData.status;
    this.priority = jobData.priority;
    this.attempt = jobData.attempt;
    this.progress = jobData.progress;
    this.stalledCount = jobData.stalledCount;

    // Timestamps
    this.createdAt = jobData.createdAt;
    this.scheduledFor = jobData.scheduledFor;
    this.claimedAt = jobData.claimedAt;
    this.completedAt = jobData.completedAt;
    this.updatedAt = jobData.updatedAt;

    // Server info
    this.processedBy = jobData.processedBy;

    // Full data
    this._config = jobData.config;
    this._payload = jobData.payload;
    this._result = jobData.result;
    this._errors = jobData.errors;
  }

  /**
   * Get job metadata
   * @returns {Object} Job metadata
   */
  getMetadata() {
    return {
      jobId: this.jobId,
      type: this.type,
      version: this.version,
      meshId: this.meshId,
      status: this.status,
      priority: this.priority,
      attempt: this.attempt,
      progress: this.progress,
      stalledCount: this.stalledCount,
      createdAt: this.createdAt,
      scheduledFor: this.scheduledFor,
      claimedAt: this.claimedAt,
      completedAt: this.completedAt,
      updatedAt: this.updatedAt,
      processedBy: this.processedBy,
    };
  }

  /**
   * Get job configuration
   * @returns {Object} Job config
   */
  getConfig() {
    return this._config;
  }

  /**
   * Get job payload
   * @returns {any} Job payload
   */
  getPayload() {
    return this._payload;
  }

  /**
   * Get job result
   * @returns {any} Job result
   */
  getResult() {
    return this._result;
  }

  /**
   * Get job errors
   * @returns {Array} Job errors
   */
  getErrors() {
    return this._errors || [];
  }

  /**
   * Set progress (0-100)
   * @param {number} percent - Progress percentage
   * @returns {Promise<void>}
   */
  async setProgress(percent) {
    if (percent < 0 || percent > 100) {
      throw new Error('Progress must be between 0 and 100');
    }

    const NS = 'bridgemq';
    const metaKey = `${NS}:job:${this.jobId}:meta`;
    
    await this.redis.hset(metaKey, 'progress', percent);
    this.progress = percent;
  }

  /**
   * Log a message (console.log wrapper for job context)
   * @param {string} message - Log message
   */
  log(message) {
    console.log(`[Job ${this.jobId}] ${message}`);
  }

  /**
   * Trigger retry (throw error to signal retry to worker)
   */
  retry() {
    throw new Error('RETRY_JOB');
  }

  /**
   * Check if job is pending
   * @returns {boolean}
   */
  isPending() {
    return this.status === 'pending';
  }

  /**
   * Check if job is active
   * @returns {boolean}
   */
  isActive() {
    return this.status === 'active';
  }

  /**
   * Check if job is completed
   * @returns {boolean}
   */
  isCompleted() {
    return this.status === 'completed';
  }

  /**
   * Check if job is failed
   * @returns {boolean}
   */
  isFailed() {
    return this.status === 'failed';
  }

  /**
   * Check if job is cancelled
   * @returns {boolean}
   */
  isCancelled() {
    return this.status === 'cancelled';
  }

  /**
   * Check if job is scheduled
   * @returns {boolean}
   */
  isScheduled() {
    return this.status === 'scheduled';
  }

  /**
   * Check if job is stalled
   * @returns {boolean}
   */
  isStalled() {
    return this.status === 'stalled';
  }

  /**
   * Get time since creation (ms)
   * @returns {number} Time elapsed
   */
  getAge() {
    return Date.now() - this.createdAt;
  }

  /**
   * Get processing time (ms)
   * @returns {number|null} Processing time or null if not completed
   */
  getProcessingTime() {
    if (!this.claimedAt || !this.completedAt) {
      return null;
    }
    return this.completedAt - this.claimedAt;
  }

  /**
   * Get wait time (time between creation and claim)
   * @returns {number|null} Wait time or null if not claimed
   */
  getWaitTime() {
    if (!this.claimedAt) {
      return null;
    }
    return this.claimedAt - this.createdAt;
  }

  /**
   * Create Job instance from job ID
   * @param {Redis} redis - Redis client
   * @param {string} jobId - Job ID
   * @returns {Promise<Job|null>} Job instance or null
   */
  static async fromId(redis, jobId) {
    const jobData = await JobStorage.getJob(redis, jobId);
    
    if (!jobData) {
      return null;
    }

    return new Job(jobData, redis);
  }

  /**
   * Serialize job to JSON
   * @returns {Object} JSON representation
   */
  toJSON() {
    return {
      ...this.getMetadata(),
      config: this._config,
      payload: this._payload,
      result: this._result,
      errors: this._errors,
    };
  }
}

module.exports = Job;
