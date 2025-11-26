const JobStorage = require('../storage/JobStorage');

/**
 * Cleaner - Clean up expired data every 5 minutes
 * 
 * PURPOSE: Periodic cleanup of stale data to prevent memory bloat
 * 
 * CLEANUP TASKS:
 * - Remove completed jobs older than TTL
 * - Remove cancelled jobs older than TTL
 * - Remove dead server registrations
 * - Clean up orphaned data structures
 * - Prune old metrics data
 * 
 * FEATURES:
 * - Configurable TTL per job status
 * - Batch deletion to avoid blocking
 * - Cleanup statistics tracking
 * - Graceful shutdown
 */
class Cleaner {
  /**
   * Create Cleaner service
   * @param {Redis} redis - Redis client
   * @param {Object} options - Service options
   */
  constructor(redis, options = {}) {
    this.redis = redis;
    this.options = {
      intervalMs: options.intervalMs || 300000, // Clean every 5 minutes
      completedJobTTL: options.completedJobTTL || 86400000, // 24 hours
      failedJobTTL: options.failedJobTTL || 604800000, // 7 days
      cancelledJobTTL: options.cancelledJobTTL || 86400000, // 24 hours
      deadServerTTL: options.deadServerTTL || 300000, // 5 minutes
      batchSize: options.batchSize || 100,
      enabled: options.enabled !== false,
    };

    this.running = false;
    this.intervalId = null;
    this.stats = {
      jobsDeleted: 0,
      serversRemoved: 0,
      errors: 0,
      lastRun: null,
    };
  }

  /**
   * Start the service
   */
  start() {
    if (this.running || !this.options.enabled) {
      return;
    }

    this.running = true;
    console.log('[Cleaner] Starting service');

    this.intervalId = setInterval(async () => {
      await this._cleanup();
    }, this.options.intervalMs);
  }

  /**
   * Stop the service
   */
  stop() {
    if (!this.running) {
      return;
    }

    this.running = false;
    console.log('[Cleaner] Stopping service');

    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
  }

  /**
   * Run cleanup tasks (internal)
   * @private
   */
  async _cleanup() {
    try {
      await this._cleanupCompletedJobs();
      await this._cleanupCancelledJobs();
      await this._cleanupDeadServers();

      this.stats.lastRun = Date.now();
    } catch (error) {
      this.stats.errors++;
      console.error('[Cleaner] Error during cleanup:', error.message);
    }
  }

  /**
   * Clean up completed jobs
   * @private
   */
  async _cleanupCompletedJobs() {
    const cutoffTime = Date.now() - this.options.completedJobTTL;
    const deleted = await this._deleteOldJobs('completed', cutoffTime);
    
    if (deleted > 0) {
      this.stats.jobsDeleted += deleted;
      console.log(`[Cleaner] Deleted ${deleted} completed jobs`);
    }
  }

  /**
   * Clean up cancelled jobs
   * @private
   */
  async _cleanupCancelledJobs() {
    const cutoffTime = Date.now() - this.options.cancelledJobTTL;
    const deleted = await this._deleteOldJobs('cancelled', cutoffTime);
    
    if (deleted > 0) {
      this.stats.jobsDeleted += deleted;
      console.log(`[Cleaner] Deleted ${deleted} cancelled jobs`);
    }
  }

  /**
   * Delete old jobs by status
   * @private
   * @param {string} status - Job status
   * @param {number} cutoffTime - Cutoff timestamp
   * @returns {Promise<number>} Number of deleted jobs
   */
  async _deleteOldJobs(status, cutoffTime) {
    const pattern = 'bridgemq:job:*:meta';
    const keys = await this.redis.keys(pattern);
    
    let deleted = 0;

    for (const key of keys) {
      const jobMeta = await this.redis.hgetall(key);
      
      if (
        jobMeta.status === status &&
        parseInt(jobMeta.completedAt || jobMeta.updatedAt, 10) < cutoffTime
      ) {
        const jobId = key.split(':')[3];
        await JobStorage.deleteJob(this.redis, jobId);
        deleted++;
      }

      // Batch processing
      if (deleted >= this.options.batchSize) {
        break;
      }
    }

    return deleted;
  }

  /**
   * Clean up dead server registrations
   * @private
   */
  async _cleanupDeadServers() {
    const pattern = 'bridgemq:server:*';
    const keys = await this.redis.keys(pattern);
    const now = Date.now();
    const cutoffTime = now - this.options.deadServerTTL;
    
    let removed = 0;

    for (const key of keys) {
      const server = await this.redis.hgetall(key);
      const lastHeartbeat = parseInt(server.lastHeartbeat || 0, 10);

      if (lastHeartbeat < cutoffTime) {
        await this.redis.del(key);
        removed++;
        console.log(`[Cleaner] Removed dead server: ${server.serverId}`);
      }
    }

    if (removed > 0) {
      this.stats.serversRemoved += removed;
    }
  }

  /**
   * Get service statistics
   * @returns {Object} Statistics
   */
  getStats() {
    return {
      ...this.stats,
      running: this.running,
    };
  }

  /**
   * Reset statistics
   */
  resetStats() {
    this.stats.jobsDeleted = 0;
    this.stats.serversRemoved = 0;
    this.stats.errors = 0;
    this.stats.lastRun = null;
  }

  /**
   * Check if service is running
   * @returns {boolean} Running status
   */
  isRunning() {
    return this.running;
  }

  /**
   * Run cleanup immediately (on-demand)
   * @returns {Promise<void>}
   */
  async cleanupNow() {
    await this._cleanup();
  }
}

module.exports = Cleaner;
