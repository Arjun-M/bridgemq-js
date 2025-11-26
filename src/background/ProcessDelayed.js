const scripts = require('../scripts');

/**
 * ProcessDelayed - Process delayed jobs every second
 * 
 * PURPOSE: Background service to move delayed jobs to pending queues
 * 
 * FEATURES:
 * - Interval-based execution (1 second)
 * - Batch processing (100 jobs per run)
 * - Error handling and retry
 * - Graceful shutdown
 * - Performance monitoring
 * 
 * LOGIC:
 * 1. Every 1 second, scan delayed sorted set
 * 2. Find jobs with scheduledFor <= now
 * 3. Move to pending queues via Lua script
 * 4. Update job status to 'pending'
 * 5. Publish job.scheduled event
 */
class ProcessDelayed {
  /**
   * Create ProcessDelayed service
   * @param {Redis} redis - Redis client
   * @param {Object} options - Service options
   */
  constructor(redis, options = {}) {
    this.redis = redis;
    this.options = {
      intervalMs: options.intervalMs || 1000, // Check every second
      batchSize: options.batchSize || 100,
      enabled: options.enabled !== false,
    };

    this.running = false;
    this.intervalId = null;
    this.stats = {
      processed: 0,
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
    console.log('[ProcessDelayed] Starting service');

    this.intervalId = setInterval(async () => {
      await this._processDelayed();
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
    console.log('[ProcessDelayed] Stopping service');

    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
  }

  /**
   * Process delayed jobs (internal)
   * @private
   */
  async _processDelayed() {
    try {
      const result = await scripts.processDelayed(
        this.redis,
        this.options.batchSize,
      );

      if (result.processed > 0) {
        this.stats.processed += result.processed;
        console.log(
          `[ProcessDelayed] Processed ${result.processed} delayed jobs`,
        );
      }

      this.stats.lastRun = Date.now();
    } catch (error) {
      this.stats.errors++;
      console.error(
        '[ProcessDelayed] Error processing delayed jobs:',
        error.message,
      );
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
    this.stats.processed = 0;
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
}

module.exports = ProcessDelayed;
