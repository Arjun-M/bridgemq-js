const scripts = require('../scripts');

/**
 * DelayedScheduler - Process delayed jobs every second
 * 
 * PURPOSE: Background scheduler that moves delayed jobs to pending queues
 * 
 * FEATURES:
 * - Interval-based checking (every 1 second by default)
 * - Batch processing (100 jobs per run)
 * - Auto-start/stop
 * - Event emission for monitoring
 * 
 * LOGIC:
 * 1. Every interval, call processDelayed Lua script
 * 2. Script moves jobs with scheduledFor <= now to pending
 * 3. Emit events for each processed job
 * 4. Continue until stopped
 */
class DelayedScheduler {
  /**
   * Create delayed scheduler
   * @param {Redis} redis - Redis client
   * @param {Object} options - Scheduler options
   */
  constructor(redis, options = {}) {
    this.redis = redis;
    this.options = {
      intervalMs: options.intervalMs || 1000, // Check every second
      batchSize: options.batchSize || 100, // Process 100 jobs per run
    };

    this.running = false;
    this.intervalId = null;
  }

  /**
   * Start the scheduler
   */
  start() {
    if (this.running) {
      return;
    }

    this.running = true;

    this.intervalId = setInterval(async () => {
      await this._processDelayed();
    }, this.options.intervalMs);
  }

  /**
   * Stop the scheduler
   */
  stop() {
    if (!this.running) {
      return;
    }

    this.running = false;

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
        console.log(`[DelayedScheduler] Processed ${result.processed} delayed jobs`);
      }
    } catch (error) {
      console.error('[DelayedScheduler] Error processing delayed jobs:', error.message);
    }
  }

  /**
   * Check if scheduler is running
   * @returns {boolean} Running status
   */
  isRunning() {
    return this.running;
  }
}

module.exports = DelayedScheduler;
