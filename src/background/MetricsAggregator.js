const MetricsStorage = require('../storage/MetricsStorage');

/**
 * MetricsAggregator - Aggregate and flush metrics every minute
 * 
 * PURPOSE: Periodic metrics aggregation and persistence
 * 
 * FEATURES:
 * - Counter aggregation
 * - Rate calculation (jobs/min, jobs/sec)
 * - Time-series data updates
 * - Throughput metrics
 * - Batch metric persistence
 * 
 * AGGREGATIONS:
 * - jobs_per_minute
 * - failures_per_minute
 * - avg_processing_time
 * - queue_depth_samples
 * - throughput_rate
 * 
 * LOGIC:
 * 1. Every 1 minute, collect metrics
 * 2. Calculate rates and aggregates
 * 3. Persist to Redis
 * 4. Update time-series data
 * 5. Reset counters for next period
 */
class MetricsAggregator {
  /**
   * Create MetricsAggregator service
   * @param {Redis} redis - Redis client
   * @param {Object} options - Service options
   */
  constructor(redis, options = {}) {
    this.redis = redis;
    this.options = {
      intervalMs: options.intervalMs || 60000, // Aggregate every minute
      meshId: options.meshId,
      enabled: options.enabled !== false,
    };

    this.running = false;
    this.intervalId = null;
    this.counters = {
      jobsCreated: 0,
      jobsCompleted: 0,
      jobsFailed: 0,
    };
    this.stats = {
      aggregations: 0,
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
    console.log('[MetricsAggregator] Starting service');

    this.intervalId = setInterval(async () => {
      await this._aggregate();
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
    console.log('[MetricsAggregator] Stopping service');

    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
  }

  /**
   * Run aggregation (internal)
   * @private
   */
  async _aggregate() {
    if (!this.options.meshId) {
      return;
    }

    try {
      // Calculate time bucket (YYYY-MM-DD-HH:MM)
      const now = new Date();
      const bucket = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}-${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}`;

      // Record jobs per minute
      if (this.counters.jobsCompleted > 0) {
        await MetricsStorage.recordBucket(
          this.redis,
          this.options.meshId,
          'jobs-per-min',
          bucket,
          this.counters.jobsCompleted,
        );
      }

      // Record failures per minute
      if (this.counters.jobsFailed > 0) {
        await MetricsStorage.recordBucket(
          this.redis,
          this.options.meshId,
          'failed-per-min',
          bucket,
          this.counters.jobsFailed,
        );
      }

      // Get current throughput
      const throughput = await MetricsStorage.getThroughput(
        this.redis,
        this.options.meshId,
      );

      console.log(
        `[MetricsAggregator] Bucket ${bucket}: ${throughput.completed} completed, ${throughput.failed} failed`,
      );

      // Reset counters
      this._resetCounters();

      this.stats.aggregations++;
      this.stats.lastRun = Date.now();
    } catch (error) {
      this.stats.errors++;
      console.error(
        '[MetricsAggregator] Error during aggregation:',
        error.message,
      );
    }
  }

  /**
   * Increment job created counter
   * @param {number} count - Count to add
   */
  incrementJobsCreated(count = 1) {
    this.counters.jobsCreated += count;
  }

  /**
   * Increment job completed counter
   * @param {number} count - Count to add
   */
  incrementJobsCompleted(count = 1) {
    this.counters.jobsCompleted += count;
  }

  /**
   * Increment job failed counter
   * @param {number} count - Count to add
   */
  incrementJobsFailed(count = 1) {
    this.counters.jobsFailed += count;
  }

  /**
   * Reset all counters
   * @private
   */
  _resetCounters() {
    this.counters.jobsCreated = 0;
    this.counters.jobsCompleted = 0;
    this.counters.jobsFailed = 0;
  }

  /**
   * Get current counters
   * @returns {Object} Counters
   */
  getCounters() {
    return { ...this.counters };
  }

  /**
   * Get service statistics
   * @returns {Object} Statistics
   */
  getStats() {
    return {
      ...this.stats,
      running: this.running,
      counters: this.getCounters(),
    };
  }

  /**
   * Reset statistics
   */
  resetStats() {
    this.stats.aggregations = 0;
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
   * Run aggregation immediately (on-demand)
   * @returns {Promise<void>}
   */
  async aggregateNow() {
    await this._aggregate();
  }
}

module.exports = MetricsAggregator;
