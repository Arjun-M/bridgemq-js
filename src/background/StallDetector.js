const scripts = require('../scripts');
const ServerStorage = require('../storage/ServerStorage');

/**
 * StallDetector - Detect and recover stalled jobs every 30 seconds
 * 
 * PURPOSE: Monitor active jobs and recover jobs stuck in processing
 * 
 * FEATURES:
 * - Periodic stall checking (30 seconds)
 * - Configurable stall timeout
 * - Automatic retry or DLQ movement
 * - Per-server active job scanning
 * - Stall statistics tracking
 * 
 * STALL DETECTION:
 * - Job claimed but not completed within timeout
 * - Server crashed or network partition
 * - Worker stuck in infinite loop
 * - Unhandled exceptions
 * 
 * RECOVERY:
 * - Increment stall counter
 * - Retry if under stall limit
 * - Move to DLQ if limit exceeded
 */
class StallDetector {
  /**
   * Create StallDetector service
   * @param {Redis} redis - Redis client
   * @param {Object} options - Service options
   */
  constructor(redis, options = {}) {
    this.redis = redis;
    this.options = {
      intervalMs: options.intervalMs || 30000, // Check every 30 seconds
      stallTimeoutMs: options.stallTimeoutMs || 300000, // 5 minutes
      maxStallCount: options.maxStallCount || 3,
      batchSize: options.batchSize || 100,
      enabled: options.enabled !== false,
    };

    this.running = false;
    this.intervalId = null;
    this.stats = {
      detected: 0,
      recovered: 0,
      dlqMoved: 0,
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
    console.log('[StallDetector] Starting service');

    this.intervalId = setInterval(async () => {
      await this._detectStalled();
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
    console.log('[StallDetector] Stopping service');

    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
  }

  /**
   * Detect stalled jobs (internal)
   * @private
   */
  async _detectStalled() {
    try {
      // Get all servers to scan their active jobs
      const servers = await this._getAllServers();

      for (const serverId of servers) {
        await this._checkServerActiveJobs(serverId);
      }

      this.stats.lastRun = Date.now();
    } catch (error) {
      this.stats.errors++;
      console.error(
        '[StallDetector] Error detecting stalled jobs:',
        error.message,
      );
    }
  }

  /**
   * Get all server IDs
   * @private
   * @returns {Promise<Array<string>>} Server IDs
   */
  async _getAllServers() {
    const pattern = 'bridgemq:server:*';
    const keys = await this.redis.keys(pattern);
    
    return keys.map((key) => {
      const parts = key.split(':');
      return parts[parts.length - 1];
    });
  }

  /**
   * Check active jobs for a server
   * @private
   * @param {string} serverId - Server ID
   */
  async _checkServerActiveJobs(serverId) {
    try {
      const result = await scripts.detectStalled(
        this.redis,
        serverId,
        this.options.stallTimeoutMs,
        this.options.maxStallCount,
        this.options.batchSize,
      );

      if (result.detected > 0) {
        this.stats.detected += result.detected;
        this.stats.recovered += result.recovered;
        this.stats.dlqMoved += result.dlqMoved;

        console.log(
          `[StallDetector] Server ${serverId}: ${result.detected} stalled, ${result.recovered} recovered, ${result.dlqMoved} to DLQ`,
        );
      }
    } catch (error) {
      console.error(
        `[StallDetector] Error checking server ${serverId}:`,
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
    this.stats.detected = 0;
    this.stats.recovered = 0;
    this.stats.dlqMoved = 0;
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
   * Set stall timeout
   * @param {number} timeoutMs - Timeout in milliseconds
   */
  setStallTimeout(timeoutMs) {
    this.options.stallTimeoutMs = timeoutMs;
  }

  /**
   * Get stall timeout
   * @returns {number} Timeout in milliseconds
   */
  getStallTimeout() {
    return this.options.stallTimeoutMs;
  }
}

module.exports = StallDetector;
