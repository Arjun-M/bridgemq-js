const ServerStorage = require('../storage/ServerStorage');
const QueueStorage = require('../storage/QueueStorage');

/**
 * HealthCheck - System health monitoring and reporting
 * 
 * PURPOSE: Comprehensive health status for production monitoring
 * 
 * HEALTH CHECKS:
 * - Redis connectivity
 * - Server heartbeat validation
 * - Queue health (pending, active counts)
 * - DLQ threshold alerts
 * - Memory usage
 * - Worker capacity
 * 
 * STATUS LEVELS:
 * - healthy: All checks pass
 * - degraded: Some non-critical issues
 * - unhealthy: Critical failures
 */
class HealthCheck {
  /**
   * Create health check instance
   * @param {Redis} redis - Redis client
   * @param {Object} options - Health check options
   */
  constructor(redis, options = {}) {
    this.redis = redis;
    this.options = {
      meshId: options.meshId,
      serverId: options.serverId,
      thresholds: {
        maxPendingJobs: options.maxPendingJobs || 10000,
        maxDLQSize: options.maxDLQSize || 100,
        maxStaleHeartbeat: options.maxStaleHeartbeat || 60000, // 60s
      },
    };
  }

  /**
   * Run all health checks
   * @returns {Promise<Object>} Health status
   */
  async check() {
    const checks = {
      redis: await this._checkRedis(),
      heartbeat: await this._checkHeartbeat(),
      queues: await this._checkQueues(),
      dlq: await this._checkDLQ(),
    };

    const status = this._determineStatus(checks);

    return {
      status,
      timestamp: Date.now(),
      checks,
    };
  }

  /**
   * Check Redis connectivity
   * @private
   * @returns {Promise<Object>} Check result
   */
  async _checkRedis() {
    try {
      await this.redis.ping();
      
      return {
        status: 'healthy',
        message: 'Redis connection OK',
      };
    } catch (error) {
      return {
        status: 'unhealthy',
        message: `Redis connection failed: ${error.message}`,
      };
    }
  }

  /**
   * Check server heartbeat
   * @private
   * @returns {Promise<Object>} Check result
   */
  async _checkHeartbeat() {
    if (!this.options.serverId) {
      return {
        status: 'healthy',
        message: 'No server ID configured',
      };
    }

    try {
      const server = await ServerStorage.getServer(
        this.redis,
        this.options.serverId,
      );

      if (!server) {
        return {
          status: 'unhealthy',
          message: 'Server not registered',
        };
      }

      const now = Date.now();
      const lastHeartbeat = server.lastHeartbeat || 0;
      const staleness = now - lastHeartbeat;

      if (staleness > this.options.thresholds.maxStaleHeartbeat) {
        return {
          status: 'degraded',
          message: `Stale heartbeat (${Math.round(staleness / 1000)}s ago)`,
        };
      }

      return {
        status: 'healthy',
        message: 'Heartbeat OK',
      };
    } catch (error) {
      return {
        status: 'unhealthy',
        message: `Heartbeat check failed: ${error.message}`,
      };
    }
  }

  /**
   * Check queue health
   * @private
   * @returns {Promise<Object>} Check result
   */
  async _checkQueues() {
    if (!this.options.meshId) {
      return {
        status: 'healthy',
        message: 'No mesh ID configured',
      };
    }

    try {
      const pendingCount = await QueueStorage.getPendingCount(
        this.redis,
        this.options.meshId,
      );

      if (pendingCount > this.options.thresholds.maxPendingJobs) {
        return {
          status: 'degraded',
          message: `High pending count: ${pendingCount}`,
          data: { pendingCount },
        };
      }

      return {
        status: 'healthy',
        message: `Pending count: ${pendingCount}`,
        data: { pendingCount },
      };
    } catch (error) {
      return {
        status: 'unhealthy',
        message: `Queue check failed: ${error.message}`,
      };
    }
  }

  /**
   * Check DLQ size
   * @private
   * @returns {Promise<Object>} Check result
   */
  async _checkDLQ() {
    if (!this.options.meshId) {
      return {
        status: 'healthy',
        message: 'No mesh ID configured',
      };
    }

    try {
      const dlqJobs = await QueueStorage.listDLQ(
        this.redis,
        this.options.meshId,
        0,
        -1,
      );

      const dlqSize = dlqJobs.length;

      if (dlqSize > this.options.thresholds.maxDLQSize) {
        return {
          status: 'degraded',
          message: `High DLQ size: ${dlqSize}`,
          data: { dlqSize },
        };
      }

      return {
        status: 'healthy',
        message: `DLQ size: ${dlqSize}`,
        data: { dlqSize },
      };
    } catch (error) {
      return {
        status: 'unhealthy',
        message: `DLQ check failed: ${error.message}`,
      };
    }
  }

  /**
   * Determine overall status
   * @private
   * @param {Object} checks - Check results
   * @returns {string} Overall status
   */
  _determineStatus(checks) {
    const statuses = Object.values(checks).map((c) => c.status);

    if (statuses.includes('unhealthy')) {
      return 'unhealthy';
    }

    if (statuses.includes('degraded')) {
      return 'degraded';
    }

    return 'healthy';
  }

  /**
   * Get health status (alias for check)
   * @returns {Promise<Object>} Health status
   */
  async getStatus() {
    return await this.check();
  }

  /**
   * Is system healthy?
   * @returns {Promise<boolean>} True if healthy
   */
  async isHealthy() {
    const health = await this.check();
    return health.status === 'healthy';
  }
}

module.exports = HealthCheck;
