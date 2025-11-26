const { throwError } = require('../utils/Errors');

/**
 * MetricsStorage - Metrics collection and aggregation
 * 
 * PURPOSE: Track job processing metrics and statistics
 * 
 * FEATURES:
 * - Increment counters (created, completed, failed, etc.)
 * - Record processing times
 * - Track failures and success rates
 * - Get mesh statistics
 * - Time-series metrics for dashboards
 * 
 * ERROR CODES:
 * - 9004: STORAGE_WRITE_FAILURE
 * - 9005: STORAGE_READ_FAILURE
 */

const NS = 'bridgemq';

class MetricsStorage {
  /**
   * Increment a metric counter
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @param {string} metric - Metric name
   * @param {number} value - Increment value (default 1)
   * @returns {Promise<void>}
   */
  static async increment(redis, meshId, metric, value = 1) {
    try {
      const statsKey = `${NS}:stats:${meshId}:counters`;
      await redis.hincrby(statsKey, metric, value);
    } catch (error) {
      // Non-critical, don't throw
      console.error(`Failed to increment metric ${metric}:`, error.message);
    }
  }

  /**
   * Record processing time for a job
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @param {string} jobId - Job ID
   * @param {number} processingTimeMs - Processing time in milliseconds
   * @returns {Promise<void>}
   */
  static async recordProcessingTime(redis, meshId, jobId, processingTimeMs) {
    try {
      // Store in time-series (simple list for now)
      const timeSeriesKey = `${NS}:metrics:${meshId}:processing-times`;
      
      const entry = JSON.stringify({
        jobId,
        time: processingTimeMs,
        timestamp: Date.now(),
      });

      await redis.rpush(timeSeriesKey, entry);
      
      // Keep only last 1000 entries
      await redis.ltrim(timeSeriesKey, -1000, -1);

      // Update average (simplified calculation)
      await this.increment(redis, meshId, 'total:processing-time', processingTimeMs);
      await this.increment(redis, meshId, 'total:processing-count', 1);
    } catch (error) {
      // Non-critical, don't throw
      console.error(`Failed to record processing time for job ${jobId}:`, error.message);
    }
  }

  /**
   * Record job failure
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @param {string} jobId - Job ID
   * @returns {Promise<void>}
   */
  static async recordFailure(redis, meshId, jobId) {
    try {
      await this.increment(redis, meshId, 'total:failed', 1);

      // Store in time-series
      const failuresKey = `${NS}:metrics:${meshId}:failures`;
      
      const entry = JSON.stringify({
        jobId,
        timestamp: Date.now(),
      });

      await redis.rpush(failuresKey, entry);
      await redis.ltrim(failuresKey, -1000, -1);
    } catch (error) {
      console.error(`Failed to record failure for job ${jobId}:`, error.message);
    }
  }

  /**
   * Get statistics for a mesh
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @returns {Promise<Object>} Statistics object
   */
  static async getStats(redis, meshId) {
    try {
      const statsKey = `${NS}:stats:${meshId}:counters`;
      const counters = await redis.hgetall(statsKey);

      // Convert to numbers
      const stats = {};
      for (const [key, value] of Object.entries(counters)) {
        stats[key] = parseInt(value, 10);
      }

      // Calculate derived metrics
      const totalCreated = stats['total:created'] || 0;
      const totalCompleted = stats['total:completed'] || 0;
      const totalFailed = stats['total:failed'] || 0;
      const totalProcessed = totalCompleted + totalFailed;

      stats.successRate = totalProcessed > 0
        ? (totalCompleted / totalProcessed) * 100
        : 0;

      stats.failureRate = totalProcessed > 0
        ? (totalFailed / totalProcessed) * 100
        : 0;

      // Calculate average processing time
      const totalProcessingTime = stats['total:processing-time'] || 0;
      const totalProcessingCount = stats['total:processing-count'] || 0;
      
      stats.averageProcessingTime = totalProcessingCount > 0
        ? totalProcessingTime / totalProcessingCount
        : 0;

      return stats;
    } catch (error) {
      throwError(9005, 'STORAGE_READ_FAILURE', {
        message: 'Failed to get stats',
        meshId,
        error: error.message,
      });
    }
  }

  /**
   * Get time-series metrics for a mesh
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @param {string} metric - Metric name (processing-times|failures)
   * @param {number} limit - Number of entries to retrieve (default 100)
   * @returns {Promise<Array>} Time-series data
   */
  static async getTimeSeries(redis, meshId, metric, limit = 100) {
    try {
      const timeSeriesKey = `${NS}:metrics:${meshId}:${metric}`;
      const entries = await redis.lrange(timeSeriesKey, -limit, -1);

      return entries.map((entry) => JSON.parse(entry));
    } catch (error) {
      return [];
    }
  }

  /**
   * Record metric in time bucket (for rate calculations)
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @param {string} metric - Metric name
   * @param {string} bucket - Time bucket (e.g., '2025-01-15-10:30')
   * @param {number} value - Value to increment
   * @returns {Promise<void>}
   */
  static async recordBucket(redis, meshId, metric, bucket, value = 1) {
    try {
      const bucketKey = `${NS}:metrics:${meshId}:${metric}:${bucket}`;
      
      await redis.incrby(bucketKey, value);
      
      // Set TTL for 24 hours
      await redis.expire(bucketKey, 86400);
    } catch (error) {
      console.error(`Failed to record bucket metric ${metric}:`, error.message);
    }
  }

  /**
   * Get bucket metric
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @param {string} metric - Metric name
   * @param {string} bucket - Time bucket
   * @returns {Promise<number>} Metric value
   */
  static async getBucket(redis, meshId, metric, bucket) {
    try {
      const bucketKey = `${NS}:metrics:${meshId}:${metric}:${bucket}`;
      const value = await redis.get(bucketKey);
      return parseInt(value || 0, 10);
    } catch (error) {
      return 0;
    }
  }

  /**
   * Get current throughput (jobs per minute)
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @returns {Promise<Object>} Throughput metrics
   */
  static async getThroughput(redis, meshId) {
    try {
      const now = new Date();
      const currentBucket = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}-${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}`;

      const completed = await this.getBucket(redis, meshId, 'jobs-per-min', currentBucket);
      const failed = await this.getBucket(redis, meshId, 'failed-per-min', currentBucket);

      return {
        completed,
        failed,
        total: completed + failed,
        bucket: currentBucket,
      };
    } catch (error) {
      return {
        completed: 0,
        failed: 0,
        total: 0,
      };
    }
  }

  /**
   * Clear all metrics for a mesh
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @returns {Promise<void>}
   */
  static async clearMetrics(redis, meshId) {
    try {
      const pattern = `${NS}:metrics:${meshId}:*`;
      const keys = await redis.keys(pattern);

      if (keys.length > 0) {
        await redis.del(...keys);
      }

      // Clear stats
      const statsKey = `${NS}:stats:${meshId}:counters`;
      await redis.del(statsKey);
    } catch (error) {
      throwError(9004, 'STORAGE_WRITE_FAILURE', {
        message: 'Failed to clear metrics',
        meshId,
        error: error.message,
      });
    }
  }

  /**
   * Get global statistics (across all meshes)
   * @param {Redis} redis - Redis client
   * @returns {Promise<Object>} Global stats
   */
  static async getGlobalStats(redis) {
    try {
      const pattern = `${NS}:stats:*:counters`;
      const keys = await redis.keys(pattern);

      const allStats = await Promise.all(
        keys.map(async (key) => {
          const meshId = key.split(':')[2];
          return this.getStats(redis, meshId);
        }),
      );

      // Aggregate
      const global = {
        'total:created': 0,
        'total:completed': 0,
        'total:failed': 0,
        'total:cancelled': 0,
        'total:stalled': 0,
        successRate: 0,
        failureRate: 0,
        averageProcessingTime: 0,
      };

      let totalProcessingTime = 0;
      let totalProcessingCount = 0;

      for (const stats of allStats) {
        global['total:created'] += stats['total:created'] || 0;
        global['total:completed'] += stats['total:completed'] || 0;
        global['total:failed'] += stats['total:failed'] || 0;
        global['total:cancelled'] += stats['total:cancelled'] || 0;
        global['total:stalled'] += stats['total:stalled'] || 0;

        totalProcessingTime += stats['total:processing-time'] || 0;
        totalProcessingCount += stats['total:processing-count'] || 0;
      }

      const totalProcessed = global['total:completed'] + global['total:failed'];
      
      global.successRate = totalProcessed > 0
        ? (global['total:completed'] / totalProcessed) * 100
        : 0;

      global.failureRate = totalProcessed > 0
        ? (global['total:failed'] / totalProcessed) * 100
        : 0;

      global.averageProcessingTime = totalProcessingCount > 0
        ? totalProcessingTime / totalProcessingCount
        : 0;

      return global;
    } catch (error) {
      return {
        'total:created': 0,
        'total:completed': 0,
        'total:failed': 0,
        successRate: 0,
        failureRate: 0,
      };
    }
  }
}

module.exports = MetricsStorage;
