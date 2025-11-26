const { throwError } = require('../utils/Errors');

/**
 * QueueStorage - Queue operations and job movement
 * 
 * PURPOSE: Handle all queue-related operations for job management
 * 
 * FEATURES:
 * - Push/pull jobs to/from priority queues
 * - Delayed job queue management
 * - Dead Letter Queue (DLQ) operations
 * - Queue size and info queries
 * - Job movement between queues
 * 
 * ERROR CODES:
 * - 9004: STORAGE_WRITE_FAILURE
 * - 9005: STORAGE_READ_FAILURE
 */

const NS = 'bridgemq';

class QueueStorage {
  /**
   * Push job to pending queue
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @param {string} type - Job type
   * @param {number} priority - Priority (1-10)
   * @param {string} jobId - Job ID
   * @param {number} score - Score (timestamp or priority)
   * @returns {Promise<void>}
   */
  static async pushToQueue(redis, meshId, type, priority, jobId, score = null) {
    try {
      const queueKey = `${NS}:queue:${meshId}:${type}:p${priority}`;
      const scoreValue = score || Date.now();
      
      await redis.zadd(queueKey, scoreValue, jobId);

      // Also add to pending index
      const pendingKey = `${NS}:pending:${meshId}`;
      await redis.zadd(pendingKey, priority, jobId);
    } catch (error) {
      throwError(9004, 'STORAGE_WRITE_FAILURE', {
        message: 'Failed to push to queue',
        meshId,
        type,
        priority,
        jobId,
        error: error.message,
      });
    }
  }

  /**
   * Pull job from queue (simple FIFO within priority)
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @param {string} type - Job type
   * @param {number} priority - Priority
   * @returns {Promise<string|null>} Job ID or null
   */
  static async pullFromQueue(redis, meshId, type, priority) {
    try {
      const queueKey = `${NS}:queue:${meshId}:${type}:p${priority}`;
      
      // Get lowest score (oldest job)
      const jobs = await redis.zrange(queueKey, 0, 0);
      
      if (jobs.length === 0) {
        return null;
      }

      const jobId = jobs[0];

      // Remove from queue
      await redis.zrem(queueKey, jobId);

      // Remove from pending index
      const pendingKey = `${NS}:pending:${meshId}`;
      await redis.zrem(pendingKey, jobId);

      return jobId;
    } catch (error) {
      return null;
    }
  }

  /**
   * Remove job from queue
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @param {string} type - Job type
   * @param {string} jobId - Job ID
   * @returns {Promise<void>}
   */
  static async removeFromQueue(redis, meshId, type, jobId) {
    try {
      // Try all priorities
      for (let priority = 1; priority <= 10; priority++) {
        const queueKey = `${NS}:queue:${meshId}:${type}:p${priority}`;
        await redis.zrem(queueKey, jobId);
      }

      // Remove from pending index
      const pendingKey = `${NS}:pending:${meshId}`;
      await redis.zrem(pendingKey, jobId);
    } catch (error) {
      throwError(9004, 'STORAGE_WRITE_FAILURE', {
        message: 'Failed to remove from queue',
        meshId,
        type,
        jobId,
        error: error.message,
      });
    }
  }

  /**
   * Get queue size
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @param {string} type - Job type
   * @returns {Promise<number>} Queue size
   */
  static async getQueueSize(redis, meshId, type) {
    try {
      let totalSize = 0;

      for (let priority = 1; priority <= 10; priority++) {
        const queueKey = `${NS}:queue:${meshId}:${type}:p${priority}`;
        const size = await redis.zcard(queueKey);
        totalSize += size;
      }

      return totalSize;
    } catch (error) {
      return 0;
    }
  }

  /**
   * Push job to delayed queue
   * @param {Redis} redis - Redis client
   * @param {string} jobId - Job ID
   * @param {number} timestamp - Scheduled execution timestamp
   * @returns {Promise<void>}
   */
  static async pushToDelayed(redis, jobId, timestamp) {
    try {
      const delayedKey = `${NS}:delayed`;
      await redis.zadd(delayedKey, timestamp, jobId);
    } catch (error) {
      throwError(9004, 'STORAGE_WRITE_FAILURE', {
        message: 'Failed to push to delayed queue',
        jobId,
        timestamp,
        error: error.message,
      });
    }
  }

  /**
   * Pull ready delayed jobs
   * @param {Redis} redis - Redis client
   * @param {number} now - Current timestamp
   * @param {number} limit - Max jobs to pull
   * @returns {Promise<Array>} Job IDs
   */
  static async pullReadyDelayed(redis, now, limit = 100) {
    try {
      const delayedKey = `${NS}:delayed`;
      
      // Get jobs with scheduledFor <= now
      const jobIds = await redis.zrangebyscore(delayedKey, 0, now, 'LIMIT', 0, limit);

      // Remove from delayed queue
      if (jobIds.length > 0) {
        await redis.zrem(delayedKey, ...jobIds);
      }

      return jobIds;
    } catch (error) {
      return [];
    }
  }

  /**
   * Push job to Dead Letter Queue
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @param {string} jobId - Job ID
   * @returns {Promise<void>}
   */
  static async pushToDLQ(redis, meshId, jobId) {
    try {
      const dlqKey = `${NS}:dlq:${meshId}`;
      await redis.rpush(dlqKey, jobId);
    } catch (error) {
      throwError(9004, 'STORAGE_WRITE_FAILURE', {
        message: 'Failed to push to DLQ',
        meshId,
        jobId,
        error: error.message,
      });
    }
  }

  /**
   * List Dead Letter Queue jobs
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @param {number} start - Start index (default 0)
   * @param {number} end - End index (default -1 for all)
   * @returns {Promise<Array>} Job IDs
   */
  static async listDLQ(redis, meshId, start = 0, end = -1) {
    try {
      const dlqKey = `${NS}:dlq:${meshId}`;
      return await redis.lrange(dlqKey, start, end);
    } catch (error) {
      return [];
    }
  }

  /**
   * Remove job from DLQ
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @param {string} jobId - Job ID
   * @returns {Promise<void>}
   */
  static async removeFromDLQ(redis, meshId, jobId) {
    try {
      const dlqKey = `${NS}:dlq:${meshId}`;
      await redis.lrem(dlqKey, 1, jobId);
    } catch (error) {
      throwError(9004, 'STORAGE_WRITE_FAILURE', {
        message: 'Failed to remove from DLQ',
        meshId,
        jobId,
        error: error.message,
      });
    }
  }

  /**
   * Get pending count for mesh
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @returns {Promise<number>} Pending job count
   */
  static async getPendingCount(redis, meshId) {
    try {
      const pendingKey = `${NS}:pending:${meshId}`;
      return await redis.zcard(pendingKey);
    } catch (error) {
      return 0;
    }
  }

  /**
   * Get active count for server
   * @param {Redis} redis - Redis client
   * @param {string} serverId - Server ID
   * @returns {Promise<number>} Active job count
   */
  static async getActiveCount(redis, serverId) {
    try {
      const activeKey = `${NS}:active:${serverId}`;
      return await redis.hlen(activeKey);
    } catch (error) {
      return 0;
    }
  }

  /**
   * Get delayed count
   * @param {Redis} redis - Redis client
   * @returns {Promise<number>} Delayed job count
   */
  static async getDelayedCount(redis) {
    try {
      const delayedKey = `${NS}:delayed`;
      return await redis.zcard(delayedKey);
    } catch (error) {
      return 0;
    }
  }

  /**
   * Move job to active
   * @param {Redis} redis - Redis client
   * @param {string} jobId - Job ID
   * @param {string} serverId - Server ID
   * @returns {Promise<void>}
   */
  static async moveToActive(redis, jobId, serverId) {
    try {
      const activeKey = `${NS}:active:${serverId}`;
      const now = Date.now();
      await redis.hset(activeKey, jobId, now);
    } catch (error) {
      throwError(9004, 'STORAGE_WRITE_FAILURE', {
        message: 'Failed to move job to active',
        jobId,
        serverId,
        error: error.message,
      });
    }
  }

  /**
   * Move job to pending (from active)
   * @param {Redis} redis - Redis client
   * @param {string} jobId - Job ID
   * @param {string} serverId - Server ID
   * @param {string} meshId - Mesh ID
   * @param {string} type - Job type
   * @param {number} priority - Priority
   * @returns {Promise<void>}
   */
  static async moveToPending(redis, jobId, serverId, meshId, type, priority) {
    try {
      // Remove from active
      const activeKey = `${NS}:active:${serverId}`;
      await redis.hdel(activeKey, jobId);

      // Add to pending queue
      await this.pushToQueue(redis, meshId, type, priority, jobId);
    } catch (error) {
      throwError(9004, 'STORAGE_WRITE_FAILURE', {
        message: 'Failed to move job to pending',
        jobId,
        serverId,
        error: error.message,
      });
    }
  }

  /**
   * Move job to delayed (from active)
   * @param {Redis} redis - Redis client
   * @param {string} jobId - Job ID
   * @param {string} serverId - Server ID
   * @param {number} timestamp - Scheduled timestamp
   * @returns {Promise<void>}
   */
  static async moveToDelayed(redis, jobId, serverId, timestamp) {
    try {
      // Remove from active
      const activeKey = `${NS}:active:${serverId}`;
      await redis.hdel(activeKey, jobId);

      // Add to delayed queue
      await this.pushToDelayed(redis, jobId, timestamp);
    } catch (error) {
      throwError(9004, 'STORAGE_WRITE_FAILURE', {
        message: 'Failed to move job to delayed',
        jobId,
        serverId,
        error: error.message,
      });
    }
  }
}

module.exports = QueueStorage;
