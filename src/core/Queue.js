const QueueStorage = require('../storage/QueueStorage');
const { throwError } = require('../utils/Errors');

/**
 * Queue - Internal queue management abstraction
 * 
 * PURPOSE: Simplified interface for queue operations
 * 
 * FEATURES:
 * - Enqueue/dequeue jobs
 * - Priority handling
 * - Job movement between states
 * - Queue size queries
 * - Remove jobs from queues
 */
class Queue {
  /**
   * Create queue instance
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @param {string} type - Job type
   */
  constructor(redis, meshId, type) {
    this.redis = redis;
    this.meshId = meshId;
    this.type = type;
  }

  /**
   * Enqueue a job
   * @param {string} jobId - Job ID
   * @param {number} priority - Priority (1-10)
   * @param {number} score - Score (timestamp or priority)
   * @returns {Promise<void>}
   */
  async enqueue(jobId, priority = 5, score = null) {
    await QueueStorage.pushToQueue(
      this.redis,
      this.meshId,
      this.type,
      priority,
      jobId,
      score,
    );
  }

  /**
   * Dequeue a job (simple FIFO within priority)
   * @param {number} priority - Priority level
   * @returns {Promise<string|null>} Job ID or null
   */
  async dequeue(priority = 5) {
    return await QueueStorage.pullFromQueue(
      this.redis,
      this.meshId,
      this.type,
      priority,
    );
  }

  /**
   * Move job to active state
   * @param {string} jobId - Job ID
   * @param {string} serverId - Server ID
   * @returns {Promise<void>}
   */
  async moveToActive(jobId, serverId) {
    await QueueStorage.moveToActive(this.redis, jobId, serverId);
  }

  /**
   * Move job to pending state
   * @param {string} jobId - Job ID
   * @param {string} serverId - Server ID
   * @param {number} priority - Priority
   * @returns {Promise<void>}
   */
  async moveToPending(jobId, serverId, priority = 5) {
    await QueueStorage.moveToPending(
      this.redis,
      jobId,
      serverId,
      this.meshId,
      this.type,
      priority,
    );
  }

  /**
   * Move job to delayed state
   * @param {string} jobId - Job ID
   * @param {string} serverId - Server ID
   * @param {number} timestamp - Scheduled timestamp
   * @returns {Promise<void>}
   */
  async moveToDelayed(jobId, serverId, timestamp) {
    await QueueStorage.moveToDelayed(this.redis, jobId, serverId, timestamp);
  }

  /**
   * Remove job from queue
   * @param {string} jobId - Job ID
   * @returns {Promise<void>}
   */
  async remove(jobId) {
    await QueueStorage.removeFromQueue(this.redis, this.meshId, this.type, jobId);
  }

  /**
   * Get queue size
   * @returns {Promise<number>} Queue size
   */
  async size() {
    return await QueueStorage.getQueueSize(this.redis, this.meshId, this.type);
  }

  /**
   * Move job to DLQ
   * @param {string} jobId - Job ID
   * @returns {Promise<void>}
   */
  async moveToDLQ(jobId) {
    await QueueStorage.pushToDLQ(this.redis, this.meshId, jobId);
  }
}

module.exports = Queue;
