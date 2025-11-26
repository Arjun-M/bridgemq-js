const JobStorage = require('../storage/JobStorage');
const QueueStorage = require('../storage/QueueStorage');

/**
 * DependencyResolver - Manage job dependencies and execution ordering
 * 
 * PURPOSE: Ensure jobs wait for dependencies before execution
 * 
 * FEATURES:
 * - Track job dependencies (waitFor)
 * - Check dependency readiness
 * - Atomic dependency resolution
 * - Auto-trigger dependent jobs when ready
 * 
 * LOGIC:
 * 1. Job created with waitFor: [jobId1, jobId2]
 * 2. Stays in 'waiting' state (not in pending queue)
 * 3. When parent completes, check if all deps satisfied
 * 4. If yes, move to pending queue
 * 5. If no, remain waiting
 * 
 * REDIS KEYS:
 * - job:{jobId}:depends - Set of dependency job IDs
 * - job:{parentId}:waiters - Set of jobs waiting on this job
 */
class DependencyResolver {
  /**
   * Create dependency resolver
   * @param {Redis} redis - Redis client
   */
  constructor(redis) {
    this.redis = redis;
    this.NS = 'bridgemq';
  }

  /**
   * Add dependency for a job
   * @param {string} jobId - Job ID
   * @param {string} parentJobId - Parent job ID to wait for
   * @returns {Promise<void>}
   */
  async addDependency(jobId, parentJobId) {
    const dependsKey = `${this.NS}:job:${jobId}:depends`;
    const waitersKey = `${this.NS}:job:${parentJobId}:waiters`;

    await this.redis.sadd(dependsKey, parentJobId);
    await this.redis.sadd(waitersKey, jobId);
  }

  /**
   * Remove dependency for a job
   * @param {string} jobId - Job ID
   * @param {string} parentJobId - Parent job ID
   * @returns {Promise<void>}
   */
  async removeDependency(jobId, parentJobId) {
    const dependsKey = `${this.NS}:job:${jobId}:depends`;
    const waitersKey = `${this.NS}:job:${parentJobId}:waiters`;

    await this.redis.srem(dependsKey, parentJobId);
    await this.redis.srem(waitersKey, jobId);
  }

  /**
   * Get all dependencies for a job
   * @param {string} jobId - Job ID
   * @returns {Promise<Array>} Parent job IDs
   */
  async getDependencies(jobId) {
    const dependsKey = `${this.NS}:job:${jobId}:depends`;
    return await this.redis.smembers(dependsKey);
  }

  /**
   * Get all jobs waiting on a parent job
   * @param {string} parentJobId - Parent job ID
   * @returns {Promise<Array>} Waiting job IDs
   */
  async getWaiters(parentJobId) {
    const waitersKey = `${this.NS}:job:${parentJobId}:waiters`;
    return await this.redis.smembers(waitersKey);
  }

  /**
   * Check if job's dependencies are satisfied
   * @param {string} jobId - Job ID
   * @returns {Promise<boolean>} True if all dependencies satisfied
   */
  async areDependenciesSatisfied(jobId) {
    const dependencies = await this.getDependencies(jobId);

    if (dependencies.length === 0) {
      return true; // No dependencies
    }

    // Check each dependency
    for (const depId of dependencies) {
      const depMeta = await JobStorage.getJobMeta(this.redis, depId);

      if (!depMeta) {
        // Dependency job not found - consider satisfied
        continue;
      }

      if (depMeta.status !== 'completed') {
        return false; // Dependency not completed
      }
    }

    return true; // All dependencies satisfied
  }

  /**
   * Check dependency and trigger if ready
   * @param {string} jobId - Job ID
   * @returns {Promise<boolean>} True if triggered
   */
  async checkAndTrigger(jobId) {
    const satisfied = await this.areDependenciesSatisfied(jobId);

    if (!satisfied) {
      return false;
    }

    // Get job metadata
    const meta = await JobStorage.getJobMeta(this.redis, jobId);

    if (!meta) {
      return false;
    }

    // Move to pending queue
    await QueueStorage.pushToQueue(
      this.redis,
      meta.meshId,
      meta.type,
      meta.priority,
      jobId,
      Date.now(),
    );

    // Update status
    await JobStorage.setJobStatus(this.redis, jobId, 'pending');

    return true;
  }

  /**
   * Trigger all waiting jobs for a completed parent
   * @param {string} parentJobId - Parent job ID
   * @returns {Promise<Array>} Triggered job IDs
   */
  async triggerWaiters(parentJobId) {
    const waiters = await this.getWaiters(parentJobId);
    const triggered = [];

    for (const waiterId of waiters) {
      // Remove this dependency
      await this.removeDependency(waiterId, parentJobId);

      // Check if all dependencies satisfied
      const wasTriggered = await this.checkAndTrigger(waiterId);

      if (wasTriggered) {
        triggered.push(waiterId);
      }
    }

    return triggered;
  }

  /**
   * Clear all dependencies for a job
   * @param {string} jobId - Job ID
   * @returns {Promise<void>}
   */
  async clearDependencies(jobId) {
    const dependencies = await this.getDependencies(jobId);

    for (const depId of dependencies) {
      await this.removeDependency(jobId, depId);
    }
  }

  /**
   * Clear all waiters for a job
   * @param {string} parentJobId - Parent job ID
   * @returns {Promise<void>}
   */
  async clearWaiters(parentJobId) {
    const waiters = await this.getWaiters(parentJobId);

    for (const waiterId of waiters) {
      await this.removeDependency(waiterId, parentJobId);
    }
  }

  /**
   * Get dependency graph for a job
   * @param {string} jobId - Job ID
   * @param {number} depth - Recursion depth (default 5)
   * @returns {Promise<Object>} Dependency tree
   */
  async getDependencyGraph(jobId, depth = 5) {
    if (depth <= 0) {
      return null;
    }

    const dependencies = await this.getDependencies(jobId);
    const graph = {
      jobId,
      dependencies: [],
    };

    for (const depId of dependencies) {
      const depGraph = await this.getDependencyGraph(depId, depth - 1);
      graph.dependencies.push(depGraph);
    }

    return graph;
  }
}

module.exports = DependencyResolver;
