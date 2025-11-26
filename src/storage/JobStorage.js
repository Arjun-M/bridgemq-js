const msgpack = require('msgpack-lite');
const { throwError } = require('../utils/Errors');
const scripts = require('../scripts');

/**
 * JobStorage - Complete job lifecycle management
 * 
 * PURPOSE: Handle all job CRUD operations and status transitions
 * 
 * FEATURES:
 * - Create jobs with idempotency and deduplication
 * - Retrieve job metadata, config, payload, result, errors
 * - Update job status and progress
 * - Cancel and replay jobs
 * - List jobs with filters
 * 
 * ERROR CODES:
 * - 1001: INVALID_PAYLOAD
 * - 1002: INVALID_CONFIG
 * - 9001: REDIS_FAILURE
 * - 9004: STORAGE_WRITE_FAILURE
 * - 9005: STORAGE_READ_FAILURE
 */

const NS = 'bridgemq';

class JobStorage {
  /**
   * Create a new job
   * @param {Redis} redis - Redis client
   * @param {Object} scripts - Lua scripts instance
   * @param {Object} jobData - Job creation data
   * @returns {Promise<string>} Job ID
   */
  static async createJob(redis, scripts, jobData) {
    const {
      jobId,
      type,
      version = '1.0',
      payload,
      config = {},
      meshId = 'default',
      idempotencyKey,
      fingerprintHash,
    } = jobData;

    if (!jobId || !type) {
      throwError(1001, 'INVALID_PAYLOAD', {
        message: 'jobId and type are required',
      });
    }

    try {
      // Prepare metadata
      const meta = {
        jobId,
        type,
        version,
        meshId,
        priority: config.priority || 5,
        status: config.schedule && config.schedule.delay ? 'scheduled' : 'pending',
        attempt: 0,
        scheduledFor: config.schedule && config.schedule.delay
          ? Date.now() + config.schedule.delay
          : config.schedule && config.schedule.runAt
            ? config.schedule.runAt
            : Date.now(),
      };

      // Serialize payload
      const serializedPayload = msgpack.encode(payload);

      // Call Lua script to create job atomically
      const result = await scripts.createJob(redis, {
        jobId,
        meta,
        config,
        payload: serializedPayload.toString('base64'),
        idempotencyKey,
        fingerprintHash,
      });

      return result.jobId;
    } catch (error) {
      throwError(9004, 'STORAGE_WRITE_FAILURE', {
        message: 'Failed to create job',
        jobId,
        error: error.message,
      });
    }
  }

  /**
   * Get complete job information
   * @param {Redis} redis - Redis client
   * @param {string} jobId - Job ID
   * @returns {Promise<Object>} Job data
   */
  static async getJob(redis, jobId) {
    try {
      const [meta, config, payload, result, errors] = await Promise.all([
        this.getJobMeta(redis, jobId),
        this.getJobConfig(redis, jobId),
        this.getJobPayload(redis, jobId),
        this.getJobResult(redis, jobId),
        this.getJobErrors(redis, jobId),
      ]);

      return {
        ...meta,
        config,
        payload,
        result,
        errors,
      };
    } catch (error) {
      throwError(9005, 'STORAGE_READ_FAILURE', {
        message: 'Failed to get job',
        jobId,
        error: error.message,
      });
    }
  }

  /**
   * Get job metadata
   * @param {Redis} redis - Redis client
   * @param {string} jobId - Job ID
   * @returns {Promise<Object>} Job metadata
   */
  static async getJobMeta(redis, jobId) {
    try {
      const key = `${NS}:job:${jobId}:meta`;
      const meta = await redis.hgetall(key);

      if (!meta || Object.keys(meta).length === 0) {
        return null;
      }

      // Convert numeric fields
      return {
        ...meta,
        priority: parseInt(meta.priority, 10),
        attempt: parseInt(meta.attempt, 10),
        stalledCount: parseInt(meta.stalledCount, 10),
        progress: parseFloat(meta.progress),
        createdAt: parseInt(meta.createdAt, 10),
        scheduledFor: parseInt(meta.scheduledFor, 10),
        startedAt: meta.startedAt ? parseInt(meta.startedAt, 10) : null,
        claimedAt: meta.claimedAt ? parseInt(meta.claimedAt, 10) : null,
        completedAt: meta.completedAt ? parseInt(meta.completedAt, 10) : null,
        updatedAt: parseInt(meta.updatedAt, 10),
      };
    } catch (error) {
      return null;
    }
  }

  /**
   * Get job configuration
   * @param {Redis} redis - Redis client
   * @param {string} jobId - Job ID
   * @returns {Promise<Object>} Job config
   */
  static async getJobConfig(redis, jobId) {
    try {
      const key = `${NS}:job:${jobId}:config`;
      const configJson = await redis.get(key);
      return configJson ? JSON.parse(configJson) : null;
    } catch (error) {
      return null;
    }
  }

  /**
   * Get job payload
   * @param {Redis} redis - Redis client
   * @param {string} jobId - Job ID
   * @returns {Promise<any>} Job payload
   */
  static async getJobPayload(redis, jobId) {
    try {
      const key = `${NS}:job:${jobId}:payload`;
      const payloadBase64 = await redis.get(key);
      
      if (!payloadBase64) {
        return null;
      }

      const buffer = Buffer.from(payloadBase64, 'base64');
      return msgpack.decode(buffer);
    } catch (error) {
      return null;
    }
  }

  /**
   * Get job result
   * @param {Redis} redis - Redis client
   * @param {string} jobId - Job ID
   * @returns {Promise<any>} Job result
   */
  static async getJobResult(redis, jobId) {
    try {
      const key = `${NS}:job:${jobId}:result`;
      const resultJson = await redis.get(key);
      return resultJson ? JSON.parse(resultJson) : null;
    } catch (error) {
      return null;
    }
  }

  /**
   * Get job errors
   * @param {Redis} redis - Redis client
   * @param {string} jobId - Job ID
   * @returns {Promise<Array>} Job errors
   */
  static async getJobErrors(redis, jobId) {
    try {
      const key = `${NS}:job:${jobId}:errors`;
      const errors = await redis.lrange(key, 0, -1);
      return errors.map((err) => JSON.parse(err));
    } catch (error) {
      return [];
    }
  }

  /**
   * Set job status
   * @param {Redis} redis - Redis client
   * @param {string} jobId - Job ID
   * @param {string} status - New status
   * @returns {Promise<void>}
   */
  static async setJobStatus(redis, jobId, status) {
    try {
      const key = `${NS}:job:${jobId}:meta`;
      await redis.hmset(key, {
        status,
        updatedAt: Date.now(),
      });
    } catch (error) {
      throwError(9004, 'STORAGE_WRITE_FAILURE', {
        message: 'Failed to set job status',
        jobId,
        status,
        error: error.message,
      });
    }
  }

  /**
   * Append error to job errors list
   * @param {Redis} redis - Redis client
   * @param {string} jobId - Job ID
   * @param {Object} error - Error object
   * @returns {Promise<void>}
   */
  static async appendError(redis, jobId, error) {
    try {
      const key = `${NS}:job:${jobId}:errors`;
      const errorEntry = JSON.stringify({
        ...error,
        timestamp: Date.now(),
      });
      await redis.rpush(key, errorEntry);
      await redis.ltrim(key, -10, -1); // Keep last 10 errors
    } catch (error) {
      // Non-critical, don't throw
      console.error(`Failed to append error for job ${jobId}:`, error.message);
    }
  }

  /**
   * Delete a job completely
   * @param {Redis} redis - Redis client
   * @param {string} jobId - Job ID
   * @returns {Promise<void>}
   */
  static async deleteJob(redis, jobId) {
    try {
      const keys = [
        `${NS}:job:${jobId}:meta`,
        `${NS}:job:${jobId}:config`,
        `${NS}:job:${jobId}:payload`,
        `${NS}:job:${jobId}:result`,
        `${NS}:job:${jobId}:errors`,
        `${NS}:job:${jobId}:depends`,
        `${NS}:job:${jobId}:waiters`,
      ];

      await redis.del(...keys);
    } catch (error) {
      throwError(9004, 'STORAGE_WRITE_FAILURE', {
        message: 'Failed to delete job',
        jobId,
        error: error.message,
      });
    }
  }

  /**
   * Cancel a job
   * @param {Redis} redis - Redis client
   * @param {string} jobId - Job ID
   * @returns {Promise<boolean>} True if cancelled
   */
  static async cancelJob(redis, jobId) {
    try {
      const meta = await this.getJobMeta(redis, jobId);
      
      if (!meta) {
        return false;
      }

      // Can only cancel pending or scheduled jobs
      if (meta.status !== 'pending' && meta.status !== 'scheduled') {
        return false;
      }

      await this.setJobStatus(redis, jobId, 'cancelled');
      return true;
    } catch (error) {
      return false;
    }
  }

  /**
   * Replay a job (create new job with same data)
   * @param {Redis} redis - Redis client
   * @param {Object} scripts - Lua scripts instance
   * @param {string} jobId - Original job ID
   * @returns {Promise<string>} New job ID
   */
  static async replayJob(redis, scripts, jobId) {
    try {
      const job = await this.getJob(redis, jobId);
      
      if (!job) {
        throwError(9005, 'STORAGE_READ_FAILURE', {
          message: 'Job not found for replay',
          jobId,
        });
      }

      // Create new job with same data
      const { v4: uuidv4 } = require('uuid');
      const newJobId = uuidv4();

      return await this.createJob(redis, scripts, {
        jobId: newJobId,
        type: job.type,
        version: job.version,
        payload: job.payload,
        config: job.config,
        meshId: job.meshId,
      });
    } catch (error) {
      throwError(9004, 'STORAGE_WRITE_FAILURE', {
        message: 'Failed to replay job',
        jobId,
        error: error.message,
      });
    }
  }

  /**
   * List jobs with filters
   * @param {Redis} redis - Redis client
   * @param {Object} filters - Filter options
   * @returns {Promise<Array>} Job list
   */
  static async listJobs(redis, filters = {}) {
    const {
      meshId,
      type,
      status,
      limit = 100,
    } = filters;

    try {
      // This is a simplified implementation
      // In production, you'd use more efficient indexing
      const pattern = `${NS}:job:*:meta`;
      const keys = await redis.keys(pattern);
      
      const jobs = await Promise.all(
        keys.slice(0, limit).map(async (key) => {
          const jobId = key.split(':')[3];
          return this.getJob(redis, jobId);
        }),
      );

      // Filter results
      return jobs.filter((job) => {
        if (!job) return false;
        if (meshId && job.meshId !== meshId) return false;
        if (type && job.type !== type) return false;
        if (status && job.status !== status) return false;
        return true;
      });
    } catch (error) {
      throwError(9005, 'STORAGE_READ_FAILURE', {
        message: 'Failed to list jobs',
        error: error.message,
      });
    }
  }

  /**
   * Claim a job for processing (uses Lua script)
   * @param {Redis} redis - Redis client
   * @param {Object} scripts - Lua scripts instance
   * @param {Object} server - Server info
   * @returns {Promise<string|null>} Job ID or null
   */
  static async claimJob(redis, scripts, server) {
    try {
      const { serverId, meshId, capabilities } = server;
      
      const jobId = await scripts.claimJob(
        redis,
        serverId,
        meshId,
        capabilities,
      );

      return jobId;
    } catch (error) {
      return null;
    }
  }

  /**
   * Complete a job (uses Lua script)
   * @param {Redis} redis - Redis client
   * @param {Object} scripts - Lua scripts instance
   * @param {string} jobId - Job ID
   * @param {string} serverId - Server ID
   * @param {any} result - Job result
   * @returns {Promise<Object>} Completion result
   */
  static async completeJob(redis, scripts, jobId, serverId, result) {
    try {
      return await scripts.completeJob(redis, jobId, serverId, result, 'completed');
    } catch (error) {
      throwError(9004, 'STORAGE_WRITE_FAILURE', {
        message: 'Failed to complete job',
        jobId,
        error: error.message,
      });
    }
  }

  /**
   * Fail a job (uses Lua script)
   * @param {Redis} redis - Redis client
   * @param {Object} scripts - Lua scripts instance
   * @param {string} jobId - Job ID
   * @param {string} serverId - Server ID
   * @param {Object} error - Error object
   * @returns {Promise<Object>} Retry result
   */
  static async failJob(redis, scripts, jobId, serverId, error) {
    try {
      return await scripts.retryJob(redis, jobId, serverId, error);
    } catch (err) {
      throwError(9004, 'STORAGE_WRITE_FAILURE', {
        message: 'Failed to fail job',
        jobId,
        error: err.message,
      });
    }
  }
}

module.exports = JobStorage;
