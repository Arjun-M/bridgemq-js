const { v4: uuidv4 } = require('uuid');
const RedisConnection = require('../connection/RedisConnection');
const PubSub = require('../connection/PubSub');
const scripts = require('../scripts');
const JobStorage = require('../storage/JobStorage');
const ServerStorage = require('../storage/ServerStorage');
const QueueStorage = require('../storage/QueueStorage');
const MetricsStorage = require('../storage/MetricsStorage');
const { throwError } = require('../utils/Errors');
const EventEmitter = require('./EventEmitter');

/**
 * Client - Main BridgeMQ client (primary user-facing API)
 * 
 * PURPOSE: Unified interface for job creation, worker management, and monitoring
 * 
 * CRITICAL BEHAVIOR:
 * - init() AUTO-CREATES mesh via ServerStorage.registerServer()
 * - No separate mesh creation API exposed to users
 * - Combines producer and consumer capabilities
 * 
 * FEATURES:
 * - Job creation with idempotency and deduplication
 * - Job querying and management (get, cancel, replay)
 * - Queue operations (pause, resume, info)
 * - Event streaming and monitoring
 * - Server/mesh management
 */
class Client {
  /**
   * Create BridgeMQ client
   * @param {Object} config - Client configuration
   */
  constructor(config) {
    this.config = {
      redis: config.redis || {},
      server: config.server || {},
      mesh: config.mesh || {},
      behavior: config.behavior || {},
    };

    // Validate required config
    if (!this.config.server.serverId) {
      this.config.server.serverId = uuidv4();
    }

    if (!this.config.mesh.meshId) {
      throwError(1002, 'INVALID_CONFIG', {
        message: 'mesh.meshId is required',
      });
    }

    // Internal state
    this.redis = null;
    this.pubsub = null;
    this.initialized = false;
    this.events = new EventEmitter();
    this.heartbeatInterval = null;
  }

  /**
   * Initialize client (AUTO-CREATES mesh if it doesn't exist)
   * @returns {Promise<void>}
   */
  async init() {
    if (this.initialized) {
      return;
    }

    try {
      // Connect to Redis
      const connection = new RedisConnection(this.config.redis);
      await connection.connect();
      this.redis = connection.getClient();

      // Load Lua scripts
      await scripts.load(this.redis);

      // Setup Pub/Sub
      this.pubsub = new PubSub(this.config.redis);
      await this.pubsub.connect();

      // Subscribe to events
      await this._setupEventSubscriptions();

      // Register server (AUTO-CREATES mesh)
      await ServerStorage.registerServer(this.redis, {
        serverId: this.config.server.serverId,
        stack: this.config.server.stack || 'nodejs',
        capabilities: this.config.server.capabilities || [],
        meshId: this.config.mesh.meshId,
        meshName: this.config.mesh.name,
        meshDescription: this.config.mesh.description,
        meshConfig: this.config.mesh.config || {},
        region: this.config.server.region,
        resources: this.config.server.resources || {},
        metadata: this.config.server.metadata || {},
      });

      // Start heartbeat
      const heartbeatInterval = this.config.server.heartbeatIntervalMs || 10000;
      this.heartbeatInterval = ServerStorage.startHeartbeat(
        this.redis,
        this.config.server.serverId,
        heartbeatInterval,
      );

      this.initialized = true;
    } catch (error) {
      throwError(9001, 'REDIS_FAILURE', {
        message: 'Failed to initialize client',
        error: error.message,
      });
    }
  }

  /**
   * Shutdown client gracefully
   * @returns {Promise<void>}
   */
  async shutdown() {
    if (!this.initialized) {
      return;
    }

    try {
      // Stop heartbeat
      if (this.heartbeatInterval) {
        clearInterval(this.heartbeatInterval);
        this.heartbeatInterval = null;
      }

      // Deregister server
      await ServerStorage.deregisterServer(
        this.redis,
        this.config.server.serverId,
      );

      // Disconnect Pub/Sub
      if (this.pubsub) {
        await this.pubsub.disconnect();
      }

      // Close Redis connection
      if (this.redis) {
        await this.redis.quit();
      }

      this.initialized = false;
    } catch (error) {
      console.error('Failed to shutdown gracefully:', error.message);
    }
  }

  /**
   * Create a new job
   * @param {Object} jobData - Job creation data
   * @returns {Promise<string>} Job ID
   */
  async createJob(jobData) {
    if (!this.initialized) {
      throwError(9001, 'REDIS_FAILURE', {
        message: 'Client not initialized',
      });
    }

    const {
      type,
      payload,
      idempotency,
      config = {},
    } = jobData;

    const jobId = uuidv4();

    // Calculate fingerprint for auto-dedup if enabled
    let fingerprintHash = null;
    if (config.behavior && config.behavior.deduplication) {
      const crypto = require('crypto');
      const fingerprint = JSON.stringify({ type, payload });
      fingerprintHash = crypto.createHash('sha256').update(fingerprint).digest('hex');
    }

    return await JobStorage.createJob(this.redis, scripts, {
      jobId,
      type,
      version: jobData.version || '1.0',
      payload,
      config: {
        ...this.config.behavior,
        ...config,
        priority: config.priority || 5,
      },
      meshId: config.meshId || this.config.mesh.meshId,
      idempotencyKey: idempotency ? idempotency.key : null,
      fingerprintHash,
    });
  }

  /**
   * Get job information
   * @param {string} jobId - Job ID
   * @returns {Promise<Object>} Job data
   */
  async getJob(jobId) {
    if (!this.initialized) {
      throwError(9001, 'REDIS_FAILURE', {
        message: 'Client not initialized',
      });
    }

    return await JobStorage.getJob(this.redis, jobId);
  }

  /**
   * Get job payload
   * @param {string} jobId - Job ID
   * @returns {Promise<any>} Job payload
   */
  async getJobPayload(jobId) {
    if (!this.initialized) {
      throwError(9001, 'REDIS_FAILURE', {
        message: 'Client not initialized',
      });
    }

    return await JobStorage.getJobPayload(this.redis, jobId);
  }

  /**
   * Get job result
   * @param {string} jobId - Job ID
   * @returns {Promise<any>} Job result
   */
  async getJobResult(jobId) {
    if (!this.initialized) {
      throwError(9001, 'REDIS_FAILURE', {
        message: 'Client not initialized',
      });
    }

    return await JobStorage.getJobResult(this.redis, jobId);
  }

  /**
   * Get job errors
   * @param {string} jobId - Job ID
   * @returns {Promise<Array>} Job errors
   */
  async getJobErrors(jobId) {
    if (!this.initialized) {
      throwError(9001, 'REDIS_FAILURE', {
        message: 'Client not initialized',
      });
    }

    return await JobStorage.getJobErrors(this.redis, jobId);
  }

  /**
   * Cancel a job
   * @param {string} jobId - Job ID
   * @returns {Promise<boolean>} True if cancelled
   */
  async cancelJob(jobId) {
    if (!this.initialized) {
      throwError(9001, 'REDIS_FAILURE', {
        message: 'Client not initialized',
      });
    }

    return await JobStorage.cancelJob(this.redis, jobId);
  }

  /**
   * Replay a job (create new job with same data)
   * @param {string} jobId - Original job ID
   * @returns {Promise<string>} New job ID
   */
  async replayJob(jobId) {
    if (!this.initialized) {
      throwError(9001, 'REDIS_FAILURE', {
        message: 'Client not initialized',
      });
    }

    return await JobStorage.replayJob(this.redis, scripts, jobId);
  }

  /**
   * Delete a job
   * @param {string} jobId - Job ID
   * @returns {Promise<void>}
   */
  async deleteJob(jobId) {
    if (!this.initialized) {
      throwError(9001, 'REDIS_FAILURE', {
        message: 'Client not initialized',
      });
    }

    return await JobStorage.deleteJob(this.redis, jobId);
  }

  /**
   * List jobs with filters
   * @param {Object} filters - Filter options
   * @returns {Promise<Array>} Job list
   */
  async listJobs(filters = {}) {
    if (!this.initialized) {
      throwError(9001, 'REDIS_FAILURE', {
        message: 'Client not initialized',
      });
    }

    return await JobStorage.listJobs(this.redis, {
      meshId: this.config.mesh.meshId,
      ...filters,
    });
  }

  /**
   * Get statistics for current mesh
   * @returns {Promise<Object>} Statistics
   */
  async getStats() {
    if (!this.initialized) {
      throwError(9001, 'REDIS_FAILURE', {
        message: 'Client not initialized',
      });
    }

    return await MetricsStorage.getStats(this.redis, this.config.mesh.meshId);
  }

  /**
   * Get pending job count
   * @returns {Promise<number>} Pending count
   */
  async getPendingCount() {
    if (!this.initialized) {
      throwError(9001, 'REDIS_FAILURE', {
        message: 'Client not initialized',
      });
    }

    return await QueueStorage.getPendingCount(this.redis, this.config.mesh.meshId);
  }

  /**
   * Get active job count for this server
   * @returns {Promise<number>} Active count
   */
  async getActiveCount() {
    if (!this.initialized) {
      throwError(9001, 'REDIS_FAILURE', {
        message: 'Client not initialized',
      });
    }

    return await QueueStorage.getActiveCount(this.redis, this.config.server.serverId);
  }

  /**
   * Get delayed job count
   * @returns {Promise<number>} Delayed count
   */
  async getDelayedCount() {
    if (!this.initialized) {
      throwError(9001, 'REDIS_FAILURE', {
        message: 'Client not initialized',
      });
    }

    return await QueueStorage.getDelayedCount(this.redis);
  }

  /**
   * Join additional mesh
   * @param {Object} meshInfo - Mesh information
   * @returns {Promise<void>}
   */
  async joinMesh(meshInfo) {
    if (!this.initialized) {
      throwError(9001, 'REDIS_FAILURE', {
        message: 'Client not initialized',
      });
    }

    return await ServerStorage.joinMesh(
      this.redis,
      meshInfo,
      this.config.server.serverId,
    );
  }

  /**
   * Leave a mesh
   * @param {string} meshId - Mesh ID
   * @returns {Promise<void>}
   */
  async leaveMesh(meshId) {
    if (!this.initialized) {
      throwError(9001, 'REDIS_FAILURE', {
        message: 'Client not initialized',
      });
    }

    return await ServerStorage.leaveMesh(
      this.redis,
      meshId,
      this.config.server.serverId,
    );
  }

  /**
   * Setup event subscriptions
   * @private
   */
  async _setupEventSubscriptions() {
    const NS = 'bridgemq';

    // Subscribe to global events
    await this.pubsub.subscribe(`${NS}:events:global`, (data) => {
      this.events.emit(data.event, data);
    });

    // Subscribe to mesh events
    await this.pubsub.subscribe(
      `${NS}:events:mesh:${this.config.mesh.meshId}`,
      (data) => {
        this.events.emit(data.event, data);
      },
    );

    // Subscribe to server events
    await this.pubsub.subscribe(
      `${NS}:events:server:${this.config.server.serverId}`,
      (data) => {
        this.events.emit(data.event, data);
      },
    );
  }
}

module.exports = Client;
