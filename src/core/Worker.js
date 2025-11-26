const JobStorage = require('../storage/JobStorage');
const scripts = require('../scripts');
const { throwError } = require('../utils/Errors');
const EventEmitter = require('./EventEmitter');

/**
 * Worker - Job processing worker with concurrency control
 * 
 * PURPOSE: Execute jobs with configurable concurrency and graceful shutdown
 * 
 * FEATURES:
 * - Handler registration per job type
 * - Concurrent job processing with limit
 * - Automatic job claiming from queues
 * - Progress tracking and reporting
 * - Graceful shutdown (finish active jobs)
 * - Error handling and retry coordination
 * 
 * LOGIC:
 * 1. start() begins claim loop
 * 2. Claim job if under concurrency limit
 * 3. Execute handler with job data
 * 4. Complete or fail job based on result
 * 5. Repeat until stop() called
 */
class Worker {
  /**
   * Create worker instance
   * @param {Object} client - BridgeMQ client instance
   */
  constructor(client) {
    this.client = client;
    this.handlers = new Map();
    this.running = false;
    this.activeJobs = new Set();
    this.concurrency = client.config.server.concurrency || 1;
    this.events = new EventEmitter();
    this.claimInterval = null;
  }

  /**
   * Register job handler
   * @param {string} type - Job type
   * @param {Function} handler - Handler function(job)
   */
  registerHandler(type, handler) {
    if (typeof handler !== 'function') {
      throwError(1002, 'INVALID_CONFIG', {
        message: 'Handler must be a function',
      });
    }

    this.handlers.set(type, handler);
  }

  /**
   * Register multiple handlers
   * @param {Object} handlers - Map of type -> handler
   */
  registerHandlers(handlers) {
    for (const [type, handler] of Object.entries(handlers)) {
      this.registerHandler(type, handler);
    }
  }

  /**
   * Start worker processing loop
   * @returns {Promise<void>}
   */
  async start() {
    if (this.running) {
      return;
    }

    if (!this.client.initialized) {
      throwError(9001, 'REDIS_FAILURE', {
        message: 'Client not initialized',
      });
    }

    this.running = true;
    this.events.emit('worker.started', {
      serverId: this.client.config.server.serverId,
      concurrency: this.concurrency,
    });

    // Start claim loop
    this._startClaimLoop();
  }

  /**
   * Stop worker (graceful shutdown)
   * @param {number} timeoutMs - Max time to wait for active jobs (default 30000)
   * @returns {Promise<void>}
   */
  async stop(timeoutMs = 30000) {
    if (!this.running) {
      return;
    }

    this.running = false;

    // Stop claiming new jobs
    if (this.claimInterval) {
      clearInterval(this.claimInterval);
      this.claimInterval = null;
    }

    // Wait for active jobs to complete
    const startTime = Date.now();
    
    while (this.activeJobs.size > 0) {
      if (Date.now() - startTime > timeoutMs) {
        console.warn(`Worker shutdown timeout: ${this.activeJobs.size} jobs still active`);
        break;
      }
      
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    this.events.emit('worker.stopped', {
      serverId: this.client.config.server.serverId,
    });
  }

  /**
   * Set concurrency level
   * @param {number} n - New concurrency limit
   */
  setConcurrency(n) {
    if (n < 1) {
      throwError(1002, 'INVALID_CONFIG', {
        message: 'Concurrency must be >= 1',
      });
    }

    this.concurrency = n;
  }

  /**
   * Increase concurrency
   * @param {number} n - Amount to increase
   */
  increaseConcurrency(n = 1) {
    this.concurrency += n;
  }

  /**
   * Decrease concurrency
   * @param {number} n - Amount to decrease
   */
  decreaseConcurrency(n = 1) {
    this.concurrency = Math.max(1, this.concurrency - n);
  }

  /**
   * Start claim loop (internal)
   * @private
   */
  _startClaimLoop() {
    const claimIntervalMs = 100; // Check every 100ms

    this.claimInterval = setInterval(async () => {
      if (!this.running) {
        return;
      }

      // Check if we can claim more jobs
      if (this.activeJobs.size >= this.concurrency) {
        return;
      }

      try {
        await this._claimAndProcess();
      } catch (error) {
        console.error('Error in claim loop:', error.message);
      }
    }, claimIntervalMs);
  }

  /**
   * Claim and process a job
   * @private
   */
  async _claimAndProcess() {
    // Claim job
    const jobId = await JobStorage.claimJob(
      this.client.redis,
      scripts,
      {
        serverId: this.client.config.server.serverId,
        meshId: this.client.config.mesh.meshId,
        capabilities: this.client.config.server.capabilities || [],
      },
    );

    if (!jobId) {
      return; // No job available
    }

    // Track active job
    this.activeJobs.add(jobId);

    // Process job asynchronously
    this._processJob(jobId)
      .finally(() => {
        this.activeJobs.delete(jobId);
      });
  }

  /**
   * Process a claimed job
   * @private
   * @param {string} jobId - Job ID
   */
  async _processJob(jobId) {
    try {
      // Get full job data
      const job = await JobStorage.getJob(this.client.redis, jobId);

      if (!job) {
        console.error(`Job ${jobId} not found after claiming`);
        return;
      }

      // Emit start event
      this.events.emit('job.start', { jobId, type: job.type });

      // Get handler for job type
      const handler = this.handlers.get(job.type);

      if (!handler) {
        // No handler registered
        const error = {
          code: 3003,
          type: 'WORKER_CAPABILITY_MISMATCH',
          message: `No handler registered for job type: ${job.type}`,
        };

        await this._failJob(jobId, error);
        return;
      }

      // Create job wrapper with helper methods
      const jobWrapper = this._createJobWrapper(job);

      // Execute handler
      const result = await handler(jobWrapper);

      // Complete job
      await JobStorage.completeJob(
        this.client.redis,
        scripts,
        jobId,
        this.client.config.server.serverId,
        result,
      );

      // Emit complete event
      this.events.emit('job.complete', { jobId, result });
    } catch (error) {
      // Handler threw error - fail job
      await this._failJob(jobId, {
        message: error.message,
        stack: error.stack,
      });

      this.events.emit('job.fail', { jobId, error });
    }
  }

  /**
   * Fail a job
   * @private
   * @param {string} jobId - Job ID
   * @param {Object} error - Error object
   */
  async _failJob(jobId, error) {
    try {
      await JobStorage.failJob(
        this.client.redis,
        scripts,
        jobId,
        this.client.config.server.serverId,
        error,
      );
    } catch (err) {
      console.error(`Failed to fail job ${jobId}:`, err.message);
    }
  }

  /**
   * Create job wrapper with helper methods
   * @private
   * @param {Object} job - Job data
   * @returns {Object} Job wrapper
   */
  _createJobWrapper(job) {
    const self = this;

    return {
      // Job metadata
      jobId: job.jobId,
      type: job.type,
      version: job.version,
      status: job.status,
      attempt: job.attempt,
      priority: job.priority,
      createdAt: job.createdAt,
      claimedAt: job.claimedAt,

      // Get methods
      getMetadata() {
        return {
          jobId: job.jobId,
          type: job.type,
          version: job.version,
          status: job.status,
          attempt: job.attempt,
          priority: job.priority,
        };
      },

      getConfig() {
        return job.config;
      },

      getPayload() {
        return job.payload;
      },

      // Progress tracking
      async setProgress(percent) {
        const NS = 'bridgemq';
        const metaKey = `${NS}:job:${job.jobId}:meta`;
        
        await self.client.redis.hset(metaKey, 'progress', percent);
        
        self.events.emit('job.progress', {
          jobId: job.jobId,
          progress: percent,
        });
      },

      // Logging
      async log(message) {
        console.log(`[Job ${job.jobId}] ${message}`);
      },

      // Retry (for manual retry from handler)
      async retry() {
        throw new Error('RETRY_JOB'); // Signal retry to wrapper
      },
    };
  }

  /**
   * Get active job count
   * @returns {number} Active job count
   */
  getActiveCount() {
    return this.activeJobs.size;
  }

  /**
   * Check if worker is running
   * @returns {boolean} Running status
   */
  isRunning() {
    return this.running;
  }
}

module.exports = Worker;
