/**
 * BridgeMQ - Main API Entry Point
 * Ultra-advanced distributed job queue system
 */

import { v4 as uuidv4 } from 'uuid';
import { RedisClient, RedisConfig } from './core/redis/client';
import { RedisKeys } from './core/redis/keys';
import { JobStore } from './core/storage/jobStore';
import { WorkerStore } from './core/storage/workerStore';
import { JobLock } from './core/locks/jobLock';
import { Logger, LogLevel } from './core/utils/logger';
import {
  BridgeMQConfig,
  Job,
  JobHandler,
  JobStatus,
  JobResult,
  QueueStats,
  WorkerMetadata,
} from './types';
import os from 'os';

export class BridgeMQ {
  private config: BridgeMQConfig;
  private redis!: RedisClient;
  private keys!: RedisKeys;
  private jobStore!: JobStore;
  private workerStore!: WorkerStore;
  private jobLock!: JobLock;
  private logger: Logger;
  private handlers: Map<string, JobHandler> = new Map();
  private running: boolean = false;
  private processingLoop?: NodeJS.Timeout;
  private heartbeatInterval?: NodeJS.Timeout;

  constructor(config: BridgeMQConfig) {
    this.config = {
      ...config,
      concurrency: config.concurrency || 1,
      lockTTL: config.lockTTL || 30,
      heartbeatInterval: config.heartbeatInterval || 10000,
      namespace: config.namespace || 'bridge',
    };
    this.logger = new Logger('BridgeMQ');
  }

  /**
   * Initialize BridgeMQ - connect to Redis and setup core components
   */
  async initialize(): Promise<void> {
    this.logger.info('Initializing BridgeMQ', {
      serverId: this.config.serverId,
      mode: this.config.mode,
    });

    // Setup Redis connection
    const redisConfig: RedisConfig =
      typeof this.config.redis === 'string'
        ? { connectionString: this.config.redis }
        : this.config.redis;

    this.redis = new RedisClient(redisConfig);
    await this.redis.ping();

    // Initialize core components
    this.keys = new RedisKeys(this.config.namespace);
    this.jobStore = new JobStore(this.redis, this.keys);
    this.workerStore = new WorkerStore(this.redis, this.keys);
    this.jobLock = new JobLock(this.redis, this.keys);

    // Register worker if in worker or both mode
    if (this.config.mode === 'worker' || this.config.mode === 'both') {
      await this.registerWorker();
      this.startHeartbeat();
    }

    this.logger.info('BridgeMQ initialized successfully');
  }

  /**
   * Dispatch a job to the queue
   */
  async dispatch(jobData: Partial<Job>): Promise<Job> {
    const job: Job = {
      jobId: uuidv4(),
      type: jobData.type!,
      payload: jobData.payload || {},
      schedule: jobData.schedule || {},
      retry: jobData.retry || {
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 2000,
        },
      },
      priority: jobData.priority || 5,
      dependsOn: jobData.dependsOn || [],
      target: jobData.target || {},
      metadata: jobData.metadata || {},
      createdAt: Date.now(),
      attempts: 0,
    };

    // Save job data
    await this.jobStore.saveJob(job);

    // Route to appropriate queue based on schedule
    if (job.schedule.cron) {
      // Cron job - calculate next run and add to cron queue
      const nextRun = this.calculateNextCronRun(job.schedule.cron, job.schedule.timezone);
      await this.jobStore.enqueueCron(job.jobId, nextRun);
    } else if (job.schedule.delay || job.schedule.runAt) {
      // Delayed job
      const runAt = job.schedule.runAt || Date.now() + (job.schedule.delay || 0);
      await this.jobStore.enqueueDelayed(job.jobId, runAt);
    } else {
      // Immediate job
      await this.jobStore.enqueueWaiting(job.jobId, job.priority);
    }

    this.logger.info('Job dispatched', { jobId: job.jobId, type: job.type });
    return job;
  }

  /**
   * Register a job handler (worker mode)
   */
  process(type: string, handler: JobHandler): void {
    this.handlers.set(type, handler);
    this.logger.info('Registered handler', { type });
  }

  /**
   * Start processing jobs (worker mode)
   */
  async start(): Promise<void> {
    if (this.config.mode !== 'worker' && this.config.mode !== 'both') {
      throw new Error('Cannot start processing in producer mode');
    }

    this.running = true;
    this.logger.info('Starting job processing');

    // Start processing loop
    this.processingLoop = setInterval(() => {
      this.processJobs();
    }, 1000);
  }

  /**
   * Graceful shutdown
   */
  async shutdown(): Promise<void> {
    this.logger.info('Shutting down gracefully...');
    this.running = false;

    if (this.processingLoop) {
      clearInterval(this.processingLoop);
    }

    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
    }

    // Unregister worker
    if (this.config.mode === 'worker' || this.config.mode === 'both') {
      await this.workerStore.removeWorker(this.config.serverId);
    }

    await this.redis.disconnect();
    this.logger.info('Shutdown complete');
  }

  /**
   * Get job status
   */
  async getJobStatus(jobId: string): Promise<JobStatus | null> {
    return await this.jobStore.getStatus(jobId);
  }

  /**
   * Get job result
   */
  async getJobResult(jobId: string): Promise<JobResult | null> {
    return await this.jobStore.getResult(jobId);
  }

  /**
   * Get queue statistics
   */
  async getStats(): Promise<QueueStats> {
    const client = this.redis.getClient();
    const [waiting, active, delayed, cron, dead] = await Promise.all([
      client.llen(this.keys.queueWaiting()),
      client.scard(this.keys.queueActive()),
      client.zcard(this.keys.queueDelayed()),
      client.zcard(this.keys.queueCron()),
      client.llen(this.keys.queueDeadLetter()),
    ]);

    const workers = await this.workerStore.getActiveWorkers();

    return {
      waiting,
      active,
      delayed,
      cron,
      dead,
      workers: workers.length,
    };
  }

  // ========== Private Methods ==========

  private async registerWorker(): Promise<void> {
    const metadata: WorkerMetadata = {
      serverId: this.config.serverId,
      stack: this.config.stack,
      region: this.config.region,
      capabilities: this.config.capabilities || [],
      lastSeen: Date.now(),
      pid: process.pid,
      hostname: os.hostname(),
    };

    await this.workerStore.registerWorker(metadata);
  }

  private startHeartbeat(): void {
    this.heartbeatInterval = setInterval(async () => {
      await this.workerStore.updateHeartbeat(
        this.config.serverId,
        this.config.lockTTL || 30
      );
    }, this.config.heartbeatInterval);
  }

  private async processJobs(): Promise<void> {
    if (!this.running) return;

    try {
      // Simple job claiming (would use Lua script in production)
      const client = this.redis.getClient();
      const jobId = await client.lpop(this.keys.queueWaiting());

      if (!jobId) return;

      const job = await this.jobStore.getJob(jobId);
      if (!job) return;

      // Get handler
      const handler = this.handlers.get(job.type);
      if (!handler) {
        this.logger.warn('No handler registered', { type: job.type });
        await this.jobStore.enqueueWaiting(jobId);
        return;
      }

      // Acquire lock
      const lockAcquired = await this.jobLock.acquire(
        jobId,
        this.config.serverId,
        this.config.lockTTL
      );
      if (!lockAcquired) {
        await this.jobStore.enqueueWaiting(jobId);
        return;
      }

      // Mark as active
      await this.jobStore.setStatus(jobId, JobStatus.ACTIVE);

      // Process job
      const startTime = Date.now();
      try {
        const result = await handler(job);

        // Job succeeded
        const jobResult: JobResult = {
          success: true,
          data: result,
          completedAt: Date.now(),
          processingTime: Date.now() - startTime,
        };

        await this.jobStore.setResult(jobId, jobResult);
        await this.jobStore.setStatus(jobId, JobStatus.COMPLETED);
        this.logger.info('Job completed', { jobId, type: job.type });
      } catch (error) {
        // Job failed
        const jobResult: JobResult = {
          success: false,
          error: {
            message: (error as Error).message,
            stack: (error as Error).stack,
            name: (error as Error).name,
          },
          completedAt: Date.now(),
          processingTime: Date.now() - startTime,
        };

        await this.jobStore.setResult(jobId, jobResult);

        // Retry or dead letter
        if (job.attempts < job.retry.attempts) {
          job.attempts++;
          await this.jobStore.saveJob(job);
          await this.jobStore.enqueueWaiting(jobId);
          this.logger.info('Job retrying', { jobId, attempt: job.attempts });
        } else {
          await this.jobStore.moveToDeadLetter(jobId);
          this.logger.error('Job failed permanently', error as Error, { jobId });
        }
      } finally {
        // Release lock
        await this.jobLock.release(jobId, this.config.serverId);
        await this.jobStore.removeFromActive(jobId);
      }
    } catch (error) {
      this.logger.error('Error processing jobs', error as Error);
    }
  }

  private calculateNextCronRun(cron: string, timezone?: string): number {
    // Simplified - would use cron-parser in production
    return Date.now() + 60000; // 1 minute from now
  }
}

export * from './types';
export { LogLevel };
