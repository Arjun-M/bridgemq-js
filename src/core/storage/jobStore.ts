/**
 * Job storage operations in Redis
 */

import { RedisClient } from '../redis/client';
import { RedisKeys } from '../redis/keys';
import { Job, JobStatus, JobResult } from '../../types';
import { Logger } from '../utils/logger';

export class JobStore {
  private redis: RedisClient;
  private keys: RedisKeys;
  private logger: Logger;

  constructor(redis: RedisClient, keys: RedisKeys) {
    this.redis = redis;
    this.keys = keys;
    this.logger = new Logger('JobStore');
  }

  async saveJob(job: Job): Promise<void> {
    const client = this.redis.getClient();
    const key = this.keys.jobData(job.jobId);
    await client.set(key, JSON.stringify(job));
    this.logger.debug('Saved job', { jobId: job.jobId, type: job.type });
  }

  async getJob(jobId: string): Promise<Job | null> {
    const client = this.redis.getClient();
    const key = this.keys.jobData(jobId);
    const data = await client.get(key);
    return data ? JSON.parse(data) : null;
  }

  async deleteJob(jobId: string): Promise<void> {
    const client = this.redis.getClient();
    const keys = this.keys.allJobKeys(jobId);
    if (keys.length > 0) {
      await client.del(...keys);
    }
    this.logger.debug('Deleted job', { jobId });
  }

  async setStatus(jobId: string, status: JobStatus): Promise<void> {
    const client = this.redis.getClient();
    const key = this.keys.jobStatus(jobId);
    await client.set(key, status);
  }

  async getStatus(jobId: string): Promise<JobStatus | null> {
    const client = this.redis.getClient();
    const key = this.keys.jobStatus(jobId);
    const status = await client.get(key);
    return status as JobStatus | null;
  }

  async setResult(jobId: string, result: JobResult): Promise<void> {
    const client = this.redis.getClient();
    const key = this.keys.jobResult(jobId);
    await client.set(key, JSON.stringify(result), 'EX', 86400); // 24h TTL
  }

  async getResult(jobId: string): Promise<JobResult | null> {
    const client = this.redis.getClient();
    const key = this.keys.jobResult(jobId);
    const data = await client.get(key);
    return data ? JSON.parse(data) : null;
  }

  async setProgress(jobId: string, percent: number, message?: string): Promise<void> {
    const client = this.redis.getClient();
    const key = this.keys.jobProgress(jobId);
    await client.set(key, JSON.stringify({ percent, message, updatedAt: Date.now() }));
  }

  async enqueueWaiting(jobId: string, priority: number = 5): Promise<void> {
    const client = this.redis.getClient();
    // Higher priority jobs go to the front
    if (priority >= 7) {
      await client.lpush(this.keys.queueWaiting(), jobId);
    } else {
      await client.rpush(this.keys.queueWaiting(), jobId);
    }
    await this.setStatus(jobId, JobStatus.WAITING);
  }

  async enqueueDelayed(jobId: string, timestamp: number): Promise<void> {
    const client = this.redis.getClient();
    await client.zadd(this.keys.queueDelayed(), timestamp, jobId);
    await this.setStatus(jobId, JobStatus.DELAYED);
  }

  async enqueueCron(jobId: string, nextRun: number): Promise<void> {
    const client = this.redis.getClient();
    await client.zadd(this.keys.queueCron(), nextRun, jobId);
    await this.setStatus(jobId, JobStatus.CRON);
  }

  async moveToDeadLetter(jobId: string): Promise<void> {
    const client = this.redis.getClient();
    await client.lpush(this.keys.queueDeadLetter(), jobId);
    await this.setStatus(jobId, JobStatus.DEAD);
  }

  async removeFromActive(jobId: string): Promise<void> {
    const client = this.redis.getClient();
    await client.srem(this.keys.queueActive(), jobId);
  }
}
