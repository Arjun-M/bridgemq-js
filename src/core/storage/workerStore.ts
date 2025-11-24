/**
 * Worker registry and heartbeat management
 */

import { RedisClient } from '../redis/client';
import { RedisKeys } from '../redis/keys';
import { WorkerMetadata } from '../../types';
import { Logger } from '../utils/logger';

export class WorkerStore {
  private redis: RedisClient;
  private keys: RedisKeys;
  private logger: Logger;

  constructor(redis: RedisClient, keys: RedisKeys) {
    this.redis = redis;
    this.keys = keys;
    this.logger = new Logger('WorkerStore');
  }

  async registerWorker(metadata: WorkerMetadata): Promise<void> {
    const client = this.redis.getClient();
    const key = this.keys.workerMetadata(metadata.serverId);
    await client.set(key, JSON.stringify(metadata));
    await client.sadd(this.keys.workersSet(), metadata.serverId);
    this.logger.info('Worker registered', { serverId: metadata.serverId });
  }

  async updateHeartbeat(serverId: string, ttl: number = 30): Promise<void> {
    const client = this.redis.getClient();
    const key = this.keys.workerHeartbeat(serverId);
    await client.set(key, Date.now().toString(), 'EX', ttl);
  }

  async getWorker(serverId: string): Promise<WorkerMetadata | null> {
    const client = this.redis.getClient();
    const key = this.keys.workerMetadata(serverId);
    const data = await client.get(key);
    return data ? JSON.parse(data) : null;
  }

  async getAllWorkers(): Promise<WorkerMetadata[]> {
    const client = this.redis.getClient();
    const serverIds = await client.smembers(this.keys.workersSet());
    const workers: WorkerMetadata[] = [];
    for (const serverId of serverIds) {
      const worker = await this.getWorker(serverId);
      if (worker) workers.push(worker);
    }
    return workers;
  }

  async isWorkerAlive(serverId: string): Promise<boolean> {
    const client = this.redis.getClient();
    const key = this.keys.workerHeartbeat(serverId);
    const exists = await client.exists(key);
    return exists === 1;
  }

  async removeWorker(serverId: string): Promise<void> {
    const client = this.redis.getClient();
    await client.del(
      this.keys.workerMetadata(serverId),
      this.keys.workerHeartbeat(serverId)
    );
    await client.srem(this.keys.workersSet(), serverId);
    this.logger.info('Worker removed', { serverId });
  }

  async getActiveWorkers(): Promise<WorkerMetadata[]> {
    const allWorkers = await this.getAllWorkers();
    const activeWorkers: WorkerMetadata[] = [];
    for (const worker of allWorkers) {
      if (await this.isWorkerAlive(worker.serverId)) {
        activeWorkers.push(worker);
      }
    }
    return activeWorkers;
  }
}
