import { RedisClient } from '../redis/client';
import { RedisKeys } from '../redis/keys';

export class JobLock {
  private redis: RedisClient;
  private keys: RedisKeys;

  constructor(redis: RedisClient, keys: RedisKeys) {
    this.redis = redis;
    this.keys = keys;
  }

  async acquire(jobId: string, workerId: string, ttl: number = 30): Promise<boolean> {
    const client = this.redis.getClient();
    const key = this.keys.jobLock(jobId);
    const result = await client.set(key, workerId, 'NX', 'EX', ttl);
    return result === 'OK';
  }

  async release(jobId: string, workerId: string): Promise<boolean> {
    const client = this.redis.getClient();
    const script = `
      if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
      else
        return 0
      end
    `;
    const result = await client.eval(script, 1, this.keys.jobLock(jobId), workerId);
    return result === 1;
  }

  async renew(jobId: string, workerId: string, ttl: number = 30): Promise<boolean> {
    const client = this.redis.getClient();
    const script = `
      if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("expire", KEYS[1], ARGV[2])
      else
        return 0
      end
    `;
    const result = await client.eval(script, 1, this.keys.jobLock(jobId), workerId, ttl);
    return result === 1;
  }

  async isLocked(jobId: string): Promise<boolean> {
    const client = this.redis.getClient();
    const exists = await client.exists(this.keys.jobLock(jobId));
    return exists === 1;
  }

  async getOwner(jobId: string): Promise<string | null> {
    const client = this.redis.getClient();
    return await client.get(this.keys.jobLock(jobId));
  }
}
