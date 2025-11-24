/**
 * Redis client wrapper with connection management
 * Supports both local and cloud Redis (Upstash, AWS, Azure, etc.)
 */

import Redis, { RedisOptions } from 'ioredis';
import { Logger } from '../utils/logger';

export interface RedisConfig {
  host?: string;
  port?: number;
  username?: string;
  password?: string;
  db?: number;
  tls?: boolean | object;
  connectionString?: string;
  maxRetriesPerRequest?: number;
  enableReadyCheck?: boolean;
  lazyConnect?: boolean;
}

export class RedisClient {
  private client: Redis;
  private logger: Logger;
  private config: RedisConfig;

  constructor(config: RedisConfig) {
    this.config = config;
    this.logger = new Logger('RedisClient');
    this.client = this.createClient();
    this.setupEventHandlers();
  }

  /**
   * Create Redis client from config
   */
  private createClient(): Redis {
    // Connection string takes precedence
    if (this.config.connectionString) {
      this.logger.info('Connecting to Redis using connection string');
      return new Redis(this.config.connectionString, {
        maxRetriesPerRequest: this.config.maxRetriesPerRequest ?? 3,
        enableReadyCheck: this.config.enableReadyCheck ?? true,
        lazyConnect: this.config.lazyConnect ?? false,
      });
    }

    // Explicit config object
    const options: RedisOptions = {
      host: this.config.host || 'localhost',
      port: this.config.port || 6379,
      db: this.config.db || 0,
      maxRetriesPerRequest: this.config.maxRetriesPerRequest ?? 3,
      enableReadyCheck: this.config.enableReadyCheck ?? true,
      lazyConnect: this.config.lazyConnect ?? false,
    };

    if (this.config.username) {
      options.username = this.config.username;
    }

    if (this.config.password) {
      options.password = this.config.password;
    }

    if (this.config.tls) {
      options.tls = typeof this.config.tls === 'object' ? this.config.tls : {};
    }

    this.logger.info('Connecting to Redis', {
      host: options.host,
      port: options.port,
      db: options.db,
      tls: !!options.tls,
    });

    return new Redis(options);
  }

  /**
   * Setup event handlers for connection monitoring
   */
  private setupEventHandlers(): void {
    this.client.on('connect', () => {
      this.logger.info('Redis connection established');
    });

    this.client.on('ready', () => {
      this.logger.info('Redis client ready');
    });

    this.client.on('error', (error) => {
      this.logger.error('Redis error', error);
    });

    this.client.on('close', () => {
      this.logger.warn('Redis connection closed');
    });

    this.client.on('reconnecting', (delay: number) => {
      this.logger.info('Redis reconnecting', { delay });
    });

    this.client.on('end', () => {
      this.logger.warn('Redis connection ended');
    });
  }

  /**
   * Get the underlying ioredis instance
   */
  getClient(): Redis {
    return this.client;
  }

  /**
   * Ping Redis to check connection
   */
  async ping(): Promise<boolean> {
    try {
      const result = await this.client.ping();
      return result === 'PONG';
    } catch (error) {
      this.logger.error('Ping failed', error as Error);
      return false;
    }
  }

  /**
   * Load and define a Lua script
   */
  async defineScript(name: string, script: string): Promise<string> {
    try {
      const sha = await this.client.script('LOAD', script);
      this.logger.debug('Loaded Lua script', { name, sha });
      return sha;
    } catch (error) {
      this.logger.error('Failed to load Lua script', error as Error, { name });
      throw error;
    }
  }

  /**
   * Execute a Lua script by SHA
   */
  async evalsha(sha: string, keys: string[], args: (string | number)[]): Promise<any> {
    try {
      return await this.client.evalsha(sha, keys.length, ...keys, ...args);
    } catch (error) {
      this.logger.error('Evalsha failed', error as Error, { sha });
      throw error;
    }
  }

  /**
   * Execute a Lua script directly
   */
  async eval(script: string, keys: string[], args: (string | number)[]): Promise<any> {
    try {
      return await this.client.eval(script, keys.length, ...keys, ...args);
    } catch (error) {
      this.logger.error('Eval failed', error as Error);
      throw error;
    }
  }

  /**
   * Check if connected and ready
   */
  isReady(): boolean {
    return this.client.status === 'ready';
  }

  /**
   * Gracefully disconnect
   */
  async disconnect(): Promise<void> {
    this.logger.info('Disconnecting from Redis');
    await this.client.quit();
  }

  /**
   * Force disconnect (use for emergencies)
   */
  async forceDisconnect(): Promise<void> {
    this.logger.warn('Force disconnecting from Redis');
    this.client.disconnect();
  }
}
