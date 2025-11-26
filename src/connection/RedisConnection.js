const Redis = require('ioredis');
const { throwError } = require('../utils/Errors');

/**
 * RedisConnection - Manages a single Redis connection with TLS support
 * 
 * PURPOSE: Provides reliable Redis connectivity with automatic reconnection,
 * exponential backoff, and connection health monitoring.
 * 
 * FEATURES:
 * - TLS/SSL support with certificate validation
 * - Exponential backoff for reconnection attempts
 * - Connection timeout configuration
 * - Ping/health check capability
 * - Graceful disconnect handling
 * 
 * ERROR CODES:
 * - 9001: REDISFAILURE - Connection or operation failure
 */
class RedisConnection {
  /**
   * Create a new Redis connection
   * @param {Object} opts - Connection options
   * @param {string} opts.host - Redis host (default: 'localhost')
   * @param {number} opts.port - Redis port (default: 6379)
   * @param {number} opts.db - Redis database number (default: 0)
   * @param {string} opts.username - Redis username (optional)
   * @param {string} opts.password - Redis password (optional)
   * @param {Object} opts.tls - TLS configuration (optional)
   * @param {boolean} opts.tls.enabled - Enable TLS
   * @param {Buffer|string} opts.tls.ca - CA certificate
   * @param {boolean} opts.tls.rejectUnauthorized - Reject unauthorized certs
   * @param {number} opts.connectionTimeoutMs - Connection timeout (default: 5000ms)
   * @param {number} opts.maxRetries - Max reconnection attempts (default: 10)
   */
  constructor(opts = {}) {
    this.opts = {
      host: opts.host || 'localhost',
      port: opts.port || 6379,
      db: opts.db || 0,
      username: opts.username,
      password: opts.password,
      connectionTimeoutMs: opts.connectionTimeoutMs || 5000,
      maxRetries: opts.maxRetries || 10,
      ...opts,
    };

    this.client = null;
    this.connected = false;
    this.reconnecting = false;
    this.retryCount = 0;
  }

  /**
   * Connect to Redis with exponential backoff retry logic
   * @returns {Promise<Redis>} Connected Redis client
   * @throws {Error} Connection failure after max retries
   */
  async connect() {
    if (this.connected && this.client) {
      return this.client;
    }

    const redisConfig = {
      host: this.opts.host,
      port: this.opts.port,
      db: this.opts.db,
      username: this.opts.username,
      password: this.opts.password,
      connectTimeout: this.opts.connectionTimeoutMs,
      maxRetriesPerRequest: 3,
      enableReadyCheck: true,
      enableOfflineQueue: true,
      retryStrategy: (times) => this._retryStrategy(times),
      reconnectOnError: (err) => this._reconnectOnError(err),
    };

    // Add TLS configuration if enabled
    if (this.opts.tls && this.opts.tls.enabled) {
      redisConfig.tls = {
        ca: this.opts.tls.ca,
        rejectUnauthorized: this.opts.tls.rejectUnauthorized !== false,
      };
    }

    try {
      this.client = new Redis(redisConfig);

      // Setup event handlers
      this._setupEventHandlers();

      // Wait for connection to be ready
      await this._waitForReady();

      this.connected = true;
      this.retryCount = 0;

      return this.client;
    } catch (error) {
      throwError(9001, 'REDISFAILURE', {
        message: 'Failed to connect to Redis',
        host: this.opts.host,
        port: this.opts.port,
        error: error.message,
      });
    }
  }

  /**
   * Disconnect from Redis gracefully
   * @returns {Promise<void>}
   */
  async disconnect() {
    if (!this.client) {
      return;
    }

    try {
      await this.client.quit();
      this.connected = false;
      this.client = null;
    } catch (error) {
      // Force disconnect if graceful quit fails
      if (this.client) {
        this.client.disconnect();
        this.connected = false;
        this.client = null;
      }
    }
  }

  /**
   * Ping Redis to check connection health
   * @returns {Promise<boolean>} True if connection is healthy
   */
  async ping() {
    if (!this.client || !this.connected) {
      return false;
    }

    try {
      const result = await this.client.ping();
      return result === 'PONG';
    } catch (error) {
      return false;
    }
  }

  /**
   * Get the underlying Redis client
   * @returns {Redis} ioredis client instance
   * @throws {Error} If not connected
   */
  getClient() {
    if (!this.client || !this.connected) {
      throwError(9001, 'REDISFAILURE', {
        message: 'Redis client not connected',
      });
    }
    return this.client;
  }

  /**
   * Check if connection is active
   * @returns {boolean} Connection status
   */
  isConnected() {
    return this.connected && this.client && this.client.status === 'ready';
  }

  /**
   * Setup event handlers for Redis client
   * @private
   */
  _setupEventHandlers() {
    this.client.on('connect', () => {
      this.reconnecting = false;
    });

    this.client.on('ready', () => {
      this.connected = true;
      this.retryCount = 0;
    });

    this.client.on('error', (error) => {
      console.error('[RedisConnection] Error:', error.message);
    });

    this.client.on('close', () => {
      this.connected = false;
    });

    this.client.on('reconnecting', (delay) => {
      this.reconnecting = true;
      console.log(`[RedisConnection] Reconnecting in ${delay}ms...`);
    });

    this.client.on('end', () => {
      this.connected = false;
      this.reconnecting = false;
    });
  }

  /**
   * Wait for Redis connection to be ready
   * @private
   * @returns {Promise<void>}
   */
  async _waitForReady() {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Connection timeout'));
      }, this.opts.connectionTimeoutMs);

      if (this.client.status === 'ready') {
        clearTimeout(timeout);
        resolve();
        return;
      }

      this.client.once('ready', () => {
        clearTimeout(timeout);
        resolve();
      });

      this.client.once('error', (error) => {
        clearTimeout(timeout);
        reject(error);
      });
    });
  }

  /**
   * Retry strategy with exponential backoff
   * @private
   * @param {number} times - Number of retry attempts
   * @returns {number|null} Delay in ms or null to stop retrying
   */
  _retryStrategy(times) {
    if (times > this.opts.maxRetries) {
      console.error(`[RedisConnection] Max retries (${this.opts.maxRetries}) exceeded`);
      return null; // Stop retrying
    }

    this.retryCount = times;

    // Exponential backoff: min(1000 * 2^(times-1), 30000)
    const delay = Math.min(1000 * Math.pow(2, times - 1), 30000);

    // Add jitter (±20% randomness)
    const jitter = delay * 0.2 * (Math.random() - 0.5);

    return Math.floor(delay + jitter);
  }

  /**
   * Determine if reconnection should occur on error
   * @private
   * @param {Error} err - Redis error
   * @returns {boolean|number} True/delay to reconnect, false to stop
   */
  _reconnectOnError(err) {
    const targetErrors = [
      'READONLY',
      'ECONNRESET',
      'ETIMEDOUT',
      'ENOTFOUND',
    ];

    if (targetErrors.some((target) => err.message.includes(target))) {
      return true; // Reconnect on these errors
    }

    return false; // Don't reconnect on other errors
  }
}

module.exports = RedisConnection;
