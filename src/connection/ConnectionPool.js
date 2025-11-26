const RedisConnection = require('./RedisConnection');
const { throwError } = require('../utils/Errors');

/**
 * ConnectionPool - Manages a pool of Redis connections for concurrent operations
 * 
 * PURPOSE: Provides connection pooling to handle concurrent Redis operations
 * efficiently without overwhelming a single connection.
 * 
 * FEATURES:
 * - Dynamic pool sizing based on configuration
 * - Connection health checks and stale connection detection
 * - Acquire/release pattern with timeout
 * - Automatic pool exhaustion handling
 * - Connection reuse and recycling
 * 
 * LOGIC:
 * 1. Create pool of N connections on initialization
 * 2. acquire() waits for available connection or creates new one (up to max)
 * 3. release() returns connection to available pool
 * 4. Health checks remove stale connections
 * 5. close() drains pool gracefully
 * 
 * ERROR CODES:
 * - 9001: REDISFAILURE - Pool exhaustion or connection failure
 */
class ConnectionPool {
  /**
   * Create a connection pool
   * @param {Object} opts - Pool configuration
   * @param {Object} opts.redis - Redis connection options (passed to RedisConnection)
   * @param {number} opts.minConnections - Minimum connections to maintain (default: 2)
   * @param {number} opts.maxConnections - Maximum connections allowed (default: 10)
   * @param {number} opts.acquireTimeoutMs - Max wait time for connection (default: 3000ms)
   * @param {number} opts.healthCheckIntervalMs - Health check frequency (default: 30000ms)
   */
  constructor(opts = {}) {
    this.opts = {
      redis: opts.redis || {},
      minConnections: opts.minConnections || 2,
      maxConnections: opts.maxConnections || 10,
      acquireTimeoutMs: opts.acquireTimeoutMs || 3000,
      healthCheckIntervalMs: opts.healthCheckIntervalMs || 30000,
    };

    // Pool state
    this.availableConnections = [];
    this.inUseConnections = new Set();
    this.totalConnections = 0;
    this.waitQueue = [];
    this.closed = false;

    // Health check timer
    this.healthCheckTimer = null;
  }

  /**
   * Initialize the connection pool
   * @returns {Promise<void>}
   */
  async createPool() {
    if (this.closed) {
      throwError(9001, 'REDISFAILURE', {
        message: 'Cannot create pool: already closed',
      });
    }

    // Create minimum number of connections
    const promises = [];
    for (let i = 0; i < this.opts.minConnections; i++) {
      promises.push(this._createConnection());
    }

    await Promise.all(promises);

    // Start health check timer
    this._startHealthCheck();
  }

  /**
   * Acquire a connection from the pool
   * @returns {Promise<Redis>} Redis client instance
   * @throws {Error} If pool exhausted or timeout
   */
  async acquire() {
    if (this.closed) {
      throwError(9001, 'REDISFAILURE', {
        message: 'Cannot acquire: pool closed',
      });
    }

    // Try to get available connection
    if (this.availableConnections.length > 0) {
      const conn = this.availableConnections.pop();
      this.inUseConnections.add(conn);
      return conn.getClient();
    }

    // Create new connection if under max limit
    if (this.totalConnections < this.opts.maxConnections) {
      const conn = await this._createConnection();
      this.inUseConnections.add(conn);
      return conn.getClient();
    }

    // Wait for connection to become available
    return this._waitForConnection();
  }

  /**
   * Release a connection back to the pool
   * @param {Redis} client - Redis client to release
   */
  async release(client) {
    if (this.closed) {
      return;
    }

    // Find connection wrapper
    const conn = this._findConnection(client);
    if (!conn) {
      return; // Connection not managed by this pool
    }

    // Remove from in-use set
    this.inUseConnections.delete(conn);

    // Check if there are waiting requests
    if (this.waitQueue.length > 0) {
      const { resolve } = this.waitQueue.shift();
      this.inUseConnections.add(conn);
      resolve(client);
      return;
    }

    // Return to available pool
    this.availableConnections.push(conn);
  }

  /**
   * Close all connections in the pool
   * @returns {Promise<void>}
   */
  async close() {
    if (this.closed) {
      return;
    }

    this.closed = true;

    // Stop health check
    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
      this.healthCheckTimer = null;
    }

    // Reject all waiting requests
    this.waitQueue.forEach(({ reject }) => {
      reject(new Error('Pool closed'));
    });
    this.waitQueue = [];

    // Close all connections
    const allConnections = [
      ...this.availableConnections,
      ...Array.from(this.inUseConnections),
    ];

    await Promise.all(
      allConnections.map((conn) => conn.disconnect()),
    );

    this.availableConnections = [];
    this.inUseConnections.clear();
    this.totalConnections = 0;
  }

  /**
   * Get pool statistics
   * @returns {Object} Pool stats
   */
  getStats() {
    return {
      total: this.totalConnections,
      available: this.availableConnections.length,
      inUse: this.inUseConnections.size,
      waiting: this.waitQueue.length,
      minConnections: this.opts.minConnections,
      maxConnections: this.opts.maxConnections,
    };
  }

  /**
   * Create a new connection and add to pool
   * @private
   * @returns {Promise<RedisConnection>}
   */
  async _createConnection() {
    const conn = new RedisConnection(this.opts.redis);
    await conn.connect();

    this.totalConnections += 1;
    this.availableConnections.push(conn);

    return conn;
  }

  /**
   * Wait for a connection to become available
   * @private
   * @returns {Promise<Redis>}
   */
  _waitForConnection() {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        // Remove from wait queue
        const index = this.waitQueue.findIndex((item) => item.resolve === resolve);
        if (index !== -1) {
          this.waitQueue.splice(index, 1);
        }

        reject(new Error('Connection acquire timeout'));
      }, this.opts.acquireTimeoutMs);

      this.waitQueue.push({
        resolve: (client) => {
          clearTimeout(timeout);
          resolve(client);
        },
        reject: (error) => {
          clearTimeout(timeout);
          reject(error);
        },
      });
    });
  }

  /**
   * Find connection wrapper by client instance
   * @private
   * @param {Redis} client - Redis client
   * @returns {RedisConnection|null}
   */
  _findConnection(client) {
    // Check in-use connections
    for (const conn of this.inUseConnections) {
      if (conn.client === client) {
        return conn;
      }
    }

    // Check available connections
    for (const conn of this.availableConnections) {
      if (conn.client === client) {
        return conn;
      }
    }

    return null;
  }

  /**
   * Start periodic health check
   * @private
   */
  _startHealthCheck() {
    this.healthCheckTimer = setInterval(async () => {
      await this._performHealthCheck();
    }, this.opts.healthCheckIntervalMs);
  }

  /**
   * Perform health check on all connections
   * @private
   */
  async _performHealthCheck() {
    const staleConnections = [];

    // Check available connections
    for (const conn of this.availableConnections) {
      const healthy = await conn.ping();
      if (!healthy) {
        staleConnections.push(conn);
      }
    }

    // Remove stale connections
    for (const conn of staleConnections) {
      const index = this.availableConnections.indexOf(conn);
      if (index !== -1) {
        this.availableConnections.splice(index, 1);
        this.totalConnections -= 1;
        await conn.disconnect();
      }
    }

    // Maintain minimum connections
    while (this.totalConnections < this.opts.minConnections) {
      try {
        await this._createConnection();
      } catch (error) {
        console.error('[ConnectionPool] Failed to create connection during health check:', error.message);
        break;
      }
    }
  }
}

module.exports = ConnectionPool;
