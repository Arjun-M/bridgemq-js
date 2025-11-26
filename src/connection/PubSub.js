const Redis = require('ioredis');
const msgpack = require('msgpack-lite');
const { throwError } = require('../utils/Errors');

/**
 * PubSub - Redis Pub/Sub handler for event broadcasting across servers
 * 
 * PURPOSE: Provides Redis Pub/Sub capabilities for real-time event distribution
 * across all servers in the BridgeMQ mesh. Uses a separate dedicated connection
 * to avoid blocking regular operations.
 * 
 * FEATURES:
 * - Separate Redis connection for Pub/Sub (required by ioredis)
 * - MessagePack serialization for efficient data transfer
 * - Pattern-based subscriptions with wildcards
 * - Automatic reconnection handling
 * - Message handler registration and deregistration
 * - Graceful unsubscribe on shutdown
 * 
 * CHANNELS:
 * - bridgemq:events:global - All system events
 * - bridgemq:events:mesh:{meshId} - Mesh-specific events
 * - bridgemq:events:job:{jobId} - Job-specific events
 * - bridgemq:events:server:{serverId} - Server-specific events
 * - bridgemq:events:type:{jobType} - Job type events
 * 
 * LOGIC:
 * 1. Create separate Redis connection for Pub/Sub
 * 2. subscribe() registers channel and handler
 * 3. Messages deserialized from MessagePack
 * 4. Handlers invoked with parsed data
 * 5. publish() serializes and broadcasts messages
 * 6. Automatic reconnection preserves subscriptions
 * 
 * ERROR CODES:
 * - 9001: REDISFAILURE - Connection or subscription failure
 * - 9006: EVENTPUBLISHFAILURE - Failed to publish event
 */
class PubSub {
  /**
   * Create PubSub instance
   * @param {Object} opts - Configuration options
   * @param {string} opts.host - Redis host
   * @param {number} opts.port - Redis port
   * @param {number} opts.db - Redis database
   * @param {string} opts.username - Redis username (optional)
   * @param {string} opts.password - Redis password (optional)
   * @param {Object} opts.tls - TLS configuration (optional)
   * @param {number} opts.connectionTimeoutMs - Connection timeout (default: 5000ms)
   */
  constructor(opts = {}) {
    this.opts = {
      host: opts.host || 'localhost',
      port: opts.port || 6379,
      db: opts.db || 0,
      username: opts.username,
      password: opts.password,
      connectionTimeoutMs: opts.connectionTimeoutMs || 5000,
      ...opts,
    };

    this.subscriber = null;
    this.publisher = null;
    this.connected = false;

    // Message handlers by channel
    this.handlers = new Map();

    // Pattern subscriptions (channels with wildcards)
    this.patterns = new Map();
  }

  /**
   * Connect to Redis and setup Pub/Sub
   * @returns {Promise<void>}
   */
  async connect() {
    if (this.connected) {
      return;
    }

    try {
      // Create subscriber connection
      this.subscriber = await this._createRedisClient();

      // Create publisher connection (separate for best practice)
      this.publisher = await this._createRedisClient();

      // Setup message handlers
      this._setupMessageHandlers();

      this.connected = true;
    } catch (error) {
      throwError(9001, 'REDISFAILURE', {
        message: 'Failed to connect PubSub to Redis',
        error: error.message,
      });
    }
  }

  /**
   * Disconnect from Redis Pub/Sub
   * @returns {Promise<void>}
   */
  async disconnect() {
    if (!this.connected) {
      return;
    }

    try {
      // Unsubscribe from all channels
      if (this.subscriber) {
        await this.subscriber.unsubscribe();
        await this.subscriber.quit();
      }

      if (this.publisher) {
        await this.publisher.quit();
      }

      this.subscriber = null;
      this.publisher = null;
      this.connected = false;
      this.handlers.clear();
      this.patterns.clear();
    } catch (error) {
      // Force disconnect on error
      if (this.subscriber) {
        this.subscriber.disconnect();
      }
      if (this.publisher) {
        this.publisher.disconnect();
      }
      this.connected = false;
    }
  }

  /**
   * Subscribe to a channel
   * @param {string} channel - Channel name (supports wildcards: * and ?)
   * @param {Function} handler - Message handler function(data, channel)
   * @returns {Promise<void>}
   */
  async subscribe(channel, handler) {
    if (!this.connected) {
      throwError(9001, 'REDISFAILURE', {
        message: 'PubSub not connected',
      });
    }

    try {
      // Check if channel contains wildcard patterns
      const isPattern = channel.includes('*') || channel.includes('?');

      if (isPattern) {
        // Use psubscribe for pattern matching
        await this.subscriber.psubscribe(channel);
        this.patterns.set(channel, handler);
      } else {
        // Regular channel subscription
        await this.subscriber.subscribe(channel);
        this.handlers.set(channel, handler);
      }
    } catch (error) {
      throwError(9001, 'REDISFAILURE', {
        message: `Failed to subscribe to channel: ${channel}`,
        error: error.message,
      });
    }
  }

  /**
   * Unsubscribe from a channel
   * @param {string} channel - Channel name
   * @returns {Promise<void>}
   */
  async unsubscribe(channel) {
    if (!this.connected) {
      return;
    }

    try {
      const isPattern = channel.includes('*') || channel.includes('?');

      if (isPattern) {
        await this.subscriber.punsubscribe(channel);
        this.patterns.delete(channel);
      } else {
        await this.subscriber.unsubscribe(channel);
        this.handlers.delete(channel);
      }
    } catch (error) {
      console.error(`[PubSub] Failed to unsubscribe from ${channel}:`, error.message);
    }
  }

  /**
   * Publish message to a channel
   * @param {string} channel - Channel name
   * @param {Object} data - Message data (will be serialized)
   * @returns {Promise<number>} Number of subscribers that received the message
   */
  async publish(channel, data) {
    if (!this.connected) {
      throwError(9001, 'REDISFAILURE', {
        message: 'PubSub not connected',
      });
    }

    try {
      // Serialize data using MessagePack
      const serialized = msgpack.encode(data);

      // Publish to channel
      const count = await this.publisher.publish(channel, serialized);

      return count;
    } catch (error) {
      throwError(9006, 'EVENTPUBLISHFAILURE', {
        message: `Failed to publish to channel: ${channel}`,
        error: error.message,
      });
    }
  }

  /**
   * Register a message handler function (alternative to passing in subscribe)
   * @param {Function} fn - Global message handler function(channel, data)
   */
  onMessage(fn) {
    this.globalHandler = fn;
  }

  /**
   * Create Redis client with configuration
   * @private
   * @returns {Promise<Redis>}
   */
  async _createRedisClient() {
    const config = {
      host: this.opts.host,
      port: this.opts.port,
      db: this.opts.db,
      username: this.opts.username,
      password: this.opts.password,
      connectTimeout: this.opts.connectionTimeoutMs,
      enableReadyCheck: true,
      maxRetriesPerRequest: null, // Important for Pub/Sub
      retryStrategy: (times) => {
        const delay = Math.min(times * 1000, 30000);
        return delay;
      },
    };

    // Add TLS if configured
    if (this.opts.tls && this.opts.tls.enabled) {
      config.tls = {
        ca: this.opts.tls.ca,
        rejectUnauthorized: this.opts.tls.rejectUnauthorized !== false,
      };
    }

    const client = new Redis(config);

    // Wait for ready
    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Connection timeout'));
      }, this.opts.connectionTimeoutMs);

      client.once('ready', () => {
        clearTimeout(timeout);
        resolve();
      });

      client.once('error', (error) => {
        clearTimeout(timeout);
        reject(error);
      });
    });

    return client;
  }

  /**
   * Setup message event handlers
   * @private
   */
  _setupMessageHandlers() {
    // Regular message handler
    this.subscriber.on('message', (channel, message) => {
      this._handleMessage(channel, message);
    });

    // Pattern message handler
    this.subscriber.on('pmessage', (pattern, channel, message) => {
      this._handlePatternMessage(pattern, channel, message);
    });

    // Subscription events
    this.subscriber.on('subscribe', (channel, count) => {
      // console.log(`[PubSub] Subscribed to ${channel} (${count} total subscriptions)`);
    });

    this.subscriber.on('psubscribe', (pattern, count) => {
      // console.log(`[PubSub] Pattern subscribed to ${pattern} (${count} total)`);
    });

    // Error handling
    this.subscriber.on('error', (error) => {
      console.error('[PubSub] Subscriber error:', error.message);
    });

    this.publisher.on('error', (error) => {
      console.error('[PubSub] Publisher error:', error.message);
    });

    // Reconnection
    this.subscriber.on('reconnecting', () => {
      console.log('[PubSub] Reconnecting subscriber...');
    });
  }

  /**
   * Handle incoming message
   * @private
   * @param {string} channel - Channel name
   * @param {Buffer} message - Message buffer
   */
  _handleMessage(channel, message) {
    try {
      // Deserialize MessagePack
      const data = msgpack.decode(message);

      // Call registered handler for this channel
      const handler = this.handlers.get(channel);
      if (handler) {
        handler(data, channel);
      }

      // Call global handler if registered
      if (this.globalHandler) {
        this.globalHandler(channel, data);
      }
    } catch (error) {
      console.error(`[PubSub] Failed to handle message on ${channel}:`, error.message);
    }
  }

  /**
   * Handle pattern message
   * @private
   * @param {string} pattern - Pattern that matched
   * @param {string} channel - Actual channel name
   * @param {Buffer} message - Message buffer
   */
  _handlePatternMessage(pattern, channel, message) {
    try {
      // Deserialize MessagePack
      const data = msgpack.decode(message);

      // Call pattern handler
      const handler = this.patterns.get(pattern);
      if (handler) {
        handler(data, channel, pattern);
      }

      // Call global handler if registered
      if (this.globalHandler) {
        this.globalHandler(channel, data);
      }
    } catch (error) {
      console.error(`[PubSub] Failed to handle pattern message on ${channel}:`, error.message);
    }
  }
}

module.exports = PubSub;
