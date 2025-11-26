const { throwError } = require('../utils/Errors');

/**
 * ServerStorage - Server registration and mesh management
 * 
 * PURPOSE: Handle server registration with AUTO-CREATE mesh capability
 * 
 * CRITICAL BEHAVIOR:
 * - Mesh is AUTO-CREATED during server registration if it doesn't exist
 * - No separate createMesh API - mesh creation is implicit
 * - Server registration is the canonical way to establish meshes
 * 
 * FEATURES:
 * - Register/deregister servers
 * - Automatic mesh creation on first server registration
 * - Heartbeat management (30s TTL, refreshed periodically)
 * - Capability and stack indexing
 * - Server status management (online/offline/draining)
 * 
 * ERROR CODES:
 * - 9001: REDIS_FAILURE
 * - 9004: STORAGE_WRITE_FAILURE
 * - 9005: STORAGE_READ_FAILURE
 */

const NS = 'bridgemq';

class ServerStorage {
  /**
   * Register a server (AUTO-CREATES mesh if it doesn't exist)
   * @param {Redis} redis - Redis client
   * @param {Object} serverInfo - Server registration data
   * @returns {Promise<void>}
   */
  static async registerServer(redis, serverInfo) {
    const {
      serverId,
      stack,
      capabilities = [],
      meshId,
      meshName,
      meshDescription,
      meshConfig = {},
      region,
      resources = {},
      metadata = {},
    } = serverInfo;

    if (!serverId || !stack || !meshId) {
      throwError(9004, 'STORAGE_WRITE_FAILURE', {
        message: 'serverId, stack, and meshId are required',
      });
    }

    try {
      const now = Date.now();

      // 1. AUTO-CREATE MESH if it doesn't exist
      const meshKey = `${NS}:mesh:${meshId}`;
      const meshExists = await redis.exists(meshKey);

      if (!meshExists) {
        // Create mesh metadata
        await redis.hmset(meshKey, {
          meshId,
          name: meshName || meshId,
          description: meshDescription || '',
          createdAt: now,
          config: JSON.stringify(meshConfig),
        });
      }

      // 2. Register server
      const serverKey = `${NS}:server:${serverId}`;
      await redis.hmset(serverKey, {
        serverId,
        stack,
        capabilities: JSON.stringify(capabilities),
        meshIds: JSON.stringify([meshId]),
        region: region || '',
        resources: JSON.stringify(resources),
        metadata: JSON.stringify(metadata),
        registeredAt: now,
        lastHeartbeat: now,
        status: 'online',
        currentLoad: 0,
        totalProcessed: 0,
        totalFailed: 0,
      });

      // Set TTL for server (30 seconds - refreshed by heartbeat)
      await redis.expire(serverKey, 30);

      // 3. Add server to mesh members
      const meshMembersKey = `${NS}:mesh:${meshId}:members`;
      await redis.sadd(meshMembersKey, serverId);

      // 4. Update capability index
      for (const capability of capabilities) {
        const capKey = `${NS}:capability:${capability}`;
        await redis.sadd(capKey, serverId);
      }

      // 5. Update stack index
      const stackKey = `${NS}:stack:${stack}`;
      await redis.sadd(stackKey, serverId);

      // 6. Update region index if provided
      if (region) {
        const regionKey = `${NS}:region:${region}`;
        await redis.sadd(regionKey, serverId);
      }
    } catch (error) {
      throwError(9004, 'STORAGE_WRITE_FAILURE', {
        message: 'Failed to register server',
        serverId,
        error: error.message,
      });
    }
  }

  /**
   * Deregister a server
   * @param {Redis} redis - Redis client
   * @param {string} serverId - Server ID
   * @returns {Promise<void>}
   */
  static async deregisterServer(redis, serverId) {
    try {
      // Get server info before deleting
      const serverInfo = await this.getServerInfo(redis, serverId);
      
      if (!serverInfo) {
        return;
      }

      // Remove from mesh members
      const meshIds = JSON.parse(serverInfo.meshIds || '[]');
      for (const meshId of meshIds) {
        const meshMembersKey = `${NS}:mesh:${meshId}:members`;
        await redis.srem(meshMembersKey, serverId);
      }

      // Remove from capability indexes
      const capabilities = JSON.parse(serverInfo.capabilities || '[]');
      for (const capability of capabilities) {
        const capKey = `${NS}:capability:${capability}`;
        await redis.srem(capKey, serverId);
      }

      // Remove from stack index
      const stackKey = `${NS}:stack:${serverInfo.stack}`;
      await redis.srem(stackKey, serverId);

      // Remove from region index
      if (serverInfo.region) {
        const regionKey = `${NS}:region:${serverInfo.region}`;
        await redis.srem(regionKey, serverId);
      }

      // Delete server key
      const serverKey = `${NS}:server:${serverId}`;
      await redis.del(serverKey);

      // Delete active jobs list
      const activeKey = `${NS}:active:${serverId}`;
      await redis.del(activeKey);
    } catch (error) {
      throwError(9004, 'STORAGE_WRITE_FAILURE', {
        message: 'Failed to deregister server',
        serverId,
        error: error.message,
      });
    }
  }

  /**
   * Start heartbeat for a server
   * @param {Redis} redis - Redis client
   * @param {string} serverId - Server ID
   * @param {number} intervalMs - Heartbeat interval (default 10000ms)
   * @returns {NodeJS.Timer} Heartbeat interval ID
   */
  static startHeartbeat(redis, serverId, intervalMs = 10000) {
    return setInterval(async () => {
      await this.refreshHeartbeat(redis, serverId);
    }, intervalMs);
  }

  /**
   * Refresh heartbeat for a server
   * @param {Redis} redis - Redis client
   * @param {string} serverId - Server ID
   * @returns {Promise<void>}
   */
  static async refreshHeartbeat(redis, serverId) {
    try {
      const serverKey = `${NS}:server:${serverId}`;
      const now = Date.now();

      await redis.hmset(serverKey, {
        lastHeartbeat: now,
      });

      // Refresh TTL
      await redis.expire(serverKey, 30);
    } catch (error) {
      // Non-critical, don't throw
      console.error(`Failed to refresh heartbeat for server ${serverId}:`, error.message);
    }
  }

  /**
   * Set server status
   * @param {Redis} redis - Redis client
   * @param {string} serverId - Server ID
   * @param {string} status - Status (online|offline|draining)
   * @returns {Promise<void>}
   */
  static async setStatus(redis, serverId, status) {
    try {
      const serverKey = `${NS}:server:${serverId}`;
      await redis.hset(serverKey, 'status', status);
    } catch (error) {
      throwError(9004, 'STORAGE_WRITE_FAILURE', {
        message: 'Failed to set server status',
        serverId,
        status,
        error: error.message,
      });
    }
  }

  /**
   * Join additional mesh (server can be in multiple meshes)
   * @param {Redis} redis - Redis client
   * @param {Object} meshInfo - Mesh information
   * @param {string} serverId - Server ID
   * @returns {Promise<void>}
   */
  static async joinMesh(redis, meshInfo, serverId) {
    const {
      meshId,
      name,
      description,
      config = {},
    } = meshInfo;

    if (!meshId || !serverId) {
      throwError(9004, 'STORAGE_WRITE_FAILURE', {
        message: 'meshId and serverId are required',
      });
    }

    try {
      const now = Date.now();

      // AUTO-CREATE mesh if it doesn't exist
      const meshKey = `${NS}:mesh:${meshId}`;
      const meshExists = await redis.exists(meshKey);

      if (!meshExists) {
        await redis.hmset(meshKey, {
          meshId,
          name: name || meshId,
          description: description || '',
          createdAt: now,
          config: JSON.stringify(config),
        });
      }

      // Add server to mesh members
      const meshMembersKey = `${NS}:mesh:${meshId}:members`;
      await redis.sadd(meshMembersKey, serverId);

      // Update server's meshIds list
      const serverKey = `${NS}:server:${serverId}`;
      const serverInfo = await redis.hgetall(serverKey);
      
      if (serverInfo) {
        const meshIds = JSON.parse(serverInfo.meshIds || '[]');
        if (!meshIds.includes(meshId)) {
          meshIds.push(meshId);
          await redis.hset(serverKey, 'meshIds', JSON.stringify(meshIds));
        }
      }
    } catch (error) {
      throwError(9004, 'STORAGE_WRITE_FAILURE', {
        message: 'Failed to join mesh',
        meshId,
        serverId,
        error: error.message,
      });
    }
  }

  /**
   * Leave a mesh
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @param {string} serverId - Server ID
   * @returns {Promise<void>}
   */
  static async leaveMesh(redis, meshId, serverId) {
    try {
      // Remove from mesh members
      const meshMembersKey = `${NS}:mesh:${meshId}:members`;
      await redis.srem(meshMembersKey, serverId);

      // Update server's meshIds list
      const serverKey = `${NS}:server:${serverId}`;
      const serverInfo = await redis.hgetall(serverKey);
      
      if (serverInfo) {
        const meshIds = JSON.parse(serverInfo.meshIds || '[]');
        const updatedMeshIds = meshIds.filter((id) => id !== meshId);
        await redis.hset(serverKey, 'meshIds', JSON.stringify(updatedMeshIds));
      }
    } catch (error) {
      throwError(9004, 'STORAGE_WRITE_FAILURE', {
        message: 'Failed to leave mesh',
        meshId,
        serverId,
        error: error.message,
      });
    }
  }

  /**
   * Get server information
   * @param {Redis} redis - Redis client
   * @param {string} serverId - Server ID
   * @returns {Promise<Object|null>} Server info
   */
  static async getServerInfo(redis, serverId) {
    try {
      const serverKey = `${NS}:server:${serverId}`;
      const serverInfo = await redis.hgetall(serverKey);

      if (!serverInfo || Object.keys(serverInfo).length === 0) {
        return null;
      }

      return {
        ...serverInfo,
        capabilities: JSON.parse(serverInfo.capabilities || '[]'),
        meshIds: JSON.parse(serverInfo.meshIds || '[]'),
        resources: JSON.parse(serverInfo.resources || '{}'),
        metadata: JSON.parse(serverInfo.metadata || '{}'),
        registeredAt: parseInt(serverInfo.registeredAt, 10),
        lastHeartbeat: parseInt(serverInfo.lastHeartbeat, 10),
        currentLoad: parseFloat(serverInfo.currentLoad),
        totalProcessed: parseInt(serverInfo.totalProcessed, 10),
        totalFailed: parseInt(serverInfo.totalFailed, 10),
      };
    } catch (error) {
      return null;
    }
  }

  /**
   * List servers in a mesh
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @returns {Promise<Array>} Server list
   */
  static async listServers(redis, meshId) {
    try {
      const meshMembersKey = `${NS}:mesh:${meshId}:members`;
      const serverIds = await redis.smembers(meshMembersKey);

      const servers = await Promise.all(
        serverIds.map((serverId) => this.getServerInfo(redis, serverId)),
      );

      return servers.filter((server) => server !== null);
    } catch (error) {
      throwError(9005, 'STORAGE_READ_FAILURE', {
        message: 'Failed to list servers',
        meshId,
        error: error.message,
      });
    }
  }
}

module.exports = ServerStorage;
