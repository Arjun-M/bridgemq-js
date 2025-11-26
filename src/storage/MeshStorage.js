const { throwError } = require('../utils/Errors');

/**
 * MeshStorage - Read-only mesh information access
 * 
 * PURPOSE: Provide read-only access to mesh data
 * 
 * CRITICAL: NO createMesh API - meshes are created automatically
 * by ServerStorage.registerServer() and ServerStorage.joinMesh()
 * 
 * FEATURES:
 * - Get mesh metadata and configuration
 * - List all meshes
 * - Internal helpers for member management (used by ServerStorage)
 * 
 * ERROR CODES:
 * - 9005: STORAGE_READ_FAILURE
 */

const NS = 'bridgemq';

class MeshStorage {
  /**
   * Get mesh information
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @returns {Promise<Object|null>} Mesh info
   */
  static async getMesh(redis, meshId) {
    try {
      const meshKey = `${NS}:mesh:${meshId}`;
      const meshInfo = await redis.hgetall(meshKey);

      if (!meshInfo || Object.keys(meshInfo).length === 0) {
        return null;
      }

      // Get member count
      const meshMembersKey = `${NS}:mesh:${meshId}:members`;
      const memberCount = await redis.scard(meshMembersKey);

      return {
        ...meshInfo,
        config: JSON.parse(meshInfo.config || '{}'),
        createdAt: parseInt(meshInfo.createdAt, 10),
        memberCount,
      };
    } catch (error) {
      return null;
    }
  }

  /**
   * List all meshes
   * @param {Redis} redis - Redis client
   * @returns {Promise<Array>} Mesh list
   */
  static async listMeshes(redis) {
    try {
      const pattern = `${NS}:mesh:*`;
      const keys = await redis.keys(pattern);

      // Filter out mesh:*:members keys
      const meshKeys = keys.filter((key) => !key.includes(':members'));

      const meshes = await Promise.all(
        meshKeys.map(async (key) => {
          const meshId = key.split(':')[2];
          return this.getMesh(redis, meshId);
        }),
      );

      return meshes.filter((mesh) => mesh !== null);
    } catch (error) {
      throwError(9005, 'STORAGE_READ_FAILURE', {
        message: 'Failed to list meshes',
        error: error.message,
      });
    }
  }

  /**
   * Add member to mesh (INTERNAL - used by ServerStorage)
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @param {string} serverId - Server ID
   * @returns {Promise<void>}
   */
  static async addMember(redis, meshId, serverId) {
    try {
      const meshMembersKey = `${NS}:mesh:${meshId}:members`;
      await redis.sadd(meshMembersKey, serverId);
    } catch (error) {
      console.error(`Failed to add member ${serverId} to mesh ${meshId}:`, error.message);
    }
  }

  /**
   * Remove member from mesh (INTERNAL - used by ServerStorage)
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @param {string} serverId - Server ID
   * @returns {Promise<void>}
   */
  static async removeMember(redis, meshId, serverId) {
    try {
      const meshMembersKey = `${NS}:mesh:${meshId}:members`;
      await redis.srem(meshMembersKey, serverId);
    } catch (error) {
      console.error(`Failed to remove member ${serverId} from mesh ${meshId}:`, error.message);
    }
  }

  /**
   * Get mesh members
   * @param {Redis} redis - Redis client
   * @param {string} meshId - Mesh ID
   * @returns {Promise<Array>} Server IDs
   */
  static async getMembers(redis, meshId) {
    try {
      const meshMembersKey = `${NS}:mesh:${meshId}:members`;
      return await redis.smembers(meshMembersKey);
    } catch (error) {
      return [];
    }
  }
}

module.exports = MeshStorage;
