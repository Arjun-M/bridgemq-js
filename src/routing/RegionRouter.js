const ServerStorage = require('../storage/ServerStorage');

/**
 * RegionRouter - Geographic routing and region preferences
 * 
 * PURPOSE: Route jobs based on geographic regions for latency optimization
 * 
 * FEATURES:
 * - Region-aware routing
 * - Fallback to other regions
 * - Latency-based selection
 * - Region priority lists
 * - Cross-region failover
 * 
 * LOGIC:
 * 1. Try primary region first
 * 2. If no servers available, try fallback regions
 * 3. Select best server within region
 * 4. Track region availability
 */
class RegionRouter {
  /**
   * Create region router
   * @param {Redis} redis - Redis client
   * @param {Object} options - Router options
   */
  constructor(redis, options = {}) {
    this.redis = redis;
    this.options = {
      enableFallback: options.enableFallback !== false,
      fallbackPriority: options.fallbackPriority || [],
      preferLocalRegion: options.preferLocalRegion !== false,
    };
  }

  /**
   * Route job to region
   * @param {string} meshId - Mesh ID
   * @param {string} preferredRegion - Preferred region
   * @param {Object} requirements - Additional requirements
   * @returns {Promise<Array<Object>>} Available servers in region
   */
  async routeToRegion(meshId, preferredRegion, requirements = {}) {
    // Try preferred region first
    let servers = await this._getServersInRegion(meshId, preferredRegion);

    // Filter by additional requirements
    if (requirements.capabilities) {
      servers = this._filterByCapabilities(servers, requirements.capabilities);
    }

    if (servers.length > 0) {
      return servers;
    }

    // Fallback to other regions if enabled
    if (this.options.enableFallback) {
      return await this._fallbackToOtherRegions(meshId, preferredRegion, requirements);
    }

    return [];
  }

  /**
   * Get servers in specific region
   * @private
   * @param {string} meshId - Mesh ID
   * @param {string} region - Region name
   * @returns {Promise<Array<Object>>} Servers in region
   */
  async _getServersInRegion(meshId, region) {
    const allServers = await ServerStorage.listServers(this.redis, meshId);
    
    return allServers.filter(
      (server) => server.region === region && server.status === 'online',
    );
  }

  /**
   * Fallback to other regions
   * @private
   * @param {string} meshId - Mesh ID
   * @param {string} excludeRegion - Region to exclude
   * @param {Object} requirements - Requirements
   * @returns {Promise<Array<Object>>} Available servers
   */
  async _fallbackToOtherRegions(meshId, excludeRegion, requirements) {
    // Use fallback priority if specified
    if (this.options.fallbackPriority.length > 0) {
      for (const region of this.options.fallbackPriority) {
        if (region === excludeRegion) {
          continue;
        }

        const servers = await this._getServersInRegion(meshId, region);
        const filtered = this._filterByCapabilities(
          servers,
          requirements.capabilities,
        );

        if (filtered.length > 0) {
          return filtered;
        }
      }
    }

    // No priority list, try any available region
    const allServers = await ServerStorage.listServers(this.redis, meshId);
    const otherRegionServers = allServers.filter(
      (server) =>
        server.region !== excludeRegion && server.status === 'online',
    );

    return this._filterByCapabilities(
      otherRegionServers,
      requirements.capabilities,
    );
  }

  /**
   * Filter servers by capabilities
   * @private
   * @param {Array<Object>} servers - Server list
   * @param {Array<string>} capabilities - Required capabilities
   * @returns {Array<Object>} Filtered servers
   */
  _filterByCapabilities(servers, capabilities) {
    if (!capabilities || capabilities.length === 0) {
      return servers;
    }

    return servers.filter((server) => {
      const serverCaps = server.capabilities || [];
      return capabilities.every((cap) => serverCaps.includes(cap));
    });
  }

  /**
   * Get available regions for mesh
   * @param {string} meshId - Mesh ID
   * @returns {Promise<Array<string>>} Region list
   */
  async getAvailableRegions(meshId) {
    const servers = await ServerStorage.listServers(this.redis, meshId);
    const regions = new Set();

    for (const server of servers) {
      if (server.region && server.status === 'online') {
        regions.add(server.region);
      }
    }

    return Array.from(regions);
  }

  /**
   * Get region statistics
   * @param {string} meshId - Mesh ID
   * @returns {Promise<Object>} Region stats
   */
  async getRegionStats(meshId) {
    const servers = await ServerStorage.listServers(this.redis, meshId);
    const stats = {};

    for (const server of servers) {
      const region = server.region || 'unknown';

      if (!stats[region]) {
        stats[region] = {
          total: 0,
          online: 0,
          offline: 0,
          draining: 0,
        };
      }

      stats[region].total++;
      
      if (server.status === 'online') {
        stats[region].online++;
      } else if (server.status === 'offline') {
        stats[region].offline++;
      } else if (server.status === 'draining') {
        stats[region].draining++;
      }
    }

    return stats;
  }

  /**
   * Check if region is available
   * @param {string} meshId - Mesh ID
   * @param {string} region - Region name
   * @returns {Promise<boolean>} True if available
   */
  async isRegionAvailable(meshId, region) {
    const servers = await this._getServersInRegion(meshId, region);
    return servers.length > 0;
  }

  /**
   * Get closest region based on priority
   * @param {string} preferredRegion - Preferred region
   * @param {Array<string>} availableRegions - Available regions
   * @returns {string|null} Closest region
   */
  getClosestRegion(preferredRegion, availableRegions) {
    if (availableRegions.includes(preferredRegion)) {
      return preferredRegion;
    }

    // Use fallback priority
    for (const region of this.options.fallbackPriority) {
      if (availableRegions.includes(region)) {
        return region;
      }
    }

    // Return first available region
    return availableRegions[0] || null;
  }

  /**
   * Set fallback priority
   * @param {Array<string>} priority - Priority list
   */
  setFallbackPriority(priority) {
    this.options.fallbackPriority = priority;
  }

  /**
   * Get fallback priority
   * @returns {Array<string>} Priority list
   */
  getFallbackPriority() {
    return this.options.fallbackPriority;
  }
}

module.exports = RegionRouter;
