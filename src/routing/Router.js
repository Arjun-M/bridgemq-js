const ServerStorage = require('../storage/ServerStorage');
const CapabilityMatcher = require('./CapabilityMatcher');
const LoadBalancer = require('./LoadBalancer');

/**
 * Router - Route jobs to appropriate servers based on capabilities
 * 
 * PURPOSE: Intelligent job distribution across mesh servers
 * 
 * FEATURES:
 * - Server discovery by capabilities
 * - Capability matching and validation
 * - Load balancing strategy selection
 * - Fallback handling
 * - Region-aware routing
 * 
 * LOGIC:
 * 1. Get job requirements (capabilities, region, etc.)
 * 2. Discover eligible servers
 * 3. Filter by capabilities
 * 4. Apply load balancing strategy
 * 5. Return selected server(s)
 */
class Router {
  /**
   * Create router instance
   * @param {Redis} redis - Redis client
   * @param {Object} options - Router options
   */
  constructor(redis, options = {}) {
    this.redis = redis;
    this.options = {
      strategy: options.strategy || 'least-busy', // round-robin, least-busy, random
      enableRegionRouting: options.enableRegionRouting !== false,
      fallbackToAnyServer: options.fallbackToAnyServer !== false,
    };

    this.capabilityMatcher = new CapabilityMatcher();
    this.loadBalancer = new LoadBalancer(this.options.strategy);
  }

  /**
   * Route job to appropriate server
   * @param {Object} jobConfig - Job configuration
   * @param {string} meshId - Mesh ID
   * @returns {Promise<string|null>} Server ID or null
   */
  async routeJob(jobConfig, meshId) {
    const requirements = this._extractRequirements(jobConfig);

    // Get eligible servers
    const eligibleServers = await this._discoverEligibleServers(
      meshId,
      requirements,
    );

    if (eligibleServers.length === 0) {
      return null; // No eligible servers
    }

    // Select server using load balancing strategy
    const selectedServer = this.loadBalancer.selectServer(eligibleServers);

    return selectedServer ? selectedServer.serverId : null;
  }

  /**
   * Discover eligible servers for job
   * @param {string} meshId - Mesh ID
   * @param {Object} requirements - Job requirements
   * @returns {Promise<Array>} Eligible servers
   */
  async _discoverEligibleServers(meshId, requirements) {
    // Get all servers in mesh
    const allServers = await ServerStorage.listServers(this.redis, meshId);

    // Filter by status (only online servers)
    let eligibleServers = allServers.filter(
      (server) => server.status === 'online',
    );

    // Filter by capabilities if required
    if (requirements.capabilities && requirements.capabilities.length > 0) {
      eligibleServers = eligibleServers.filter((server) =>
        this.capabilityMatcher.matchesRequirements(
          server.capabilities,
          requirements.capabilities,
        ),
      );
    }

    // Filter by region if enabled and specified
    if (this.options.enableRegionRouting && requirements.region) {
      const regionServers = eligibleServers.filter(
        (server) => server.region === requirements.region,
      );

      // Use region servers if available, otherwise fallback
      if (regionServers.length > 0 || !this.options.fallbackToAnyServer) {
        eligibleServers = regionServers;
      }
    }

    // Filter by stack if specified
    if (requirements.stack) {
      eligibleServers = eligibleServers.filter(
        (server) => server.stack === requirements.stack,
      );
    }

    return eligibleServers;
  }

  /**
   * Extract requirements from job config
   * @private
   * @param {Object} jobConfig - Job configuration
   * @returns {Object} Requirements
   */
  _extractRequirements(jobConfig) {
    const requirements = {};

    if (jobConfig.target) {
      requirements.capabilities = jobConfig.target.capabilities || [];
      requirements.region = jobConfig.target.region;
      requirements.stack = jobConfig.target.stack;
      requirements.serverId = jobConfig.target.serverId; // Pin to specific server
    }

    return requirements;
  }

  /**
   * Get available servers for mesh
   * @param {string} meshId - Mesh ID
   * @returns {Promise<Array>} Server list
   */
  async getAvailableServers(meshId) {
    const servers = await ServerStorage.listServers(this.redis, meshId);
    return servers.filter((server) => server.status === 'online');
  }

  /**
   * Get servers by capability
   * @param {string} meshId - Mesh ID
   * @param {string} capability - Required capability
   * @returns {Promise<Array>} Servers with capability
   */
  async getServersByCapability(meshId, capability) {
    const allServers = await ServerStorage.listServers(this.redis, meshId);
    
    return allServers.filter((server) =>
      this.capabilityMatcher.hasCapability(server.capabilities, capability),
    );
  }

  /**
   * Get servers by region
   * @param {string} meshId - Mesh ID
   * @param {string} region - Region name
   * @returns {Promise<Array>} Servers in region
   */
  async getServersByRegion(meshId, region) {
    const allServers = await ServerStorage.listServers(this.redis, meshId);
    return allServers.filter((server) => server.region === region);
  }

  /**
   * Set load balancing strategy
   * @param {string} strategy - Strategy name
   */
  setStrategy(strategy) {
    this.options.strategy = strategy;
    this.loadBalancer.setStrategy(strategy);
  }

  /**
   * Get current strategy
   * @returns {string} Strategy name
   */
  getStrategy() {
    return this.options.strategy;
  }

  /**
   * Check if server is eligible for job
   * @param {Object} server - Server object
   * @param {Object} requirements - Job requirements
   * @returns {boolean} True if eligible
   */
  isServerEligible(server, requirements) {
    // Check status
    if (server.status !== 'online') {
      return false;
    }

    // Check capabilities
    if (requirements.capabilities && requirements.capabilities.length > 0) {
      if (
        !this.capabilityMatcher.matchesRequirements(
          server.capabilities,
          requirements.capabilities,
        )
      ) {
        return false;
      }
    }

    // Check region
    if (requirements.region && server.region !== requirements.region) {
      return false;
    }

    // Check stack
    if (requirements.stack && server.stack !== requirements.stack) {
      return false;
    }

    // Check specific server ID
    if (requirements.serverId && server.serverId !== requirements.serverId) {
      return false;
    }

    return true;
  }
}

module.exports = Router;
