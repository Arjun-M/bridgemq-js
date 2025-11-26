const QueueStorage = require('../storage/QueueStorage');

/**
 * LoadBalancer - Distribute load across available servers
 * 
 * PURPOSE: Select servers using various load balancing strategies
 * 
 * STRATEGIES:
 * - round-robin: Rotate through servers sequentially
 * - least-busy: Select server with lowest active job count
 * - random: Random selection
 * - weighted: Based on server resources
 * - affinity: Sticky sessions by job type or key
 * 
 * FEATURES:
 * - Multiple strategy support
 * - Server affinity/stickiness
 * - Weighted selection
 * - Active load tracking
 */
class LoadBalancer {
  /**
   * Create load balancer
   * @param {string} strategy - Load balancing strategy
   */
  constructor(strategy = 'least-busy') {
    this.strategy = strategy;
    this.roundRobinIndex = 0;
    this.affinityMap = new Map();
  }

  /**
   * Select server from list
   * @param {Array<Object>} servers - Available servers
   * @param {Object} options - Selection options
   * @returns {Object|null} Selected server
   */
  selectServer(servers, options = {}) {
    if (!servers || servers.length === 0) {
      return null;
    }

    if (servers.length === 1) {
      return servers[0];
    }

    switch (this.strategy) {
      case 'round-robin':
        return this._selectRoundRobin(servers);
      
      case 'least-busy':
        return this._selectLeastBusy(servers);
      
      case 'random':
        return this._selectRandom(servers);
      
      case 'weighted':
        return this._selectWeighted(servers);
      
      case 'affinity':
        return this._selectAffinity(servers, options.affinityKey);
      
      default:
        return this._selectLeastBusy(servers);
    }
  }

  /**
   * Round-robin selection
   * @private
   * @param {Array<Object>} servers - Server list
   * @returns {Object} Selected server
   */
  _selectRoundRobin(servers) {
    const server = servers[this.roundRobinIndex % servers.length];
    this.roundRobinIndex++;
    return server;
  }

  /**
   * Least-busy selection (lowest current load)
   * @private
   * @param {Array<Object>} servers - Server list
   * @returns {Object} Selected server
   */
  _selectLeastBusy(servers) {
    let leastBusyServer = servers[0];
    let lowestLoad = servers[0].currentLoad || 0;

    for (const server of servers) {
      const load = server.currentLoad || 0;
      
      if (load < lowestLoad) {
        lowestLoad = load;
        leastBusyServer = server;
      }
    }

    return leastBusyServer;
  }

  /**
   * Random selection
   * @private
   * @param {Array<Object>} servers - Server list
   * @returns {Object} Selected server
   */
  _selectRandom(servers) {
    const randomIndex = Math.floor(Math.random() * servers.length);
    return servers[randomIndex];
  }

  /**
   * Weighted selection based on resources
   * @private
   * @param {Array<Object>} servers - Server list
   * @returns {Object} Selected server
   */
  _selectWeighted(servers) {
    // Calculate total weight
    let totalWeight = 0;
    const weights = [];

    for (const server of servers) {
      // Weight based on available resources
      const cpu = server.resources?.cpu || 1;
      const memory = server.resources?.memory || 1;
      const weight = cpu * memory * (1 - (server.currentLoad || 0));
      
      weights.push(weight);
      totalWeight += weight;
    }

    // Select server based on weighted probability
    let random = Math.random() * totalWeight;
    
    for (let i = 0; i < servers.length; i++) {
      random -= weights[i];
      
      if (random <= 0) {
        return servers[i];
      }
    }

    // Fallback to last server
    return servers[servers.length - 1];
  }

  /**
   * Affinity selection (sticky sessions)
   * @private
   * @param {Array<Object>} servers - Server list
   * @param {string} affinityKey - Affinity key (job type, user ID, etc.)
   * @returns {Object} Selected server
   */
  _selectAffinity(servers, affinityKey) {
    if (!affinityKey) {
      return this._selectLeastBusy(servers);
    }

    // Check if we have existing affinity
    if (this.affinityMap.has(affinityKey)) {
      const serverId = this.affinityMap.get(affinityKey);
      const server = servers.find((s) => s.serverId === serverId);
      
      if (server) {
        return server; // Use existing affinity
      }
      
      // Server no longer available, remove affinity
      this.affinityMap.delete(affinityKey);
    }

    // Create new affinity
    const server = this._selectLeastBusy(servers);
    this.affinityMap.set(affinityKey, server.serverId);
    
    return server;
  }

  /**
   * Set load balancing strategy
   * @param {string} strategy - Strategy name
   */
  setStrategy(strategy) {
    this.strategy = strategy;
  }

  /**
   * Get current strategy
   * @returns {string} Strategy name
   */
  getStrategy() {
    return this.strategy;
  }

  /**
   * Clear affinity map
   */
  clearAffinity() {
    this.affinityMap.clear();
  }

  /**
   * Remove specific affinity
   * @param {string} affinityKey - Affinity key
   */
  removeAffinity(affinityKey) {
    this.affinityMap.delete(affinityKey);
  }

  /**
   * Get affinity for key
   * @param {string} affinityKey - Affinity key
   * @returns {string|null} Server ID or null
   */
  getAffinity(affinityKey) {
    return this.affinityMap.get(affinityKey) || null;
  }

  /**
   * Select multiple servers (for broadcast)
   * @param {Array<Object>} servers - Server list
   * @param {number} count - Number of servers to select
   * @returns {Array<Object>} Selected servers
   */
  selectMultiple(servers, count) {
    if (!servers || servers.length === 0) {
      return [];
    }

    const selectedCount = Math.min(count, servers.length);
    const selected = [];

    // Use random selection for multiple servers
    const shuffled = [...servers].sort(() => Math.random() - 0.5);
    
    for (let i = 0; i < selectedCount; i++) {
      selected.push(shuffled[i]);
    }

    return selected;
  }

  /**
   * Calculate server score for selection
   * @param {Object} server - Server object
   * @returns {number} Score (higher is better)
   */
  calculateServerScore(server) {
    const load = server.currentLoad || 0;
    const cpu = server.resources?.cpu || 1;
    const memory = server.resources?.memory || 1;
    
    // Score = available resources / current load
    const availableResources = cpu * memory;
    const score = availableResources / (load + 1); // +1 to avoid division by zero
    
    return score;
  }
}

module.exports = LoadBalancer;
