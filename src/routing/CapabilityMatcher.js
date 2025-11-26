/**
 * CapabilityMatcher - Match server capabilities to job requirements
 * 
 * PURPOSE: Intelligent capability matching with wildcard support
 * 
 * FEATURES:
 * - Exact capability matching
 * - Wildcard support (* matches any)
 * - Multi-requirement validation
 * - Capability caching for performance
 * - Priority-based matching
 * 
 * CAPABILITY SYNTAX:
 * - 'email' - Exact match required
 * - 'email:*' - Any email capability
 * - 'email:sendgrid' - Specific provider
 * - '*' - Matches any capability
 * 
 * EXAMPLES:
 * Server: ['email:sendgrid', 'sms:twilio', 'push']
 * 
 * Matches:
 * - ['email:sendgrid'] ✓
 * - ['email:*'] ✓
 * - ['sms'] ✗ (requires exact or wildcard)
 * - ['push'] ✓
 */
class CapabilityMatcher {
  constructor() {
    this.cache = new Map();
  }

  /**
   * Check if server capabilities match requirements
   * @param {Array<string>} serverCapabilities - Server capabilities
   * @param {Array<string>} requirements - Required capabilities
   * @returns {boolean} True if all requirements met
   */
  matchesRequirements(serverCapabilities, requirements) {
    if (!requirements || requirements.length === 0) {
      return true; // No requirements
    }

    if (!serverCapabilities || serverCapabilities.length === 0) {
      return false; // Server has no capabilities
    }

    // Check if server has all required capabilities
    for (const requirement of requirements) {
      if (!this.hasCapability(serverCapabilities, requirement)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Check if server has specific capability
   * @param {Array<string>} serverCapabilities - Server capabilities
   * @param {string} requirement - Required capability
   * @returns {boolean} True if server has capability
   */
  hasCapability(serverCapabilities, requirement) {
    // Check cache
    const cacheKey = `${serverCapabilities.join(',')}:${requirement}`;
    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey);
    }

    let result = false;

    // Wildcard requirement matches any capability
    if (requirement === '*') {
      result = serverCapabilities.length > 0;
    } else if (requirement.includes('*')) {
      // Wildcard matching (e.g., 'email:*')
      result = this._matchWildcard(serverCapabilities, requirement);
    } else {
      // Exact match
      result = serverCapabilities.includes(requirement);
    }

    // Cache result
    this.cache.set(cacheKey, result);

    return result;
  }

  /**
   * Match capability with wildcard
   * @private
   * @param {Array<string>} serverCapabilities - Server capabilities
   * @param {string} pattern - Pattern with wildcard
   * @returns {boolean} True if matches
   */
  _matchWildcard(serverCapabilities, pattern) {
    // Convert pattern to regex
    const regexPattern = pattern
      .replace(/\*/g, '.*')
      .replace(/\?/g, '.');
    
    const regex = new RegExp(`^${regexPattern}$`);

    // Check if any server capability matches pattern
    return serverCapabilities.some((cap) => regex.test(cap));
  }

  /**
   * Get matching capabilities
   * @param {Array<string>} serverCapabilities - Server capabilities
   * @param {string} pattern - Pattern (may include wildcards)
   * @returns {Array<string>} Matching capabilities
   */
  getMatchingCapabilities(serverCapabilities, pattern) {
    if (pattern === '*') {
      return serverCapabilities;
    }

    if (pattern.includes('*') || pattern.includes('?')) {
      const regexPattern = pattern
        .replace(/\*/g, '.*')
        .replace(/\?/g, '.');
      
      const regex = new RegExp(`^${regexPattern}$`);

      return serverCapabilities.filter((cap) => regex.test(cap));
    }

    // Exact match
    return serverCapabilities.filter((cap) => cap === pattern);
  }

  /**
   * Parse capability string into components
   * @param {string} capability - Capability string
   * @returns {Object} Parsed capability
   */
  parseCapability(capability) {
    const parts = capability.split(':');
    
    return {
      category: parts[0] || '',
      provider: parts[1] || null,
      version: parts[2] || null,
      raw: capability,
    };
  }

  /**
   * Check if two capabilities are compatible
   * @param {string} cap1 - First capability
   * @param {string} cap2 - Second capability
   * @returns {boolean} True if compatible
   */
  areCompatible(cap1, cap2) {
    if (cap1 === cap2) {
      return true;
    }

    const parsed1 = this.parseCapability(cap1);
    const parsed2 = this.parseCapability(cap2);

    // Same category is minimum requirement
    if (parsed1.category !== parsed2.category) {
      return false;
    }

    // If either has wildcard provider, they're compatible
    if (!parsed1.provider || !parsed2.provider) {
      return true;
    }

    // Provider must match
    return parsed1.provider === parsed2.provider;
  }

  /**
   * Score capability match quality (0-100)
   * @param {string} serverCapability - Server capability
   * @param {string} requirement - Required capability
   * @returns {number} Match score
   */
  scoreMatch(serverCapability, requirement) {
    if (serverCapability === requirement) {
      return 100; // Perfect match
    }

    const parsed1 = this.parseCapability(serverCapability);
    const parsed2 = this.parseCapability(requirement);

    let score = 0;

    // Category match
    if (parsed1.category === parsed2.category) {
      score += 50;
    }

    // Provider match
    if (parsed1.provider === parsed2.provider) {
      score += 30;
    }

    // Version match
    if (parsed1.version === parsed2.version) {
      score += 20;
    }

    return score;
  }

  /**
   * Find best matching server
   * @param {Array<Object>} servers - Server list
   * @param {Array<string>} requirements - Required capabilities
   * @returns {Object|null} Best matching server
   */
  findBestMatch(servers, requirements) {
    if (!servers || servers.length === 0) {
      return null;
    }

    let bestServer = null;
    let bestScore = -1;

    for (const server of servers) {
      if (this.matchesRequirements(server.capabilities, requirements)) {
        const score = this._calculateServerScore(server.capabilities, requirements);
        
        if (score > bestScore) {
          bestScore = score;
          bestServer = server;
        }
      }
    }

    return bestServer;
  }

  /**
   * Calculate server match score
   * @private
   * @param {Array<string>} serverCapabilities - Server capabilities
   * @param {Array<string>} requirements - Requirements
   * @returns {number} Score
   */
  _calculateServerScore(serverCapabilities, requirements) {
    let totalScore = 0;

    for (const requirement of requirements) {
      for (const serverCap of serverCapabilities) {
        const score = this.scoreMatch(serverCap, requirement);
        totalScore += score;
      }
    }

    return totalScore;
  }

  /**
   * Clear capability cache
   */
  clearCache() {
    this.cache.clear();
  }

  /**
   * Get cache size
   * @returns {number} Cache size
   */
  getCacheSize() {
    return this.cache.size;
  }
}

module.exports = CapabilityMatcher;
