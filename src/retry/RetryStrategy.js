/**
 * RetryStrategy - Base class for all retry strategies
 * 
 * PURPOSE: Abstract interface for retry delay calculation
 * 
 * FEATURES:
 * - Strategy interface definition
 * - Jitter application (prevent thundering herd)
 * - Max delay enforcement
 * - Retry eligibility checking
 * 
 * JITTER:
 * Adds randomness to prevent all retries happening simultaneously
 * Formula: delay ± (delay * jitterFactor * random(-1, 1))
 */
class RetryStrategy {
  /**
   * Create retry strategy
   * @param {Object} options - Strategy options
   */
  constructor(options = {}) {
    this.options = {
      baseDelayMs: options.baseDelayMs || 1000,
      maxDelayMs: options.maxDelayMs || 60000,
      maxAttempts: options.maxAttempts || 3,
      jitterFactor: options.jitterFactor || 0.2, // 20% jitter
    };
  }

  /**
   * Calculate delay for attempt (must be implemented by subclass)
   * @param {number} attempt - Attempt number (1-based)
   * @returns {number} Delay in milliseconds
   */
  calculateDelay(attempt) {
    throw new Error('calculateDelay() must be implemented by subclass');
  }

  /**
   * Calculate delay with jitter
   * @param {number} attempt - Attempt number
   * @returns {number} Delay with jitter applied
   */
  calculateDelayWithJitter(attempt) {
    const baseDelay = this.calculateDelay(attempt);
    const jitter = this._applyJitter(baseDelay);
    return Math.min(jitter, this.options.maxDelayMs);
  }

  /**
   * Apply jitter to delay
   * @private
   * @param {number} delay - Base delay
   * @returns {number} Delay with jitter
   */
  _applyJitter(delay) {
    const jitterRange = delay * this.options.jitterFactor;
    const jitter = (Math.random() * 2 - 1) * jitterRange; // Random between -range and +range
    return Math.floor(delay + jitter);
  }

  /**
   * Check if retry should be attempted
   * @param {number} attempt - Current attempt number
   * @returns {boolean} True if should retry
   */
  shouldRetry(attempt) {
    return attempt < this.options.maxAttempts;
  }

  /**
   * Get max attempts
   * @returns {number} Max attempts
   */
  getMaxAttempts() {
    return this.options.maxAttempts;
  }

  /**
   * Get base delay
   * @returns {number} Base delay in ms
   */
  getBaseDelay() {
    return this.options.baseDelayMs;
  }

  /**
   * Get max delay
   * @returns {number} Max delay in ms
   */
  getMaxDelay() {
    return this.options.maxDelayMs;
  }

  /**
   * Get strategy name
   * @returns {string} Strategy name
   */
  getName() {
    return this.constructor.name;
  }

  /**
   * Serialize strategy config
   * @returns {Object} Config object
   */
  toJSON() {
    return {
      type: this.getName(),
      options: this.options,
    };
  }

  /**
   * Create strategy from config
   * @param {Object} config - Strategy config
   * @returns {RetryStrategy} Strategy instance
   */
  static fromJSON(config) {
    const { type, options } = config;
    
    // Import strategy class dynamically
    const ExponentialBackoff = require('./ExponentialBackoff');
    const LinearBackoff = require('./LinearBackoff');

    switch (type) {
      case 'ExponentialBackoff':
        return new ExponentialBackoff(options);
      
      case 'LinearBackoff':
        return new LinearBackoff(options);
      
      default:
        throw new Error(`Unknown retry strategy: ${type}`);
    }
  }
}

module.exports = RetryStrategy;
