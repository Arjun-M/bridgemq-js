const RetryStrategy = require('./RetryStrategy');

/**
 * ExponentialBackoff - Exponential backoff retry strategy
 * 
 * PURPOSE: Increase delay exponentially with each retry
 * 
 * FORMULA: min(baseDelay * 2^(attempt-1), maxDelay)
 * 
 * EXAMPLES (baseDelay=1000ms, maxDelay=60000ms):
 * - Attempt 1: 1000ms (1s)
 * - Attempt 2: 2000ms (2s)
 * - Attempt 3: 4000ms (4s)
 * - Attempt 4: 8000ms (8s)
 * - Attempt 5: 16000ms (16s)
 * - Attempt 6: 32000ms (32s)
 * - Attempt 7: 60000ms (60s, capped)
 * 
 * BENEFITS:
 * - Quickly backs off from transient failures
 * - Prevents overwhelming failing services
 * - Industry standard for distributed systems
 */
class ExponentialBackoff extends RetryStrategy {
  /**
   * Create exponential backoff strategy
   * @param {Object} options - Strategy options
   */
  constructor(options = {}) {
    super({
      baseDelayMs: options.baseDelayMs || 1000,
      maxDelayMs: options.maxDelayMs || 60000,
      maxAttempts: options.maxAttempts || 3,
      jitterFactor: options.jitterFactor || 0.2,
      multiplier: options.multiplier || 2,
    });
  }

  /**
   * Calculate exponential delay
   * @param {number} attempt - Attempt number (1-based)
   * @returns {number} Delay in milliseconds
   */
  calculateDelay(attempt) {
    const multiplier = this.options.multiplier || 2;
    const delay = this.options.baseDelayMs * Math.pow(multiplier, attempt - 1);
    return Math.min(delay, this.options.maxDelayMs);
  }

  /**
   * Get delay schedule
   * @returns {Array<number>} Delay for each attempt
   */
  getDelaySchedule() {
    const schedule = [];
    
    for (let i = 1; i <= this.options.maxAttempts; i++) {
      schedule.push(this.calculateDelay(i));
    }

    return schedule;
  }
}

module.exports = ExponentialBackoff;
