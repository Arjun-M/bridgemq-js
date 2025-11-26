const RetryStrategy = require('./RetryStrategy');

/**
 * LinearBackoff - Linear backoff retry strategy
 * 
 * PURPOSE: Increase delay linearly with each retry
 * 
 * FORMULA: min(baseDelay + (attempt - 1) * step, maxDelay)
 * 
 * EXAMPLES (baseDelay=1000ms, step=2000ms, maxDelay=60000ms):
 * - Attempt 1: 1000ms (1s)
 * - Attempt 2: 3000ms (3s)
 * - Attempt 3: 5000ms (5s)
 * - Attempt 4: 7000ms (7s)
 * - Attempt 5: 9000ms (9s)
 * 
 * BENEFITS:
 * - Predictable delay increases
 * - Good for rate-limited APIs
 * - Easier to reason about than exponential
 */
class LinearBackoff extends RetryStrategy {
  /**
   * Create linear backoff strategy
   * @param {Object} options - Strategy options
   */
  constructor(options = {}) {
    super({
      baseDelayMs: options.baseDelayMs || 1000,
      maxDelayMs: options.maxDelayMs || 60000,
      maxAttempts: options.maxAttempts || 3,
      jitterFactor: options.jitterFactor || 0.2,
      stepMs: options.stepMs || 2000,
    });
  }

  /**
   * Calculate linear delay
   * @param {number} attempt - Attempt number (1-based)
   * @returns {number} Delay in milliseconds
   */
  calculateDelay(attempt) {
    const step = this.options.stepMs || 2000;
    const delay = this.options.baseDelayMs + (attempt - 1) * step;
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

module.exports = LinearBackoff;
