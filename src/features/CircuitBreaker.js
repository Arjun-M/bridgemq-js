/**
 * CircuitBreaker - Prevent cascade failures
 * 
 * PURPOSE: Stop calling failing services to allow recovery
 * 
 * STATES:
 * - CLOSED: Normal operation, requests pass through
 * - OPEN: Failures exceeded, reject all requests
 * - HALF_OPEN: Testing if service recovered
 * 
 * LOGIC:
 * 1. Start in CLOSED state
 * 2. Track success/failure rate
 * 3. If failures exceed threshold → OPEN
 * 4. After timeout → HALF_OPEN
 * 5. If test succeeds → CLOSED
 * 6. If test fails → OPEN
 * 
 * EXAMPLE:
 * threshold: 5 failures in 10 requests
 * timeout: 30 seconds
 * 
 * - 6 failures → Circuit OPEN
 * - Wait 30s → Circuit HALF_OPEN
 * - 1 success → Circuit CLOSED
 */
class CircuitBreaker {
  constructor(options = {}) {
    this.options = {
      failureThreshold: options.failureThreshold || 5,
      successThreshold: options.successThreshold || 2,
      timeout: options.timeout || 30000,
      windowSize: options.windowSize || 10,
    };

    this.state = 'CLOSED';
    this.failures = 0;
    this.successes = 0;
    this.lastFailureTime = null;
    this.nextAttemptTime = null;
  }

  async execute(fn) {
    if (this.state === 'OPEN') {
      if (Date.now() >= this.nextAttemptTime) {
        this.state = 'HALF_OPEN';
      } else {
        throw new Error('Circuit breaker is OPEN');
      }
    }

    try {
      const result = await fn();
      this._onSuccess();
      return result;
    } catch (error) {
      this._onFailure();
      throw error;
    }
  }

  _onSuccess() {
    this.failures = 0;
    
    if (this.state === 'HALF_OPEN') {
      this.successes++;
      
      if (this.successes >= this.options.successThreshold) {
        this.state = 'CLOSED';
        this.successes = 0;
      }
    }
  }

  _onFailure() {
    this.failures++;
    this.lastFailureTime = Date.now();
    this.successes = 0;

    if (this.failures >= this.options.failureThreshold) {
      this.state = 'OPEN';
      this.nextAttemptTime = Date.now() + this.options.timeout;
    }
  }

  getState() {
    return this.state;
  }

  reset() {
    this.state = 'CLOSED';
    this.failures = 0;
    this.successes = 0;
    this.lastFailureTime = null;
    this.nextAttemptTime = null;
  }

  forceOpen() {
    this.state = 'OPEN';
    this.nextAttemptTime = Date.now() + this.options.timeout;
  }

  forceClosed() {
    this.state = 'CLOSED';
    this.failures = 0;
    this.successes = 0;
  }
}

module.exports = CircuitBreaker;
