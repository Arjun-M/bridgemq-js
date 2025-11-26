/**
 * PriorityQueue - Handle job prioritization and aging
 * 
 * PURPOSE: Advanced priority management with age-based boosting
 * 
 * FEATURES:
 * - Priority levels 1-10 (10 = highest)
 * - Age-based priority boosting (prevent starvation)
 * - Score calculation for fair selection
 * - Dynamic priority adjustment
 * 
 * LOGIC:
 * 1. Jobs start with base priority (1-10)
 * 2. As jobs age, priority increases
 * 3. Score = base_priority + (age_seconds / age_factor)
 * 4. Jobs selected by highest score
 * 5. Prevents low-priority jobs from starving
 * 
 * EXAMPLE:
 * Job A: priority=5, age=100s, ageFactor=100
 *   score = 5 + (100/100) = 6.0
 * 
 * Job B: priority=4, age=500s, ageFactor=100
 *   score = 4 + (500/100) = 9.0
 * 
 * Job B selected despite lower base priority due to age
 */
class PriorityQueue {
  /**
   * Create priority queue
   * @param {Object} options - Queue options
   */
  constructor(options = {}) {
    this.options = {
      ageFactor: options.ageFactor || 100, // Age boost factor (seconds)
      maxPriority: options.maxPriority || 10,
      minPriority: options.minPriority || 1,
    };
  }

  /**
   * Calculate job score (priority + age boost)
   * @param {Object} job - Job metadata
   * @returns {number} Job score
   */
  calculateScore(job) {
    const basePriority = job.priority || 5;
    const now = Date.now();
    const age = (now - job.createdAt) / 1000; // Age in seconds
    
    // Age boost increases priority over time
    const ageBoost = age / this.options.ageFactor;
    
    return basePriority + ageBoost;
  }

  /**
   * Calculate priority boost based on age
   * @param {number} ageSeconds - Job age in seconds
   * @returns {number} Priority boost
   */
  calculateAgeBoost(ageSeconds) {
    return ageSeconds / this.options.ageFactor;
  }

  /**
   * Get effective priority (base + age boost)
   * @param {number} basePriority - Base priority (1-10)
   * @param {number} ageSeconds - Job age in seconds
   * @returns {number} Effective priority
   */
  getEffectivePriority(basePriority, ageSeconds) {
    const boost = this.calculateAgeBoost(ageSeconds);
    return Math.min(basePriority + boost, this.options.maxPriority);
  }

  /**
   * Select best job from list based on score
   * @param {Array} jobs - List of job objects
   * @returns {Object|null} Best job or null
   */
  selectBestJob(jobs) {
    if (!jobs || jobs.length === 0) {
      return null;
    }

    let bestJob = null;
    let bestScore = -Infinity;

    for (const job of jobs) {
      const score = this.calculateScore(job);
      
      if (score > bestScore) {
        bestScore = score;
        bestJob = job;
      }
    }

    return bestJob;
  }

  /**
   * Sort jobs by score (highest first)
   * @param {Array} jobs - List of job objects
   * @returns {Array} Sorted jobs
   */
  sortByScore(jobs) {
    return jobs.sort((a, b) => {
      const scoreA = this.calculateScore(a);
      const scoreB = this.calculateScore(b);
      return scoreB - scoreA; // Descending
    });
  }

  /**
   * Validate priority value
   * @param {number} priority - Priority value
   * @returns {boolean} True if valid
   */
  isValidPriority(priority) {
    return (
      typeof priority === 'number' &&
      priority >= this.options.minPriority &&
      priority <= this.options.maxPriority
    );
  }

  /**
   * Normalize priority to valid range
   * @param {number} priority - Priority value
   * @returns {number} Normalized priority
   */
  normalizePriority(priority) {
    if (priority < this.options.minPriority) {
      return this.options.minPriority;
    }
    if (priority > this.options.maxPriority) {
      return this.options.maxPriority;
    }
    return Math.round(priority);
  }

  /**
   * Boost priority by a factor
   * @param {number} basePriority - Base priority
   * @param {number} factor - Boost factor (1.0 = no change, 2.0 = double)
   * @returns {number} Boosted priority
   */
  boostPriority(basePriority, factor) {
    const boosted = basePriority * factor;
    return this.normalizePriority(boosted);
  }

  /**
   * Get priority level name
   * @param {number} priority - Priority value
   * @returns {string} Priority level name
   */
  getPriorityLevel(priority) {
    if (priority >= 9) return 'critical';
    if (priority >= 7) return 'high';
    if (priority >= 5) return 'normal';
    if (priority >= 3) return 'low';
    return 'very-low';
  }

  /**
   * Calculate expected wait time based on priority and queue depth
   * @param {number} priority - Job priority
   * @param {number} queueDepth - Number of jobs ahead
   * @param {number} avgProcessingTime - Average processing time (ms)
   * @returns {number} Expected wait time (ms)
   */
  estimateWaitTime(priority, queueDepth, avgProcessingTime = 1000) {
    // Higher priority jobs wait less
    const priorityFactor = (this.options.maxPriority - priority + 1) / this.options.maxPriority;
    return queueDepth * avgProcessingTime * priorityFactor;
  }
}

module.exports = PriorityQueue;
