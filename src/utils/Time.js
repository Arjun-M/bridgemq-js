/**
 * Time - Time and date utilities
 * 
 * PURPOSE: Time manipulation and conversion utilities
 * 
 * FEATURES:
 * - Timestamp conversion
 * - Time arithmetic (add/subtract)
 * - Duration formatting
 * - Timezone handling
 * - Relative time (ago, fromNow)
 * 
 * USAGE:
 * const timestamp = Time.now();
 * const future = Time.addSeconds(timestamp, 60);
 * const ago = Time.ago(timestamp);
 */
class Time {
  /**
   * Get current timestamp
   * @returns {number} Current timestamp in milliseconds
   */
  static now() {
    return Date.now();
  }

  /**
   * Get current timestamp in seconds
   * @returns {number} Current timestamp in seconds
   */
  static nowSeconds() {
    return Math.floor(Date.now() / 1000);
  }

  /**
   * Add seconds to timestamp
   * @param {number} timestamp - Base timestamp
   * @param {number} seconds - Seconds to add
   * @returns {number} New timestamp
   */
  static addSeconds(timestamp, seconds) {
    return timestamp + (seconds * 1000);
  }

  /**
   * Add minutes to timestamp
   * @param {number} timestamp - Base timestamp
   * @param {number} minutes - Minutes to add
   * @returns {number} New timestamp
   */
  static addMinutes(timestamp, minutes) {
    return timestamp + (minutes * 60 * 1000);
  }

  /**
   * Add hours to timestamp
   * @param {number} timestamp - Base timestamp
   * @param {number} hours - Hours to add
   * @returns {number} New timestamp
   */
  static addHours(timestamp, hours) {
    return timestamp + (hours * 60 * 60 * 1000);
  }

  /**
   * Add days to timestamp
   * @param {number} timestamp - Base timestamp
   * @param {number} days - Days to add
   * @returns {number} New timestamp
   */
  static addDays(timestamp, days) {
    return timestamp + (days * 24 * 60 * 60 * 1000);
  }

  /**
   * Format duration in human-readable form
   * @param {number} ms - Duration in milliseconds
   * @returns {string} Formatted duration
   */
  static formatDuration(ms) {
    if (ms < 1000) {
      return `${ms}ms`;
    }

    const seconds = Math.floor(ms / 1000);
    if (seconds < 60) {
      return `${seconds}s`;
    }

    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) {
      return `${minutes}m ${seconds % 60}s`;
    }

    const hours = Math.floor(minutes / 60);
    if (hours < 24) {
      return `${hours}h ${minutes % 60}m`;
    }

    const days = Math.floor(hours / 24);
    return `${days}d ${hours % 24}h`;
  }

  /**
   * Get relative time string (e.g., "5 minutes ago")
   * @param {number} timestamp - Timestamp
   * @returns {string} Relative time
   */
  static ago(timestamp) {
    const now = Date.now();
    const diff = now - timestamp;

    if (diff < 1000) {
      return 'just now';
    }

    const seconds = Math.floor(diff / 1000);
    if (seconds < 60) {
      return `${seconds} second${seconds > 1 ? 's' : ''} ago`;
    }

    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) {
      return `${minutes} minute${minutes > 1 ? 's' : ''} ago`;
    }

    const hours = Math.floor(minutes / 60);
    if (hours < 24) {
      return `${hours} hour${hours > 1 ? 's' : ''} ago`;
    }

    const days = Math.floor(hours / 24);
    return `${days} day${days > 1 ? 's' : ''} ago`;
  }

  /**
   * Get relative time from now (e.g., "in 5 minutes")
   * @param {number} timestamp - Future timestamp
   * @returns {string} Relative time
   */
  static fromNow(timestamp) {
    const now = Date.now();
    const diff = timestamp - now;

    if (diff < 0) {
      return this.ago(timestamp);
    }

    if (diff < 1000) {
      return 'now';
    }

    const seconds = Math.floor(diff / 1000);
    if (seconds < 60) {
      return `in ${seconds} second${seconds > 1 ? 's' : ''}`;
    }

    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) {
      return `in ${minutes} minute${minutes > 1 ? 's' : ''}`;
    }

    const hours = Math.floor(minutes / 60);
    if (hours < 24) {
      return `in ${hours} hour${hours > 1 ? 's' : ''}`;
    }

    const days = Math.floor(hours / 24);
    return `in ${days} day${days > 1 ? 's' : ''}`;
  }

  /**
   * Convert seconds to milliseconds
   * @param {number} seconds - Seconds
   * @returns {number} Milliseconds
   */
  static secondsToMs(seconds) {
    return seconds * 1000;
  }

  /**
   * Convert milliseconds to seconds
   * @param {number} ms - Milliseconds
   * @returns {number} Seconds
   */
  static msToSeconds(ms) {
    return Math.floor(ms / 1000);
  }

  /**
   * Check if timestamp is in the past
   * @param {number} timestamp - Timestamp to check
   * @returns {boolean} True if in past
   */
  static isPast(timestamp) {
    return timestamp < Date.now();
  }

  /**
   * Check if timestamp is in the future
   * @param {number} timestamp - Timestamp to check
   * @returns {boolean} True if in future
   */
  static isFuture(timestamp) {
    return timestamp > Date.now();
  }
}

module.exports = Time;
