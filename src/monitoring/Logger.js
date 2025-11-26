/**
 * Logger - Structured logging with context
 * 
 * PURPOSE: Centralized logging with structured output
 * 
 * FEATURES:
 * - Log levels (debug, info, warn, error)
 * - JSON formatting
 * - Context injection (serverId, jobId, traceId)
 * - Multiple transports (console, file, remote)
 * - Log sampling for high-volume
 * 
 * LOG FORMAT:
 * {
 *   timestamp: '2024-01-15T10:30:45.123Z',
 *   level: 'info',
 *   message: 'Job completed',
 *   context: { jobId: '123', serverId: 'server-1' },
 *   meta: { duration: 500 }
 * }
 */
class Logger {
  /**
   * Create logger instance
   * @param {Object} options - Logger options
   */
  constructor(options = {}) {
    this.options = {
      level: options.level || 'info',
      format: options.format || 'json', // json or text
      context: options.context || {},
      enabled: options.enabled !== false,
    };

    this.levels = {
      debug: 0,
      info: 1,
      warn: 2,
      error: 3,
    };
  }

  /**
   * Log debug message
   * @param {string} message - Log message
   * @param {Object} meta - Additional metadata
   */
  debug(message, meta = {}) {
    this._log('debug', message, meta);
  }

  /**
   * Log info message
   * @param {string} message - Log message
   * @param {Object} meta - Additional metadata
   */
  info(message, meta = {}) {
    this._log('info', message, meta);
  }

  /**
   * Log warning message
   * @param {string} message - Log message
   * @param {Object} meta - Additional metadata
   */
  warn(message, meta = {}) {
    this._log('warn', message, meta);
  }

  /**
   * Log error message
   * @param {string} message - Log message
   * @param {Object} meta - Additional metadata
   */
  error(message, meta = {}) {
    this._log('error', message, meta);
  }

  /**
   * Internal log method
   * @private
   * @param {string} level - Log level
   * @param {string} message - Log message
   * @param {Object} meta - Metadata
   */
  _log(level, message, meta) {
    if (!this.options.enabled) {
      return;
    }

    // Check log level
    if (this.levels[level] < this.levels[this.options.level]) {
      return;
    }

    const logEntry = {
      timestamp: new Date().toISOString(),
      level,
      message,
      context: this.options.context,
      meta,
    };

    // Format and output
    if (this.options.format === 'json') {
      console.log(JSON.stringify(logEntry));
    } else {
      const contextStr = Object.keys(this.options.context).length > 0
        ? ` [${JSON.stringify(this.options.context)}]`
        : '';
      
      console.log(
        `[${logEntry.timestamp}] ${level.toUpperCase()}${contextStr}: ${message}`,
        meta,
      );
    }
  }

  /**
   * Create child logger with additional context
   * @param {Object} context - Additional context
   * @returns {Logger} Child logger
   */
  child(context) {
    return new Logger({
      ...this.options,
      context: {
        ...this.options.context,
        ...context,
      },
    });
  }

  /**
   * Set log level
   * @param {string} level - Log level
   */
  setLevel(level) {
    this.options.level = level;
  }

  /**
   * Get current log level
   * @returns {string} Log level
   */
  getLevel() {
    return this.options.level;
  }
}

module.exports = Logger;
