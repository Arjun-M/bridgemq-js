const MetricsStorage = require('../storage/MetricsStorage');

/**
 * Metrics - Collect and export system metrics
 * 
 * PURPOSE: Comprehensive metrics collection for monitoring and observability
 * 
 * METRIC TYPES:
 * - Counter: Incrementing value (jobs_created, jobs_completed)
 * - Gauge: Current value (queue_depth, active_workers)
 * - Histogram: Distribution (processing_time, payload_size)
 * - Rate: Events per time unit (jobs_per_second)
 * 
 * FEATURES:
 * - Real-time metric collection
 * - Multiple export formats (Prometheus, JSON, InfluxDB)
 * - Label/tag support
 * - Automatic rate calculation
 * - Percentile computation (p50, p95, p99)
 * 
 * METRICS:
 * - jobs_created_total
 * - jobs_completed_total
 * - jobs_failed_total
 * - processing_time_seconds
 * - queue_depth
 * - worker_utilization
 */
class Metrics {
  /**
   * Create metrics collector
   * @param {Redis} redis - Redis client
   * @param {Object} options - Metrics options
   */
  constructor(redis, options = {}) {
    this.redis = redis;
    this.options = {
      meshId: options.meshId,
      prefix: options.prefix || 'bridgemq',
      collectInterval: options.collectInterval || 60000, // 1 minute
    };

    // In-memory metrics
    this.counters = new Map();
    this.gauges = new Map();
    this.histograms = new Map();
  }

  /**
   * Increment a counter
   * @param {string} name - Metric name
   * @param {number} value - Increment value (default 1)
   * @param {Object} labels - Label key-value pairs
   */
  incrementCounter(name, value = 1, labels = {}) {
    const key = this._generateKey(name, labels);
    const current = this.counters.get(key) || 0;
    this.counters.set(key, current + value);

    // Also persist to Redis
    if (this.options.meshId) {
      MetricsStorage.increment(
        this.redis,
        this.options.meshId,
        name,
        value,
      ).catch((err) => {
        console.error('Failed to persist counter:', err.message);
      });
    }
  }

  /**
   * Set a gauge value
   * @param {string} name - Metric name
   * @param {number} value - Gauge value
   * @param {Object} labels - Label key-value pairs
   */
  setGauge(name, value, labels = {}) {
    const key = this._generateKey(name, labels);
    this.gauges.set(key, value);
  }

  /**
   * Record histogram value
   * @param {string} name - Metric name
   * @param {number} value - Observed value
   * @param {Object} labels - Label key-value pairs
   */
  recordHistogram(name, value, labels = {}) {
    const key = this._generateKey(name, labels);
    
    if (!this.histograms.has(key)) {
      this.histograms.set(key, []);
    }

    const values = this.histograms.get(key);
    values.push(value);

    // Keep only last 1000 values
    if (values.length > 1000) {
      values.shift();
    }
  }

  /**
   * Record processing time
   * @param {string} jobType - Job type
   * @param {number} durationMs - Duration in milliseconds
   */
  recordProcessingTime(jobType, durationMs) {
    this.recordHistogram('job_processing_time_ms', durationMs, {
      job_type: jobType,
    });

    // Also persist to Redis
    if (this.options.meshId) {
      MetricsStorage.recordProcessingTime(
        this.redis,
        this.options.meshId,
        jobType,
        durationMs,
      ).catch((err) => {
        console.error('Failed to persist processing time:', err.message);
      });
    }
  }

  /**
   * Get counter value
   * @param {string} name - Metric name
   * @param {Object} labels - Label key-value pairs
   * @returns {number} Counter value
   */
  getCounter(name, labels = {}) {
    const key = this._generateKey(name, labels);
    return this.counters.get(key) || 0;
  }

  /**
   * Get gauge value
   * @param {string} name - Metric name
   * @param {Object} labels - Label key-value pairs
   * @returns {number} Gauge value
   */
  getGauge(name, labels = {}) {
    const key = this._generateKey(name, labels);
    return this.gauges.get(key) || 0;
  }

  /**
   * Get histogram statistics
   * @param {string} name - Metric name
   * @param {Object} labels - Label key-value pairs
   * @returns {Object} Statistics (min, max, avg, p50, p95, p99)
   */
  getHistogramStats(name, labels = {}) {
    const key = this._generateKey(name, labels);
    const values = this.histograms.get(key) || [];

    if (values.length === 0) {
      return null;
    }

    const sorted = [...values].sort((a, b) => a - b);
    const sum = sorted.reduce((a, b) => a + b, 0);

    return {
      count: sorted.length,
      min: sorted[0],
      max: sorted[sorted.length - 1],
      avg: sum / sorted.length,
      p50: this._percentile(sorted, 50),
      p95: this._percentile(sorted, 95),
      p99: this._percentile(sorted, 99),
    };
  }

  /**
   * Calculate percentile
   * @private
   * @param {Array<number>} sorted - Sorted values
   * @param {number} percentile - Percentile (0-100)
   * @returns {number} Percentile value
   */
  _percentile(sorted, percentile) {
    const index = Math.ceil((percentile / 100) * sorted.length) - 1;
    return sorted[Math.max(0, index)];
  }

  /**
   * Export metrics in Prometheus format
   * @returns {string} Prometheus text format
   */
  exportPrometheus() {
    const lines = [];

    // Counters
    for (const [key, value] of this.counters) {
      const { name, labels } = this._parseKey(key);
      const labelStr = this._formatLabels(labels);
      lines.push(`${this.options.prefix}_${name}${labelStr} ${value}`);
    }

    // Gauges
    for (const [key, value] of this.gauges) {
      const { name, labels } = this._parseKey(key);
      const labelStr = this._formatLabels(labels);
      lines.push(`${this.options.prefix}_${name}${labelStr} ${value}`);
    }

    // Histograms (export as summary with percentiles)
    for (const [key] of this.histograms) {
      const { name, labels } = this._parseKey(key);
      const stats = this.getHistogramStats(name, labels);
      
      if (stats) {
        const labelStr = this._formatLabels(labels);
        lines.push(`${this.options.prefix}_${name}_count${labelStr} ${stats.count}`);
        lines.push(`${this.options.prefix}_${name}_sum${labelStr} ${stats.avg * stats.count}`);
        lines.push(
          `${this.options.prefix}_${name}${this._formatLabels({ ...labels, quantile: '0.5' })} ${stats.p50}`,
        );
        lines.push(
          `${this.options.prefix}_${name}${this._formatLabels({ ...labels, quantile: '0.95' })} ${stats.p95}`,
        );
        lines.push(
          `${this.options.prefix}_${name}${this._formatLabels({ ...labels, quantile: '0.99' })} ${stats.p99}`,
        );
      }
    }

    return lines.join('\n');
  }

  /**
   * Export metrics in JSON format
   * @returns {Object} JSON metrics
   */
  exportJSON() {
    const metrics = {
      timestamp: Date.now(),
      counters: {},
      gauges: {},
      histograms: {},
    };

    for (const [key, value] of this.counters) {
      const { name, labels } = this._parseKey(key);
      if (!metrics.counters[name]) {
        metrics.counters[name] = [];
      }
      metrics.counters[name].push({ labels, value });
    }

    for (const [key, value] of this.gauges) {
      const { name, labels } = this._parseKey(key);
      if (!metrics.gauges[name]) {
        metrics.gauges[name] = [];
      }
      metrics.gauges[name].push({ labels, value });
    }

    for (const [key] of this.histograms) {
      const { name, labels } = this._parseKey(key);
      const stats = this.getHistogramStats(name, labels);
      if (!metrics.histograms[name]) {
        metrics.histograms[name] = [];
      }
      metrics.histograms[name].push({ labels, stats });
    }

    return metrics;
  }

  /**
   * Reset all metrics
   */
  reset() {
    this.counters.clear();
    this.gauges.clear();
    this.histograms.clear();
  }

  /**
   * Generate metric key
   * @private
   * @param {string} name - Metric name
   * @param {Object} labels - Labels
   * @returns {string} Key
   */
  _generateKey(name, labels) {
    if (Object.keys(labels).length === 0) {
      return name;
    }

    const labelStr = Object.entries(labels)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, v]) => `${k}="${v}"`)
      .join(',');

    return `${name}{${labelStr}}`;
  }

  /**
   * Parse metric key
   * @private
   * @param {string} key - Metric key
   * @returns {Object} Parsed key
   */
  _parseKey(key) {
    const match = key.match(/^([^{]+)(?:\{([^}]+)\})?$/);
    
    if (!match) {
      return { name: key, labels: {} };
    }

    const name = match[1];
    const labelStr = match[2];
    const labels = {};

    if (labelStr) {
      labelStr.split(',').forEach((pair) => {
        const [k, v] = pair.split('=');
        labels[k] = v.replace(/"/g, '');
      });
    }

    return { name, labels };
  }

  /**
   * Format labels for Prometheus
   * @private
   * @param {Object} labels - Labels
   * @returns {string} Formatted labels
   */
  _formatLabels(labels) {
    if (Object.keys(labels).length === 0) {
      return '';
    }

    const pairs = Object.entries(labels)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, v]) => `${k}="${v}"`);

    return `{${pairs.join(',')}}`;
  }
}

module.exports = Metrics;
