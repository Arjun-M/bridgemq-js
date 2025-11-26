const { v4: uuidv4 } = require('uuid');

/**
 * Tracer - Distributed tracing for job execution
 * 
 * PURPOSE: Track job execution across distributed system
 * 
 * FEATURES:
 * - Span creation and management
 * - Context propagation
 * - Parent-child span relationships
 * - Timing measurements
 * - OpenTelemetry compatible
 * - Export to Jaeger, Zipkin
 * 
 * SPAN LIFECYCLE:
 * 1. startSpan() creates new span with traceId
 * 2. Child spans inherit traceId
 * 3. Add tags/logs to span
 * 4. finishSpan() records duration
 * 5. Export to tracing backend
 */
class Tracer {
  /**
   * Create tracer instance
   * @param {Object} options - Tracer options
   */
  constructor(options = {}) {
    this.options = {
      serviceName: options.serviceName || 'bridgemq',
      enabled: options.enabled !== false,
      sampleRate: options.sampleRate || 1.0, // 1.0 = 100% sampling
    };

    this.spans = new Map();
  }

  /**
   * Start a new span
   * @param {string} operationName - Operation name
   * @param {Object} options - Span options
   * @returns {Object} Span object
   */
  startSpan(operationName, options = {}) {
    if (!this.options.enabled || !this._shouldSample()) {
      return this._createNoopSpan();
    }

    const spanId = uuidv4();
    const traceId = options.traceId || uuidv4();
    const parentSpanId = options.parentSpanId || null;

    const span = {
      spanId,
      traceId,
      parentSpanId,
      operationName,
      startTime: Date.now(),
      endTime: null,
      tags: {},
      logs: [],
      serviceName: this.options.serviceName,
    };

    this.spans.set(spanId, span);

    return this._createSpanAPI(span);
  }

  /**
   * Create span API wrapper
   * @private
   * @param {Object} span - Span object
   * @returns {Object} Span API
   */
  _createSpanAPI(span) {
    const self = this;

    return {
      setTag(key, value) {
        span.tags[key] = value;
        return this;
      },

      log(message, data = {}) {
        span.logs.push({
          timestamp: Date.now(),
          message,
          data,
        });
        return this;
      },

      finish() {
        span.endTime = Date.now();
        self._exportSpan(span);
        self.spans.delete(span.spanId);
      },

      getContext() {
        return {
          traceId: span.traceId,
          spanId: span.spanId,
        };
      },

      createChildSpan(operationName) {
        return self.startSpan(operationName, {
          traceId: span.traceId,
          parentSpanId: span.spanId,
        });
      },
    };
  }

  /**
   * Create no-op span (when disabled or not sampled)
   * @private
   * @returns {Object} No-op span API
   */
  _createNoopSpan() {
    return {
      setTag: () => this,
      log: () => this,
      finish: () => {},
      getContext: () => ({}),
      createChildSpan: () => this._createNoopSpan(),
    };
  }

  /**
   * Check if span should be sampled
   * @private
   * @returns {boolean} True if should sample
   */
  _shouldSample() {
    return Math.random() < this.options.sampleRate;
  }

  /**
   * Export span to tracing backend
   * @private
   * @param {Object} span - Span object
   */
  _exportSpan(span) {
    // Calculate duration
    const duration = span.endTime - span.startTime;

    // Log span (in production, send to Jaeger/Zipkin)
    console.log(`[Trace] ${span.operationName} (${duration}ms)`, {
      traceId: span.traceId,
      spanId: span.spanId,
      tags: span.tags,
    });
  }

  /**
   * Inject trace context into carrier
   * @param {Object} spanContext - Span context
   * @param {Object} carrier - Carrier object (e.g., job payload)
   */
  inject(spanContext, carrier) {
    carrier._trace = {
      traceId: spanContext.traceId,
      spanId: spanContext.spanId,
    };
  }

  /**
   * Extract trace context from carrier
   * @param {Object} carrier - Carrier object
   * @returns {Object|null} Span context or null
   */
  extract(carrier) {
    return carrier._trace || null;
  }

  /**
   * Get all active spans
   * @returns {Array} Active spans
   */
  getActiveSpans() {
    return Array.from(this.spans.values());
  }

  /**
   * Get span by ID
   * @param {string} spanId - Span ID
   * @returns {Object|null} Span or null
   */
  getSpan(spanId) {
    return this.spans.get(spanId) || null;
  }
}

module.exports = Tracer;
