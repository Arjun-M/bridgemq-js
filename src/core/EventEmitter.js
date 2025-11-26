/**
 * EventEmitter - Internal event system for component communication
 * 
 * PURPOSE: Lightweight pub/sub for internal events (not Redis Pub/Sub)
 * 
 * FEATURES:
 * - Register/unregister event listeners
 * - Emit events with data
 * - One-time listeners
 * - Error-safe event emission
 * - Listener count tracking
 * 
 * USAGE:
 * const events = new EventEmitter();
 * events.on('job.complete', (data) => console.log(data));
 * events.emit('job.complete', { jobId: '123' });
 */
class EventEmitter {
  constructor() {
    this.listeners = new Map();
  }

  /**
   * Register event listener
   * @param {string} event - Event name
   * @param {Function} handler - Event handler
   */
  on(event, handler) {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, []);
    }

    this.listeners.get(event).push({
      handler,
      once: false,
    });
  }

  /**
   * Register one-time event listener
   * @param {string} event - Event name
   * @param {Function} handler - Event handler
   */
  once(event, handler) {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, []);
    }

    this.listeners.get(event).push({
      handler,
      once: true,
    });
  }

  /**
   * Remove event listener
   * @param {string} event - Event name
   * @param {Function} handler - Event handler to remove
   */
  off(event, handler) {
    if (!this.listeners.has(event)) {
      return;
    }

    const handlers = this.listeners.get(event);
    const index = handlers.findIndex((h) => h.handler === handler);

    if (index !== -1) {
      handlers.splice(index, 1);
    }

    // Clean up empty listener arrays
    if (handlers.length === 0) {
      this.listeners.delete(event);
    }
  }

  /**
   * Remove all listeners for an event
   * @param {string} event - Event name
   */
  removeAllListeners(event) {
    if (event) {
      this.listeners.delete(event);
    } else {
      this.listeners.clear();
    }
  }

  /**
   * Emit an event
   * @param {string} event - Event name
   * @param {any} data - Event data
   */
  emit(event, data) {
    if (!this.listeners.has(event)) {
      return;
    }

    const handlers = this.listeners.get(event);
    const onceHandlers = [];

    // Call all handlers
    for (const { handler, once } of handlers) {
      try {
        handler(data);
      } catch (error) {
        console.error(`Error in event handler for ${event}:`, error);
      }

      if (once) {
        onceHandlers.push(handler);
      }
    }

    // Remove one-time handlers
    for (const handler of onceHandlers) {
      this.off(event, handler);
    }
  }

  /**
   * Get listener count for an event
   * @param {string} event - Event name
   * @returns {number} Listener count
   */
  listenerCount(event) {
    if (!this.listeners.has(event)) {
      return 0;
    }

    return this.listeners.get(event).length;
  }

  /**
   * Get all event names
   * @returns {Array<string>} Event names
   */
  eventNames() {
    return Array.from(this.listeners.keys());
  }

  /**
   * Get listeners for an event
   * @param {string} event - Event name
   * @returns {Array<Function>} Event handlers
   */
  listeners(event) {
    if (!this.listeners.has(event)) {
      return [];
    }

    return this.listeners.get(event).map((h) => h.handler);
  }
}

module.exports = EventEmitter;
