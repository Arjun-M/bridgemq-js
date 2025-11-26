const { v4: uuidv4, v1: uuidv1 } = require('uuid');
const crypto = require('crypto');

/**
 * UUID - Generate unique identifiers
 * 
 * PURPOSE: Provide various UUID generation methods
 * 
 * TYPES:
 * - UUID v4: Random UUIDs (cryptographically secure)
 * - UUID v1: Time-based UUIDs (includes timestamp)
 * - Short ID: Base62 encoded (shorter representation)
 * - Custom: Prefixed UUIDs for namespacing
 * 
 * USAGE:
 * const jobId = UUID.v4();
 * const timeId = UUID.v1();
 * const shortId = UUID.short();
 * const customId = UUID.prefixed('job');
 */
class UUID {
  /**
   * Generate UUID v4 (random)
   * @returns {string} UUID v4
   */
  static v4() {
    return uuidv4();
  }

  /**
   * Generate UUID v1 (time-based)
   * @returns {string} UUID v1
   */
  static v1() {
    return uuidv1();
  }

  /**
   * Generate short ID (base62 encoded)
   * @param {number} length - Length of ID (default 8)
   * @returns {string} Short ID
   */
  static short(length = 8) {
    const chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
    let result = '';
    const bytes = crypto.randomBytes(length);

    for (let i = 0; i < length; i++) {
      result += chars[bytes[i] % chars.length];
    }

    return result;
  }

  /**
   * Generate prefixed UUID
   * @param {string} prefix - Prefix string
   * @returns {string} Prefixed UUID
   */
  static prefixed(prefix) {
    return `${prefix}_${uuidv4()}`;
  }

  /**
   * Generate numeric ID
   * @returns {string} Numeric ID (timestamp + random)
   */
  static numeric() {
    const timestamp = Date.now().toString();
    const random = Math.floor(Math.random() * 1000000).toString().padStart(6, '0');
    return `${timestamp}${random}`;
  }

  /**
   * Validate UUID v4 format
   * @param {string} id - ID to validate
   * @returns {boolean} True if valid UUID v4
   */
  static isValidV4(id) {
    const regex = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    return regex.test(id);
  }

  /**
   * Validate UUID v1 format
   * @param {string} id - ID to validate
   * @returns {boolean} True if valid UUID v1
   */
  static isValidV1(id) {
    const regex = /^[0-9a-f]{8}-[0-9a-f]{4}-1[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    return regex.test(id);
  }

  /**
   * Extract timestamp from UUID v1
   * @param {string} id - UUID v1
   * @returns {number|null} Timestamp or null
   */
  static extractTimestamp(id) {
    if (!this.isValidV1(id)) {
      return null;
    }

    const parts = id.split('-');
    const timeLow = parseInt(parts[0], 16);
    const timeMid = parseInt(parts[1], 16);
    const timeHi = parseInt(parts[2].substring(1), 16);

    const timestamp = (timeHi * Math.pow(2, 48)) + (timeMid * Math.pow(2, 32)) + timeLow;
    const unixTime = (timestamp - 0x01b21dd213814000) / 10000;

    return Math.floor(unixTime);
  }
}

module.exports = UUID;
