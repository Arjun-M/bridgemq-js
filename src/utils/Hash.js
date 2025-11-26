const crypto = require('crypto');

/**
 * Hash - Hashing and fingerprint generation
 * 
 * PURPOSE: Generate hashes and fingerprints for deduplication
 * 
 * FEATURES:
 * - SHA1/SHA256/MD5 hashing
 * - HMAC generation
 * - Deduplication fingerprints
 * - Content-based hashing
 * 
 * USAGE:
 * const hash = Hash.sha256('data');
 * const hmac = Hash.hmac('data', 'secret');
 * const fingerprint = Hash.fingerprint(jobPayload);
 */
class Hash {
  /**
   * Generate SHA256 hash
   * @param {string|Buffer} data - Data to hash
   * @returns {string} Hex-encoded hash
   */
  static sha256(data) {
    return crypto.createHash('sha256').update(data).digest('hex');
  }

  /**
   * Generate SHA1 hash
   * @param {string|Buffer} data - Data to hash
   * @returns {string} Hex-encoded hash
   */
  static sha1(data) {
    return crypto.createHash('sha1').update(data).digest('hex');
  }

  /**
   * Generate MD5 hash
   * @param {string|Buffer} data - Data to hash
   * @returns {string} Hex-encoded hash
   */
  static md5(data) {
    return crypto.createHash('md5').update(data).digest('hex');
  }

  /**
   * Generate HMAC-SHA256
   * @param {string|Buffer} data - Data to sign
   * @param {string} secret - Secret key
   * @returns {string} Hex-encoded HMAC
   */
  static hmac(data, secret) {
    return crypto.createHmac('sha256', secret).update(data).digest('hex');
  }

  /**
   * Generate fingerprint for object (for deduplication)
   * @param {any} obj - Object to fingerprint
   * @returns {string} SHA256 fingerprint
   */
  static fingerprint(obj) {
    const str = JSON.stringify(obj, Object.keys(obj).sort());
    return this.sha256(str);
  }

  /**
   * Generate short hash (first N characters)
   * @param {string|Buffer} data - Data to hash
   * @param {number} length - Hash length (default 8)
   * @returns {string} Short hash
   */
  static shortHash(data, length = 8) {
    return this.sha256(data).substring(0, length);
  }

  /**
   * Verify HMAC
   * @param {string|Buffer} data - Data
   * @param {string} signature - HMAC signature
   * @param {string} secret - Secret key
   * @returns {boolean} True if valid
   */
  static verifyHmac(data, signature, secret) {
    const expected = this.hmac(data, secret);
    return crypto.timingSafeEqual(
      Buffer.from(expected),
      Buffer.from(signature),
    );
  }

  /**
   * Generate random hex string
   * @param {number} bytes - Number of bytes (default 32)
   * @returns {string} Random hex string
   */
  static random(bytes = 32) {
    return crypto.randomBytes(bytes).toString('hex');
  }
}

module.exports = Hash;
