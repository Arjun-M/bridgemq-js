const crypto = require('crypto');

/**
 * Authentication - Server authentication
 * 
 * PURPOSE: Authenticate servers in mesh
 * 
 * FEATURES:
 * - Server signing
 * - Signature validation
 * - Secret management
 * - Token generation
 */
class Authentication {
  constructor(secret) {
    if (!secret) {
      throw new Error('Authentication secret is required');
    }
    this.secret = secret;
  }

  sign(data) {
    const payload = JSON.stringify(data);
    const signature = crypto
      .createHmac('sha256', this.secret)
      .update(payload)
      .digest('hex');

    return {
      payload,
      signature,
      timestamp: Date.now(),
    };
  }

  verify(signedData) {
    const { payload, signature, timestamp } = signedData;
    
    const expected = crypto
      .createHmac('sha256', this.secret)
      .update(payload)
      .digest('hex');

    if (!crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(signature))) {
      return false;
    }

    // Check timestamp (5 minute window)
    const now = Date.now();
    const age = now - timestamp;
    
    return age >= 0 && age < 300000;
  }

  generateToken() {
    return crypto.randomBytes(32).toString('hex');
  }
}

module.exports = Authentication;
