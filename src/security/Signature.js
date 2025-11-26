const crypto = require('crypto');

/**
 * Signature - Data signing and verification
 * 
 * PURPOSE: Sign data and verify signatures
 * 
 * FEATURES:
 * - HMAC-SHA256 signing
 * - Constant-time comparison
 * - Replay protection
 * - Nonce support
 */
class Signature {
  static sign(data, secret, nonce = null) {
    const payload = JSON.stringify({ data, nonce, timestamp: Date.now() });
    const signature = crypto
      .createHmac('sha256', secret)
      .update(payload)
      .digest('hex');

    return { payload, signature };
  }

  static verify(signedData, secret, maxAge = 300000) {
    const { payload, signature } = signedData;
    
    const expected = crypto
      .createHmac('sha256', secret)
      .update(payload)
      .digest('hex');

    if (!crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(signature))) {
      return { valid: false, reason: 'Invalid signature' };
    }

    const parsed = JSON.parse(payload);
    const age = Date.now() - parsed.timestamp;

    if (age < 0 || age > maxAge) {
      return { valid: false, reason: 'Expired signature' };
    }

    return { valid: true, data: parsed.data, nonce: parsed.nonce };
  }

  static generateNonce() {
    return crypto.randomBytes(16).toString('hex');
  }
}

module.exports = Signature;
