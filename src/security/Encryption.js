const crypto = require('crypto');

/**
 * Encryption - Payload encryption/decryption
 * 
 * PURPOSE: Secure payload encryption for sensitive data
 * 
 * ALGORITHM: AES-256-GCM (authenticated encryption)
 * 
 * FEATURES:
 * - Strong encryption (AES-256)
 * - Authentication (GCM mode)
 * - Random IV per encryption
 * - Integrity protection
 */
class Encryption {
  constructor(key) {
    if (!key || key.length !== 32) {
      throw new Error('Encryption key must be 32 bytes');
    }
    this.key = Buffer.from(key);
  }

  encrypt(data) {
    const iv = crypto.randomBytes(16);
    const cipher = crypto.createCipheriv('aes-256-gcm', this.key, iv);
    
    const plaintext = JSON.stringify(data);
    let encrypted = cipher.update(plaintext, 'utf8', 'base64');
    encrypted += cipher.final('base64');
    
    const authTag = cipher.getAuthTag();

    return {
      ciphertext: encrypted,
      iv: iv.toString('base64'),
      authTag: authTag.toString('base64'),
    };
  }

  decrypt(encryptedData) {
    const { ciphertext, iv, authTag } = encryptedData;
    
    const decipher = crypto.createDecipheriv(
      'aes-256-gcm',
      this.key,
      Buffer.from(iv, 'base64'),
    );
    
    decipher.setAuthTag(Buffer.from(authTag, 'base64'));
    
    let decrypted = decipher.update(ciphertext, 'base64', 'utf8');
    decrypted += decipher.final('utf8');
    
    return JSON.parse(decrypted);
  }

  static generateKey() {
    return crypto.randomBytes(32);
  }
}

module.exports = Encryption;
