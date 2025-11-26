const { throwError } = require('./Errors');

/**
 * Validation - Input validation and schema checking
 * 
 * PURPOSE: Validate job data, config, and user input
 * 
 * VALIDATES:
 * - Job type format
 * - Payload structure
 * - Config parameters
 * - Priority range
 * - Timeouts
 * 
 * ERROR CODES:
 * - 1001: INVALID_PAYLOAD
 * - 1002: INVALID_CONFIG
 * - 1003: INVALID_JOB_TYPE
 */
class Validation {
  static validateJobType(type) {
    if (!type || typeof type !== 'string') {
      throwError(1003, 'INVALID_JOB_TYPE', { message: 'Job type must be a non-empty string' });
    }

    if (type.length > 100) {
      throwError(1003, 'INVALID_JOB_TYPE', { message: 'Job type must be <= 100 characters' });
    }

    if (!/^[a-zA-Z0-9_-]+$/.test(type)) {
      throwError(1003, 'INVALID_JOB_TYPE', { message: 'Job type must contain only alphanumeric, underscore, or dash' });
    }
  }

  static validatePayload(payload) {
    if (payload === undefined || payload === null) {
      throwError(1001, 'INVALID_PAYLOAD', { message: 'Payload is required' });
    }

    try {
      JSON.stringify(payload);
    } catch (error) {
      throwError(1001, 'INVALID_PAYLOAD', { message: 'Payload must be JSON-serializable' });
    }
  }

  static validateConfig(config) {
    if (config.priority !== undefined) {
      if (typeof config.priority !== 'number' || config.priority < 1 || config.priority > 10) {
        throwError(1002, 'INVALID_CONFIG', { message: 'Priority must be a number between 1 and 10' });
      }
    }

    if (config.timeout !== undefined) {
      if (typeof config.timeout !== 'number' || config.timeout < 0) {
        throwError(1002, 'INVALID_CONFIG', { message: 'Timeout must be a positive number' });
      }
    }

    if (config.delay !== undefined) {
      if (typeof config.delay !== 'number' || config.delay < 0) {
        throwError(1002, 'INVALID_CONFIG', { message: 'Delay must be a positive number' });
      }
    }
  }

  static validateJobId(jobId) {
    if (!jobId || typeof jobId !== 'string') {
      throwError(1001, 'INVALID_PAYLOAD', { message: 'Job ID must be a non-empty string' });
    }
  }

  static validateMeshId(meshId) {
    if (!meshId || typeof meshId !== 'string') {
      throwError(1002, 'INVALID_CONFIG', { message: 'Mesh ID must be a non-empty string' });
    }
  }
}

module.exports = Validation;
