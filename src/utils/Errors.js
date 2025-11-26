/**
 * Errors - Custom error classes with error codes
 * 
 * PURPOSE: Structured error handling with taxonomy
 * 
 * ERROR CODE TAXONOMY:
 * 1xxx: Validation errors
 * 2xxx: Job lifecycle errors
 * 3xxx: Worker errors
 * 4xxx: Routing errors
 * 5xxx: Rate limiting errors
 * 6xxx: Dependency errors
 * 7xxx: Workflow errors
 * 8xxx: Security errors
 * 9xxx: Storage errors
 */

class BridgeMQError extends Error {
  constructor(code, type, context = {}) {
    super(context.message || type);
    this.name = 'BridgeMQError';
    this.code = code;
    this.type = type;
    this.context = context;
    Error.captureStackTrace(this, this.constructor);
  }

  toJSON() {
    return {
      name: this.name,
      code: this.code,
      type: this.type,
      message: this.message,
      context: this.context,
    };
  }
}

function throwError(code, type, context) {
  throw new BridgeMQError(code, type, context);
}

module.exports = {
  BridgeMQError,
  throwError,
};
