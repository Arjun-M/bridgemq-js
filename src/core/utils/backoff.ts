/**
 * Backoff strategies for job retries
 */

export enum BackoffType {
  FIXED = 'fixed',
  EXPONENTIAL = 'exponential',
  LINEAR = 'linear',
}

export interface BackoffConfig {
  type: BackoffType;
  delay: number; // Base delay in ms
  maxDelay?: number; // Maximum delay cap
  jitter?: boolean; // Add randomness to prevent thundering herd
}

/**
 * Calculate retry delay based on backoff strategy
 */
export function calculateBackoff(
  attempt: number,
  config: BackoffConfig
): number {
  let delay: number;

  switch (config.type) {
    case BackoffType.FIXED:
      delay = config.delay;
      break;

    case BackoffType.EXPONENTIAL:
      // delay * 2^attempt
      delay = config.delay * Math.pow(2, attempt);
      break;

    case BackoffType.LINEAR:
      // delay * attempt
      delay = config.delay * (attempt + 1);
      break;

    default:
      delay = config.delay;
  }

  // Apply max delay cap
  if (config.maxDelay) {
    delay = Math.min(delay, config.maxDelay);
  }

  // Apply jitter (±25% randomness)
  if (config.jitter) {
    const jitterRange = delay * 0.25;
    delay += Math.random() * jitterRange * 2 - jitterRange;
  }

  return Math.floor(delay);
}

/**
 * Default backoff configurations
 */
export const DEFAULT_BACKOFF: BackoffConfig = {
  type: BackoffType.EXPONENTIAL,
  delay: 2000, // 2 seconds
  maxDelay: 60000, // 1 minute
  jitter: true,
};
