/**
 * Time and timestamp utilities
 */

export function nowMs(): number {
  return Date.now();
}

export function nowSec(): number {
  return Math.floor(Date.now() / 1000);
}

export function msToSec(ms: number): number {
  return Math.floor(ms / 1000);
}

export function secToMs(sec: number): number {
  return sec * 1000;
}

export function delayMs(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function futureTimestamp(delayMs: number): number {
  return nowMs() + delayMs;
}

export function isExpired(timestamp: number): boolean {
  return nowMs() >= timestamp;
}

export function timeUntil(timestamp: number): number {
  return Math.max(0, timestamp - nowMs());
}
