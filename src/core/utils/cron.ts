/**
 * Cron expression parsing and next execution time calculation
 * Uses cron-parser library for robust parsing
 */

import cronParser from 'cron-parser';

export interface CronOptions {
  expression: string;
  timezone?: string;
  startDate?: Date;
}

/**
 * Get the next execution timestamp for a cron expression
 */
export function getNextCronTimestamp(options: CronOptions): number {
  const { expression, timezone, startDate } = options;

  try {
    const interval = cronParser.parseExpression(expression, {
      currentDate: startDate || new Date(),
      tz: timezone,
    });

    const next = interval.next().toDate();
    return next.getTime();
  } catch (error) {
    throw new Error(`Invalid cron expression "${expression}": ${error}`);
  }
}

/**
 * Validate a cron expression
 */
export function isValidCron(expression: string): boolean {
  try {
    cronParser.parseExpression(expression);
    return true;
  } catch {
    return false;
  }
}

/**
 * Get the next N execution times for a cron expression
 */
export function getNextNCronTimestamps(
  options: CronOptions,
  count: number
): number[] {
  const { expression, timezone, startDate } = options;

  try {
    const interval = cronParser.parseExpression(expression, {
      currentDate: startDate || new Date(),
      tz: timezone,
    });

    const timestamps: number[] = [];
    for (let i = 0; i < count; i++) {
      timestamps.push(interval.next().toDate().getTime());
    }
    return timestamps;
  } catch (error) {
    throw new Error(`Invalid cron expression "${expression}": ${error}`);
  }
}
