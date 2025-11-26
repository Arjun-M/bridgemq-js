const cronParser = require('cron-parser');
const { v4: uuidv4 } = require('uuid');
const JobStorage = require('../storage/JobStorage');
const scripts = require('../scripts');

/**
 * CronScheduler - Schedule recurring jobs via cron expressions
 * 
 * PURPOSE: Create jobs on a recurring schedule using cron syntax
 * 
 * FEATURES:
 * - Standard cron expression parsing
 * - Multiple cron jobs
 * - Next-run calculation
 * - Automatic job creation
 * - Start/stop control
 * 
 * LOGIC:
 * 1. Parse cron expression
 * 2. Calculate next run time
 * 3. When time arrives, create job
 * 4. Calculate next occurrence
 * 5. Repeat indefinitely
 * 
 * CRON FORMAT:
 * ┌───────────── second (0-59, optional)
 * │ ┌───────────── minute (0-59)
 * │ │ ┌───────────── hour (0-23)
 * │ │ │ ┌───────────── day of month (1-31)
 * │ │ │ │ ┌───────────── month (1-12)
 * │ │ │ │ │ ┌───────────── day of week (0-7, 0 or 7 is Sunday)
 * │ │ │ │ │ │
 * * * * * * *
 * 
 * EXAMPLES:
 * '0 * * * *'      - Every hour at minute 0
 * '0 0 * * *'      - Every day at midnight
 * '*/5 * * * *'    - Every 5 minutes
 * '0 9 * * 1-5'    - Weekdays at 9am
 */
class CronScheduler {
  /**
   * Create cron scheduler
   * @param {Redis} redis - Redis client
   * @param {Object} options - Scheduler options
   */
  constructor(redis, options = {}) {
    this.redis = redis;
    this.options = {
      checkIntervalMs: options.checkIntervalMs || 1000, // Check every second
    };

    this.cronJobs = new Map();
    this.running = false;
    this.intervalId = null;
  }

  /**
   * Add a cron job
   * @param {string} name - Job name (unique identifier)
   * @param {string} cronExpression - Cron expression
   * @param {Object} jobTemplate - Job template (type, payload, config)
   * @returns {void}
   */
  addCronJob(name, cronExpression, jobTemplate) {
    try {
      // Parse and validate cron expression
      const interval = cronParser.parseExpression(cronExpression);
      
      this.cronJobs.set(name, {
        name,
        cronExpression,
        jobTemplate,
        interval,
        nextRun: interval.next().toDate(),
        lastRun: null,
      });
    } catch (error) {
      throw new Error(`Invalid cron expression: ${error.message}`);
    }
  }

  /**
   * Remove a cron job
   * @param {string} name - Job name
   * @returns {boolean} True if removed
   */
  removeCronJob(name) {
    return this.cronJobs.delete(name);
  }

  /**
   * Get all cron jobs
   * @returns {Array} Cron job list
   */
  getCronJobs() {
    return Array.from(this.cronJobs.values()).map((job) => ({
      name: job.name,
      cronExpression: job.cronExpression,
      nextRun: job.nextRun,
      lastRun: job.lastRun,
    }));
  }

  /**
   * Start the scheduler
   */
  start() {
    if (this.running) {
      return;
    }

    this.running = true;

    this.intervalId = setInterval(async () => {
      await this._checkAndRunJobs();
    }, this.options.checkIntervalMs);
  }

  /**
   * Stop the scheduler
   */
  stop() {
    if (!this.running) {
      return;
    }

    this.running = false;

    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
  }

  /**
   * Check and run due jobs (internal)
   * @private
   */
  async _checkAndRunJobs() {
    const now = new Date();

    for (const [name, cronJob] of this.cronJobs) {
      try {
        // Check if it's time to run
        if (now >= cronJob.nextRun) {
          await this._runCronJob(cronJob);
          
          // Calculate next run
          const interval = cronParser.parseExpression(cronJob.cronExpression);
          cronJob.nextRun = interval.next().toDate();
          cronJob.lastRun = now;
        }
      } catch (error) {
        console.error(`[CronScheduler] Error running cron job ${name}:`, error.message);
      }
    }
  }

  /**
   * Run a cron job (create job from template)
   * @private
   * @param {Object} cronJob - Cron job definition
   */
  async _runCronJob(cronJob) {
    const { jobTemplate } = cronJob;

    // Generate job ID
    const jobId = uuidv4();

    // Create job with template data
    await JobStorage.createJob(this.redis, scripts, {
      jobId,
      type: jobTemplate.type,
      version: jobTemplate.version || '1.0',
      payload: jobTemplate.payload,
      config: jobTemplate.config || {},
      meshId: jobTemplate.meshId || 'default',
    });

    console.log(`[CronScheduler] Created job ${jobId} from cron ${cronJob.name}`);
  }

  /**
   * Get next run time for a cron job
   * @param {string} name - Job name
   * @returns {Date|null} Next run time
   */
  getNextRun(name) {
    const cronJob = this.cronJobs.get(name);
    return cronJob ? cronJob.nextRun : null;
  }

  /**
   * Check if scheduler is running
   * @returns {boolean} Running status
   */
  isRunning() {
    return this.running;
  }
}

module.exports = CronScheduler;
