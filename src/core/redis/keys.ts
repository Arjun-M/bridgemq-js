/**
 * Redis key generation and management
 * All keys are namespaced to support multi-tenancy
 */

export interface KeyOptions {
  namespace?: string;
}

export class RedisKeys {
  private namespace: string;

  constructor(namespace: string = 'bridge') {
    this.namespace = namespace;
  }

  /**
   * Generate a namespaced key
   */
  private key(...parts: string[]): string {
    return [this.namespace, ...parts].join(':');
  }

  // ============ Worker Keys ============

  workerMetadata(serverId: string): string {
    return this.key('workers', serverId);
  }

  workerHeartbeat(serverId: string): string {
    return this.key('workers', 'heartbeat', serverId);
  }

  workersSet(): string {
    return this.key('workers', 'active');
  }

  // ============ Job Keys ============

  jobData(jobId: string): string {
    return this.key('jobs', 'data', jobId);
  }

  jobStatus(jobId: string): string {
    return this.key('jobs', 'status', jobId);
  }

  jobResult(jobId: string): string {
    return this.key('jobs', 'result', jobId);
  }

  jobLock(jobId: string): string {
    return this.key('jobs', 'lock', jobId);
  }

  jobProgress(jobId: string): string {
    return this.key('jobs', 'progress', jobId);
  }

  // ============ Queue Keys ============

  queueWaiting(): string {
    return this.key('jobs', 'waiting');
  }

  queueActive(): string {
    return this.key('jobs', 'active');
  }

  queueDelayed(): string {
    return this.key('jobs', 'delayed');
  }

  queueCron(): string {
    return this.key('jobs', 'cron');
  }

  queueDeadLetter(): string {
    return this.key('jobs', 'deadletter');
  }

  queuePaused(queueName?: string): string {
    if (queueName) {
      return this.key('queues', 'paused', queueName);
    }
    return this.key('queues', 'paused');
  }

  // ============ Rate Limiting Keys ============

  rateLimitConcurrent(jobType: string): string {
    return this.key('ratelimit', 'concurrent', jobType);
  }

  rateLimitMinute(jobType: string): string {
    return this.key('ratelimit', 'minute', jobType);
  }

  // ============ Metrics Keys ============

  metricsJobsProcessed(): string {
    return this.key('metrics', 'jobs', 'processed');
  }

  metricsJobsFailed(): string {
    return this.key('metrics', 'jobs', 'failed');
  }

  metricsProcessingTime(): string {
    return this.key('metrics', 'processing', 'time');
  }

  // ============ Scheduler Keys ============

  schedulerLock(): string {
    return this.key('scheduler', 'lock');
  }

  schedulerLeader(): string {
    return this.key('scheduler', 'leader');
  }

  // ============ Event Stream Keys ============

  eventStream(): string {
    return this.key('events', 'stream');
  }

  // ============ Helper Methods ============

  /**
   * Get all job keys for a specific job ID
   */
  allJobKeys(jobId: string): string[] {
    return [
      this.jobData(jobId),
      this.jobStatus(jobId),
      this.jobResult(jobId),
      this.jobLock(jobId),
      this.jobProgress(jobId),
    ];
  }

  /**
   * Extract job ID from a job data key
   */
  extractJobId(key: string): string | null {
    const prefix = this.key('jobs', 'data', '');
    if (key.startsWith(prefix)) {
      return key.substring(prefix.length);
    }
    return null;
  }

  /**
   * Get namespace
   */
  getNamespace(): string {
    return this.namespace;
  }
}
