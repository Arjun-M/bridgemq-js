/**
 * Core type definitions for BridgeMQ
 */

export interface JobSchedule {
  delay?: number; // Delay in ms
  cron?: string; // Cron expression
  runAt?: number; // Specific timestamp
  timezone?: string; // Timezone for cron
}

export interface BackoffConfig {
  type: 'fixed' | 'exponential' | 'linear';
  delay: number; // Base delay in ms
  maxDelay?: number;
  jitter?: boolean;
}

export interface RetryConfig {
  attempts: number;
  backoff: BackoffConfig;
  skipOn?: string[]; // Error names to skip retry
  rerouteOn?: string[]; // Error names to reroute
}

export interface RateLimitConfig {
  maxConcurrent?: number;
  maxPerMinute?: number;
}

export interface JobTarget {
  stack?: string[];
  capabilities?: string[];
  region?: string[];
  server?: string; // Specific server ID
  mode?: 'any' | 'all';
}

export interface Job {
  jobId: string;
  type: string;
  payload: any;
  schedule: JobSchedule;
  retry: RetryConfig;
  priority: number; // 1-10, higher = more important
  dependsOn?: string[]; // Job dependencies
  target: JobTarget;
  metadata?: Record<string, any>;
  createdAt?: number;
  attempts?: number;
}

export enum JobStatus {
  WAITING = 'waiting',
  ACTIVE = 'active',
  COMPLETED = 'completed',
  FAILED = 'failed',
  DELAYED = 'delayed',
  CRON = 'cron',
  DEAD = 'dead',
}

export interface JobResult {
  success: boolean;
  data?: any;
  error?: {
    message: string;
    stack?: string;
    name?: string;
  };
  completedAt: number;
  processingTime: number; // ms
}

export interface WorkerMetadata {
  serverId: string;
  stack: string;
  region: string;
  capabilities: string[];
  lastSeen: number;
  pid?: number;
  hostname?: string;
}

export interface BridgeMQConfig {
  name?: string;
  serverId: string;
  stack: string;
  region: string;
  capabilities?: string[];
  mode?: 'worker' | 'producer' | 'both';
  redis: {
    host?: string;
    port?: number;
    username?: string;
    password?: string;
    db?: number;
    tls?: boolean | object;
  } | string; // Connection string
  namespace?: string;
  concurrency?: number; // Max concurrent jobs per worker
  lockTTL?: number; // Lock timeout in seconds
  heartbeatInterval?: number; // Heartbeat interval in ms
  schedulerInterval?: number; // Scheduler check interval in ms
}

export interface QueueStats {
  waiting: number;
  active: number;
  delayed: number;
  cron: number;
  dead: number;
  workers: number;
}

export type JobHandler = (job: Job) => Promise<any>;

export interface JobProgress {
  percent: number;
  message?: string;
  data?: any;
}
