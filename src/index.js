/**
 * BridgeMQ - Distributed Task Queue System
 * 
 * Main SDK Entry Point
 */

// Core Classes
const Client = require('./core/Client');
const Worker = require('./core/Worker');
const Queue = require('./core/Queue');
const Job = require('./core/Job');

// Storage Layer
const JobStorage = require('./storage/JobStorage');
const QueueStorage = require('./storage/QueueStorage');
const ServerStorage = require('./storage/ServerStorage');
const MeshStorage = require('./storage/MeshStorage');
const MetricsStorage = require('./storage/MetricsStorage');

// Routing
const Router = require('./routing/Router');
const CapabilityMatcher = require('./routing/CapabilityMatcher');
const LoadBalancer = require('./routing/LoadBalancer');
const RegionRouter = require('./routing/RegionRouter');

// Retry System
const RetryStrategy = require('./retry/RetryStrategy');
const ExponentialBackoff = require('./retry/ExponentialBackoff');
const LinearBackoff = require('./retry/LinearBackoff');
const RetryManager = require('./retry/RetryManager');

// Advanced Features
const Idempotency = require('./features/Idempotency');
const RateLimiter = require('./features/RateLimiter');
const Batching = require('./features/Batching');
const CircuitBreaker = require('./features/CircuitBreaker');
const Template = require('./features/Template');

// Monitoring
const Metrics = require('./monitoring/Metrics');
const Tracer = require('./monitoring/Tracer');
const Logger = require('./monitoring/Logger');
const HealthCheck = require('./monitoring/HealthCheck');

// Background Services
const ProcessDelayed = require('./background/ProcessDelayed');
const StallDetector = require('./background/StallDetector');
const Cleaner = require('./background/Cleaner');
const MetricsAggregator = require('./background/MetricsAggregator');

// Scheduling
const DelayedScheduler = require('./scheduler/DelayedScheduler');
const CronScheduler = require('./scheduler/CronScheduler');
const DependencyResolver = require('./scheduler/DependencyResolver');
const PriorityQueue = require('./scheduler/PriorityQueue');

// Workflow Engine
const Chain = require('./workflow/Chain');
const Workflow = require('./workflow/Workflow');
const Transaction = require('./workflow/Transaction');
const DependencyGraph = require('./workflow/DependencyGraph');

// Security
const Encryption = require('./security/Encryption');
const Authentication = require('./security/Authentication');
const Signature = require('./security/Signature');

// Utilities
const UUID = require('./utils/UUID');
const Time = require('./utils/Time');
const Hash = require('./utils/Hash');
const Validation = require('./utils/Validation');
const { BridgeMQError, throwError } = require('./utils/Errors');

// Redis Connection
const RedisConnection = require('./redis/RedisConnection');

// Lua Scripts
const scripts = require('./scripts');

/**
 * Main BridgeMQ exports
 */
module.exports = {
  // Core
  Client,
  Worker,
  Queue,
  Job,

  // Storage
  JobStorage,
  QueueStorage,
  ServerStorage,
  MeshStorage,
  MetricsStorage,

  // Routing
  Router,
  CapabilityMatcher,
  LoadBalancer,
  RegionRouter,

  // Retry
  RetryStrategy,
  ExponentialBackoff,
  LinearBackoff,
  RetryManager,

  // Features
  Idempotency,
  RateLimiter,
  Batching,
  CircuitBreaker,
  Template,

  // Monitoring
  Metrics,
  Tracer,
  Logger,
  HealthCheck,

  // Background
  ProcessDelayed,
  StallDetector,
  Cleaner,
  MetricsAggregator,

  // Scheduling
  DelayedScheduler,
  CronScheduler,
  DependencyResolver,
  PriorityQueue,

  // Workflow
  Chain,
  Workflow,
  Transaction,
  DependencyGraph,

  // Security
  Encryption,
  Authentication,
  Signature,

  // Utilities
  UUID,
  Time,
  Hash,
  Validation,
  BridgeMQError,
  throwError,

  // Redis
  RedisConnection,

  // Scripts
  scripts,
};
