# BridgeMQ

> Distributed task queue system with mesh architecture for Node.js

[![npm version](https://badge.fury.io/js/bridgemq.svg)](https://www.npmjs.com/package/bridgemq)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Features

- **Distributed Mesh Architecture** - Auto-discovering server network
- **Intelligent Routing** - Capability-based job routing
- **Retry Strategies** - Exponential/linear backoff with jitter
- **Workflow Engine** - Job chains, workflows, distributed transactions
- **Rate Limiting** - Token bucket algorithm
- **Observability** - Metrics, tracing, structured logging
- **Security** - AES-256-GCM encryption, HMAC authentication
- **Background Services** - Automatic cleanup, stall detection

## Installation

```bash
npm install bridgemq
```

## Quick Start

### Producer

```javascript
const { Client } = require('bridgemq');

const client = new Client({
  redis: { host: 'localhost', port: 6379 },
  meshId: 'my-mesh',
});

await client.init();

const jobId = await client.createJob({
  type: 'send-email',
  payload: { to: 'user@example.com', subject: 'Hello!' },
  config: {
    priority: 5,
    retry: { maxAttempts: 3 },
  },
});
```

### Consumer

```javascript
const { Worker } = require('bridgemq');

const worker = new Worker({
  redis: { host: 'localhost', port: 6379 },
  meshId: 'my-mesh',
  capabilities: ['email'],
});

worker.registerHandler('send-email', async (job) => {
  await sendEmail(job.payload);
  return { sent: true };
});

await worker.start();
```

## Core Concepts

### Mesh Architecture

Servers auto-register and discover each other:

```javascript
// Producer in US-East
const producer = new Client({ meshId: 'mesh-us-east' });

// Worker in US-West
const worker = new Worker({
  meshId: 'mesh-us-west',
  region: 'us-west',
  capabilities: ['video:ffmpeg', 'gpu:cuda'],
});
```

### Workflows

```javascript
const { Workflow } = require('bridgemq');

const workflow = new Workflow(client, 'order-fulfillment');
await workflow
  .addStep('validate', { type: 'validate-order' })
  .addStep('charge', { type: 'charge-card' })
  .addStep('ship', { type: 'ship-order' })
  .execute({ orderId: '123' });
```

### Retry Strategies

```javascript
await client.createJob({
  type: 'api-call',
  config: {
    retry: {
      maxAttempts: 5,
      strategy: 'exponential', // or 'linear'
      baseDelayMs: 1000,
      maxDelayMs: 60000,
    },
  },
});
```

## API Overview

### Client

- `createJob(jobData)` - Create new job
- `getJob(jobId)` - Get job status
- `cancelJob(jobId)` - Cancel job
- `getQueue(meshId)` - Get queue stats

### Worker

- `registerHandler(type, handler)` - Register job handler
- `start()` - Start processing
- `stop()` - Graceful shutdown

### Storage

- `JobStorage` - Job persistence
- `QueueStorage` - Queue operations
- `ServerStorage` - Server registry
- `MeshStorage` - Mesh management

## Advanced Features

### Rate Limiting

```javascript
const { RateLimiter } = require('bridgemq');
const limiter = new RateLimiter(redis);

await limiter.checkAndQueue('api-key', 100, 60); // 100 per minute
```

### Circuit Breaker

```javascript
const { CircuitBreaker } = require('bridgemq');
const breaker = new CircuitBreaker({ failureThreshold: 5 });

await breaker.execute(async () => {
  return await externalAPI.call();
});
```

### Monitoring

```javascript
const { Metrics } = require('bridgemq');
const metrics = new Metrics(redis, { meshId: 'prod' });

metrics.incrementCounter('jobs_processed');
metrics.recordProcessingTime('email', 250);

console.log(metrics.exportPrometheus());
```

## Benchmarks

- **Throughput**: 10,000+ jobs/sec per worker
- **Latency**: <5ms job creation, <10ms claim
- **Memory**: ~50MB per worker (idle)
- **Redis**: ~1KB per job

## Examples

See `/examples` directory:
- `basic/producer.js` - Simple job creation
- `basic/consumer.js` - Basic worker
- `advanced/workflow.js` - Complex workflows
- `advanced/multi-mesh.js` - Multi-region setup
- `real-world/email-service.js` - Production example

## Documentation

- [API Reference](./API_REFERENCE.md)
- [Contributing](./CONTRIBUTING.md)
- [Changelog](./CHANGELOG.md)

## License

MIT © Arjun-M
