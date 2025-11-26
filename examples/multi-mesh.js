/**
 * Multi-Mesh Routing Example
 * 
 * Demonstrates:
 * - Cross-mesh communication
 * - Capability-based routing
 * - Region-aware job placement
 */

const { Client, Worker } = require('../../src');

async function setupProducer() {
  const producer = new Client({
    redis: { host: 'localhost', port: 6379 },
    meshId: 'mesh-us-east',
    serverId: 'producer-us',
  });

  await producer.init();

  // Create job with capability requirements
  const jobId = await producer.createJob({
    type: 'process-video',
    payload: { videoId: 'vid-123', resolution: '4K' },
    config: {
      target: {
        capabilities: ['video:ffmpeg', 'gpu:cuda'],
        region: 'us-west',
      },
    },
  });

  console.log('Job created with routing:', jobId);
}

async function setupWorkerUSWest() {
  const worker = new Worker({
    redis: { host: 'localhost', port: 6379 },
    meshId: 'mesh-us-west',
    serverId: 'worker-us-west-1',
    region: 'us-west',
    capabilities: ['video:ffmpeg', 'gpu:cuda'],
  });

  worker.registerHandler('process-video', async (job) => {
    console.log('[US-West] Processing video:', job.payload.videoId);
    // GPU-accelerated processing
    return { status: 'processed', outputUrl: 'https://...' };
  });

  await worker.start();
}

async function setupWorkerEurope() {
  const worker = new Worker({
    redis: { host: 'localhost', port: 6379 },
    meshId: 'mesh-eu-west',
    serverId: 'worker-eu-1',
    region: 'eu-west',
    capabilities: ['video:ffmpeg'],
  });

  worker.registerHandler('process-video', async (job) => {
    console.log('[Europe] Processing video:', job.payload.videoId);
    // CPU processing
    return { status: 'processed', outputUrl: 'https://...' };
  });

  await worker.start();
}

// Start everything
Promise.all([
  setupWorkerUSWest(),
  setupWorkerEurope(),
]).then(() => {
  setTimeout(setupProducer, 2000);
});
