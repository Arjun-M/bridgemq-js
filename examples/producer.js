/**
 * Basic Producer Example
 * 
 * Demonstrates:
 * - Client initialization
 * - Job creation
 * - Status monitoring
 */

const { Client } = require('../../src');

async function main() {
  // Initialize client
  const client = new Client({
    redis: {
      host: 'localhost',
      port: 6379,
    },
    meshId: 'example-mesh',
    serverId: 'producer-1',
  });

  await client.init();
  console.log('✓ Client initialized');

  // Create a simple job
  const jobId = await client.createJob({
    type: 'send-email',
    version: '1.0.0',
    payload: {
      to: 'user@example.com',
      subject: 'Welcome!',
      body: 'Thanks for signing up.',
    },
    config: {
      priority: 5,
      timeout: 30000, // 30 seconds
      retry: {
        maxAttempts: 3,
        strategy: 'exponential',
        baseDelayMs: 1000,
      },
    },
  });

  console.log(`✓ Job created: ${jobId}`);

  // Monitor job status
  const checkStatus = async () => {
    const job = await client.getJob(jobId);
    console.log(`Job status: ${job.status}`);

    if (job.status === 'completed') {
      console.log('✓ Job completed successfully!');
      console.log('Result:', job.result);
      clearInterval(interval);
      await client.shutdown();
    } else if (job.status === 'failed') {
      console.log('✗ Job failed');
      console.log('Errors:', job.errors);
      clearInterval(interval);
      await client.shutdown();
    }
  };

  const interval = setInterval(checkStatus, 1000);
}

main().catch(console.error);
