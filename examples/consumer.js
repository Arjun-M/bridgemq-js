/**
 * Basic Consumer Example
 * 
 * Demonstrates:
 * - Worker setup
 * - Handler registration
 * - Error handling
 */

const { Worker } = require('../../src');

async function main() {
  // Initialize worker
  const worker = new Worker({
    redis: {
      host: 'localhost',
      port: 6379,
    },
    meshId: 'example-mesh',
    serverId: 'worker-1',
    capabilities: ['email:sendgrid'],
  });

  // Register job handler
  worker.registerHandler('send-email', async (job) => {
    console.log(`Processing job ${job.jobId}`);
    console.log('Payload:', job.payload);

    try {
      // Simulate email sending
      const { to, subject, body } = job.payload;
      
      console.log(`Sending email to ${to}...`);
      await new Promise((resolve) => setTimeout(resolve, 2000));
      
      console.log('✓ Email sent successfully');

      // Return result
      return {
        messageId: `msg_${Date.now()}`,
        sentAt: new Date().toISOString(),
        recipient: to,
      };
    } catch (error) {
      console.error('✗ Failed to send email:', error.message);
      throw error;
    }
  });

  // Start worker
  await worker.start();
  console.log('✓ Worker started and waiting for jobs...');

  // Graceful shutdown
  process.on('SIGTERM', async () => {
    console.log('Shutting down worker...');
    await worker.stop();
    process.exit(0);
  });

  process.on('SIGINT', async () => {
    console.log('Shutting down worker...');
    await worker.stop();
    process.exit(0);
  });
}

main().catch(console.error);
