/**
 * Real-World Email Service Implementation
 * 
 * Demonstrates:
 * - Rate limiting
 * - Retry logic
 * - Template usage
 * - Monitoring
 */

const { Client, Worker, Template, RateLimiter, Metrics, Logger } = require('../../src');

class EmailService {
  constructor() {
    this.client = new Client({
      redis: { host: 'localhost', port: 6379 },
      meshId: 'email-mesh',
      serverId: 'email-producer',
    });

    this.worker = new Worker({
      redis: { host: 'localhost', port: 6379 },
      meshId: 'email-mesh',
      serverId: 'email-worker-1',
      capabilities: ['email:sendgrid'],
    });

    this.template = new Template(this.client.redis);
    this.rateLimiter = new RateLimiter(this.client.redis);
    this.metrics = new Metrics(this.client.redis, { meshId: 'email-mesh' });
    this.logger = new Logger({ level: 'info', format: 'json' });
  }

  async init() {
    await this.client.init();

    // Save email templates
    await this.template.save('welcome-email', {
      type: 'send-email',
      config: {
        priority: 7,
        retry: { maxAttempts: 3, strategy: 'exponential' },
      },
      payload: {
        subject: 'Welcome to {{appName}}!',
        template: 'welcome',
      },
    });

    // Register worker handler
    this.worker.registerHandler('send-email', async (job) => {
      const logger = this.logger.child({ jobId: job.jobId });
      
      try {
        // Check rate limit (100 emails per minute)
        const allowed = await this.rateLimiter.checkLimit('email-send', 100, 60);
        if (!allowed) {
          logger.warn('Rate limit exceeded');
          throw new Error('Rate limit exceeded');
        }

        // Send email (mock)
        logger.info('Sending email', {
          to: job.payload.to,
          subject: job.payload.subject,
        });

        await this._sendViaProvider(job.payload);

        // Record metrics
        this.metrics.incrementCounter('emails_sent', 1, {
          template: job.payload.template || 'none',
        });

        logger.info('Email sent successfully');

        return {
          messageId: `msg_${Date.now()}`,
          provider: 'sendgrid',
          sentAt: new Date().toISOString(),
        };
      } catch (error) {
        this.metrics.incrementCounter('emails_failed', 1);
        logger.error('Email failed', { error: error.message });
        throw error;
      }
    });

    await this.worker.start();
  }

  async _sendViaProvider(payload) {
    // Simulate sending via SendGrid
    await new Promise((resolve) => setTimeout(resolve, 500));
    // In production: await sendgrid.send(payload);
  }

  async sendWelcomeEmail(email, userName) {
    // Apply template with variables
    const jobData = await this.template.apply('welcome-email', {
      payload: {
        to: email,
        variables: {
          appName: 'MyApp',
          userName,
        },
      },
    });

    const jobId = await this.client.createJob(jobData);
    this.logger.info('Welcome email queued', { jobId, recipient: email });

    return jobId;
  }

  async shutdown() {
    await this.worker.stop();
    await this.client.shutdown();
  }
}

// Usage
async function main() {
  const emailService = new EmailService();
  await emailService.init();

  // Send welcome emails
  await emailService.sendWelcomeEmail('user1@example.com', 'Alice');
  await emailService.sendWelcomeEmail('user2@example.com', 'Bob');

  // Graceful shutdown after 30 seconds
  setTimeout(() => {
    emailService.shutdown();
  }, 30000);
}

main().catch(console.error);
