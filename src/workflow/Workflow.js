/**
 * Workflow - Multi-step workflow orchestration
 * 
 * PURPOSE: Manage complex multi-step workflows
 * 
 * FEATURES:
 * - Step management
 * - State tracking
 * - Failure handling
 * - Parallel execution
 * 
 * USAGE:
 * const workflow = new Workflow(client, 'order-workflow');
 * workflow
 *   .addStep('validate', { type: 'validate-order' })
 *   .addStep('charge', { type: 'charge-card', waitFor: ['validate'] })
 *   .addStep('ship', { type: 'ship-order', waitFor: ['charge'] })
 *   .execute({ orderId: '123' });
 */
class Workflow {
  constructor(client, name) {
    this.client = client;
    this.name = name;
    this.steps = new Map();
  }

  addStep(stepId, jobData) {
    this.steps.set(stepId, {
      stepId,
      jobData,
      status: 'pending',
      jobId: null,
    });
    return this;
  }

  async execute(context = {}) {
    const jobIds = {};

    for (const [stepId, step] of this.steps) {
      const payload = {
        ...step.jobData.payload,
        _workflow: {
          name: this.name,
          stepId,
          context,
        },
      };

      const jobId = await this.client.createJob({
        ...step.jobData,
        payload,
      });

      step.jobId = jobId;
      step.status = 'created';
      jobIds[stepId] = jobId;
    }

    return { workflowId: this.name, steps: jobIds };
  }

  async getStatus() {
    const status = {};

    for (const [stepId, step] of this.steps) {
      if (step.jobId) {
        const job = await this.client.getJob(step.jobId);
        status[stepId] = {
          jobId: step.jobId,
          status: job?.status || 'unknown',
        };
      } else {
        status[stepId] = { status: 'pending' };
      }
    }

    return status;
  }
}

module.exports = Workflow;
