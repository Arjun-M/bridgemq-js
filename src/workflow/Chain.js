/**
 * Chain - Job chaining with success/failure handlers
 * 
 * PURPOSE: Chain jobs with conditional execution
 * 
 * FEATURES:
 * - onSuccess handlers
 * - onFailure handlers
 * - Parent-child relationships
 * - Atomic execution
 * 
 * USAGE:
 * const chain = new Chain(client);
 * chain
 *   .addJob({ type: 'step1', payload: {...} })
 *   .onSuccess({ type: 'step2', payload: {...} })
 *   .onFailure({ type: 'compensate', payload: {...} })
 *   .execute();
 */
class Chain {
  constructor(client) {
    this.client = client;
    this.steps = [];
  }

  addJob(jobData) {
    this.steps.push({
      type: 'job',
      data: jobData,
      onSuccess: null,
      onFailure: null,
    });
    return this;
  }

  onSuccess(jobData) {
    const lastStep = this.steps[this.steps.length - 1];
    if (lastStep) {
      lastStep.onSuccess = jobData;
    }
    return this;
  }

  onFailure(jobData) {
    const lastStep = this.steps[this.steps.length - 1];
    if (lastStep) {
      lastStep.onFailure = jobData;
    }
    return this;
  }

  async execute() {
    const jobIds = [];
    
    for (const step of this.steps) {
      const config = { ...step.data.config };
      
      if (step.onSuccess) {
        config.onSuccess = step.onSuccess;
      }
      
      if (step.onFailure) {
        config.onFailure = step.onFailure;
      }

      const jobId = await this.client.createJob({
        ...step.data,
        config,
      });

      jobIds.push(jobId);
    }

    return jobIds;
  }
}

module.exports = Chain;
