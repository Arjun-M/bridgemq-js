/**
 * Transaction - Distributed transaction management
 * 
 * PURPOSE: Saga pattern for distributed transactions
 * 
 * FEATURES:
 * - Saga pattern
 * - Compensation actions
 * - Rollback handling
 * - Failure recovery
 */
class Transaction {
  constructor(client) {
    this.client = client;
    this.actions = [];
  }

  addAction(action, compensation) {
    this.actions.push({ action, compensation });
    return this;
  }

  async execute() {
    const executed = [];
    
    try {
      for (const { action } of this.actions) {
        const jobId = await this.client.createJob(action);
        executed.push(jobId);
      }

      return { success: true, jobIds: executed };
    } catch (error) {
      await this._rollback(executed);
      return { success: false, error: error.message };
    }
  }

  async _rollback(executed) {
    for (let i = executed.length - 1; i >= 0; i--) {
      const compensation = this.actions[i].compensation;
      if (compensation) {
        try {
          await this.client.createJob(compensation);
        } catch (error) {
          console.error('Compensation failed:', error);
        }
      }
    }
  }
}

module.exports = Transaction;
