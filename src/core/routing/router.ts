import { Job, WorkerMetadata } from '../../types';

export class JobRouter {
  matchesTarget(job: Job, worker: WorkerMetadata): boolean {
    const target = job.target;
    if (!target) return true;

    // Priority 1: Server-specific
    if (target.server) {
      return target.server === worker.serverId;
    }

    // Priority 2: Stack/capabilities/region
    const mode = target.mode || 'any';
    const checks: boolean[] = [];

    if (target.stack && target.stack.length > 0) {
      checks.push(this.checkArray(target.stack, [worker.stack], mode));
    }

    if (target.capabilities && target.capabilities.length > 0) {
      checks.push(this.checkArray(target.capabilities, worker.capabilities, mode));
    }

    if (target.region && target.region.length > 0) {
      checks.push(this.checkArray(target.region, [worker.region], mode));
    }

    if (checks.length === 0) return true;

    return checks.every((c) => c);
  }

  private checkArray(required: string[], available: string[], mode: 'any' | 'all'): boolean {
    if (mode === 'any') {
      return required.some((r) => available.includes(r));
    } else {
      return required.every((r) => available.includes(r));
    }
  }
}
