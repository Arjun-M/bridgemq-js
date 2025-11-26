/**
 * DependencyGraph - Manage complex job dependencies
 * 
 * PURPOSE: Graph operations for job dependencies
 * 
 * FEATURES:
 * - Graph operations
 * - Cycle detection
 * - Readiness checking
 * - Topological sort
 */
class DependencyGraph {
  constructor() {
    this.nodes = new Map();
    this.edges = new Map();
  }

  addNode(nodeId, data = {}) {
    this.nodes.set(nodeId, data);
    if (!this.edges.has(nodeId)) {
      this.edges.set(nodeId, []);
    }
  }

  addEdge(from, to) {
    if (!this.edges.has(from)) {
      this.edges.set(from, []);
    }
    this.edges.get(from).push(to);
  }

  hasCycle() {
    const visited = new Set();
    const recStack = new Set();

    for (const node of this.nodes.keys()) {
      if (this._hasCycleUtil(node, visited, recStack)) {
        return true;
      }
    }

    return false;
  }

  _hasCycleUtil(node, visited, recStack) {
    if (recStack.has(node)) {
      return true;
    }

    if (visited.has(node)) {
      return false;
    }

    visited.add(node);
    recStack.add(node);

    const neighbors = this.edges.get(node) || [];
    for (const neighbor of neighbors) {
      if (this._hasCycleUtil(neighbor, visited, recStack)) {
        return true;
      }
    }

    recStack.delete(node);
    return false;
  }

  topologicalSort() {
    const visited = new Set();
    const stack = [];

    for (const node of this.nodes.keys()) {
      if (!visited.has(node)) {
        this._topologicalSortUtil(node, visited, stack);
      }
    }

    return stack.reverse();
  }

  _topologicalSortUtil(node, visited, stack) {
    visited.add(node);

    const neighbors = this.edges.get(node) || [];
    for (const neighbor of neighbors) {
      if (!visited.has(neighbor)) {
        this._topologicalSortUtil(neighbor, visited, stack);
      }
    }

    stack.push(node);
  }

  getDependencies(nodeId) {
    return this.edges.get(nodeId) || [];
  }

  isReady(nodeId, completed) {
    const dependencies = this.getDependencies(nodeId);
    return dependencies.every((dep) => completed.has(dep));
  }
}

module.exports = DependencyGraph;
