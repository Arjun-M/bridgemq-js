/**
 * Template - Job template management
 * 
 * PURPOSE: Reusable job configurations and inheritance
 * 
 * FEATURES:
 * - Template storage and retrieval
 * - Config merging and inheritance
 * - Variable substitution
 * - Template validation
 * 
 * USAGE:
 * const template = new Template(redis);
 * 
 * // Save template
 * await template.save('email-template', {
 *   type: 'send-email',
 *   config: {
 *     priority: 5,
 *     retry: { maxAttempts: 3 }
 *   }
 * });
 * 
 * // Create job from template
 * const jobData = await template.apply('email-template', {
 *   payload: { to: 'user@example.com' }
 * });
 */
class Template {
  constructor(redis, options = {}) {
    this.redis = redis;
    this.options = {
      namespace: options.namespace || 'bridgemq',
    };
  }

  async save(name, template) {
    const key = `${this.options.namespace}:template:${name}`;
    await this.redis.set(key, JSON.stringify(template));
  }

  async get(name) {
    const key = `${this.options.namespace}:template:${name}`;
    const data = await this.redis.get(key);
    return data ? JSON.parse(data) : null;
  }

  async delete(name) {
    const key = `${this.options.namespace}:template:${name}`;
    await this.redis.del(key);
  }

  async list() {
    const pattern = `${this.options.namespace}:template:*`;
    const keys = await this.redis.keys(pattern);
    return keys.map((key) => key.split(':').pop());
  }

  async apply(name, overrides = {}) {
    const template = await this.get(name);
    
    if (!template) {
      throw new Error(`Template not found: ${name}`);
    }

    return this._merge(template, overrides);
  }

  _merge(template, overrides) {
    return {
      type: overrides.type || template.type,
      version: overrides.version || template.version,
      payload: { ...template.payload, ...overrides.payload },
      config: this._deepMerge(template.config || {}, overrides.config || {}),
    };
  }

  _deepMerge(target, source) {
    const result = { ...target };
    
    for (const key in source) {
      if (source[key] && typeof source[key] === 'object' && !Array.isArray(source[key])) {
        result[key] = this._deepMerge(result[key] || {}, source[key]);
      } else {
        result[key] = source[key];
      }
    }

    return result;
  }

  substituteVariables(template, variables) {
    const str = JSON.stringify(template);
    let result = str;

    for (const [key, value] of Object.entries(variables)) {
      const regex = new RegExp(`\\$\\{${key}\\}`, 'g');
      result = result.replace(regex, value);
    }

    return JSON.parse(result);
  }
}

module.exports = Template;
