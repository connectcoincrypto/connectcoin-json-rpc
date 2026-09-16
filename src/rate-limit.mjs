import { isIP } from 'node:net';
import { performance } from 'node:perf_hooks';

/** Canonicalize the actual peer address, never a field supplied in a request. */
export function normalizeIp(address) {
  if (typeof address !== 'string') throw new TypeError('Invalid peer IP');
  const clean = address.split('%', 1)[0];
  const family = isIP(clean);
  if (family === 4) return clean;
  if (family !== 6) throw new TypeError('Invalid peer IP');
  // WHATWG URL serialization provides one spelling for equivalent IPv6 addresses.
  const canonical = new URL(`http://[${clean}]/`).hostname.slice(1, -1);
  const mapped = /^::ffff:([0-9a-f]{1,4}):([0-9a-f]{1,4})$/.exec(canonical);
  if (!mapped) return canonical;
  const high = Number.parseInt(mapped[1], 16);
  const low = Number.parseInt(mapped[2], 16);
  return `${high >>> 8}.${high & 255}.${low >>> 8}.${low & 255}`;
}

export function canonicalBlockHash(value) {
  return typeof value === 'string' && /^[0-9a-fA-F]{64}$/.test(value)
    ? value.toLowerCase() : null;
}

/** Sliding-window quotas shared by every socket, with bounded storage. */
export class RateLimiter {
  constructor({ windowMs = 60_000, methodLimit = 60, blockLimit = 10,
    invalidBlockLimit = 60, maxIps = 10_000, maxKeysPerIp = 1_024,
    maxKeys = 100_000, now = () => performance.now() } = {}) {
    for (const [key, value] of Object.entries({ windowMs, methodLimit, blockLimit,
      invalidBlockLimit, maxIps, maxKeysPerIp, maxKeys })) {
      if (!Number.isSafeInteger(value) || value <= 0) throw new TypeError(`Invalid ${key}`);
    }
    this.options = { windowMs, methodLimit, blockLimit, invalidBlockLimit,
      maxIps, maxKeysPerIp, maxKeys };
    this.now = now;
    this.ips = new Map();
    this.keyCount = 0;
    this.lastSweep = -Infinity;
  }

  prune(time = this.now()) {
    const cutoff = time - this.options.windowMs;
    for (const [ip, keys] of this.ips) {
      for (const [key, timestamps] of keys) {
        while (timestamps.length && timestamps[0] <= cutoff) timestamps.shift();
        if (!timestamps.length) { keys.delete(key); this.keyCount--; }
      }
      if (!keys.size) this.ips.delete(ip);
    }
    this.lastSweep = time;
  }

  /** Unknown/malformed requests must use one shared method name, not attacker keys. */
  consume({ ip, method, blockHash = null }) {
    ip = normalizeIp(ip);
    const time = this.now();
    if (time - this.lastSweep >= 1_000) this.prune(time);
    let key;
    let limit;
    if (method === 'getblockbounties') {
      const hash = canonicalBlockHash(blockHash);
      key = hash ? `block:${hash}` : 'invalid-block';
      limit = hash ? this.options.blockLimit : this.options.invalidBlockLimit;
    } else {
      // The allowlist, enforced by transport, keeps the method namespace bounded.
      key = `method:${method}`;
      limit = this.options.methodLimit;
    }
    let keys = this.ips.get(ip);
    let timestamps = keys?.get(key);
    const cutoff = time - this.options.windowMs;
    if (timestamps) {
      while (timestamps.length && timestamps[0] <= cutoff) timestamps.shift();
    }
    if (!timestamps) {
      if ((!keys && this.ips.size >= this.options.maxIps)
          || (keys && keys.size >= this.options.maxKeysPerIp)
          || this.keyCount >= this.options.maxKeys) {
        return { allowed: false, retryAfterMs: this.options.windowMs, reason: 'capacity' };
      }
      if (!keys) { keys = new Map(); this.ips.set(ip, keys); }
      timestamps = [];
      keys.set(key, timestamps);
      this.keyCount++;
    }
    if (timestamps.length >= limit) {
      return { allowed: false, retryAfterMs: Math.max(1, timestamps[0] + this.options.windowMs - time),
        reason: 'quota' };
    }
    timestamps.push(time);
    return { allowed: true, remaining: limit - timestamps.length, retryAfterMs: 0 };
  }

  get stats() { return { ips: this.ips.size, keys: this.keyCount }; }
}
