import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';

export function loadConfig(filename, env = process.env) {
  const path = resolve(filename ?? 'config.json');
  const config = JSON.parse(readFileSync(path, 'utf8'));
  if (!config || Array.isArray(config) || typeof config !== 'object') throw new Error('Configuration must be a JSON object');
  const names = ['host', 'port', 'database', 'backend', 'pollIntervalMs', 'api', 'transport'];
  if (Object.keys(config).some(k => !names.includes(k))) throw new Error('Unknown top-level configuration key');
  const host = config.host ?? '127.0.0.1';
  const port = config.port ?? 48190;
  const pollIntervalMs = config.pollIntervalMs ?? 2000;
  if (typeof host !== 'string' || host.length > 255 || !Number.isInteger(port) || port < 1 || port > 65535) throw new Error('Invalid listen host or port');
  if (!Number.isInteger(pollIntervalMs) || pollIntervalMs < 250 || pollIntervalMs > 300000) throw new Error('pollIntervalMs must be between 250 and 300000');
  if (config.database != null && (typeof config.database !== 'string' || !config.database.length)) throw new Error('Invalid database path');
  if (!config.backend || typeof config.backend !== 'object' || Array.isArray(config.backend)) throw new Error('backend configuration is required');
  const backendNames = ['url', 'cookieFile', 'username', 'password', 'timeoutMs', 'maxResponseBytes'];
  if (Object.keys(config.backend).some(k => !backendNames.includes(k))) throw new Error('Unknown backend configuration key');
  const backend = { ...config.backend };
  if (backend.cookieFile) backend.cookieFile = resolve(dirname(path), backend.cookieFile);
  if (env.CONNECTCOIN_RPC_USER != null) backend.username = env.CONNECTCOIN_RPC_USER;
  if (env.CONNECTCOIN_RPC_PASSWORD != null) backend.password = env.CONNECTCOIN_RPC_PASSWORD;
  const api = config.api ?? {};
  if (config.transport != null && (typeof config.transport !== 'object' || Array.isArray(config.transport))) throw new Error('transport must be an object');
  const bounds = {
    pageSize: [1, 500], maxSubscriptionsPerIP: [1, 10000], maxSubscriptions: [1, 100000],
    maxJournalEvents: [1, 100000], maxJournalBytes: [1024, 128 * 1024 * 1024], maxTransactionBytes: [100, 4000000],
  };
  if (!api || typeof api !== 'object' || Array.isArray(api)) throw new Error('api must be an object');
  for (const [key, value] of Object.entries(api)) {
    if (!Object.hasOwn(bounds, key) || !Number.isSafeInteger(value) || value < bounds[key][0] || value > bounds[key][1]) throw new Error(`Invalid api option ${key}`);
  }
  return { host, port, pollIntervalMs, backend, api, transport: config.transport ?? {},
    database: resolve(dirname(path), config.database ?? 'data/index.sqlite') };
}
