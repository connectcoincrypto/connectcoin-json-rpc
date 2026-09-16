import http from 'node:http';
import { readFile } from 'node:fs/promises';

const ALLOWED_METHODS = new Set([
  'getblockchaininfo', 'getblockhash', 'getblock', 'getrawmempool',
  'getrawtransaction', 'sendrawtransaction',
]);
const AMOUNT_KEYS = new Set(['value', 'fee', 'amount', 'base', 'modified', 'ancestor', 'descendant', 'mempool_sequence']);

// Node 24 supplies the original numeric token to the reviver. Never recover a
// monetary value from an already-rounded binary floating-point number.
export function parseNodeJson(text) {
  return JSON.parse(text, (key, value, context) => {
    if (typeof value === 'number' && AMOUNT_KEYS.has(key)) {
      if (!context?.source) throw new Error('Node.js 24 or newer is required for exact RPC amounts');
      return context.source;
    }
    return value;
  });
}

export function toConnects(value) {
  if (typeof value !== 'string') throw new Error('RPC monetary values must be lossless decimal strings');
  const match = /^(-?)(\d+)(?:\.(\d+))?(?:[eE]([+-]?\d+))?$/.exec(value);
  if (!match) throw new Error('Invalid RPC monetary value');
  const exponent = Number(match[4] ?? 0);
  if (!Number.isInteger(exponent) || Math.abs(exponent) > 100) throw new Error('RPC amount exponent outside range');
  const digits = match[2] + (match[3] ?? '');
  const places = 10 + exponent - (match[3]?.length ?? 0);
  let amount;
  if (places >= 0) amount = BigInt(digits) * (10n ** BigInt(places));
  else {
    const divisor = 10n ** BigInt(-places);
    if (BigInt(digits) % divisor !== 0n) throw new Error('RPC amount has sub-connect precision');
    amount = BigInt(digits) / divisor;
  }
  if (match[1]) amount = -amount;
  if (amount < 0n || amount > 1_000_000_000_000_000_000n) throw new Error('RPC output amount outside MoneyRange');
  return amount.toString();
}

export class BackendError extends Error {
  constructor(message, code = null) { super(message); this.name = 'BackendError'; this.code = code; }
}

export class NodeBackend {
  constructor({ url = 'http://127.0.0.1:48178/', cookieFile, username, password,
    timeoutMs = 30_000, maxResponseBytes = 256 * 1024 * 1024 } = {}) {
    const parsed = new URL(url);
    if (parsed.protocol !== 'http:' || !['127.0.0.1', '[::1]'].includes(parsed.hostname)
      || parsed.username || parsed.password || parsed.search || parsed.hash || parsed.pathname !== '/') {
      throw new Error('Backend must be a plain HTTP loopback IP URL with path / and no embedded credentials');
    }
    if (!cookieFile && (typeof username !== 'string' || typeof password !== 'string')) {
      throw new Error('Provide backend cookieFile or username and password');
    }
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || !Number.isSafeInteger(maxResponseBytes) || maxResponseBytes < 1) {
      throw new Error('Invalid backend resource limits');
    }
    this.url = parsed;
    this.cookieFile = cookieFile;
    this.username = username;
    this.password = password;
    this.timeoutMs = timeoutMs;
    this.maxResponseBytes = maxResponseBytes;
    this.agent = new http.Agent({ keepAlive: true, maxSockets: 4 });
    this.nextId = 1;
  }

  async call(method, params = []) {
    if (!ALLOWED_METHODS.has(method)) throw new BackendError('Backend method is not allowlisted');
    const credentials = this.cookieFile
      ? (await readFile(this.cookieFile, 'utf8')).trim()
      : `${this.username}:${this.password}`;
    if (!credentials.includes(':') || /[\r\n]/.test(credentials)) throw new BackendError('Invalid backend credentials');
    const id = this.nextId++;
    const body = JSON.stringify({ jsonrpc: '2.0', id, method, params });
    return new Promise((resolve, reject) => {
      let settled = false;
      const finish = (error, result) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        if (error) reject(error); else resolve(result);
      };
      const request = http.request(this.url, {
        method: 'POST', agent: this.agent,
        headers: { Authorization: `Basic ${Buffer.from(credentials).toString('base64')}`,
          'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
      }, response => {
        const chunks = [];
        let length = 0;
        response.on('data', chunk => {
          length += chunk.length;
          if (length > this.maxResponseBytes) {
            const error = new BackendError('Backend response exceeds size limit');
            response.destroy(error); request.destroy(error); finish(error); return;
          }
          chunks.push(chunk);
        });
        response.on('error', () => finish(new BackendError('Backend response interrupted')));
        response.on('end', () => {
          if (settled) return;
          let payload;
          try { payload = parseNodeJson(Buffer.concat(chunks, length).toString('utf8')); }
          catch { finish(new BackendError(`Invalid backend response (HTTP ${response.statusCode})`)); return; }
          if (!payload || payload.id !== id) { finish(new BackendError('Backend response ID mismatch')); return; }
          if (payload.error) {
            // Do not forward arbitrary backend diagnostics (paths, credentials,
            // or local operator information) to the public protocol.
            finish(new BackendError('Backend RPC rejected request', payload.error.code)); return;
          }
          if (response.statusCode !== 200 || !Object.hasOwn(payload, 'result')) {
            finish(new BackendError(`Backend HTTP error ${response.statusCode}`)); return;
          }
          finish(null, payload.result);
        });
      });
      const timer = setTimeout(() => {
        const error = new BackendError('Backend request timed out');
        request.destroy(error); finish(error);
      }, this.timeoutMs);
      request.on('error', () => finish(new BackendError('Backend connection failed')));
      request.end(body);
    });
  }

  transaction(txid, blockHash) {
    return this.call('getrawtransaction', blockHash ? [txid, true, blockHash] : [txid, true]);
  }

  close() { this.agent.destroy(); }
}
