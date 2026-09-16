import net from 'node:net';
import { randomUUID } from 'node:crypto';
import { TextDecoder } from 'node:util';
import { RpcError } from './errors.mjs';
import { RateLimiter, canonicalBlockHash, normalizeIp } from './rate-limit.mjs';

export const DEFAULT_TRANSPORT_OPTIONS = Object.freeze({
  maxConnections: 128,
  maxConnectionsPerIp: 8,
  maxConcurrentRequests: 32,
  maxPendingRequestsPerConnection: 16,
  maxQueuedInputBytes: 2 * 1024 * 1024,
  maxFrameBytes: 1024 * 1024,
  maxResponseBytes: 2 * 1024 * 1024,
  maxQueuedOutputBytes: 4 * 1024 * 1024,
  idleTimeoutMs: 120_000,
  frameTimeoutMs: 15_000,
  requestTimeoutMs: 60_000,
});

function publicError(error) {
  if (error instanceof RpcError) {
    return { code: error.code, message: error.message, ...(error.data === undefined ? {} : { data: error.data }) };
  }
  return { code: -32603, message: 'Internal error' };
}

function validId(id) {
  return (typeof id === 'string' && Buffer.byteLength(id) <= 128)
    || (typeof id === 'number' && Number.isSafeInteger(id));
}

/**
 * Newline-delimited JSON-RPC over native plaintext TCP. Batches and client
 * notifications are not part of this profile: every client request needs an id.
 * Dispatch may return a result or an async iterable, delivered using stream.*
 * notifications after a normal response containing the stream identifier.
 */
export function createRpcServer({ dispatch, allowedMethods, classifyBountyHash = () => null,
  limiter = new RateLimiter(), options = {} } = {}) {
  if (typeof dispatch !== 'function') throw new TypeError('dispatch is required');
  const methods = new Set(allowedMethods);
  if (!methods.size || [...methods].some((method) => typeof method !== 'string')) {
    throw new TypeError('An explicit allowedMethods list is required');
  }
  const limits = { ...DEFAULT_TRANSPORT_OPTIONS, ...options };
  for (const [name, value] of Object.entries(limits)) {
    if (!Object.hasOwn(DEFAULT_TRANSPORT_OPTIONS, name)
        || !Number.isSafeInteger(value) || value <= 0) throw new TypeError(`Invalid transport option ${name}`);
  }
  const sockets = new Set();
  const ipConnections = new Map();
  let activeRequests = 0;
  const server = net.createServer({ allowHalfOpen: false }, (socket) => {
    let ip;
    try { ip = normalizeIp(socket.remoteAddress); } catch { socket.destroy(); return; }
    const count = ipConnections.get(ip) ?? 0;
    if (sockets.size >= limits.maxConnections || count >= limits.maxConnectionsPerIp) {
      socket.destroy();
      return;
    }
    sockets.add(socket);
    ipConnections.set(ip, count + 1);
    socket.setNoDelay(true);
    socket.setTimeout(limits.idleTimeoutMs, () => socket.destroy());
    const abort = new AbortController();
    const cleanup = new Set();
    const queue = [];
    let queuedInputBytes = 0;
    let fragmentBuffer;
    let fragmentBytes = 0;
    let frameTimer;
    let processing = false;
    let drainWaiter;
    let closed = false;

    function endFrameTimer() { clearTimeout(frameTimer); frameTimer = undefined; }
    function startFrameTimer() {
      if (!frameTimer) frameTimer = setTimeout(() => socket.destroy(), limits.frameTimeoutMs).unref();
    }
    function waitForDrain() {
      if (socket.destroyed) return Promise.reject(new Error('Connection closed'));
      if (!drainWaiter) {
        let resolve;
        let reject;
        const promise = new Promise((ok, fail) => { resolve = ok; reject = fail; });
        drainWaiter = { promise, resolve, reject };
      }
      return drainWaiter.promise;
    }
    socket.on('drain', () => { const waiter = drainWaiter; drainWaiter = undefined; waiter?.resolve(); });
    socket.on('error', () => socket.destroy());
    socket.on('close', () => {
      if (closed) return;
      closed = true;
      endFrameTimer();
      abort.abort();
      sockets.delete(socket);
      const remaining = (ipConnections.get(ip) ?? 1) - 1;
      if (remaining) ipConnections.set(ip, remaining); else ipConnections.delete(ip);
      queue.length = 0;
      fragmentBuffer = undefined;
      queuedInputBytes = 0;
      fragmentBytes = 0;
      const waiter = drainWaiter;
      drainWaiter = undefined;
      waiter?.reject(new Error('Connection closed'));
      for (const fn of cleanup) { try { Promise.resolve(fn()).catch(() => {}); } catch { /* isolated cleanup */ } }
      cleanup.clear();
    });

    async function send(message) {
      if (socket.destroyed) throw new Error('Connection closed');
      const frame = Buffer.from(`${JSON.stringify(message)}\n`, 'utf8');
      if (frame.length > limits.maxResponseBytes
          || frame.length + socket.writableLength > limits.maxQueuedOutputBytes) {
        socket.destroy();
        throw new Error('Output limit exceeded');
      }
      if (!socket.write(frame)) await waitForDrain();
    }
    const context = Object.freeze({
      ip,
      signal: abort.signal,
      notify: (method, params) => send({ jsonrpc: '2.0', method, params }),
      onClose: (fn) => {
        if (typeof fn !== 'function') throw new TypeError('onClose requires a callback');
        if (closed) { try { Promise.resolve(fn()).catch(() => {}); } catch { /* isolated cleanup */ } }
        else cleanup.add(fn);
        return () => cleanup.delete(fn);
      },
    });

    function charge(method, blockHash = null) {
      const result = limiter.consume({ ip, method, blockHash });
      if (!result.allowed) throw new RpcError(-32029, 'Rate limit exceeded', {
        retry_after_ms: Math.ceil(result.retryAfterMs), reason: result.reason,
      });
    }

    async function handle(frame) {
      let request;
      let id = null;
      try {
        let text;
        try { text = new TextDecoder('utf-8', { fatal: true }).decode(frame); }
        catch { charge('__invalid_request'); throw new RpcError(-32700, 'Invalid UTF-8'); }
        try { request = JSON.parse(text); }
        catch { charge('__invalid_request'); throw new RpcError(-32700, 'Parse error'); }
        if (!request || typeof request !== 'object' || Array.isArray(request)
            || request.jsonrpc !== '2.0' || !validId(request.id)
            || typeof request.method !== 'string' || request.method.length > 128) {
          charge('__invalid_request');
          throw new RpcError(-32600, 'Invalid request: an id and JSON-RPC 2.0 object are required');
        }
        id = request.id;
        if (!methods.has(request.method)) {
          charge('__unknown_method');
          throw new RpcError(-32601, 'Method not found');
        }
        const params = request.params ?? {};
        const namedParams = params && typeof params === 'object' && !Array.isArray(params);
        if (request.method === 'getblockbounties') {
          const hash = namedParams ? canonicalBlockHash(await classifyBountyHash(params)) : null;
          charge(request.method, hash);
        } else charge(request.method);
        if (!namedParams || request.params === null) throw new RpcError(-32602, 'Only named object parameters are supported');
        if (activeRequests >= limits.maxConcurrentRequests) {
          throw new RpcError(-32030, 'Server busy', { retry_after_ms: 1_000 });
        }
        activeRequests++;
        const deadline = setTimeout(() => socket.destroy(), limits.requestTimeoutMs).unref();
        try {
          const result = await dispatch(request.method, params, context);
          if (result?.[Symbol.asyncIterator]) {
            const streamId = randomUUID();
            await send({ jsonrpc: '2.0', id, result: { stream_id: streamId } });
            deadline.refresh();
            let sequence = 0;
            try {
              for await (const chunk of result) {
                if (socket.destroyed) break;
                await context.notify('stream.chunk', { stream_id: streamId, sequence, items: chunk });
                // Bound stalled work, not the total duration of a progressing full list.
                deadline.refresh();
                sequence++;
              }
              if (!socket.destroyed) await context.notify('stream.end', { stream_id: streamId, complete: true, chunks: sequence });
            } catch (error) {
              if (!socket.destroyed) await context.notify('stream.end', { stream_id: streamId, complete: false,
                chunks: sequence, error: publicError(error) });
            }
          } else await send({ jsonrpc: '2.0', id, result: result ?? null });
        } finally {
          clearTimeout(deadline);
          activeRequests--;
        }
      } catch (error) {
        if (!socket.destroyed) await send({ jsonrpc: '2.0', id, error: publicError(error) });
      }
    }

    async function processQueue() {
      if (processing) return;
      processing = true;
      try {
        while (queue.length && !socket.destroyed) {
          const frame = queue.shift();
          queuedInputBytes -= frame.length;
          await handle(frame);
        }
      } catch { socket.destroy(); }
      finally { processing = false; }
    }

    socket.on('data', (data) => {
      let start = 0;
      while (start < data.length && !socket.destroyed) {
        const newline = data.indexOf(10, start);
        const end = newline === -1 ? data.length : newline;
        const part = data.subarray(start, end);
        if (fragmentBytes + part.length > limits.maxFrameBytes) { socket.destroy(); return; }
        if (part.length) {
          const required = fragmentBytes + part.length;
          if (!fragmentBuffer || required > fragmentBuffer.length) {
            const size = Math.min(limits.maxFrameBytes, Math.max(required, (fragmentBuffer?.length ?? 4096) * 2));
            const next = Buffer.allocUnsafe(size);
            if (fragmentBuffer) fragmentBuffer.copy(next, 0, 0, fragmentBytes);
            fragmentBuffer = next;
          }
          part.copy(fragmentBuffer, fragmentBytes);
          fragmentBytes += part.length;
          startFrameTimer();
        }
        if (newline === -1) { void processQueue(); return; }
        endFrameTimer();
        if (queue.length >= limits.maxPendingRequestsPerConnection
            || queuedInputBytes + fragmentBytes > limits.maxQueuedInputBytes) { socket.destroy(); return; }
        const frame = fragmentBuffer?.subarray(0, fragmentBytes) ?? Buffer.alloc(0);
        fragmentBuffer = undefined;
        fragmentBytes = 0;
        queue.push(frame);
        queuedInputBytes += frame.length;
        // Defer until the current data event is fully bounded and queued.
        start = newline + 1;
      }
      void processQueue();
    });
  });
  Object.defineProperty(server, 'stats', { get: () => ({ connections: sockets.size,
    ips: ipConnections.size, activeRequests, rateLimits: limiter.stats }) });
  server.closeAllConnections = () => { for (const socket of sockets) socket.destroy(); };
  const sweeper = setInterval(() => limiter.prune(), 15_000).unref();
  server.on('close', () => clearInterval(sweeper));
  return server;
}
