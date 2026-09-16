import test from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import { once } from 'node:events';
import { createRpcServer } from '../src/transport.mjs';
import { RateLimiter } from '../src/rate-limit.mjs';
import { RpcError } from '../src/errors.mjs';

async function fixture(t, config = {}) {
  const server = createRpcServer({ allowedMethods: ['echo', 'getblockbounties', 'subscribe'],
    dispatch: async (_method, params) => params, ...config });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const clients = new Set();
  t.after(async () => {
    for (const client of clients) client.destroy();
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  });
  async function connect() {
    const socket = net.connect(server.address().port, '127.0.0.1');
    clients.add(socket);
    socket.on('error', () => {});
    let buffer = '';
    const messages = [];
    const waiters = [];
    socket.on('data', (data) => {
      buffer += data.toString('utf8');
      let newline;
      while ((newline = buffer.indexOf('\n')) !== -1) {
        const message = JSON.parse(buffer.slice(0, newline));
        buffer = buffer.slice(newline + 1);
        const waiter = waiters.shift();
        if (waiter) waiter(message); else messages.push(message);
      }
    });
    await once(socket, 'connect');
    const receive = () => messages.length ? Promise.resolve(messages.shift())
      : new Promise((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('Message timed out')), 2500);
        waiters.push((message) => { clearTimeout(timeout); resolve(message); });
      });
    return { socket, receive, request: async (method, params = {}, id = 1) => {
      socket.write(`${JSON.stringify({ jsonrpc: '2.0', id, method, params })}\n`);
      return receive();
    } };
  }
  return { server, connect };
}

test('TCP handles split UTF8 frames, multiple frames and complete frame followed by partial', async (t) => {
  const { connect } = await fixture(t);
  const client = await connect();
  const line = Buffer.from(`${JSON.stringify({ jsonrpc: '2.0', id: '1', method: 'echo', params: { text: 'olá' } })}\n`);
  const split = line.indexOf(Buffer.from('á')) + 1;
  client.socket.write(line.subarray(0, split));
  client.socket.write(line.subarray(split));
  assert.deepEqual((await client.receive()).result, { text: 'olá' });
  const first = JSON.stringify({ jsonrpc: '2.0', id: 2, method: 'echo' });
  const second = JSON.stringify({ jsonrpc: '2.0', id: 3, method: 'echo' });
  client.socket.write(`${first}\n${second.slice(0, 10)}`);
  assert.equal((await client.receive()).id, 2);
  client.socket.write(`${second.slice(10)}\n`);
  assert.equal((await client.receive()).id, 3);
});

test('rejects batches, no-id calls, positional params, malformed JSON and invalid UTF8', async (t) => {
  let dispatches = 0;
  const { connect } = await fixture(t, { dispatch: () => { dispatches++; } });
  const client = await connect();
  for (const [frame, code] of [
    ['[]\n', -32600], ['{"jsonrpc":"2.0","method":"echo"}\n', -32600],
    ['{"jsonrpc":"2.0","id":1,"method":"echo","params":[]}\n', -32602],
    ['{"jsonrpc":"2.0","id":1,"method":"echo","params":null}\n', -32602],
    ['bad json\n', -32700], [Buffer.from([0xc0, 0xaf, 0x0a]), -32700],
  ]) {
    client.socket.write(frame);
    assert.equal((await client.receive()).error.code, code);
  }
  assert.equal(dispatches, 0);
});

test('method quota survives reconnects and ignores spoofed IP params', async (t) => {
  const limiter = new RateLimiter({ methodLimit: 2 });
  const { connect } = await fixture(t, { limiter });
  const first = await connect();
  assert.equal((await first.request('echo', { ip: '1.1.1.1' })).error, undefined);
  assert.equal((await first.request('echo', { ip: '2.2.2.2' })).error, undefined);
  first.socket.destroy();
  const second = await connect();
  const rejected = await second.request('echo');
  assert.equal(rejected.error.code, -32029);
  assert.ok(rejected.error.data.retry_after_ms > 0);
});

test('block quotas canonicalize hashes and invalid/out-of-window hashes share a bucket', async (t) => {
  const current = 'a'.repeat(64);
  const other = 'b'.repeat(64);
  const limiter = new RateLimiter({ blockLimit: 2, invalidBlockLimit: 2 });
  const { connect } = await fixture(t, { limiter, classifyBountyHash: ({ block_hash: hash }) =>
    [current, other].includes(hash?.toLowerCase()) ? hash : null });
  const client = await connect();
  assert.ok((await client.request('getblockbounties', { block_hash: current })).result);
  assert.ok((await client.request('getblockbounties', { block_hash: current.toUpperCase() })).result);
  assert.equal((await client.request('getblockbounties', { block_hash: current })).error.code, -32029);
  assert.ok((await client.request('getblockbounties', { block_hash: other })).result);
  assert.ok((await client.request('getblockbounties', { block_hash: 'c'.repeat(64) })).result);
  assert.ok((await client.request('getblockbounties', { block_hash: 'not-a-hash' })).result);
  assert.equal((await client.request('getblockbounties', { block_hash: 'd'.repeat(64) })).error.code, -32029);
});

test('unknown method names share bounded quota and never dispatch', async (t) => {
  const limiter = new RateLimiter({ methodLimit: 2 });
  const { connect } = await fixture(t, { limiter });
  const client = await connect();
  assert.equal((await client.request('stop')).error.code, -32601);
  assert.equal((await client.request('walletpassphrase')).error.code, -32601);
  assert.equal((await client.request('getblocktemplate')).error.code, -32029);
  assert.equal(limiter.stats.keys, 1);
});

test('stream sends every chunk followed by explicit completion', async (t) => {
  const { connect } = await fixture(t, { dispatch: async function* () {
    for (let n = 0; n < 125; n++) yield { n };
  } });
  const client = await connect();
  const initial = await client.request('getblockbounties');
  assert.ok(initial.result.stream_id);
  for (let n = 0; n < 125; n++) {
    const chunk = await client.receive();
    assert.equal(chunk.method, 'stream.chunk');
    assert.equal(chunk.params.stream_id, initial.result.stream_id);
    assert.equal(chunk.params.sequence, n);
    assert.deepEqual(chunk.params.items, { n });
  }
  const end = await client.receive();
  assert.equal(end.method, 'stream.end');
  assert.equal(end.params.complete, true);
  assert.equal(end.params.chunks, 125);
});

test('stream error is explicitly incomplete and internal errors are sanitized', async (t) => {
  const { connect } = await fixture(t, { dispatch: async function* () {
    yield ['first'];
    throw new Error('password=do-not-expose');
  } });
  const client = await connect();
  await client.request('getblockbounties');
  await client.receive();
  const end = await client.receive();
  assert.equal(end.params.complete, false);
  assert.deepEqual(end.params.error, { code: -32603, message: 'Internal error' });
});

test('RPC errors preserve safe protocol details; disconnect runs cleanup and aborts', async (t) => {
  let closeResolve;
  const closed = new Promise((resolve) => { closeResolve = resolve; });
  let context;
  const { connect } = await fixture(t, { dispatch: async (_method, _params, ctx) => {
    context = ctx;
    ctx.onClose(closeResolve);
    throw new RpcError(-32602, 'Expected address', { field: 'address' });
  } });
  const client = await connect();
  assert.deepEqual((await client.request('echo')).error, { code: -32602, message: 'Expected address', data: { field: 'address' } });
  client.socket.destroy();
  await closed;
  assert.equal(context.signal.aborted, true);
});

test('oversized frames disconnect before parsing or dispatch', async (t) => {
  let calls = 0;
  const { connect } = await fixture(t, { options: { maxFrameBytes: 128 }, dispatch: () => { calls++; } });
  const client = await connect();
  const closed = once(client.socket, 'close');
  client.socket.write('x'.repeat(129));
  await closed;
  assert.equal(calls, 0);
});

test('oversized result disconnects without sending truncated success', async (t) => {
  const { connect } = await fixture(t, { options: { maxResponseBytes: 128 }, dispatch: () => 'x'.repeat(129) });
  const client = await connect();
  let received = false;
  client.socket.on('data', () => { received = true; });
  const closed = once(client.socket, 'close');
  client.socket.write('{"jsonrpc":"2.0","id":1,"method":"echo"}\n');
  await closed;
  assert.equal(received, false);
});

test('connection caps are shared across IP sockets', async (t) => {
  const { connect, server } = await fixture(t, { options: { maxConnectionsPerIp: 1 } });
  const first = await connect();
  const second = await connect();
  if (!second.socket.destroyed) await once(second.socket, 'close');
  assert.equal(server.stats.connections, 1);
  assert.ok((await first.request('echo')).result);
});

test('partial-frame timeout bounds slowloris connections', async (t) => {
  const { connect } = await fixture(t, { options: { frameTimeoutMs: 30 } });
  const client = await connect();
  const closed = once(client.socket, 'close');
  client.socket.write('{');
  await closed;
});

test('pipelining is bounded before requests can occupy arbitrary memory', async (t) => {
  const { connect } = await fixture(t, { options: { maxPendingRequestsPerConnection: 2 } });
  const client = await connect();
  const closed = once(client.socket, 'close');
  client.socket.write('{"jsonrpc":"2.0","id":1,"method":"echo"}\n'.repeat(3));
  await closed;
});

test('global concurrent dispatches are bounded across sockets', async (t) => {
  let unblock;
  const blocked = new Promise((resolve) => { unblock = resolve; });
  let startedResolve;
  const started = new Promise((resolve) => { startedResolve = resolve; });
  const { connect } = await fixture(t, { options: { maxConcurrentRequests: 1 }, dispatch: async () => {
    startedResolve();
    await blocked;
    return { ok: true };
  } });
  const first = await connect();
  const second = await connect();
  const firstResponse = first.request('echo');
  await started;
  assert.equal((await second.request('echo')).error.code, -32030);
  unblock();
  assert.equal((await firstResponse).result.ok, true);
});

test('stalled dispatch deadline closes client and keeps work counted until settlement', async (t) => {
  let finish;
  const blocked = new Promise((resolve) => { finish = resolve; });
  const { connect, server } = await fixture(t, { options: { requestTimeoutMs: 25 }, dispatch: () => blocked });
  const client = await connect();
  const closed = once(client.socket, 'close');
  client.socket.write('{"jsonrpc":"2.0","id":1,"method":"echo"}\n');
  await closed;
  assert.equal(server.stats.activeRequests, 1);
  finish({ ok: true });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(server.stats.activeRequests, 0);
});

test('progressing stream can exceed one request timeout without truncation', async (t) => {
  const { connect } = await fixture(t, { options: { requestTimeoutMs: 150 }, dispatch: async function* () {
    for (let n = 0; n < 4; n++) {
      await new Promise((resolve) => setTimeout(resolve, 60));
      yield n;
    }
  } });
  const client = await connect();
  await client.request('getblockbounties');
  for (let n = 0; n < 4; n++) assert.equal((await client.receive()).params.items, n);
  assert.equal((await client.receive()).params.complete, true);
});

test('notification output budget disconnects slow clients instead of growing memory', async (t) => {
  const { connect } = await fixture(t, { options: { maxQueuedOutputBytes: 256 }, dispatch: async (_m, _p, ctx) => {
    await ctx.notify('large', { text: 'x'.repeat(300) });
    return {};
  } });
  const client = await connect();
  const closed = once(client.socket, 'close');
  client.socket.write('{"jsonrpc":"2.0","id":1,"method":"subscribe"}\n');
  await closed;
});

test('explicit allowlist and validated bounds are mandatory', () => {
  assert.throws(() => createRpcServer({ dispatch: () => {} }), /allowedMethods/);
  assert.throws(() => createRpcServer({ dispatch: () => {}, allowedMethods: ['echo'], options: { maxFrameBytes: 0 } }));
  assert.throws(() => createRpcServer({ dispatch: () => {}, allowedMethods: ['echo'], options: { typo: 10 } }));
});
