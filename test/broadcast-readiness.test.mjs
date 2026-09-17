import test from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import { EventEmitter, once } from 'node:events';
import { PublicAPI, METHODS } from '../src/api.mjs';
import { createRpcServer } from '../src/transport.mjs';
import { RateLimiter } from '../src/rate-limit.mjs';

const GENESIS = 'ab'.repeat(32);
const TIP = 'cd'.repeat(32);
const TXID = 'ef'.repeat(32);
const HEX = 'aa'.repeat(50);
const SECRET = 'http://rpc-user:secret-password@127.0.0.1:18888/private';
const PUBLIC_METHODS = [
  'getchaintip', 'getrecentblockhashes', 'getblockbounties',
  'getaddressbalance', 'getaddressutxos', 'getaddresshistory',
  'gettransaction', 'sendrawtransaction', 'getbountychanges',
  'subscribebounties', 'subscribeaddress', 'subscribetip', 'unsubscribe',
];

function fixture(t, { ready = false, options = {} } = {}) {
  const indexer = new EventEmitter();
  indexer.ready = ready;
  const state = { tip: { height: 42, hash: TIP, chain: 'regtest', genesis_hash: GENESIS } };
  const store = { tip: () => state.tip, revision: () => 1 };
  const calls = [];
  const replies = {
    getblockchaininfo: () => ({ chain: 'regtest', initialblockdownload: true, blocks: 100, bestblockhash: TXID }),
    getblockhash: () => GENESIS,
    sendrawtransaction: () => TXID,
  };
  const backend = {
    call: async (method, params = []) => {
      calls.push([method, params]);
      assert.ok(Object.hasOwn(replies, method), `Unexpected backend method: ${method}`);
      return replies[method](params);
    },
  };
  const api = new PublicAPI({ store, indexer, backend, options });
  t.after(() => api.close());
  const abort = new AbortController();
  const context = { ip: '127.0.0.1', signal: abort.signal, notify: async () => {}, onClose: () => {} };
  const broadcast = params => api.dispatch('sendrawtransaction', params ?? { transaction_hex: HEX }, context);
  return { api, indexer, state, calls, replies, abort, context, broadcast };
}

function assertSafeError(error, code, message, data) {
  assert.equal(error.code, code);
  if (message !== undefined) assert.equal(error.message, message);
  assert.deepEqual(error.data, data);
  assert.ok(!error.message.includes(SECRET));
  assert.ok(!JSON.stringify(error).includes('secret-password'));
  return true;
}

async function tcpFixture(t, f, { limiter = new RateLimiter(), dispatch = f.api.dispatch } = {}) {
  const server = createRpcServer({ dispatch, allowedMethods: METHODS, limiter });
  const clients = new Set();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(async () => {
    for (const client of clients) client.destroy();
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  });
  let nextId = 1;
  async function connect() {
    const socket = net.connect(server.address().port, '127.0.0.1');
    clients.add(socket);
    socket.on('error', () => {});
    const pending = new Map();
    let buffer = '';
    socket.on('data', bytes => {
      buffer += bytes.toString('utf8');
      for (let newline; (newline = buffer.indexOf('\n')) !== -1;) {
        const message = JSON.parse(buffer.slice(0, newline));
        buffer = buffer.slice(newline + 1);
        const waiter = pending.get(message.id);
        if (waiter) { pending.delete(message.id); clearTimeout(waiter.timer); waiter.resolve(message); }
      }
    });
    socket.on('close', () => {
      for (const waiter of pending.values()) {
        clearTimeout(waiter.timer);
        waiter.reject(new Error('TCP connection closed before response'));
      }
      pending.clear();
    });
    await once(socket, 'connect');
    return { socket, request: (method, params = {}) => {
      const id = nextId++;
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => { pending.delete(id); reject(new Error('TCP response timed out')); }, 3000);
        pending.set(id, { resolve, reject, timer });
        socket.write(`${JSON.stringify({ jsonrpc: '2.0', id, method, params })}\n`);
      });
    } };
  }
  return { server, connect };
}

test('broadcast works during catchup and IBD, with fresh identity checks on every submission', async t => {
  const f = fixture(t);
  for (const ready of [false, true, false]) {
    f.indexer.ready = ready;
    assert.deepEqual(await f.broadcast(), { txid: TXID });
  }
  assert.deepEqual(f.calls, Array.from({ length: 3 }, () => [
    ['getblockchaininfo', []], ['getblockhash', [0]], ['sendrawtransaction', [HEX]],
  ]).flat());

  f.replies.getblockhash = () => TIP;
  await assert.rejects(f.broadcast(), error => assertSafeError(error, -32001));
  assert.equal(f.calls.filter(([method]) => method === 'sendrawtransaction').length, 3);
});

test('broadcast validates named parameters and hex before accessing the backend', async t => {
  const f = fixture(t, { options: { maxTransactionBytes: 50 } });
  const invalid = [
    {}, [], { transaction_hex: null }, { transaction_hex: 123 },
    { transaction_hex: '' }, { transaction_hex: 'aa'.repeat(9) },
    { transaction_hex: 'a'.repeat(21) }, { transaction_hex: 'gg'.repeat(10) },
    { transaction_hex: 'aa'.repeat(51) }, { transaction_hex: ` ${HEX}` },
    { transaction_hex: HEX, maxfeerate: 0 }, { transaction_hex: HEX, ip: '192.0.2.1' },
  ];
  for (const params of invalid) await assert.rejects(f.broadcast(params), { code: -32602 });
  await assert.rejects(f.api.dispatch('sendrawtransaction', null, f.context), { code: -32602 });
  assert.deepEqual(f.calls, []);
  assert.deepEqual(await f.broadcast({ transaction_hex: HEX.toUpperCase() }), { txid: TXID });
  assert.deepEqual(f.calls.at(-1), ['sendrawtransaction', [HEX.toUpperCase()]]);
});

test('broadcast requires a pinned indexed network identity even when the indexer claims readiness', async t => {
  const f = fixture(t);
  for (const ready of [false, true]) {
    f.indexer.ready = ready;
    for (const tip of [null, {}, { chain: 'regtest' }, { genesis_hash: GENESIS },
      { chain: '', genesis_hash: GENESIS }, { chain: 'regtest', genesis_hash: 'invalid' }]) {
      f.state.tip = tip;
      await assert.rejects(f.broadcast(), error => assertSafeError(error, -32001));
    }
  }
  assert.deepEqual(f.calls, []);
});

test('broadcast refuses a different backend chain or genesis without submitting', async t => {
  for (const [chain, genesis] of [['main', GENESIS], ['regtest', TIP]]) {
    const f = fixture(t);
    f.replies.getblockchaininfo = () => ({ chain });
    f.replies.getblockhash = () => genesis;
    await assert.rejects(f.broadcast(), error => assertSafeError(error, -32001));
    assert.ok(!f.calls.some(([method]) => method === 'sendrawtransaction'));
  }
});

test('all indexed reads and subscriptions stay unavailable during synchronization', async t => {
  const f = fixture(t);
  for (const method of PUBLIC_METHODS.filter(method => !['sendrawtransaction', 'unsubscribe'].includes(method))) {
    await assert.rejects(f.api.dispatch(method, {}, f.context), { code: -32001 }, method);
  }
  assert.deepEqual(await f.api.dispatch('unsubscribe', { subscription_id: 'absent' }, f.context), { removed: false });
  assert.deepEqual(f.calls, []);
});

test('preflight failures are safe backend-unavailable errors and never transaction rejections', async t => {
  for (const method of ['getblockchaininfo', 'getblockhash']) {
    for (const code of [undefined, -26, -8]) {
      const f = fixture(t);
      f.replies[method] = () => { throw Object.assign(new Error(SECRET), { code, data: { secret: SECRET } }); };
      await assert.rejects(f.broadcast(), error => assertSafeError(error, -32002,
        'Backend unavailable; transaction was not submitted.'));
      assert.ok(!f.calls.some(([called]) => called === 'sendrawtransaction'));
      assert.equal(f.calls.filter(([called]) => called === method).length, 1);
    }
  }
});

test('only known broadcast rejection codes expose sanitized node codes', async t => {
  for (const code of [-22, -25, -26, -27, -8]) {
    const f = fixture(t);
    f.replies.sendrawtransaction = () => { throw Object.assign(new Error(SECRET), { code, data: SECRET }); };
    await assert.rejects(f.broadcast(), error => assertSafeError(error, -32020,
      'Node rejected the transaction.', { node_code: code }));
    assert.equal(f.calls.filter(([method]) => method === 'sendrawtransaction').length, 1);
  }
  for (const code of [undefined, -28, -32603, '-26']) {
    const f = fixture(t);
    f.replies.sendrawtransaction = () => { throw Object.assign(new Error(SECRET), { code, data: SECRET }); };
    await assert.rejects(f.broadcast(), error => assertSafeError(error, -32002,
      'Backend unavailable or broadcast outcome unknown; check the txid before retrying.'));
    assert.equal(f.calls.filter(([method]) => method === 'sendrawtransaction').length, 1);
  }
});

test('cancellation before submission prevents the side effect, including during preflight', async t => {
  const f = fixture(t);
  let releaseGenesis;
  let signalStarted;
  const started = new Promise(resolve => { signalStarted = resolve; });
  f.replies.getblockhash = () => new Promise(resolve => { releaseGenesis = resolve; signalStarted(); });
  const submission = f.broadcast();
  const rejected = assert.rejects(submission, { code: -32001 });
  await started;
  f.abort.abort();
  releaseGenesis(GENESIS);
  await rejected;
  assert.ok(!f.calls.some(([method]) => method === 'sendrawtransaction'));
  await assert.rejects(f.broadcast(), { code: -32001 });
  assert.ok(!f.calls.some(([method]) => method === 'sendrawtransaction'));
});

test('broadcast readiness exception does not widen the public method whitelist', async t => {
  const f = fixture(t);
  assert.deepEqual(METHODS, PUBLIC_METHODS);
  for (const method of ['getblockchaininfo', 'getblockhash', 'getrawtransaction', 'getblock',
    'getblocktemplate', 'stop', 'dumpprivkey', 'walletpassphrase', '__proto__']) {
    await assert.rejects(f.api.dispatch(method, {}, f.context), { code: -32601 });
  }
  assert.deepEqual(f.calls, []);
});

test('TCP broadcasts retain the default 60-per-IP quota across sockets and reconnects while syncing', { timeout: 10000 }, async t => {
  const f = fixture(t);
  const { connect } = await tcpFixture(t, f, { limiter: new RateLimiter({ now: () => 100 }) });
  const first = await connect();
  const second = await connect();
  assert.equal((await first.request('getchaintip')).error.code, -32001);
  assert.equal((await first.request('sendrawtransaction', { transaction_hex: HEX, ip: '192.0.2.1' })).error.code, -32602);
  for (let n = 1; n < 60; n++) {
    const response = await (n % 2 ? first : second).request('sendrawtransaction', { transaction_hex: HEX });
    assert.deepEqual(response.result, { txid: TXID });
  }
  first.socket.destroy();
  const reconnected = await connect();
  const callsBeforeQuota = f.calls.length;
  for (const client of [second, reconnected]) {
    const response = await client.request('sendrawtransaction', { transaction_hex: HEX });
    assert.equal(response.error.code, -32029);
    assert.equal(response.error.data.reason, 'quota');
    assert.ok(response.error.data.retry_after_ms > 0);
  }
  assert.equal(f.calls.length, callsBeforeQuota);
  assert.equal(f.calls.filter(([method]) => method === 'sendrawtransaction').length, 59);
  assert.equal((await reconnected.request('getchaintip')).error.code, -32001);
  assert.equal((await reconnected.request('getblockchaininfo')).error.code, -32601);
});

test('TCP preserves safe preflight and rejection errors without exposing backend details', { timeout: 10000 }, async t => {
  const f = fixture(t);
  const { connect } = await tcpFixture(t, f);
  const client = await connect();
  f.replies.getblockhash = () => { throw Object.assign(new Error(SECRET), { code: -26, data: SECRET }); };
  assertSafeError((await client.request('sendrawtransaction', { transaction_hex: HEX })).error, -32002,
    'Backend unavailable; transaction was not submitted.');
  f.replies.getblockhash = () => GENESIS;
  f.replies.sendrawtransaction = () => { throw Object.assign(new Error(SECRET), { code: -26, data: SECRET }); };
  assertSafeError((await client.request('sendrawtransaction', { transaction_hex: HEX })).error, -32020,
    'Node rejected the transaction.', { node_code: -26 });
  assert.equal(f.calls.filter(([method]) => method === 'sendrawtransaction').length, 1);
});

test('ordinary indexed reads wait for a healthy catchup and return only the published tip', async t => {
  const f = fixture(t);
  f.indexer.pendingSync = new Promise(() => {});
  let settled = false;
  const request = f.api.dispatch('getchaintip', {}, f.context).then(value => { settled = true; return value; });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(settled, false);
  f.indexer.emit('update', { tip: f.state.tip });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(settled, false);
  f.state.tip = { ...f.state.tip, height: 43, hash: TXID };
  f.indexer.ready = true;
  f.indexer.emit('update', { tip: f.state.tip });
  assert.deepEqual(await request, f.state.tip);
  assert.equal(f.indexer.listenerCount('update'), 1);
  assert.equal(f.indexer.listenerCount('syncError'), 0);
  assert.deepEqual(f.calls, []);
});

test('a waiting indexed read rejects safely on catchup failure or cancellation and removes listeners', async t => {
  for (const failure of ['syncError', 'abort']) {
    const f = fixture(t);
    f.indexer.pendingSync = new Promise(() => {});
    const request = f.api.dispatch('getchaintip', {}, f.context);
    const rejected = assert.rejects(request, error => assertSafeError(error, -32001));
    if (failure === 'syncError') f.indexer.emit('syncError', new Error(SECRET));
    else f.abort.abort();
    await rejected;
    assert.equal(f.indexer.listenerCount('update'), 1);
    assert.equal(f.indexer.listenerCount('syncError'), 0);
    assert.deepEqual(f.calls, []);
  }
});

test('indexed reads fail promptly after sync errors, during shutdown, or without an active sync', async t => {
  for (const state of [
    { pendingSync: new Promise(() => {}), lastError: new Error(SECRET) },
    { pendingSync: new Promise(() => {}), stopping: true },
    { pendingSync: null },
  ]) {
    const f = fixture(t);
    Object.assign(f.indexer, state);
    let rejected = false;
    const request = assert.rejects(f.api.dispatch('getchaintip', {}, f.context), error => {
      rejected = true;
      return assertSafeError(error, -32001);
    });
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(rejected, true);
    await request;
    assert.equal(f.indexer.listenerCount('update'), 1);
    assert.equal(f.indexer.listenerCount('syncError'), 0);
  }
});

test('ordinary indexed reads bound healthy-catchup waits to two seconds', { timeout: 7000 }, async t => {
  const f = fixture(t);
  f.indexer.pendingSync = new Promise(() => {});
  const started = performance.now();
  await assert.rejects(f.api.dispatch('getchaintip', {}, f.context), error => assertSafeError(error, -32001));
  const elapsed = performance.now() - started;
  assert.ok(elapsed >= 1900, `Read rejected before its catchup window: ${elapsed} ms`);
  assert.ok(elapsed < 5000, `Read exceeded its bounded catchup window: ${elapsed} ms`);
  assert.equal(f.indexer.listenerCount('update'), 1);
  assert.equal(f.indexer.listenerCount('syncError'), 0);
});

test('TCP read waits for catchup while a second connection can broadcast immediately', { timeout: 10000 }, async t => {
  const f = fixture(t);
  f.indexer.pendingSync = new Promise(() => {});
  let signalReadStarted;
  const readStarted = new Promise(resolve => { signalReadStarted = resolve; });
  const { connect } = await tcpFixture(t, f, { dispatch: (method, params, context) => {
    const result = f.api.dispatch(method, params, context);
    if (method === 'getchaintip') signalReadStarted();
    return result;
  } });
  const reader = await connect();
  const sender = await connect();
  let readSettled = false;
  const read = reader.request('getchaintip').then(value => { readSettled = true; return value; });
  await readStarted;
  assert.deepEqual((await sender.request('sendrawtransaction', { transaction_hex: HEX })).result, { txid: TXID });
  assert.equal(readSettled, false);
  f.state.tip = { ...f.state.tip, height: 43, hash: TXID };
  f.indexer.ready = true;
  f.indexer.emit('update', { tip: f.state.tip });
  assert.deepEqual((await read).result, f.state.tip);
});
