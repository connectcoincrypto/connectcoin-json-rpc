import test from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import { EventEmitter, once } from 'node:events';
import { PublicAPI, METHODS } from '../src/api.mjs';
import { createRpcServer } from '../src/transport.mjs';

// Public synthetic fixtures only: these bytes are never broadcast or signed.
const MAX_RESULT_BYTES = 1_572_864;
const BLOCK = 'ab'.repeat(32);
const OTHER_BLOCK = 'cd'.repeat(32);
const GENESIS = 'ef'.repeat(32);
const HEX = 'aa'.repeat(10);
const txid = n => n.toString(16).padStart(64, '0');
const IDS = Array.from({ length: 33 }, (_, i) => txid(i + 1));
const bytes = value => Buffer.byteLength(JSON.stringify(value));

function fixture(t, count = 3) {
  const indexer = new EventEmitter();
  indexer.ready = true;
  const state = {
    tip: { height: 650, hash: BLOCK, chain: 'regtest', genesis_hash: GENESIS, mediantime: 123456 },
    anchor: BLOCK,
    revision: 1,
  };
  const locations = new Map(IDS.slice(0, count).map(id => [id, {
    status: 'confirmed', block_hash: BLOCK, block_height: 649,
  }]));
  const calls = [];
  const controller = new AbortController();
  const context = { ip: '127.0.0.1', signal: controller.signal,
    notify: async () => {}, onClose: () => {} };
  const store = {
    tip: () => ({ ...state.tip }), revision: () => state.revision,
    blockAt: height => height === 650 && state.anchor ? { height, hash: state.anchor } : null,
    transactionLocation: id => locations.has(id) ? { ...locations.get(id) } : null,
  };
  const backend = {
    raw: async () => HEX,
    rawTransaction: async (...args) => {
      calls.push(args);
      return backend.raw(...args);
    },
  };
  const api = new PublicAPI({ store, indexer, backend });
  t.after(() => { controller.abort(); api.close(); });
  const query = (ids = IDS.slice(0, count), ctx = context) => api.dispatch('gettransactions', { txids: ids }, ctx);
  return { api, store, indexer, backend, calls, locations, state, controller, context, query };
}

function gate() {
  let release;
  const promise = new Promise(resolve => { release = resolve; });
  return { promise, release };
}

function holdUntilReleased(signal, released) {
  return new Promise((resolve, reject) => {
    const aborted = () => { signal.removeEventListener('abort', aborted); reject(new Error('Synthetic backend abort')); };
    if (signal.aborted) { aborted(); return; }
    signal.addEventListener('abort', aborted, { once: true });
    released.then(() => { signal.removeEventListener('abort', aborted); resolve(HEX); });
  });
}

test('batch endpoint is allowlisted and returns canonical txids in request order, with compact raw results', async t => {
  const f = fixture(t, 32);
  assert.ok(METHODS.includes('gettransactions'));
  const ids = IDS.slice(0, 32).reverse();
  f.locations.set(ids[1], { status: 'pending' });
  f.backend.raw = async (_id, _block, { signal }) => {
    assert.equal(signal.aborted, false);
    return HEX;
  };
  const result = await f.query(ids.map(id => id.toUpperCase()));
  assert.deepEqual(result, { tip: f.state.tip,
    transactions: ids.map(id => ({ txid: id, hex: HEX })), remaining: [] });
  assert.equal(f.calls.length, 32);
  assert.deepEqual(f.calls.map(([id]) => id), ids);
  assert.equal(f.calls[0][1], BLOCK);
  assert.equal(f.calls[1][1], undefined);
  for (const [, , options] of f.calls) {
    assert.ok(options.signal instanceof AbortSignal);
    assert.ok(options.timeoutMs > 0 && options.timeoutMs <= 5_000);
    assert.equal(options.maxResponseBytes, MAX_RESULT_BYTES + 4_096);
  }
  assert.equal(f.api.batchActive, 0);
});

test('batch rejects every invalid request before any backend work', async t => {
  const f = fixture(t, 33);
  const invalid = [
    null, [], 'bad', 1, {}, { txid: IDS[0] }, { txids: null }, { txids: IDS[0] },
    { txids: [] }, { txids: IDS }, { txids: [IDS[0], IDS[0]] },
    { txids: [BLOCK, BLOCK.toUpperCase()] }, { txids: [IDS[0], null] },
    { txids: [IDS[0], 1] }, { txids: ['g'.repeat(64)] }, { txids: ['a'.repeat(63)] },
    { txids: ['a'.repeat(65)] }, { txids: [` ${IDS[0]}`] }, { txids: [IDS[0]], limit: 1 },
    { txids: [IDS[0]], max_result_bytes: MAX_RESULT_BYTES * 2 },
    { txids: [IDS[0]], timeoutMs: 100_000 }, { txids: [IDS[0]], ip: '198.51.100.1' },
  ];
  for (const params of invalid) {
    await assert.rejects(f.api.dispatch('gettransactions', params, f.context), { code: -32602 }, JSON.stringify(params));
  }
  assert.deepEqual(f.calls, []);
  assert.equal(f.api.batchActive, 0);
});

test('batch supports a single txid and rejects unavailable or unready index state', async t => {
  const f = fixture(t, 1);
  assert.equal((await f.query()).transactions.length, 1);
  await assert.rejects(f.query([IDS[1]]), { code: -32004 });
  assert.equal(f.calls.length, 1);
  f.indexer.ready = false;
  await assert.rejects(f.query(), { code: -32001 });
  assert.equal(f.calls.length, 1);
  assert.equal(f.api.batchActive, 0);
});

test('an unknown later txid fails the whole batch without partial success', async t => {
  const f = fixture(t, 1);
  await assert.rejects(f.query([IDS[0], IDS[1]]), { code: -32004 });
  assert.equal(f.api.batchActive, 0);
});

test('exact result byte accounting permits the largest even-length hex that fits', async t => {
  const f = fixture(t, 1);
  const overhead = bytes({ tip: f.state.tip, transactions: [{ txid: IDS[0], hex: '' }], remaining: [] });
  const hexLength = Math.floor((MAX_RESULT_BYTES - overhead) / 2) * 2;
  f.backend.raw = async () => 'aa'.repeat(hexLength / 2);
  const result = await f.query();
  assert.equal(result.transactions[0].hex.length, hexLength);
  assert.ok(bytes(result) <= MAX_RESULT_BYTES);
  assert.ok(MAX_RESULT_BYTES - bytes(result) <= 1);
  f.backend.raw = async () => 'aa'.repeat(hexLength / 2 + 1);
  await assert.rejects(f.query(), error => {
    assert.equal(error.code, -32021);
    assert.deepEqual(error.data, { txid: IDS[0], max_result_bytes: MAX_RESULT_BYTES });
    return true;
  });
  assert.equal(f.api.batchActive, 0);
});

test('batch size cap includes all suffix IDs and returns an ordered resumable prefix', async t => {
  const f = fixture(t, 32);
  const ids = IDS.slice(0, 32).reverse();
  const suffix = ids.slice(1);
  const overhead = bytes({ tip: f.state.tip, transactions: [{ txid: ids[0], hex: '' }], remaining: suffix });
  const hexLength = Math.floor((MAX_RESULT_BYTES - overhead) / 2) * 2;
  f.backend.raw = async id => id === ids[0] ? 'bb'.repeat(hexLength / 2) : HEX;
  const result = await f.query(ids);
  assert.deepEqual(result.transactions.map(item => item.txid), [ids[0]]);
  assert.deepEqual(result.remaining, suffix);
  assert.ok(bytes(result) <= MAX_RESULT_BYTES);
  assert.ok(MAX_RESULT_BYTES - bytes(result) <= 1);
  assert.deepEqual(f.calls.map(([id]) => id), ids.slice(0, 2));
  const rest = await f.query(result.remaining);
  assert.deepEqual(rest.transactions.map(item => item.txid), suffix);
  assert.deepEqual(rest.remaining, []);
  assert.deepEqual([...result.transactions, ...rest.transactions].map(item => item.txid), ids);
});

test('growing tip encoding during a normal chain advance cannot exceed the final publication bound', async t => {
  const f = fixture(t);
  const overhead = bytes({ tip: f.state.tip,
    transactions: [{ txid: IDS[0], hex: '' }, { txid: IDS[1], hex: HEX }], remaining: [IDS[2]] });
  const hexLength = Math.floor((MAX_RESULT_BYTES - overhead) / 2) * 2;
  f.backend.raw = async id => {
    if (id === IDS[0]) return 'aa'.repeat(hexLength / 2);
    if (id === IDS[2]) f.state.tip = { ...f.state.tip, height: 1_000_000, hash: OTHER_BLOCK };
    return HEX;
  };
  const result = await f.query();
  assert.deepEqual(result.transactions.map(item => item.txid), [IDS[0]]);
  assert.deepEqual(result.remaining, IDS.slice(1, 3));
  assert.equal(result.tip.height, 1_000_000);
  assert.ok(bytes(result) <= MAX_RESULT_BYTES);
});

test('oversized first backend response produces bounded public error and releases its slot', async t => {
  const f = fixture(t, 1);
  f.backend.raw = async () => { throw Object.assign(new Error('private response detail'), { code: 'RESPONSE_TOO_LARGE' }); };
  await assert.rejects(f.query(), error => {
    assert.equal(error.code, -32021);
    assert.deepEqual(error.data, { txid: IDS[0], max_result_bytes: MAX_RESULT_BYTES });
    assert.ok(!JSON.stringify(error).includes('private'));
    assert.ok(!error.message.includes('private'));
    return true;
  });
  assert.equal(f.api.batchActive, 0);
  f.backend.raw = async () => HEX;
  assert.equal((await f.query()).transactions.length, 1);
});

test('oversized later backend response returns only the valid prefix and leaves the full suffix', async t => {
  const f = fixture(t);
  f.backend.raw = async id => {
    if (id === IDS[1]) throw Object.assign(new Error('private response detail'), { code: 'RESPONSE_TOO_LARGE' });
    return HEX;
  };
  assert.deepEqual(await f.query(), { tip: f.state.tip,
    transactions: [{ txid: IDS[0], hex: HEX }], remaining: IDS.slice(1, 3) });
  assert.deepEqual(f.calls.map(([id]) => id), IDS.slice(0, 2));
  assert.equal(f.api.batchActive, 0);
});

test('ordinary backend failure is sanitized and cannot yield a successful partial batch', async t => {
  const f = fixture(t);
  f.backend.raw = async id => {
    if (id === IDS[1]) throw Object.assign(new Error('password=private-fixture'), { code: -5 });
    return HEX;
  };
  await assert.rejects(f.query(), error => {
    assert.equal(error.code, -32002);
    assert.ok(!error.message.includes('private-fixture'));
    assert.ok(!JSON.stringify(error).includes('private-fixture'));
    return true;
  });
  assert.deepEqual(f.calls.map(([id]) => id), IDS.slice(0, 2));
  assert.equal(f.api.batchActive, 0);
});

test('malformed raw backend values are rejected instead of being placed in a success result', async t => {
  const f = fixture(t, 1);
  for (const value of [null, undefined, 17, {}, [], '', 'aa', 'a'.repeat(21), 'zz'.repeat(10), ` ${HEX}`, `${HEX}\n`]) {
    f.backend.raw = async () => value;
    await assert.rejects(f.query(), { code: -32002 }, String(value));
    assert.equal(f.api.batchActive, 0);
  }
});

for (const [name, mutate] of [
  ['status', f => f.locations.set(IDS[0], { ...f.locations.get(IDS[0]), status: 'pending' })],
  ['block hash', f => f.locations.set(IDS[0], { ...f.locations.get(IDS[0]), block_hash: OTHER_BLOCK })],
  ['block height', f => f.locations.set(IDS[0], { ...f.locations.get(IDS[0]), block_height: 648 })],
  ['removed transaction', f => f.locations.delete(IDS[0])],
]) {
  test(`batch revalidates ${name} immediately after fetch`, async t => {
    const f = fixture(t);
    f.backend.raw = async () => { mutate(f); return HEX; };
    await assert.rejects(f.query(), { code: -32011 });
    assert.equal(f.calls.length, 1);
    assert.equal(f.api.batchActive, 0);
  });
  test(`batch revalidates an earlier transaction's ${name} before final success`, async t => {
    const f = fixture(t);
    f.backend.raw = async id => { if (id === IDS[2]) mutate(f); return HEX; };
    await assert.rejects(f.query(), { code: -32011 });
    assert.equal(f.calls.length, 3);
    assert.equal(f.api.batchActive, 0);
  });
}

test('pending transaction becoming confirmed while fetched invalidates the batch', async t => {
  const f = fixture(t, 1);
  f.locations.set(IDS[0], { status: 'pending' });
  f.backend.raw = async () => {
    f.locations.set(IDS[0], { status: 'confirmed', block_hash: BLOCK, block_height: 650 });
    return HEX;
  };
  await assert.rejects(f.query(), { code: -32011 });
  assert.equal(f.calls[0][1], undefined);
});

test('partial prefix is also revalidated when a later transaction exceeds the backend size bound', async t => {
  const f = fixture(t);
  f.backend.raw = async id => {
    if (id === IDS[1]) {
      f.locations.delete(IDS[0]);
      throw Object.assign(new Error('Synthetic size bound'), { code: 'RESPONSE_TOO_LARGE' });
    }
    return HEX;
  };
  await assert.rejects(f.query(), { code: -32011 });
  assert.equal(f.api.batchActive, 0);
});

for (const [name, mutate] of [
  ['index loses readiness', f => { f.indexer.ready = false; }],
  ['tip anchor changes', f => { f.state.anchor = OTHER_BLOCK; }],
  ['tip anchor disappears', f => { f.state.anchor = null; }],
  ['chain changes', f => { f.state.tip.chain = 'main'; }],
  ['genesis changes', f => { f.state.tip.genesis_hash = OTHER_BLOCK; }],
]) {
  test(`batch rejects stale state when ${name}`, async t => {
    const f = fixture(t);
    f.backend.raw = async () => { mutate(f); return HEX; };
    await assert.rejects(f.query(), { code: -32011 });
    assert.equal(f.api.batchActive, 0);
  });
}

test('unrelated revision and normal tip advance preserve a coherent batch', async t => {
  const f = fixture(t);
  f.backend.raw = async id => {
    if (id === IDS[1]) {
      f.state.revision++;
      f.state.tip = { ...f.state.tip, height: 651, hash: OTHER_BLOCK };
    }
    return HEX;
  };
  const result = await f.query();
  assert.deepEqual(result.transactions.map(item => item.txid), IDS.slice(0, 3));
  assert.deepEqual(result.remaining, []);
});

test('already-cancelled batch starts no backend work', async t => {
  const f = fixture(t);
  f.controller.abort();
  await assert.rejects(f.query(), { code: -32001 });
  assert.deepEqual(f.calls, []);
  assert.equal(f.api.batchActive, 0);
});

test('active cancellation propagates to the backend and releases the global batch slot', async t => {
  const f = fixture(t);
  const started = gate();
  const released = gate();
  t.after(() => released.release());
  let backendSignal;
  f.backend.raw = async (_id, _block, options) => {
    backendSignal = options.signal;
    started.release();
    return holdUntilReleased(options.signal, released.promise);
  };
  const rejected = assert.rejects(f.query(), { code: -32001 });
  await started.promise;
  assert.equal(f.api.batchActive, 1);
  f.controller.abort();
  await rejected;
  assert.equal(backendSignal.aborted, true);
  assert.equal(f.calls.length, 1);
  assert.equal(f.api.batchActive, 0);
});

test('cancellation between backend completion and publication cannot return successful bytes', async t => {
  const f = fixture(t);
  f.backend.raw = async () => { f.controller.abort(); return HEX; };
  await assert.rejects(f.query(), { code: -32001 });
  assert.equal(f.calls.length, 1);
  assert.equal(f.api.batchActive, 0);
});

test('total batch deadline aborts outstanding backend work and releases capacity', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const f = fixture(t);
  const started = gate();
  const released = gate();
  t.after(() => released.release());
  let backendSignal;
  f.backend.raw = (_id, _block, { signal }) => {
    backendSignal = signal;
    started.release();
    return holdUntilReleased(signal, released.promise);
  };
  const rejected = assert.rejects(f.query(), { code: -32002 });
  await started.promise;
  t.mock.timers.tick(29_999);
  assert.equal(backendSignal.aborted, false);
  assert.equal(f.api.batchActive, 1);
  t.mock.timers.tick(1);
  await rejected;
  assert.equal(backendSignal.aborted, true);
  assert.equal(f.calls.length, 1);
  assert.equal(f.api.batchActive, 0);
});

test('API shutdown aborts both active batches and releases their capacity', async t => {
  const f = fixture(t, 1);
  const released = gate();
  t.after(() => released.release());
  const signals = [];
  f.backend.raw = (_id, _block, { signal }) => {
    signals.push(signal);
    return holdUntilReleased(signal, released.promise);
  };
  const firstRejected = assert.rejects(f.query(), { code: -32001 });
  const secondRejected = assert.rejects(f.query(), { code: -32001 });
  assert.equal(f.api.batchActive, 2);
  f.api.close();
  await Promise.all([firstRejected, secondRejected]);
  assert.equal(signals.length, 2);
  assert.ok(signals.every(signal => signal.aborted));
  assert.equal(f.api.batchActive, 0);
  assert.equal(f.api.batchControllers.size, 0);
});

test('two active batches exhaust the global cap across different IPs and cancellation restores capacity', async t => {
  const f = fixture(t, 1);
  const released = gate();
  t.after(() => released.release());
  const secondController = new AbortController();
  t.after(() => secondController.abort());
  f.backend.raw = (_id, _block, { signal }) => holdUntilReleased(signal, released.promise);
  const first = f.query();
  const second = f.query([IDS[0]], { ...f.context, ip: '198.51.100.2', signal: secondController.signal });
  const firstRejected = assert.rejects(first, { code: -32001 });
  assert.equal(f.api.batchActive, 2);
  await assert.rejects(f.query([IDS[0]], { ...f.context, ip: '198.51.100.3' }), error => {
    assert.equal(error.code, -32030);
    assert.deepEqual(error.data, { retry_after_ms: 1_000 });
    return true;
  });
  assert.equal(f.calls.length, 2);
  f.controller.abort();
  await firstRejected;
  assert.equal(f.api.batchActive, 1);
  const third = f.query([IDS[0]], { ...f.context, ip: '198.51.100.3', signal: new AbortController().signal });
  assert.equal(f.api.batchActive, 2);
  released.release();
  assert.equal((await second).transactions.length, 1);
  assert.equal((await third).transactions.length, 1);
  assert.equal(f.api.batchActive, 0);
});

async function transportFixture(t, f) {
  const server = createRpcServer({ allowedMethods: METHODS,
    dispatch: (method, params, context) => f.api.dispatch(method, params, context) });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const sockets = new Set();
  t.after(async () => {
    for (const socket of sockets) socket.destroy();
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  });
  const connect = async () => {
    const socket = net.connect(server.address().port, '127.0.0.1');
    sockets.add(socket);
    socket.on('error', () => {});
    let buffer = '';
    let nextId = 1;
    const pending = new Map();
    socket.on('data', data => {
      buffer += data.toString('utf8');
      let newline;
      while ((newline = buffer.indexOf('\n')) >= 0) {
        const result = JSON.parse(buffer.slice(0, newline));
        buffer = buffer.slice(newline + 1);
        const waiter = pending.get(result.id);
        pending.delete(result.id);
        waiter?.resolve(result);
      }
    });
    socket.on('close', () => {
      for (const waiter of pending.values()) waiter.reject(new Error('Synthetic client disconnected'));
      pending.clear();
    });
    await once(socket, 'connect');
    const request = (params, method = 'gettransactions') => new Promise((resolve, reject) => {
      const id = nextId++;
      pending.set(id, { resolve, reject });
      socket.write(`${JSON.stringify({ jsonrpc: '2.0', id, method, params })}\n`);
    });
    return { socket, request };
  };
  return { server, connect };
}

test('TCP batch quota is eight per shared IP, counts invalid params, survives reconnection and is method-specific', { timeout: 10_000 }, async t => {
  const f = fixture(t, 1);
  const { connect } = await transportFixture(t, f);
  const first = await connect();
  const second = await connect();
  const attempts = [
    [{ txids: [IDS[0]] }, null], [{ txids: [] }, -32602], [null, -32602],
    [[], -32602], [{ txids: [IDS[0]], ip: '198.51.100.2' }, -32602],
    [{ txids: [IDS[0], IDS[0]] }, -32602], [{ txids: [IDS[0]] }, null],
    [{ txids: [IDS[0]] }, null],
  ];
  for (let i = 0; i < attempts.length; i++) {
    const [params, expectedCode] = attempts[i];
    const response = await (i % 2 ? first : second).request(params);
    if (expectedCode === null) assert.equal(response.result.transactions[0].txid, IDS[0]);
    else assert.equal(response.error.code, expectedCode);
  }
  const ninth = await second.request({ txids: [IDS[0]] });
  assert.equal(ninth.error.code, -32029);
  assert.equal(ninth.error.data.reason, 'quota');
  assert.ok(ninth.error.data.retry_after_ms > 0 && ninth.error.data.retry_after_ms <= 60_000);
  assert.equal(f.calls.length, 3);
  first.socket.destroy();
  const reconnected = await connect();
  assert.equal((await reconnected.request({ txids: [IDS[0]] })).error.code, -32029);
  assert.equal((await reconnected.request({}, 'getchaintip')).result.hash, BLOCK);
});

test('TCP disconnect aborts an active backend request and frees the API batch slot', { timeout: 10_000 }, async t => {
  const f = fixture(t, 1);
  const started = gate();
  const released = gate();
  const aborted = gate();
  t.after(() => released.release());
  let backendSignal;
  f.backend.raw = (_id, _block, { signal }) => {
    backendSignal = signal;
    signal.addEventListener('abort', () => aborted.release(), { once: true });
    started.release();
    return holdUntilReleased(signal, released.promise);
  };
  const { server, connect } = await transportFixture(t, f);
  const client = await connect();
  const disconnected = assert.rejects(client.request({ txids: [IDS[0]] }), /disconnected/);
  await started.promise;
  assert.equal(f.api.batchActive, 1);
  client.socket.destroy();
  await disconnected;
  await aborted.promise;
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(backendSignal.aborted, true);
  assert.equal(f.api.batchActive, 0);
  assert.equal(server.stats.activeRequests, 0);
});
