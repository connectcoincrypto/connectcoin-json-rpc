import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { PublicAPI, METHODS } from '../src/api.mjs';
import { normalizeAddress } from '../src/address.mjs';
import { Cursors } from '../src/cursor.mjs';

const HASH = 'ab'.repeat(32);
const OTHER = 'cd'.repeat(32);

// Construct a Bech32m address for the standard secp256k1 generator's x coordinate.
function address(hrp = 'ccrt') {
  const alpha = 'qpzry9x8gf2tvdw0s3jn54khce6mua7l';
  const key = Buffer.from('79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798', 'hex');
  let acc = 0, bits = 0;
  const words = [1];
  for (const byte of key) {
    acc = (acc << 8) | byte; bits += 8;
    while (bits >= 5) { bits -= 5; words.push((acc >>> bits) & 31); }
  }
  if (bits) words.push((acc << (5 - bits)) & 31);
  const expanded = [...hrp].map(c => c.charCodeAt(0) >> 5).concat(0, [...hrp].map(c => c.charCodeAt(0) & 31));
  let chk = 1;
  for (const v of [...expanded, ...words, 0, 0, 0, 0, 0, 0]) {
    const top = chk >>> 25; chk = ((chk & 0x1ffffff) << 5) ^ v;
    for (let i = 0; i < 5; i++) if ((top >>> i) & 1) chk ^= [0x3b6a57b2,0x26508e6d,0x1ea119fa,0x3d4233dd,0x2a1462b3][i];
  }
  chk ^= 0x2bc830a3;
  const check = Array.from({ length: 6 }, (_, i) => (chk >>> (5 * (5 - i))) & 31);
  return hrp + '1' + [...words, ...check].map(n => alpha[n]).join('');
}
const ADDRESS = address();

function fixture(options = {}) {
  const indexer = new EventEmitter(); indexer.ready = true;
  let rev = 1;
  const tip = { height: 650, hash: HASH, chain: 'regtest', mediantime: 123456 };
  const bountyRows = Array.from({ length: 251 }, (_, vout) => ({ txid: HASH, vout, amount: '10000000001', status: 'available' }));
  const historyRows = bountyRows.map((r, i) => ({ txid: i.toString(16).padStart(64, '0'), received: r.amount, balance_delta: r.amount }));
  const store = {
    tip: () => tip, revision: () => rev, recentBlocks: () => [{ height: 650, hash: HASH }],
    blockAt: () => ({ height: 650, hash: HASH }),
    isRecentBlock: h => h === HASH, bountyPage: (h, off, n) => bountyRows.slice(off, off + n),
    bountyPageAfter: (h, { after, limit }) => bountyRows.filter(r => after == null || r.txid > after.txid || r.txid === after.txid && r.vout > after.vout).slice(0, limit),
    balance: () => ({ confirmed: '10000000001', pending: '0' }),
    history: (addr, off, n) => bountyRows.slice(off, off + n).map(r => ({ txid: r.txid, received: r.amount, balance_delta: r.amount })),
    utxos: (addr, off, n) => bountyRows.slice(off, off + n), transactionLocation: txid => txid === HASH ? { block_hash: HASH, block_height: 650 } : null,
    historyPage: (addr, { after, limit }) => historyRows.filter(r => after == null || r.txid > after).slice(0, limit),
    utxoPage: (addr, { after, limit }) => bountyRows.filter(r => after == null || r.txid > after.txid || r.txid === after.txid && r.vout > after.vout).slice(0, limit),
  };
  const calls = [];
  const backend = {
    call: async (...args) => { calls.push(args); return HASH; },
    transaction: async (...args) => { calls.push(args); return { txid: HASH, hex: 'aa', vout: [] }; },
  };
  const api = new PublicAPI({ store, indexer, backend, options });
  const notices = [], cleanups = [];
  const context = { ip: '127.0.0.1', signal: new AbortController().signal,
    notify: async (method, params) => notices.push({ method, params }), onClose: fn => cleanups.push(fn) };
  return { api, indexer, store, backend, calls, context, notices, cleanups, changeRevision: () => rev++, tip };
}

test('native addresses enforce correct network, checksum, mixed case and length', () => {
  assert.equal(normalizeAddress(ADDRESS.toUpperCase(), 'regtest'), ADDRESS);
  assert.equal(normalizeAddress(address('cc'), 'main'), address('cc'));
  assert.equal(normalizeAddress(address('tcc'), 'testnet4'), address('tcc'));
  for (const bad of [ADDRESS, 'fake', '', null, 'a'.repeat(100)]) {
    assert.throws(() => normalizeAddress(bad, 'main'), { code: -32602 });
  }
  assert.throws(() => normalizeAddress('C' + ADDRESS.slice(1), 'regtest'), { code: -32602 });
  assert.throws(() => normalizeAddress(ADDRESS.slice(0, -1) + (ADDRESS.endsWith('q') ? 'p' : 'q'), 'regtest'), { code: -32602 });
});

test('cursor signatures reject tampering, restart and oversize input', () => {
  const c = new Cursors(), token = c.sign({ kind: 'hello', sequence: 1 });
  assert.deepEqual(c.read(token), { kind: 'hello', sequence: 1 });
  for (const bad of [token + 'x', '', null, 'x'.repeat(1025), token.replace(/^./, 'x')]) assert.throws(() => c.read(bad), { code: -32011 });
  assert.throws(() => new Cursors().read(token), { code: -32011 });
});

test('whitelist excludes private RPC and enforces readiness and params', async () => {
  const f = fixture();
  assert.equal(METHODS.length, 13);
  for (const method of ['getblock', 'getblocktemplate', 'stop', 'dumpprivkey', '__proto__']) {
    await assert.rejects(f.api.dispatch(method, {}, f.context), { code: -32601 });
  }
  await assert.rejects(f.api.dispatch('getchaintip', { arbitrary: true }, f.context), { code: -32602 });
  assert.equal((await f.api.dispatch('getchaintip', {}, f.context)).height, 650);
  assert.equal(f.calls.length, 0);
  f.indexer.ready = false;
  await assert.rejects(f.api.dispatch('getchaintip', {}, f.context), { code: -32001 });
});

test('bounties stream every item, normalize hash, explicitly reject outside window', async () => {
  const f = fixture();
  assert.equal(f.api.classifyBountyHash({ block_hash: HASH.toUpperCase() }), HASH);
  assert.equal(f.api.classifyBountyHash({ block_hash: OTHER }), null);
  assert.equal(f.api.classifyBountyHash({ block_hash: 'invalid' }), null);
  const stream = await f.api.dispatch('getblockbounties', { block_hash: HASH.toUpperCase() }, f.context);
  let count = 0, chunks = 0;
  for await (const chunk of stream) {
    if (chunks++ === 0) assert.equal(chunk.type, 'snapshot');
    else if (chunk.type === 'bounties') { count += chunk.items.length; assert.ok(chunk.items.length <= 100); }
  }
  assert.equal(count, 251);
  await assert.rejects(f.api.dispatch('getblockbounties', { block_hash: OTHER }, f.context), { code: -32004 });
});

test('bounty membership survives unrelated revision changes, rejects a disconnected block', async () => {
  const f = fixture();
  const stream = await f.api.dispatch('getblockbounties', { block_hash: HASH }, f.context);
  await stream.next(); await stream.next(); f.changeRevision();
  assert.equal((await stream.next()).value.items.length, 100);
  f.store.isRecentBlock = () => false;
  await assert.rejects(stream.next(), { code: -32011 });
});

test('bounty stream waits for coherent catchup and fails closed on backend sync failure', async () => {
  const f = fixture();
  const stream = await f.api.dispatch('getblockbounties', { block_hash: HASH }, f.context);
  await stream.next();
  f.indexer.ready = false;
  let delivered = false;
  const next = stream.next().then(value => { delivered = true; return value; });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(delivered, false);
  f.indexer.ready = true;
  f.indexer.emit('update', { tip: f.tip, bountyChanges: [] });
  assert.equal((await next).value.items.length, 100);
  f.indexer.ready = false;
  const failed = stream.next();
  const rejection = assert.rejects(failed, { code: -32001 });
  await new Promise(resolve => setImmediate(resolve));
  f.indexer.emit('syncError', new Error('not public'));
  await rejection;
  assert.equal(f.indexer.listenerCount('syncError'), 0);
});

test('live keyset history pages survive unrelated changes but reject reorg and mismatched queries', async () => {
  const f = fixture();
  const first = await f.api.dispatch('getaddresshistory', { address: ADDRESS }, f.context);
  assert.equal(first.items.length, 100); assert.ok(first.next_cursor);
  const second = await f.api.dispatch('getaddresshistory', { address: ADDRESS, cursor: first.next_cursor }, f.context);
  const third = await f.api.dispatch('getaddresshistory', { address: ADDRESS, cursor: second.next_cursor }, f.context);
  assert.equal(third.items.length, 51); assert.equal(third.next_cursor, null);
  await assert.rejects(f.api.dispatch('getaddressutxos', { address: ADDRESS, cursor: first.next_cursor }, f.context), { code: -32011 });
  f.changeRevision();
  assert.equal((await f.api.dispatch('getaddresshistory', { address: ADDRESS, cursor: first.next_cursor }, f.context)).items.length, 100);
  f.store.blockAt = () => ({ hash: OTHER });
  await assert.rejects(f.api.dispatch('getaddresshistory', { address: ADDRESS, cursor: first.next_cursor }, f.context), { code: -32011 });
});

test('transaction fetch and broadcast are narrow and sanitize backend errors', async () => {
  const f = fixture();
  assert.equal((await f.api.dispatch('gettransaction', { txid: HASH }, f.context)).transaction.txid, HASH);
  assert.deepEqual(f.calls[0], [HASH, HASH]);
  await assert.rejects(f.api.dispatch('gettransaction', { txid: OTHER }, f.context), { code: -32004 });
  await assert.rejects(f.api.dispatch('sendrawtransaction', { transaction_hex: 'a' }, f.context), { code: -32602 });
  assert.equal((await f.api.dispatch('sendrawtransaction', { transaction_hex: 'aa'.repeat(50) }, f.context)).txid, HASH);
  assert.deepEqual(f.calls[1], ['sendrawtransaction', ['aa'.repeat(50)]]);
  f.backend.call = async () => { const e = new Error('secret credentials!'); e.code = -26; throw e; };
  await assert.rejects(f.api.dispatch('sendrawtransaction', { transaction_hex: 'aa'.repeat(50) }, f.context), e => e.code === -32020 && !e.message.includes('secret') && e.data.node_code === -26);
});

test('change cursors are ordered, paginated, replayable and expire on bounded overflow', async () => {
  const f = fixture({ pageSize: 2, maxJournalEvents: 3 });
  const baseline = await f.api.dispatch('getbountychanges', {}, f.context);
  f.indexer.emit('update', { tip: f.tip, bountyChanges: [{ type: 'added', txid: HASH }, { type: 'spent', txid: HASH }, { type: 'added', txid: OTHER }] });
  const page = await f.api.dispatch('getbountychanges', { cursor: baseline.next_cursor }, f.context);
  assert.equal(page.changes.length, 2); assert.equal(page.has_more, true);
  const tail = await f.api.dispatch('getbountychanges', { cursor: page.next_cursor }, f.context);
  assert.equal(tail.changes.length, 1); assert.equal(tail.has_more, false);
  f.indexer.emit('update', { tip: f.tip, bountyChanges: [{ type: 'spent', txid: OTHER }] });
  await assert.rejects(f.api.dispatch('getbountychanges', { cursor: baseline.next_cursor }, f.context), { code: -32011 });
});

test('subscriptions have shared IP limits, ownership and disconnect cleanup', async () => {
  const f = fixture({ maxSubscriptionsPerIP: 2 });
  const a = await f.api.dispatch('subscribeaddress', { address: ADDRESS }, f.context);
  const b = await f.api.dispatch('subscribebounties', {}, f.context);
  assert.equal((await f.api.dispatch('subscribeaddress', { address: ADDRESS }, f.context)).subscription_id, a.subscription_id);
  const stranger = { ...f.context };
  await assert.rejects(f.api.dispatch('subscribetip', {}, stranger), { code: -32005 });
  assert.equal((await f.api.dispatch('unsubscribe', { subscription_id: a.subscription_id }, stranger)).removed, false);
  f.indexer.emit('update', { tip: f.tip, addresses: [ADDRESS], bountyChanges: [{ type: 'added', txid: HASH }] });
  assert.equal(f.notices.length, 2);
  f.indexer.ready = false;
  assert.equal((await f.api.dispatch('unsubscribe', { subscription_id: b.subscription_id }, f.context)).removed, true);
  f.cleanups.forEach(fn => fn()); assert.equal(f.api.subscriptions.size, 0);
});

test('full resync invalidates old feed cursors and signals subscribers', async () => {
  const f = fixture();
  const old = await f.api.dispatch('getbountychanges', {}, f.context);
  await f.api.dispatch('subscribebounties', {}, f.context);
  f.indexer.emit('update', { tip: f.tip, bountyChanges: [{ type: 'resync_required' }], reorg: true });
  await assert.rejects(f.api.dispatch('getbountychanges', { cursor: old.next_cursor }, f.context), { code: -32011 });
  assert.equal(f.notices[0].params.resync_required, true);
});
