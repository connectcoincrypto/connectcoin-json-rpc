import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { EventEmitter, once } from 'node:events';
import net from 'node:net';
import { PublicAPI, METHODS } from '../src/api.mjs';
import { Store } from '../src/store.mjs';
import { bountyStateDigest, bountyImmutableDigest } from '../src/bounty-codec.mjs';
import { createRpcServer } from '../src/transport.mjs';

const hash = value => createHash('sha256').update(value).digest('hex');
const bounty = { value: '0.01', type: 2, domain: 'example.com', connection_work_target: 'f'.repeat(64),
  root_certificates_version: 1, signature_algorithms_mask: 7 };
const tx = (name, outputs = [], inputs = [{ txid: hash(`external-${name}`), vout: 0 }]) => ({ txid: hash(name),
  vin: inputs, vout: outputs.map((output, n) => ({ ...output, n })) });

function fixture(t, options = {}) {
  const store = new Store(); store.pinNetwork('regtest', hash('genesis'));
  store.applyBlock({ hash: hash('genesis'), height: 0, mediantime: 1700000000,
    tx: [tx('genesis', [], [{ coinbase: '01' }])] });
  const indexer = new EventEmitter(); indexer.ready = true;
  const calls = [], notices = [];
  const backend = Object.fromEntries(['transaction', 'rawTransaction', 'call'].map(method => [method,
    async (...args) => { calls.push({ method, args }); throw new Error('Unexpected backend access'); }]));
  const context = { ip: '127.0.0.1', signal: new AbortController().signal, onClose() {},
    async notify(method, params) { notices.push({ method, params }); } };
  const api = new PublicAPI({ store, indexer, backend, options });
  t.after(() => { api.close(); store.close(); });
  const publish = change => indexer.emit('update', { ...change, tip: store.tip() });
  const mine = (name, transactions = []) => {
    const prior = store.tip();
    return store.applyBlock({ hash: hash(name), height: prior.height + 1,
      previousblockhash: prior.hash, mediantime: prior.mediantime + 10, tx: transactions });
  };
  const funding = tx('funding', [bounty, bounty]); publish(mine('funding', [funding]));
  const call = (method, params = {}) => api.dispatch(method, params, context);
  const params = () => ({ include_bounties: true, compact: true,
    known_tip: { height: store.tip().height, hash: store.tip().hash } });
  const changes = (cursor, extra = {}) => call('getbountychanges', {
    ...params(), ...(cursor === undefined ? {} : { cursor }), ...extra });
  const pool = transactions => { const update = store.replaceMempool(transactions); publish(update); return update; };
  return { store, api, indexer, backend, context, calls, notices, funding, call, changes, params, mine, publish, pool };
}

test('canonical state and immutable digests have shared fixed vectors and bind every field', () => {
  const tip = { chain: 'main', genesis_hash: 'a'.repeat(64), height: 123, hash: 'b'.repeat(64), mediantime: 1700000000 };
  const row = { txid: 'c'.repeat(64), vout: 2, block_hash: 'd'.repeat(64), block_height: 120, amount: '100000000',
    domain: 'example.com', connection_work_target: 'e'.repeat(64), root_certificates_version: 1,
    signature_algorithms_mask: 7, coinbase: false };
  assert.equal(bountyStateDigest(tip, 'opaque-cursor-v1'), 'a028924ae24da57f10b87198d8ae90f660bcf67ad7e09e80985e744f42d8a5a7');
  assert.equal(bountyImmutableDigest(row), '49ef00b7fa42a74c1576f41c0e4d9fbc1cc08025b0785bdd12e0736a8f8d3b73');
  for (const [key, value] of Object.entries(tip)) {
    const changed = typeof value === 'number' ? value + 1 : value + 'x';
    assert.notEqual(bountyStateDigest({ ...tip, [key]: changed }, 'opaque-cursor-v1'), bountyStateDigest(tip, 'opaque-cursor-v1'), key);
  }
  assert.notEqual(bountyStateDigest(tip, 'different'), bountyStateDigest(tip, 'opaque-cursor-v1'));
  for (const [key, value] of Object.entries(row)) {
    const changed = typeof value === 'boolean' ? !value : typeof value === 'number' ? value + 1 : value + 'x';
    assert.notEqual(bountyImmutableDigest({ ...row, [key]: changed }), bountyImmutableDigest(row), key);
  }
  assert.equal(bountyImmutableDigest({ ...row, confirmations: 99, status: 'spent', spending_txid: '0'.repeat(64) }), bountyImmutableDigest(row));
  assert.equal(bountyStateDigest(Object.fromEntries(Object.entries(tip).reverse()), 'opaque-cursor-v1'), bountyStateDigest(tip, 'opaque-cursor-v1'));
});

test('compact baseline is full and an authenticated unchanged checkpoint is the only short ACK', async t => {
  const f = fixture(t), baseline = await f.changes();
  assert.deepEqual(baseline.tips, []); assert.equal(baseline.compact, 1);
  assert.equal(baseline.bounty_deltas, 1); assert.equal(baseline.block_deltas, 1);
  assert.deepEqual(baseline.changes, []); assert.equal(baseline.has_more, false);
  const known_state = bountyStateDigest(baseline.tip, baseline.next_cursor);
  assert.deepEqual(await f.changes(baseline.next_cursor, { known_state }), { compact: 1, unchanged: true });
  assert.deepEqual(await f.changes(baseline.next_cursor, { known_state: known_state.toUpperCase() }), { compact: 1, unchanged: true });
  const full = await f.changes(baseline.next_cursor);
  assert.equal(full.unchanged, undefined); assert.deepEqual(full.tip, baseline.tip);
  assert.ok(Buffer.byteLength(JSON.stringify({ compact: 1, unchanged: true })) < Buffer.byteLength(JSON.stringify(full)) / 4);
  assert.deepEqual(f.calls, []);
});

test('compact false retains exact old response and subscriptions retain their old shapes', async t => {
  const f = fixture(t), initial = await f.call('getbountychanges', { include_bounties: true });
  const plain = await f.call('getbountychanges', { cursor: initial.next_cursor });
  assert.deepEqual(await f.call('getbountychanges', { cursor: initial.next_cursor, compact: false }), plain);
  assert.deepEqual(await f.call('getbountychanges', { include_bounties: true, compact: false }), initial);
  await f.call('subscribebounties');
  f.pool([tx('pending', [], [{ txid: f.funding.txid, vout: 0 }])]);
  const notice = f.notices[0].params;
  assert.equal(notice.kind, 'bounties'); assert.equal(notice.changes.length, 1);
  assert.equal(notice.changes[0].bounty, undefined); assert.equal(notice.compact, undefined); assert.equal(notice.tips, undefined);
});

for (const value of [null, 0, 1, '', 'true', [], {}]) {
  test(`compact boolean validation rejects ${JSON.stringify(value)}`, async t => {
    const f = fixture(t);
    await assert.rejects(f.changes(undefined, { compact: value }), { code: -32602 });
  });
}

test('known_state and compact require explicit compatible capability parameters', async t => {
  const f = fixture(t), baseline = await f.changes(), known_state = bountyStateDigest(baseline.tip, baseline.next_cursor);
  for (const params of [
    { compact: true }, { compact: true, include_bounties: false },
    { known_state }, { known_state, compact: false },
    { known_state, compact: true, include_bounties: true },
    { known_state, ...f.params() }, { known_state, ...f.params(), cursor: null },
    { known_state, ...f.params(), cursor: 1 },
    { known_state, include_bounties: true, compact: true, cursor: baseline.next_cursor },
    { ...f.params(), cursor: baseline.next_cursor, known_state, extra: true },
  ]) await assert.rejects(f.call('getbountychanges', params), { code: -32602 });
  for (const value of [null, 0, true, {}, [], '', 'a'.repeat(63), 'g'.repeat(64), 'a'.repeat(65)]) {
    await assert.rejects(f.changes(baseline.next_cursor, { known_state: value }), { code: -32602 });
  }
});

test('same hash but different metadata cannot receive an unchanged ACK', async t => {
  const f = fixture(t), baseline = await f.changes();
  for (const mutation of [{ chain: 'other' }, { genesis_hash: hash('other') }, { mediantime: baseline.tip.mediantime + 1 },
    { height: baseline.tip.height + 1 }, { hash: hash('other-tip') }]) {
    const result = await f.changes(baseline.next_cursor, { known_state: bountyStateDigest({ ...baseline.tip, ...mutation }, baseline.next_cursor) });
    assert.equal(result.unchanged, undefined); assert.deepEqual(result.tip, baseline.tip);
  }
  const canonical = f.store.tip.bind(f.store);
  f.store.tip = () => ({ ...canonical(), mediantime: baseline.tip.mediantime + 1 });
  const changed = await f.changes(baseline.next_cursor, { known_state: bountyStateDigest(baseline.tip, baseline.next_cursor) });
  assert.equal(changed.unchanged, undefined); assert.equal(changed.tip.mediantime, baseline.tip.mediantime + 1);
});

test('forged expired future wrong-kind and restart cursors never receive unchanged ACK', async t => {
  const f = fixture(t), baseline = await f.changes();
  const cursors = [baseline.next_cursor + 'x', f.api.changeCursor(f.api.sequence + 1),
    f.api.cursors.sign({ kind: 'history', sequence: f.api.sequence }),
    f.api.cursors.sign({ kind: 'changes', sequence: 1.5 })];
  const other = new PublicAPI({ store: f.store, indexer: f.indexer, backend: f.backend });
  cursors.push(other.changeCursor()); other.close();
  for (const cursor of cursors) await assert.rejects(f.changes(cursor, { known_state: bountyStateDigest(baseline.tip, cursor) }), { code: -32011 });
  f.publish({ reorg: true, bountyChanges: [] });
  await assert.rejects(f.changes(baseline.next_cursor, { known_state: bountyStateDigest(baseline.tip, baseline.next_cursor) }), { code: -32011 });
});

test('readiness and indexed-anchor validation precede unchanged ACK', async t => {
  const f = fixture(t), baseline = await f.changes(), extra = { known_state: bountyStateDigest(baseline.tip, baseline.next_cursor) };
  f.indexer.ready = false;
  await assert.rejects(f.changes(baseline.next_cursor, extra), { code: -32001 });
  f.indexer.ready = true;
  f.store.db.prepare('UPDATE blocks SET bounty_indexed=0 WHERE height=?').run(baseline.tip.height);
  await assert.rejects(f.changes(baseline.next_cursor, extra), { code: -32011 });
});

test('new tip, stale base and new events return full responses even with matching state digest', async t => {
  const f = fixture(t), baseline = await f.changes(), cursor = baseline.next_cursor;
  f.publish(f.mine('next-empty'));
  const state = bountyStateDigest(f.store.tip(), cursor);
  const stale = await f.changes(cursor, { known_tip: { height: baseline.tip.height, hash: baseline.tip.hash }, known_state: state });
  assert.equal(stale.unchanged, undefined); assert.equal(stale.block_delta.blocks.length, 1);
  f.pool([tx('pending-new', [], [{ txid: f.funding.txid, vout: 0 }])]);
  const event = await f.changes(cursor, { known_state: bountyStateDigest(f.store.tip(), cursor) });
  assert.equal(event.unchanged, undefined); assert.equal(event.changes.length, 1);
});

test('new events that return to the original state are never hidden behind an ACK', async t => {
  const f = fixture(t), baseline = await f.changes();
  f.pool([tx('temporary', [], [{ txid: f.funding.txid, vout: 0 }])]); f.pool([]);
  const result = await f.changes(baseline.next_cursor, { known_state: bountyStateDigest(baseline.tip, baseline.next_cursor) });
  assert.deepEqual(result.changes.map(x => x.type), ['pending_spend', 'available_again']);
  assert.equal(result.unchanged, undefined); assert.notEqual(result.next_cursor, baseline.next_cursor);
});

test('tip references preserve original publication tips across pages and later chain changes', async t => {
  const f = fixture(t, { pageSize: 1 }), baseline = await f.changes();
  f.pool([tx('both', [], [0, 1].map(vout => ({ txid: f.funding.txid, vout })))]);
  const first = await f.changes(baseline.next_cursor);
  assert.equal(first.has_more, true); assert.equal(first.changes[0].tip, undefined);
  assert.deepEqual(first.tips[first.changes[0].tip_ref], baseline.tip);
  f.publish(f.mine('tip-after-first-page'));
  const second = await f.changes(first.next_cursor);
  assert.equal(second.has_more, false); assert.notDeepEqual(second.tip, baseline.tip);
  assert.deepEqual(second.tips[second.changes[0].tip_ref], baseline.tip);
  const replay = await f.changes(baseline.next_cursor);
  assert.deepEqual(replay.changes, first.changes); assert.deepEqual(replay.tips, first.tips);
});

test('compact publication tables deduplicate tips and preserve final state and divergent legacy spender', async t => {
  const f = fixture(t), baseline = await f.changes();
  const pending = tx('coalesced-pending', [], [{ txid: f.funding.txid, vout: 0 }]);
  const added = f.store.replaceMempool([pending]), removed = f.store.replaceMempool([]);
  f.publish({ bountyChanges: [...added.bountyChanges, ...removed.bountyChanges] });
  const page = await f.changes(baseline.next_cursor), row = f.store.bountyByOutpoint(f.funding.txid, 0);
  assert.equal(page.tips.length, 1); assert.equal(page.changes.length, 2);
  assert.deepEqual(page.changes.map(e => e.tip_ref), [0, 0]);
  for (const event of page.changes) {
    assert.equal(event.bounty, undefined); assert.equal(event.bounty_ref, bountyImmutableDigest(row));
    assert.deepEqual(event.bounty_delta, { status: 'available', spending_txid: null, confirmations: row.confirmations });
  }
  assert.equal(page.changes[0].spending_txid, pending.txid);
  assert.equal(page.changes[0].bounty_delta.spending_txid, null);
  const legacy = await f.changes(baseline.next_cursor, { compact: false });
  assert.equal(legacy.compact, undefined); assert.equal(legacy.tips, undefined);
  assert.deepEqual(legacy.changes[0].bounty, row);
});

test('added events retain complete rows, even when already spent at publication', async t => {
  const f = fixture(t), baseline = await f.changes();
  const funding = tx('new-funding', [bounty]), created = f.mine('new-funding-block', [funding]);
  const spending = tx('new-spending', [], [{ txid: funding.txid, vout: 0 }]), spent = f.mine('new-spending-block', [spending]);
  f.publish({ bountyChanges: [...created.bountyChanges, ...spent.bountyChanges] });
  const page = await f.changes(baseline.next_cursor);
  assert.equal(page.changes[0].type, 'added'); assert.equal(page.changes[0].bounty.status, 'spent');
  assert.equal(page.changes[0].bounty_ref, undefined);
  assert.equal(page.changes[1].type, 'spent'); assert.equal(page.changes[1].bounty, undefined);
  assert.equal(page.changes[1].bounty_ref, bountyImmutableDigest(page.changes[0].bounty));
});

test('matured event preserves its final state and binds immutable coinbase identity', async t => {
  const f = fixture(t), row = f.store.bountyByOutpoint(f.funding.txid, 0), baseline = await f.changes();
  f.publish({ bountyChanges: [{ type: 'matured', txid: row.txid, vout: row.vout }] });
  const event = (await f.changes(baseline.next_cursor)).changes[0];
  assert.equal(event.bounty_ref, bountyImmutableDigest(row));
  assert.deepEqual(event.bounty_delta, { status: row.status, spending_txid: row.spending_txid, confirmations: row.confirmations });
});

test('compact journal overflow and window exit keep invalidation and removal semantics', async t => {
  const f = fixture(t, { maxJournalEvents: 1 }), baseline = await f.changes();
  f.pool([tx('overflow', [], [0, 1].map(vout => ({ txid: f.funding.txid, vout })))]);
  await assert.rejects(f.changes(baseline.next_cursor), { code: -32011 });
  const current = await f.changes();
  f.publish({ bountyChanges: [{ type: 'window_exit', txid: f.funding.txid, vout: 0 }] });
  const event = (await f.changes(current.next_cursor)).changes[0];
  assert.equal(event.bounty, null); assert.equal(event.bounty_ref, undefined); assert.equal(event.bounty_delta, undefined);
  assert.equal(event.txid, f.funding.txid); assert.equal(event.vout, 0);
});

test('compact read is coherent against a publication queued after response construction', async t => {
  const f = fixture(t), baseline = await f.changes();
  const pending = tx('after-ack', [], [{ txid: f.funding.txid, vout: 0 }]);
  queueMicrotask(() => f.pool([pending]));
  const ack = await f.changes(baseline.next_cursor, { known_state: bountyStateDigest(baseline.tip, baseline.next_cursor) });
  assert.deepEqual(ack, { compact: 1, unchanged: true });
  const next = await f.changes(baseline.next_cursor);
  assert.equal(next.changes.length, 1); assert.equal(next.changes[0].bounty_delta.spending_txid, pending.txid);
});

test('recent counts are opt-in, include every output state, and do not change old responses', async t => {
  const f = fixture(t);
  f.pool([tx('pending-count', [], [{ txid: f.funding.txid, vout: 0 }])]);
  const spent = tx('spent-count', [], [{ txid: f.funding.txid, vout: 1 }]);
  f.publish(f.mine('counts-all', [spent, tx('coinbase-count', [bounty], [{ coinbase: '02' }])]));
  const legacy = await f.call('getrecentblockhashes');
  assert.deepEqual(await f.call('getrecentblockhashes', { include_bounty_counts: false }), legacy);
  const counts = await f.call('getrecentblockhashes', { include_bounty_counts: true });
  assert.equal(counts.bounty_counts, 1); assert.equal(counts.window, 600); assert.deepEqual(counts.tip, legacy.tip);
  assert.deepEqual(counts.blocks.map(({ height, hash }) => ({ height, hash })), legacy.blocks);
  assert.deepEqual(counts.blocks.map(x => x.bounty_count), [1, 2, 0]);
  for (const block of counts.blocks) assert.equal(block.bounty_count, [...f.store.bounties(block.hash)].length);
  assert.equal((await f.call('getrecentblockhashes', { count: 1, include_bounty_counts: true })).blocks.length, 1);
  assert.deepEqual(f.calls, []);
});

test('recent count requests reject malformed flags and unavailable indexing, never infer zero', async t => {
  const f = fixture(t);
  for (const include_bounty_counts of [null, 0, 1, '', 'true', {}, []]) {
    await assert.rejects(f.call('getrecentblockhashes', { include_bounty_counts }), { code: -32602 });
  }
  f.indexer.ready = false;
  await assert.rejects(f.call('getrecentblockhashes', { include_bounty_counts: true }), { code: -32001 });
  f.indexer.ready = true;
  f.store.db.prepare('UPDATE blocks SET bounty_indexed=0 WHERE height=0').run();
  await assert.rejects(f.call('getrecentblockhashes', { include_bounty_counts: true }), { code: -32011 });
  assert.equal((await f.call('getrecentblockhashes', { count: 1, include_bounty_counts: true })).blocks.length, 1);
  f.store.db.prepare('UPDATE blocks SET bounty_indexed=1 WHERE height=0').run();
  f.publish(f.mine('count-hole'));
  f.publish(f.mine('count-after-hole'));
  f.store.db.prepare('DELETE FROM blocks WHERE height=2').run();
  await assert.rejects(f.call('getrecentblockhashes', { include_bounty_counts: true }), { code: -32011 });
});

test('recent counts reject stale tips and remain coherent across queued block publication', async t => {
  const f = fixture(t), before = f.store.tip();
  assert.throws(() => f.store.recentBlocksWithBountyCounts(600, { ...before, hash: hash('other') }), /Inconsistent/);
  for (const count of [0, 601, 1.1, '1']) assert.throws(() => f.store.recentBlocksWithBountyCounts(count, before), /Invalid/);
  queueMicrotask(() => f.publish(f.mine('after-counts', [tx('after-counts-funding', [bounty])])));
  const page = await f.call('getrecentblockhashes', { include_bounty_counts: true });
  assert.deepEqual(page.tip, before); assert.equal(page.blocks[0].height, before.height);
  const next = await f.call('getrecentblockhashes', { include_bounty_counts: true });
  assert.equal(next.tip.height, before.height + 1); assert.equal(next.blocks[0].bounty_count, 1);
});

test('full recent count window never infers zero for a pruned block re-entering after rollback', async t => {
  const f = fixture(t);
  for (let height = 2; height <= 601; height++) f.publish(f.mine(`window-${height}`));
  const newest = await f.call('getrecentblockhashes', { include_bounty_counts: true });
  assert.equal(newest.blocks.length, 600); assert.equal(newest.blocks.at(-1).height, 2);
  assert.ok(newest.blocks.every(block => block.bounty_count === 0));
  f.publish(f.store.rollbackTip());
  await assert.rejects(f.call('getrecentblockhashes', { include_bounty_counts: true }), { code: -32011 });
  f.store.restoreBountyBlock({ hash: hash('funding'), height: 1, mediantime: 1700000010,
    previousblockhash: hash('genesis'), tx: [f.funding] });
  const restored = await f.call('getrecentblockhashes', { include_bounty_counts: true });
  assert.equal(restored.blocks.length, 600); assert.equal(restored.blocks.at(-1).height, 1);
  assert.equal(restored.blocks.at(-1).bounty_count, 2);
  f.publish(f.mine('replacement-tip'));
  const replacement = await f.call('getrecentblockhashes', { include_bounty_counts: true });
  assert.equal(replacement.blocks[0].hash, hash('replacement-tip'));
  assert.equal(replacement.blocks.at(-1).height, 2);
});

test('raw-only transaction uses nonverbose backend with original envelope and unchanged legacy response', async t => {
  const f = fixture(t), hex = '02000000' + 'ab'.repeat(20), verbose = { hex, txid: f.funding.txid, vin: [], vout: [], size: 24 };
  f.backend.transaction = async (...args) => { f.calls.push({ method: 'verbose', args }); return verbose; };
  f.backend.rawTransaction = async (...args) => { f.calls.push({ method: 'raw', args }); return hex; };
  const legacy = await f.call('gettransaction', { txid: f.funding.txid });
  assert.deepEqual(await f.call('gettransaction', { txid: f.funding.txid, raw_only: false }), legacy);
  const raw = await f.call('gettransaction', { txid: f.funding.txid.toUpperCase(), raw_only: true });
  assert.deepEqual(raw, { ...legacy, transaction: { hex } });
  assert.equal(f.calls.at(-1).method, 'raw'); assert.equal(f.calls.at(-1).args[0], f.funding.txid);
  assert.equal(f.calls.at(-1).args[1], hash('funding')); assert.equal(f.calls.at(-1).args[2].signal, f.context.signal);
});

test('raw-only validates flags and backend hex and sanitizes backend failures', async t => {
  const f = fixture(t);
  for (const raw_only of [null, 0, 1, 'true', {}, []]) {
    await assert.rejects(f.call('gettransaction', { txid: f.funding.txid, raw_only }), { code: -32602 });
  }
  assert.deepEqual(f.calls, []);
  for (const hex of [null, 5, {}, '', 'ab'.repeat(9), 'a'.repeat(21), 'z'.repeat(20)]) {
    f.backend.rawTransaction = async () => hex;
    await assert.rejects(f.call('gettransaction', { txid: f.funding.txid, raw_only: true }), { code: -32002 });
  }
  f.backend.rawTransaction = async () => { throw new Error('PRIVATE_BACKEND_PATH'); };
  await assert.rejects(f.call('gettransaction', { txid: f.funding.txid, raw_only: true }), e => e.code === -32002 && !e.message.includes('PRIVATE'));
});

test('raw-only handles indexed pending transactions and allows unrelated normal tip advancement', async t => {
  const f = fixture(t), pending = tx('pending-raw'); f.pool([pending]);
  const hex = 'ab'.repeat(20);
  f.backend.rawTransaction = async (txid, blockHash) => {
    assert.equal(txid, pending.txid); assert.equal(blockHash, undefined);
    f.publish(f.mine('unrelated-raw-tip'));
    return hex;
  };
  const before = f.store.tip(), result = await f.call('gettransaction', { txid: pending.txid, raw_only: true });
  assert.equal(result.status, 'pending'); assert.equal(result.block_hash, undefined);
  assert.equal(result.tip.height, before.height + 1); assert.deepEqual(result.transaction, { hex });
});

for (const mutation of ['status', 'block_hash', 'block_height', 'missing', 'readiness']) {
  test(`raw-only post-fetch fence rejects changed ${mutation}`, async t => {
    const f = fixture(t), location = f.store.transactionLocation.bind(f.store);
    f.backend.rawTransaction = async () => {
      if (mutation === 'readiness') f.indexer.ready = false;
      else f.store.transactionLocation = id => mutation === 'missing' ? null : {
        ...location(id), [mutation]: mutation === 'status' ? 'pending' : mutation === 'block_height' ? 99 : hash('other-block'),
      };
      return 'ab'.repeat(20);
    };
    await assert.rejects(f.call('gettransaction', { txid: f.funding.txid, raw_only: true }), { code: -32011 });
  });
}

test('raw-only preserves configured transport response bounds', async t => {
  const f = fixture(t);
  f.backend.rawTransaction = async () => 'ab'.repeat(5000);
  const server = createRpcServer({ dispatch: f.api.dispatch, allowedMethods: METHODS,
    options: { maxResponseBytes: 1024 } });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const socket = net.connect(server.address().port, '127.0.0.1');
  t.after(async () => { socket.destroy(); await new Promise(resolve => server.close(resolve)); });
  await once(socket, 'connect'); let received = '';
  socket.on('data', data => { received += data; });
  socket.write(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'gettransaction',
    params: { txid: f.funding.txid, raw_only: true } }) + '\n');
  await once(socket, 'close');
  assert.equal(received, '');
});
