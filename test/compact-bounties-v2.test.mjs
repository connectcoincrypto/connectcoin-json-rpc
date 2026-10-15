import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { PublicAPI } from '../src/api.mjs';
import { Store } from '../src/store.mjs';
import { bountyStateDigest, compactBountyEvents } from '../src/bounty-codec.mjs';

const hash = value => createHash('sha256').update(value).digest('hex');
const output = { value: '0.01', type: 2, domain: 'example.com', connection_work_target: 'f'.repeat(64),
  root_certificates_version: 1, signature_algorithms_mask: 7 };
const tx = (name, outputs = [], inputs = [{ txid: hash(`external-${name}`), vout: 0 }]) => ({ txid: hash(name),
  vin: inputs, vout: outputs.map((row, n) => ({ ...row, n })) });

function fixture(t, options = {}) {
  const store = new Store(); store.pinNetwork('regtest', hash('genesis'));
  store.applyBlock({ hash: hash('genesis'), height: 0, mediantime: 1700000000,
    tx: [tx('genesis', [], [{ coinbase: '01' }])] });
  const indexer = new EventEmitter(); indexer.ready = true;
  const context = { ip: '127.0.0.1', signal: new AbortController().signal, onClose() {}, async notify() {} };
  const api = new PublicAPI({ store, indexer, backend: {}, options });
  t.after(() => { api.close(); store.close(); });
  const publish = update => indexer.emit('update', { ...update, tip: store.tip() });
  const mine = (name, transactions = []) => {
    const tip = store.tip();
    const update = store.applyBlock({ hash: hash(name), height: tip.height + 1, previousblockhash: tip.hash,
      mediantime: tip.mediantime + 10, tx: transactions }); publish(update); return update;
  };
  const funding = tx('funding', [output, output]); mine('funding', [funding]);
  const call = params => api.dispatch('getbountychanges', params, context);
  const initial = extra => call({ include_bounties: true, compact: true, compact_version: 2, ...extra });
  const request = (baseline, extra = {}) => call({ include_bounties: true, compact: true, compact_version: 2,
    cursor: baseline.next_cursor, known_tip: { height: baseline.tip.height, hash: baseline.tip.hash },
    known_state: bountyStateDigest(baseline.tip, baseline.next_cursor), ...extra });
  const pool = transactions => publish(store.replaceMempool(transactions));
  return { store, indexer, api, context, publish, mine, funding, call, initial, request, pool };
}

test('compact v2 requires an exact opt-in version and compatible flags', async t => {
  const f = fixture(t);
  for (const compact_version of [null, false, true, 0, 1, 3, '2', [], {}, 2.1]) {
    await assert.rejects(f.initial({ compact_version }), { code: -32602 });
  }
  for (const params of [{ compact_version: 2 }, { compact_version: 2, compact: true },
    { compact_version: 2, compact: false, include_bounties: true },
    { compact_version: 2, compact: true, include_bounties: false }]) {
    await assert.rejects(f.call(params), { code: -32602 });
  }
});

test('compact v2 initialization remains self-contained and v1 remains exact', async t => {
  const f = fixture(t), tip = f.store.tip(), known_tip = { height: tip.height, hash: tip.hash };
  const initial = await f.initial({ known_tip });
  assert.deepEqual(initial.tip, tip); assert.equal(typeof initial.next_cursor, 'string');
  assert.equal(initial.compact, 2); assert.deepEqual(initial.changes, []); assert.deepEqual(initial.tips, []);
  assert.equal(initial.has_more, false); assert.deepEqual(initial.block_delta, { base_height: tip.height, base_hash: tip.hash, blocks: [] });
  assert.equal(Object.hasOwn(initial, 'bounty_deltas'), false); assert.equal(Object.hasOwn(initial, 'block_deltas'), false);
  const v1 = await f.call({ compact: true, include_bounties: true, known_tip });
  assert.deepEqual(v1, { ...initial, compact: 1, bounty_deltas: 1, block_deltas: 1 });
  const noBase = await f.initial(); assert.equal(Object.hasOwn(noBase, 'block_delta'), false);
});

test('compact v2 unchanged ACK is exact and cursor authentication is mandatory', async t => {
  const f = fixture(t), baseline = await f.initial();
  assert.deepEqual(await f.request(baseline), { compact: 2, unchanged: true });
  for (const cursor of [baseline.next_cursor + 'x', f.api.changeCursor(f.api.sequence + 1),
    f.api.cursors.sign({ kind: 'history', sequence: f.api.sequence }),
    f.api.cursors.sign({ kind: 'changes', sequence: 0.5 })]) {
    await assert.rejects(f.request({ ...baseline, next_cursor: cursor }), { code: -32011 });
  }
  const other = new PublicAPI({ store: f.store, indexer: f.indexer, backend: {} });
  const restarted = other.changeCursor(); other.close();
  await assert.rejects(f.request({ ...baseline, next_cursor: restarted }), { code: -32011 });
  f.publish({ reorg: true, bountyChanges: [] });
  await assert.rejects(f.request(baseline), { code: -32011 });
});

test('compact v2 bound empty block advance reuses cursor and retains authoritative zero count', async t => {
  const f = fixture(t), baseline = await f.initial(); f.mine('empty-next');
  const tip = f.store.tip(), page = await f.request(baseline);
  assert.deepEqual(page, { compact: 2, tip: [tip.height, tip.hash, tip.mediantime], changes: [], tips: [],
    next_cursor: null, has_more: false, block_delta: [[null, 0]] });
});

test('compact v2 suffix preserves every intermediate hash and positive count', async t => {
  const f = fixture(t), baseline = await f.initial();
  f.mine('suffix-one'); const one = f.store.tip();
  f.mine('suffix-two', [tx('new-funding', [output, output])]); const two = f.store.tip();
  f.mine('suffix-three'); const tip = f.store.tip(), page = await f.request(baseline);
  assert.deepEqual(page.block_delta, [[one.hash, 0], [two.hash, 2], [null, 0]]);
  assert.deepEqual(page.tip, [tip.height, tip.hash, tip.mediantime]);
  assert.notEqual(page.next_cursor, null); assert.notEqual(page.next_cursor, baseline.next_cursor);
  assert.equal(page.changes.length, 2); assert.deepEqual(page.tips, [two]);
  assert.ok(page.changes.every(event => event.tip_ref === 0));
});

test('compact v2 bound state mutations alias only equal spender and current tip', async t => {
  const f = fixture(t), baseline = await f.initial();
  const pending = tx('pending', [], [{ txid: f.funding.txid, vout: 0 }]); f.pool([pending]);
  const page = await f.request(baseline), event = page.changes[0];
  assert.deepEqual(page.block_delta, []); assert.deepEqual(page.tips, []); assert.equal(event.tip_ref, -1);
  assert.equal(event.spending_txid_ref, true); assert.equal(Object.hasOwn(event, 'spending_txid'), false);
  assert.equal(event.bounty_delta.spending_txid, pending.txid); assert.equal(event.txid, f.funding.txid);
  assert.notEqual(page.next_cursor, null); assert.equal(page.has_more, false);
});

test('compact v2 divergent coalesced spender and absent spender remain distinct', async t => {
  const f = fixture(t), baseline = await f.initial();
  const pending = tx('temporary', [], [{ txid: f.funding.txid, vout: 0 }]);
  const added = f.store.replaceMempool([pending]), removed = f.store.replaceMempool([]);
  f.publish({ bountyChanges: [...added.bountyChanges, ...removed.bountyChanges] });
  const page = await f.request(baseline);
  assert.equal(page.changes[0].spending_txid, pending.txid);
  assert.equal(page.changes[0].bounty_delta.spending_txid, null);
  assert.equal(Object.hasOwn(page.changes[0], 'spending_txid_ref'), false);
  assert.equal(Object.hasOwn(page.changes[1], 'spending_txid'), false);
  assert.equal(Object.hasOwn(page.changes[1], 'spending_txid_ref'), false);
  assert.equal(page.changes[1].bounty_delta.status, 'available');
});

test('compact v2 added event removes only duplicate outer identity and retains the full row', async t => {
  const f = fixture(t), baseline = await f.initial(), funding = tx('new-added', [output]); f.mine('new-added-block', [funding]);
  const page = await f.request(baseline), event = page.changes[0];
  for (const field of ['txid', 'vout', 'block_hash', 'block_height']) assert.equal(Object.hasOwn(event, field), false);
  assert.deepEqual(event.bounty, f.store.bountyByOutpoint(funding.txid, 0));
  assert.equal(event.tip_ref, -1); assert.deepEqual(page.tips, []); assert.equal(event.type, 'added');
  assert.ok(Number.isSafeInteger(event.sequence));
  const legacy = await f.call({ compact: true, include_bounties: true, cursor: baseline.next_cursor });
  assert.equal(legacy.changes[0].txid, funding.txid); assert.equal(legacy.changes[0].vout, 0);
});

test('compact v2 encoder refuses contradictory or missing added identity before elision', async t => {
  const f = fixture(t), bounty = f.store.bountyByOutpoint(f.funding.txid, 0), tip = f.store.tip();
  const event = { type: 'added', sequence: 99, ...Object.fromEntries(['txid', 'vout', 'block_hash', 'block_height'].map(key => [key, bounty[key]])) };
  for (const field of ['txid', 'vout', 'block_hash', 'block_height']) {
    const wrong = { ...event, [field]: typeof event[field] === 'number' ? event[field] + 1 : hash('wrong') };
    assert.throws(() => compactBountyEvents([{ event: wrong, bounty, tip }], { version: 2, currentTip: tip }), { code: -32011 });
    const missing = { ...event }; delete missing[field];
    assert.throws(() => compactBountyEvents([{ event: missing, bounty, tip }], { version: 2, currentTip: tip }), { code: -32011 });
  }
});

test('compact v2 publication tip reference requires exact median time and network identity', async t => {
  const f = fixture(t), tip = f.store.tip(), bounty = f.store.bountyByOutpoint(f.funding.txid, 0);
  const event = { type: 'matured', txid: bounty.txid, vout: 0, sequence: 99 };
  for (const field of ['height', 'hash', 'mediantime', 'chain', 'genesis_hash']) {
    const currentTip = { ...tip, [field]: typeof tip[field] === 'number' ? tip[field] + 1 : `${tip[field]}-other` };
    const page = compactBountyEvents([{ event, bounty, tip }], { version: 2, currentTip });
    assert.equal(page.changes[0].tip_ref, 0); assert.deepEqual(page.tips, [tip]);
  }
});

test('compact v2 pagination preserves sequences and historical tips after a new block', async t => {
  const f = fixture(t, { pageSize: 1 }), baseline = await f.initial();
  f.pool([tx('two-pending', [], [0, 1].map(vout => ({ txid: f.funding.txid, vout })))]);
  const first = await f.request(baseline);
  assert.equal(first.has_more, true); assert.notEqual(first.next_cursor, null); assert.equal(first.changes[0].tip_ref, -1);
  f.mine('after-first-page');
  const second = await f.request({ tip: baseline.tip, next_cursor: first.next_cursor });
  assert.equal(second.has_more, false); assert.notEqual(second.next_cursor, null);
  assert.deepEqual(second.tips, [baseline.tip]); assert.equal(second.changes[0].tip_ref, 0);
  assert.ok(second.changes[0].sequence > first.changes[0].sequence);
  const replay = await f.request(baseline); assert.equal(replay.changes[0].sequence, first.changes[0].sequence);
});

for (const field of ['chain', 'genesis_hash', 'mediantime']) test(`compact v2 ${field} mismatch requires self-contained fallback`, async t => {
  const f = fixture(t), baseline = await f.initial(); f.mine('mismatch-next');
  const altered = { ...baseline.tip, [field]: typeof baseline.tip[field] === 'number' ? baseline.tip[field] + 1 : `${baseline.tip[field]}-other` };
  const page = await f.request(baseline, { known_state: bountyStateDigest(altered, baseline.next_cursor) });
  assert.deepEqual(page.tip, f.store.tip()); assert.equal(typeof page.next_cursor, 'string');
  assert.equal(Array.isArray(page.block_delta), false); assert.equal(page.block_delta.base_hash, baseline.tip.hash);
});

test('compact v2 missing state and wrong canonical base never authorize request references', async t => {
  const f = fixture(t), baseline = await f.initial(); f.mine('base-next');
  const noState = await f.call({ compact: true, compact_version: 2, include_bounties: true, cursor: baseline.next_cursor,
    known_tip: { height: baseline.tip.height, hash: baseline.tip.hash } });
  assert.deepEqual(noState.tip, f.store.tip()); assert.equal(typeof noState.next_cursor, 'string');
  const wrongTip = { ...baseline.tip, hash: hash('fork') };
  const fork = await f.request({ ...baseline, tip: wrongTip });
  assert.deepEqual(fork.tip, f.store.tip()); assert.equal(fork.block_delta, null); assert.equal(typeof fork.next_cursor, 'string');
});

test('compact v2 suffix larger than five blocks falls back without changing counts or cursor', async t => {
  const f = fixture(t), baseline = await f.initial();
  for (let index = 0; index < 6; index++) f.mine(`long-${index}`);
  const page = await f.request(baseline);
  assert.deepEqual(page.tip, f.store.tip()); assert.equal(page.block_delta, null);
  assert.equal(page.next_cursor, baseline.next_cursor); assert.equal(page.compact, 2);
});

test('compact v2 readiness and incomplete indexed counts cannot become ACKs or aliases', async t => {
  const f = fixture(t), baseline = await f.initial();
  f.indexer.ready = false; await assert.rejects(f.request(baseline), { code: -32001 }); f.indexer.ready = true;
  f.store.db.prepare('UPDATE blocks SET bounty_indexed=0 WHERE height=?').run(baseline.tip.height);
  await assert.rejects(f.request(baseline), { code: -32011 });
  f.store.db.prepare('UPDATE blocks SET bounty_indexed=1 WHERE height=?').run(baseline.tip.height);
  f.mine('incomplete-suffix');
  f.store.db.prepare('UPDATE blocks SET bounty_indexed=0 WHERE height=?').run(f.store.tip().height);
  await assert.rejects(f.request(baseline), { code: -32011 });
});

test('compact v2 window exits retain outpoints and mutations queued after ACK remain replayable', async t => {
  const f = fixture(t), baseline = await f.initial();
  queueMicrotask(() => f.publish({ bountyChanges: [{ type: 'window_exit', txid: f.funding.txid, vout: 0 }] }));
  assert.deepEqual(await f.request(baseline), { compact: 2, unchanged: true });
  const page = await f.request(baseline), event = page.changes[0];
  assert.equal(event.type, 'window_exit'); assert.equal(event.txid, f.funding.txid); assert.equal(event.vout, 0);
  assert.equal(event.bounty, null); assert.equal(event.tip_ref, -1); assert.notEqual(page.next_cursor, null);
});
