import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { PublicAPI } from '../src/api.mjs';
import { Store } from '../src/store.mjs';
import { bountyImmutableDigest, bountyStateDigest, compactBountyEvents } from '../src/bounty-codec.mjs';

const hash = value => createHash('sha256').update(value).digest('hex');
const output = { value: '0.01', type: 2, domain: 'example.com', connection_work_target: 'f'.repeat(64),
  root_certificates_version: 1, signature_algorithms_mask: 7 };
const tx = (name, outputs = [], inputs = [{ txid: hash(`external-${name}`), vout: 0 }]) => ({ txid: hash(name),
  vin: inputs, vout: outputs.map((row, n) => ({ ...row, n })) });

function fixture(t, options = {}) {
  const store = new Store(); store.pinNetwork('regtest', hash('genesis'));
  store.applyBlock({ hash: hash('genesis'), height: 0, mediantime: 1700000000, tx: [] });
  const indexer = new EventEmitter(); indexer.ready = true;
  const api = new PublicAPI({ store, indexer, backend: {}, options });
  const context = { ip: '127.0.0.1', signal: new AbortController().signal, onClose() {}, notify() {} };
  t.after(() => { api.close(); store.close(); });
  const publish = update => indexer.emit('update', { ...update, tip: store.tip() });
  const mine = (name, transactions = []) => {
    const tip = store.tip(); const update = store.applyBlock({ hash: hash(name), height: tip.height + 1,
      previousblockhash: tip.hash, mediantime: tip.mediantime + 10, tx: transactions }); publish(update); return update;
  };
  const funding = tx('funding', [output, output]); mine('funding', [funding]);
  const call = params => api.dispatch('getbountychanges', params, context);
  const initial = extra => call({ compact: true, include_bounties: true, compact_version: 3, ...extra });
  const request = (base, extra = {}) => call({ compact: true, include_bounties: true, compact_version: 3,
    cursor: base.next_cursor, known_tip: { height: base.tip.height, hash: base.tip.hash },
    known_state: bountyStateDigest(base.tip, base.next_cursor), ...extra });
  const pool = transactions => publish(store.replaceMempool(transactions));
  return { store, indexer, api, context, funding, publish, mine, call, initial, request, pool };
}

test('compact v3 is explicit and rejects incompatible flags/unknown versions', async t => {
  const f = fixture(t);
  for (const compact_version of [null, false, true, 0, 1, 4, '3', {}, [], 3.1]) {
    await assert.rejects(f.initial({ compact_version }), { code: -32602 });
  }
  for (const params of [{ compact_version: 3 }, { compact_version: 3, compact: true },
    { compact_version: 3, compact: false, include_bounties: true },
    { compact_version: 3, compact: true, include_bounties: false }]) {
    await assert.rejects(f.call(params), { code: -32602 });
  }
});

test('v3 initial response omits only defaults and preserves explicit full identity/cursor', async t => {
  const f = fixture(t), tip = f.store.tip(), known_tip = { height: tip.height, hash: tip.hash };
  const page = await f.initial({ known_tip });
  assert.deepEqual(page, { tip, compact: 3, next_cursor: f.api.changeCursor(),
    block_delta: { base_height: tip.height, base_hash: tip.hash, blocks: [] } });
  const v2 = await f.initial({ compact_version: 2, known_tip });
  assert.deepEqual(v2, { ...page, compact: 2, changes: [], tips: [], has_more: false });
  const v1 = await f.call({ compact: true, include_bounties: true, known_tip });
  assert.deepEqual(v1, { ...v2, compact: 1, bounty_deltas: 1, block_deltas: 1 });
});

test('v3 unchanged ACK authenticates cursor, full state and coherent indexed anchor', async t => {
  const f = fixture(t), base = await f.initial();
  assert.deepEqual(await f.request(base), { compact: 3, unchanged: true });
  for (const cursor of [base.next_cursor + 'x', f.api.changeCursor(f.api.sequence + 1),
    f.api.cursors.sign({ kind: 'history', sequence: f.api.sequence })]) {
    await assert.rejects(f.request({ ...base, next_cursor: cursor }), { code: -32011 });
  }
  f.store.db.prepare('UPDATE blocks SET bounty_indexed=0 WHERE height=?').run(base.tip.height);
  await assert.rejects(f.request(base), { code: -32011 });
  f.store.db.prepare('UPDATE blocks SET bounty_indexed=1 WHERE height=?').run(base.tip.height);
  f.indexer.ready = false; await assert.rejects(f.request(base), { code: -32001 });
});

test('v3 empty block advance preserves authoritative zero and omits unchanged cursor', async t => {
  const f = fixture(t), base = await f.initial(); f.mine('empty'); const tip = f.store.tip();
  assert.deepEqual(await f.request(base), { tip: [tip.height, tip.hash, tip.mediantime], compact: 3, block_delta: [[null, 0]] });
  const old = await f.request(base, { compact_version: 2 });
  assert.deepEqual(old, { tip: [tip.height, tip.hash, tip.mediantime], compact: 2, block_delta: [[null, 0]],
    next_cursor: null, has_more: false, tips: [], changes: [] });
});

test('v3 same-tip event retains full cursor and derives only confirmations', async t => {
  const f = fixture(t), base = await f.initial(), claim = tx('claim', [], [{ txid: f.funding.txid, vout: 0 }]);
  f.pool([claim]); const page = await f.request(base), event = page.changes[0];
  assert.equal(page.tip, null); assert.deepEqual(page.block_delta, []); assert.equal(typeof page.next_cursor, 'string');
  assert.notEqual(page.next_cursor, base.next_cursor); assert.equal(event.tip_ref, -1);
  assert.equal(Object.hasOwn(event.bounty_delta, 'confirmations'), false);
  assert.equal(event.bounty_delta.status, 'pending_spend'); assert.equal(event.bounty_delta.spending_txid, claim.txid);
  assert.equal(event.spending_txid_ref, true); assert.equal(event.bounty_ref, bountyImmutableDigest(f.store.bountyByOutpoint(f.funding.txid, 0)));
  const v2 = await f.request(base, { compact_version: 2 }); assert.equal(v2.changes[0].bounty_delta.confirmations, 1);
});

test('v3 historical request-base tip uses -2 while publication confirmation arithmetic remains historical', async t => {
  const f = fixture(t), base = await f.initial();
  f.pool([tx('claim', [], [{ txid: f.funding.txid, vout: 0 }])]); f.mine('next');
  const page = await f.request(base), v2 = await f.request(base, { compact_version: 2 });
  assert.equal(page.changes[0].tip_ref, -2); assert.equal(Object.hasOwn(page, 'tips'), false);
  assert.deepEqual(v2.tips, [base.tip]); assert.equal(v2.changes[0].bounty_delta.confirmations, 1);
  assert.equal(f.store.bountyByOutpoint(f.funding.txid, 0).confirmations, 2);
});

for (const field of ['height', 'hash', 'mediantime', 'chain', 'genesis_hash']) {
  test(`v3 historical base alias requires exact ${field}`, t => {
    const f = fixture(t), tip = f.store.tip(), bounty = f.store.bountyByOutpoint(f.funding.txid, 0);
    const event = { type: 'matured', txid: bounty.txid, vout: 0, sequence: 9 };
    const baseTip = { ...tip, [field]: typeof tip[field] === 'number' ? tip[field] + 1 : hash('different') };
    const encoded = compactBountyEvents([{ event, tip, bounty }], { version: 3, baseTip, currentTip: { ...tip, height: tip.height + 2 } });
    assert.equal(encoded.changes[0].tip_ref, 0); assert.deepEqual(encoded.tips, [tip]);
  });
}

test('v3 unbound requests cannot reference request tip even if height/hash agree', async t => {
  const f = fixture(t), base = await f.initial(); f.pool([tx('claim', [], [{ txid: f.funding.txid, vout: 0 }])]);
  f.mine('after');
  for (const field of ['chain', 'genesis_hash', 'mediantime']) {
    const altered = { ...base.tip, [field]: typeof base.tip[field] === 'number' ? base.tip[field] + 1 : hash('different') };
    const page = await f.request(base, { known_state: bountyStateDigest(altered, base.next_cursor) });
    assert.deepEqual(page.tip, f.store.tip()); assert.equal(Array.isArray(page.block_delta), false);
    assert.equal(page.changes[0].tip_ref, 0); assert.deepEqual(page.tips, [base.tip]);
  }
  const noState = await f.call({ compact: true, compact_version: 3, include_bounties: true,
    cursor: base.next_cursor, known_tip: { height: base.tip.height, hash: base.tip.hash } });
  assert.deepEqual(noState.tip, f.store.tip()); assert.deepEqual(noState.tips, [base.tip]);
});

test('v3 unavailable suffix and invalidated cursor retain reset/fallback semantics', async t => {
  const f = fixture(t), base = await f.initial(); f.pool([tx('claim', [], [{ txid: f.funding.txid, vout: 0 }])]);
  for (let i = 0; i < 6; i++) f.mine(`long-${i}`);
  const page = await f.request(base); assert.deepEqual(page.tip, f.store.tip()); assert.equal(page.block_delta, null);
  assert.equal(page.changes[0].tip_ref, 0); assert.deepEqual(page.tips, [base.tip]);
  f.publish({ reorg: true, bountyChanges: [] }); await assert.rejects(f.request(base), { code: -32011 });
});

test('v3 malformed original confirmations cannot be hidden by omission; v1/v2 are unchanged', t => {
  const f = fixture(t), tip = f.store.tip(), row = f.store.bountyByOutpoint(f.funding.txid, 0);
  const event = { type: 'spent', txid: row.txid, vout: row.vout, sequence: 9 };
  for (const confirmations of [undefined, null, '1', -1, 0, 2, NaN, Number.MAX_SAFE_INTEGER + 1]) {
    const bounty = { ...row, confirmations };
    assert.throws(() => compactBountyEvents([{ event, tip, bounty }], { version: 3, currentTip: tip }), { code: -32011 });
    for (const version of [1, 2]) assert.deepEqual(compactBountyEvents([{ event, tip, bounty }], { version, currentTip: tip }).changes[0].bounty_delta.confirmations, confirmations);
  }
  for (const block_height of [undefined, null, '1', -1, tip.height + 1, Number.MAX_SAFE_INTEGER + 1]) {
    assert.throws(() => compactBountyEvents([{ event, tip, bounty: { ...row, block_height } }], { version: 3 }), { code: -32011 });
  }
});

test('v3 coalesced mutations retain divergent outer spender and final state', async t => {
  const f = fixture(t), base = await f.initial(), claim = tx('temporary', [], [{ txid: f.funding.txid, vout: 0 }]);
  const a = f.store.replaceMempool([claim]), b = f.store.replaceMempool([]);
  f.publish({ bountyChanges: [...a.bountyChanges, ...b.bountyChanges] });
  const page = await f.request(base);
  assert.equal(page.changes.length, 2); assert.equal(page.changes[0].spending_txid, claim.txid);
  assert.equal(page.changes[0].bounty_delta.spending_txid, null); assert.equal(page.changes[0].bounty_delta.status, 'available');
  assert.equal(Object.hasOwn(page.changes[0], 'spending_txid_ref'), false);
  assert.equal(Object.hasOwn(page.changes[1], 'spending_txid'), false);
  assert.ok(page.changes.every(e => !Object.hasOwn(e.bounty_delta, 'confirmations')));
});

test('v3 pages preserve cursor progression, has_more:true, historical tips and added counts', async t => {
  const f = fixture(t, { pageSize: 1 }), base = await f.initial();
  const funding = tx('two-added', [output, output]); f.mine('new-block', [funding]); const addedTip = f.store.tip();
  const first = await f.request(base); assert.equal(first.has_more, true); assert.equal(first.changes.length, 1);
  assert.equal(first.changes[0].tip_ref, -1); assert.equal(first.changes[0].bounty.confirmations, 1);
  assert.deepEqual(first.block_delta, [[null, 2]]);
  f.mine('after-first');
  const second = await f.request({ tip: base.tip, next_cursor: first.next_cursor });
  assert.equal(Object.hasOwn(second, 'has_more'), false); assert.deepEqual(second.tips, [addedTip]);
  assert.equal(second.changes[0].tip_ref, 0); assert.equal(second.changes[0].bounty.confirmations, 1);
  assert.ok(second.changes[0].sequence > first.changes[0].sequence);
  assert.deepEqual(second.block_delta, [[addedTip.hash, 2], [null, 0]]);
  assert.equal((await f.request(base)).changes[0].sequence, first.changes[0].sequence);
});

test('v3 added identity mismatches remain errors and rows stay complete', t => {
  const f = fixture(t), tip = f.store.tip(), bounty = f.store.bountyByOutpoint(f.funding.txid, 0);
  const event = { type: 'added', sequence: 9, ...Object.fromEntries(['txid', 'vout', 'block_hash', 'block_height'].map(k => [k, bounty[k]])) };
  const good = compactBountyEvents([{ event, bounty, tip }], { version: 3, currentTip: tip });
  assert.deepEqual(good.changes[0].bounty, bounty);
  for (const field of ['txid', 'vout', 'block_hash', 'block_height']) {
    const bad = { ...event }; delete bad[field];
    assert.throws(() => compactBountyEvents([{ event: bad, bounty, tip }], { version: 3, currentTip: tip }), { code: -32011 });
  }
});

test('v3 window_exit retains explicit identity and queued events survive ACK', async t => {
  const f = fixture(t), base = await f.initial();
  queueMicrotask(() => f.publish({ bountyChanges: [{ type: 'window_exit', txid: f.funding.txid, vout: 0 }] }));
  assert.deepEqual(await f.request(base), { compact: 3, unchanged: true });
  const page = await f.request(base); assert.equal(page.tip, null);
  assert.deepEqual(page.changes[0], { type: 'window_exit', txid: f.funding.txid, vout: 0,
    sequence: 3, bounty: null, tip_ref: -1 });
  assert.equal(typeof page.next_cursor, 'string');
});

test('v3 coinbase maturity derives exactly 100 confirmations without changing status', async t => {
  const f = fixture(t), coinbase = tx('coinbase', [output], [{ coinbase: '01' }]);
  f.mine('coinbase-block', [coinbase]);
  while (f.store.tip().height < 100) f.mine(`before-maturity-${f.store.tip().height}`);
  const base = await f.initial(); f.mine('maturity');
  const page = await f.request(base), event = page.changes.find(row => row.type === 'matured');
  assert.equal(event.txid, coinbase.txid); assert.equal(event.bounty_delta.status, 'available');
  assert.equal(event.bounty_delta.spending_txid, null); assert.equal(Object.hasOwn(event.bounty_delta, 'confirmations'), false);
  const old = await f.request(base, { compact_version: 2 });
  assert.equal(old.changes.find(row => row.type === 'matured').bounty_delta.confirmations, 100);
});

test('v3 spent plus same-publication window exit still resets safely rather than dropping an event', async t => {
  const f = fixture(t);
  while (f.store.tip().height < 600) f.mine(`before-expiry-${f.store.tip().height}`);
  const base = await f.initial();
  f.mine('spent-at-expiry', [tx('claim-at-expiry', [], [{ txid: f.funding.txid, vout: 0 }])]);
  await assert.rejects(f.request(base), { code: -32011 });
  assert.deepEqual((await f.initial()).tip, f.store.tip());
});
