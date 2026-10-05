import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, basename, resolve } from 'node:path';
import { Store } from '../src/store.mjs';
import { PublicAPI } from '../src/api.mjs';

const ALICE = 'cc1pljr5t7srjcssh6v5ucna9528khydrjrzkd0mfmlmcc8dhfj576fsfmh0g9';
const BOB = 'cc1p8w0r8l9z0lanx4h5ghvkfvlfanfjdqn0023x7qez6g6w4nc702nq2p2437';
const CAROL = 'cc1pacjc369srd0cjfxy6raj8fr4wktpkq6us6fwxg96xtvmssj0xkaqznkk74';
const hash = value => createHash('sha256').update(String(value)).digest('hex');
const tx = (name, outputs, inputs = []) => ({ txid: hash(name), vin: inputs.length ? inputs : [{ coinbase: '01' }],
  vout: outputs.map(([address, value], n) => ({ n, value, type: 1, scriptPubKey: { address } })) });
const input = (transaction, vout = 0) => ({ txid: transaction.txid, vout });
const block = (name, previous, transactions = []) => ({ hash: hash(name), height: previous ? previous.height + 1 : 0,
  previousblockhash: previous?.hash, mediantime: 1700000000 + (previous?.height ?? -1) * 10, tx: transactions });
function fixture(t, { pageSize = 2, storeOptions, path = ':memory:' } = {}) {
  const store = new Store(path, storeOptions);
  const funding = tx('funding', [[ALICE, '10'], [BOB, '20']]);
  const genesis = block('genesis', null, [funding]);
  store.pinNetwork('main', genesis.hash); store.applyBlock(genesis); store.replaceMempool([]);
  const indexer = new EventEmitter(); indexer.ready = true;
  const api = new PublicAPI({ store, indexer, backend: {}, options: { pageSize } });
  const context = { signal: new AbortController().signal };
  t.after(() => { api.close(); store.close(); });
  const call = (method, params) => api.dispatch(method, params, context);
  const changes = (cursor, addresses = [ALICE, BOB]) => call('getaddresschanges', { addresses, cursor });
  const watermark = async addresses => (await changes(null, addresses)).next_cursor;
  const drain = async (cursor, addresses) => {
    const events = [], pages = []; let response;
    do {
      response = await changes(cursor, addresses); pages.push(response); events.push(...response.changes);
      cursor = response.next_cursor;
    } while (response.has_more);
    return { changes: events, next_cursor: cursor, tip: response.tip, pages };
  };
  return { store, api, indexer, funding, genesis, call, changes, watermark, drain };
}
const apply = (state, changes) => {
  for (const event of changes) {
    const id = `${event.address}:${event.kind}:${event.txid}:${event.vout ?? ''}`;
    if (event.action === 'remove') state.delete(id); else state.set(id, event.item);
  }
  return state;
};

test('initial address cursor is only a watermark; unchanged tips need no historical reread', async t => {
  const f = fixture(t), cursor = await f.watermark();
  let calls = 0;
  f.store.historyPage = () => { calls++; throw new Error('must not read complete histories'); };
  f.store.utxoPage = () => { calls++; throw new Error('must not read complete UTXO sets'); };
  const first = await f.changes(cursor);
  assert.deepEqual(first.changes, []); assert.equal(first.has_more, false);
  assert.equal(first.through_sequence, f.store.addressChangeState().sequence);
  assert.equal(first.journal_epoch, f.store.addressChangeState().epoch);
  const tip = block('empty', f.genesis); f.store.applyBlock(tip); f.store.replaceMempool([]);
  const next = await f.changes(first.next_cursor);
  assert.deepEqual(next.changes, []); assert.equal(next.tip.hash, tip.hash); assert.equal(calls, 0);
});

test('mempool deltas include pending-spent confirmed and pending outputs; eviction restores originals', async t => {
  const f = fixture(t), start = await f.watermark([ALICE, BOB, CAROL]);
  const parent = tx('parent', [[CAROL, '8'], [ALICE, '1']], [input(f.funding)]);
  const child = tx('child', [[BOB, '7']], [input(parent)]);
  f.store.replaceMempool([child, parent]);
  const legacy = await f.call('getaddressutxos', { address: ALICE });
  assert.equal(legacy.items.length, 1); assert.equal(legacy.items[0].txid, parent.txid);
  assert.equal(Object.hasOwn(legacy.items[0], 'pending_spent_by'), false);
  const opted = await f.call('getaddressutxos', { address: ALICE, include_pending_spent: true });
  assert.equal(opted.items.length, 2);
  assert.equal(opted.items.find(row => row.txid === f.funding.txid).pending_spent_by, parent.txid);
  const pending = await f.call('getaddressutxos', { address: CAROL, include_pending_spent: true });
  assert.equal(pending.items[0].pending_spent_by, child.txid); assert.equal(pending.items[0].status, 'pending');
  assert.equal((await f.call('getaddressutxos', { address: CAROL })).items.length, 0);
  const delta = await f.drain(start, [ALICE, BOB, CAROL]);
  assert.ok(delta.changes.some(e => e.kind === 'utxo' && e.txid === f.funding.txid && e.item?.pending_spent_by === parent.txid));
  assert.ok(delta.changes.some(e => e.kind === 'utxo' && e.txid === parent.txid && e.item?.pending_spent_by === child.txid));
  f.store.replaceMempool([]);
  const removed = await f.drain(delta.next_cursor, [ALICE, BOB, CAROL]);
  assert.ok(removed.changes.some(e => e.kind === 'utxo' && e.txid === f.funding.txid && e.item?.pending_spent_by === null));
  assert.ok(removed.changes.some(e => e.kind === 'utxo' && e.txid === parent.txid && e.action === 'remove'));
  assert.ok(removed.changes.some(e => e.kind === 'history' && e.txid === child.txid && e.action === 'remove'));
  const state = apply(apply(new Map(), delta.changes), removed.changes);
  assert.equal([...state.values()].filter(row => row.status === 'pending').length, 0);
});

test('confirmation and parent-child same-block spends replay to the final canonical state', async t => {
  const f = fixture(t), addresses = [ALICE, BOB, CAROL];
  const parent = tx('parent', [[CAROL, '8'], [ALICE, '1']], [input(f.funding)]);
  const child = tx('child', [[BOB, '7']], [input(parent)]);
  f.store.replaceMempool([parent, child]);
  const start = await f.watermark(addresses);
  const next = block('confirm', f.genesis, [parent, child]);
  f.store.applyBlock(next); f.store.replaceMempool([]);
  const delta = await f.drain(start, addresses);
  const state = apply(new Map(), delta.changes);
  assert.ok(delta.changes.some(e => e.kind === 'utxo' && e.txid === f.funding.txid && e.action === 'remove'));
  assert.ok(delta.changes.some(e => e.kind === 'utxo' && e.txid === parent.txid && e.vout === 0 && e.action === 'remove'));
  assert.equal(state.get(`${BOB}:utxo:${child.txid}:0`).status, 'confirmed');
  assert.equal(state.get(`${BOB}:utxo:${child.txid}:0`).pending_spent_by, null);
  assert.equal(state.get(`${CAROL}:history:${parent.txid}:`).status, 'confirmed');
});

test('paged deltas freeze payloads, high watermark, and tip despite eviction and later blocks', async t => {
  const f = fixture(t, { pageSize: 1 }), start = await f.watermark();
  const pending = tx('pending', [[BOB, '9']], [input(f.funding)]);
  f.store.replaceMempool([pending]);
  const first = await f.changes(start);
  assert.equal(first.changes.length, 1); assert.equal(first.has_more, true);
  const upper = f.store.addressChangeState().sequence, epoch = f.store.addressChangeState().epoch;
  assert.equal(first.through_sequence, upper); assert.equal(first.journal_epoch, epoch);
  f.store.replaceMempool([]);
  const nextBlock = block('later', f.genesis); f.store.applyBlock(nextBlock); f.store.replaceMempool([]);
  const remaining = await f.drain(first.next_cursor);
  assert.ok(remaining.changes.some(e => e.txid === pending.txid && e.action === 'upsert'));
  assert.ok(remaining.pages.every(page => page.tip.hash === f.genesis.hash));
  assert.ok(remaining.pages.every(page => page.through_sequence === upper && page.journal_epoch === epoch));
  const next = await f.drain(remaining.next_cursor);
  assert.equal(next.tip.hash, nextBlock.hash);
  assert.ok(next.pages[0].through_sequence > upper);
  assert.ok(next.changes.some(e => e.txid === pending.txid && e.action === 'remove'));
  assert.deepEqual((await f.changes(next.next_cursor)).changes, []);
});

test('watermark before a live baseline repairs both behind-cursor insertion and removal', async t => {
  const f = fixture(t), addresses = [ALICE, BOB];
  const cursor = await f.watermark(addresses);
  const state = new Map();
  for (const address of addresses) for (const kind of ['history', 'utxo']) {
    const items = kind === 'history' ? f.store.historyPage(address) : f.store.utxoPage(address, { includePendingSpent: true });
    apply(state, items.map(item => ({ address, kind, action: 'upsert', txid: item.txid, vout: item.vout, item })));
  }
  const parent = tx('baseline-race', [[BOB, '9']], [input(f.funding)]);
  f.store.replaceMempool([parent]);
  const confirmed = block('baseline-confirmed', f.genesis, [parent]); f.store.applyBlock(confirmed); f.store.replaceMempool([]);
  apply(state, (await f.drain(cursor, addresses)).changes);
  assert.equal(state.has(`${ALICE}:utxo:${f.funding.txid}:0`), false);
  assert.equal(state.get(`${BOB}:utxo:${parent.txid}:0`).status, 'confirmed');
  assert.equal(state.get(`${ALICE}:history:${parent.txid}:`).spent, '100000000000');
});

test('address scope is order/case independent, duplicate/mismatched sets and wrong networks fail closed', async t => {
  const f = fixture(t), cursor = await f.watermark();
  assert.deepEqual((await f.changes(cursor, [BOB.toUpperCase(), ALICE])).changes, []);
  await assert.rejects(f.changes(cursor, [ALICE]), { code: -32011 });
  for (const addresses of [[], [ALICE, ALICE.toUpperCase()], Array(101).fill(ALICE), ['tcc1invalid'], [null]]) {
    await assert.rejects(f.changes(null, addresses), { code: -32602 });
  }
  await assert.rejects(f.changes(cursor + 'x'), { code: -32011 });
  await assert.rejects(f.call('getaddresschanges', { addresses: [ALICE], password: 'never-accepted' }), { code: -32602 });
  await assert.rejects(f.call('getaddressutxos', { address: ALICE, include_pending_spent: 1 }), { code: -32602 });
  await assert.rejects(f.call('getaddresshistory', { address: ALICE, include_pending_spent: true }), { code: -32602 });
});

test('UTXO cursors bind the pending-spent opt-in and old default pagination remains compatible', async t => {
  const f = fixture(t, { pageSize: 1 });
  const payment = tx('payment', [[ALICE, '8'], [ALICE, '1']], [input(f.funding)]);
  f.store.replaceMempool([payment]);
  const page = await f.call('getaddressutxos', { address: ALICE, include_pending_spent: true });
  assert.ok(page.next_cursor);
  await assert.rejects(f.call('getaddressutxos', { address: ALICE, cursor: page.next_cursor }), { code: -32011 });
  assert.equal((await f.call('getaddressutxos', { address: ALICE, cursor: page.next_cursor, include_pending_spent: true })).items.length, 1);
  const legacy = await f.call('getaddressutxos', { address: ALICE });
  assert.ok(legacy.next_cursor);
  assert.equal((await f.call('getaddressutxos', { address: ALICE, cursor: legacy.next_cursor })).items.length, 1);
});

test('rollback, mutation overflow, event retention and byte retention explicitly require baseline', async t => {
  for (const options of [{ maxAddressEvents: 2 }, { maxAddressBytes: 1 }, { maxAddressMutationKeys: 1 }]) {
    const f = fixture(t, { storeOptions: options }), cursor = await f.watermark();
    const payment = tx('overflow', [[BOB, '9']], [input(f.funding)]);
    f.store.replaceMempool([payment]);
    await assert.rejects(f.changes(cursor), { code: -32011 });
    const fresh = await f.watermark();
    assert.deepEqual((await f.changes(fresh)).changes, []);
  }
  const f = fixture(t), cursor = await f.watermark();
  f.store.applyBlock(block('tip', f.genesis)); f.store.rollbackTip();
  await assert.rejects(f.changes(cursor), { code: -32011 });
});

test('irrelevant address mutations advance the cursor without replay or full scans', async t => {
  const f = fixture(t), cursor = await f.watermark([CAROL]);
  const payment = tx('unrelated', [[BOB, '9']], [input(f.funding)]); f.store.replaceMempool([payment]);
  const delta = await f.changes(cursor, [CAROL]);
  assert.deepEqual(delta.changes, []); assert.equal(delta.has_more, false);
  const decoded = f.api.addressCursors.read(delta.next_cursor);
  assert.equal(decoded.sequence, f.store.addressChangeState().sequence);
});

test('journal and signing key persist across clean process restarts', async t => {
  const folder = mkdtempSync(join(tmpdir(), 'connectcoin-address-journal-'));
  t.after(() => {
    assert.equal(dirname(resolve(folder)), resolve(tmpdir()));
    assert.ok(basename(folder).startsWith('connectcoin-address-journal-'));
    rmSync(folder, { recursive: true, force: true });
  });
  const path = join(folder, 'index.sqlite');
  let store = new Store(path), api;
  const indexer = new EventEmitter(); indexer.ready = true;
  try {
    const funding = tx('restart-funding', [[ALICE, '10']]), genesis = block('restart-genesis', null, [funding]);
    store.pinNetwork('main', genesis.hash); store.applyBlock(genesis); store.replaceMempool([]);
    api = new PublicAPI({ store, indexer, backend: {} });
    const cursor = (await api.dispatch('getaddresschanges', { addresses: [ALICE] }, {})).next_cursor;
    const payment = tx('restart-payment', [[ALICE, '9']], [input(funding)]); store.replaceMempool([payment]);
    api.close(); store.close();
    store = new Store(path); api = new PublicAPI({ store, indexer, backend: {} });
    const delta = await api.dispatch('getaddresschanges', { addresses: [ALICE], cursor }, {});
    assert.ok(delta.changes.some(e => e.txid === payment.txid && e.kind === 'history'));
  } finally { api?.close(); store.close(); }
});

test('failed mempool/block mutations roll back journal events and watermark atomically', async t => {
  const f = fixture(t), state = f.store.addressChangeState(), cursor = await f.watermark();
  const a = tx('conflict-a', [[BOB, '9']], [input(f.funding)]);
  const b = tx('conflict-b', [[BOB, '8']], [input(f.funding)]);
  assert.throws(() => f.store.replaceMempool([a, b]));
  assert.deepEqual(f.store.addressChangeState(), state);
  assert.deepEqual((await f.changes(cursor)).changes, []);
  assert.throws(() => f.store.applyBlock(block('bad-block', f.genesis, [a, a])));
  assert.deepEqual(f.store.addressChangeState(), state);
  assert.equal(f.store.tip().hash, f.genesis.hash);
});

test('unchanged overlay and empty block do not re-journal all existing pending state', async t => {
  const f = fixture(t);
  const payment = tx('unchanged-overlay', [[BOB, '9']], [input(f.funding)]); f.store.replaceMempool([payment]);
  const cursor = await f.watermark(), before = f.store.addressChangeState();
  f.store.applyBlock(block('unchanged-pending-tip', f.genesis)); f.store.replaceMempool([payment]);
  assert.deepEqual(f.store.addressChangeState(), before);
  assert.deepEqual((await f.changes(cursor)).changes, []);
});

test('replayed cache equals full index through repeated replacements, descendant eviction, confirmations and maturation', async t => {
  const f = fixture(t, { pageSize: 3 }), addresses = [ALICE, BOB, CAROL], state = new Map();
  let cursor = await f.watermark(addresses), previous = f.funding, tip = f.genesis;
  for (const address of addresses) for (const kind of ['history', 'utxo']) {
    const items = kind === 'history' ? f.store.historyPage(address, { limit: 1000 }) : f.store.utxoPage(address, { limit: 1000, includePendingSpent: true });
    apply(state, items.map(item => ({ address, kind, action: 'upsert', txid: item.txid, vout: item.vout, item })));
  }
  const compare = async () => {
    const delta = await f.drain(cursor, addresses); apply(state, delta.changes); cursor = delta.next_cursor;
    for (const address of addresses) for (const kind of ['history', 'utxo']) {
      const expected = kind === 'history' ? f.store.historyPage(address, { limit: 1000 }) : f.store.utxoPage(address, { limit: 1000, includePendingSpent: true });
      const actual = [...state].filter(([id]) => id.startsWith(`${address}:${kind}:`)).map(([, value]) => {
        const confirmations = value.status === 'pending' ? 0 : delta.tip.height - value.block_height + 1;
        return { ...value, confirmations, ...(kind === 'utxo' ? { mature: !value.coinbase || confirmations >= 100 } : {}) };
      });
      const ordered = rows => rows.sort((a, b) => a.txid.localeCompare(b.txid) || (a.vout ?? 0) - (b.vout ?? 0));
      assert.deepEqual(ordered(actual), ordered(expected));
    }
  };
  for (let round = 0; round < 12; round++) {
    const parent = tx(`cycle-parent-${round}`, [[addresses[(round + 1) % 3], '8']], [input(previous)]);
    const child = tx(`cycle-child-${round}`, [[addresses[(round + 2) % 3], '7']], [input(parent)]);
    const replacement = tx(`cycle-replacement-${round}`, [[addresses[round % 3], '6']], [input(parent)]);
    f.store.replaceMempool([parent, child]); await compare();
    f.store.replaceMempool([replacement, parent]); await compare();
    f.store.replaceMempool([]); f.store.replaceMempool([parent, replacement]); await compare();
    tip = block(`cycle-block-${round}`, tip, [parent, replacement]);
    f.store.applyBlock(tip); f.store.replaceMempool([]); await compare();
    previous = replacement;
  }
  while (tip.height < 100) { tip = block(`mature-${tip.height}`, tip); f.store.applyBlock(tip); }
  f.store.replaceMempool([]); await compare();
});

test('delta lookup seeks the bounded per-address sequence index', async t => {
  const f = fixture(t);
  const plan = f.store.db.prepare(`EXPLAIN QUERY PLAN SELECT payload FROM address_journal WHERE address IN (?,?)
    AND sequence>? AND sequence<=? ORDER BY sequence LIMIT ?`).all(ALICE, BOB, 0, 100, 100);
  assert.ok(plan.some(row => /SEARCH address_journal USING INDEX address_journal_address_sequence/.test(row.detail)));
  assert.equal(plan.some(row => /SCAN address_journal/.test(row.detail)), false);
});

test('an intervening older writer invalidates persisted cursors instead of hiding an unjournaled gap', async t => {
  const folder = mkdtempSync(join(tmpdir(), 'connectcoin-address-old-writer-'));
  t.after(() => {
    assert.equal(dirname(resolve(folder)), resolve(tmpdir()));
    assert.ok(basename(folder).startsWith('connectcoin-address-old-writer-'));
    rmSync(folder, { recursive: true, force: true });
  });
  const path = join(folder, 'index.sqlite');
  let store = new Store(path), api;
  const indexer = new EventEmitter(); indexer.ready = true;
  try {
    const funding = tx('old-writer-funding', [[ALICE, '10']]), genesis = block('old-writer-genesis', null, [funding]);
    store.pinNetwork('main', genesis.hash); store.applyBlock(genesis);
    api = new PublicAPI({ store, indexer, backend: {} });
    const cursor = (await api.dispatch('getaddresschanges', { addresses: [ALICE] }, {})).next_cursor;
    // Simulate the older Store.bump(), which knows only accounting revision.
    store.setMeta('revision', store.revision() + 1);
    api.close(); store.close(); store = new Store(path); api = new PublicAPI({ store, indexer, backend: {} });
    await assert.rejects(api.dispatch('getaddresschanges', { addresses: [ALICE], cursor }, {}), { code: -32011 });
  } finally { api?.close(); store.close(); }
});

test('small additions in a large existing mempool do not exhaust the changed-key budget', async t => {
  const f = fixture(t, { storeOptions: { maxAddressMutationKeys: 4 } });
  const funding = tx('many-inputs', Array.from({ length: 20 }, () => [ALICE, '10']));
  const next = block('many-funding', f.genesis, [funding]); f.store.applyBlock(next); f.store.replaceMempool([]);
  const pool = Array.from({ length: 10 }, (_, index) => tx(`stable-pool-${index}`, [[BOB, '9']], [input(funding, index)]));
  f.store.replaceMempool(pool);
  const cursor = await f.watermark(), epoch = f.store.addressChangeState().epoch;
  pool.push(tx('one-addition', [[BOB, '9']], [input(funding, 10)])); f.store.replaceMempool(pool);
  assert.equal(f.store.addressChangeState().epoch, epoch);
  const delta = await f.drain(cursor);
  assert.equal(delta.changes.length, 4);
  assert.ok(delta.changes.every(event => event.txid === hash('one-addition') || event.txid === funding.txid));
  const after = f.store.addressChangeState();
  f.store.applyBlock(block('large-pool-unchanged-tip', next)); f.store.replaceMempool(pool);
  assert.deepEqual(f.store.addressChangeState(), after);
});

test('global watermarks expose same-tip mempool skew across disjoint address batches', async t => {
  const f = fixture(t), sourceCursor = await f.watermark([ALICE]), destinationCursor = await f.watermark([BOB]);
  const sourceBefore = await f.changes(sourceCursor, [ALICE]);
  const payment = tx('cross-batch-payment', [[BOB, '9']], [input(f.funding)]); f.store.replaceMempool([payment]);
  const destinationAfter = await f.drain(destinationCursor, [BOB]);
  const destinationWatermark = destinationAfter.pages.at(-1);
  assert.equal(sourceBefore.tip.hash, destinationWatermark.tip.hash);
  assert.equal(sourceBefore.journal_epoch, destinationWatermark.journal_epoch);
  assert.notEqual(sourceBefore.through_sequence, destinationWatermark.through_sequence);
  const sourceAfter = await f.drain(sourceBefore.next_cursor, [ALICE]);
  assert.equal(sourceAfter.pages.at(-1).through_sequence, destinationWatermark.through_sequence);
  assert.ok(sourceAfter.changes.some(event => event.txid === f.funding.txid && event.item?.pending_spent_by === payment.txid));
  // The watermark also advances for an unaffected batch, allowing an exact
  // common-view comparison without replaying its full baseline.
  const unrelated = await f.changes(await f.watermark([CAROL]), [CAROL]);
  assert.deepEqual(unrelated.changes, []);
  assert.equal(unrelated.through_sequence, destinationWatermark.through_sequence);
});
