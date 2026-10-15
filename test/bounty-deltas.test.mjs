import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { EventEmitter, once } from 'node:events';
import net from 'node:net';
import { PublicAPI, METHODS } from '../src/api.mjs';
import { Store } from '../src/store.mjs';
import { RateLimiter } from '../src/rate-limit.mjs';
import { createRpcServer } from '../src/transport.mjs';

const hash = value => createHash('sha256').update(value).digest('hex');
const native = { value: '100', type: 1, scriptPubKey: { address: 'alice' } };
const bounty = { value: '1.0000000001', type: 2, domain: 'example.com',
  connection_work_target: 'f'.repeat(64), root_certificates_version: 1, signature_algorithms_mask: 7 };
const transaction = (name, outputs, inputs = [{ txid: hash('external'), vout: 0 }]) => ({
  txid: hash(name), vin: inputs, vout: outputs.map((output, n) => ({ ...output, n })),
});

function fixture(t, options = {}) {
  const store = new Store();
  store.pinNetwork('regtest', hash('genesis'));
  const genesisTx = transaction('genesis-tx', [native], [{ coinbase: '01' }]);
  store.applyBlock({ hash: hash('genesis'), height: 0, mediantime: 1700000000, tx: [genesisTx] });
  const indexer = new EventEmitter(); indexer.ready = true;
  const calls = [];
  const backend = Object.fromEntries(['call', 'transaction', 'rawTransaction'].map(method => [method,
    async (...args) => { calls.push([method, args]); throw new Error('No backend reads are permitted'); }]));
  const notices = [];
  const context = { ip: '127.0.0.1', signal: new AbortController().signal, onClose: () => {},
    notify: async (method, params) => notices.push({ method, params }) };
  const api = new PublicAPI({ store, indexer, backend, options });
  t.after(() => { api.close(); store.close(); });
  const publish = update => indexer.emit('update', { ...update, tip: store.tip() });
  const mine = (name, tx = []) => {
    const previous = store.tip();
    return store.applyBlock({ hash: hash(name), previousblockhash: previous.hash,
      height: previous.height + 1, mediantime: previous.mediantime + 10, tx });
  };
  const funding = transaction('funding', [bounty, bounty], [{ txid: genesisTx.txid, vout: 0 }]);
  publish(mine('funding-block', [funding]));
  const pool = transactions => { const update = store.replaceMempool(transactions); publish(update); return update; };
  const changes = (cursor, include_bounties = true) => api.dispatch('getbountychanges', {
    ...(cursor === undefined ? {} : { cursor }), include_bounties,
  }, context);
  const receipt = txid => api.dispatch('gettransactionstatus', { txid }, context);
  return { store, indexer, api, calls, notices, context, publish, mine, pool, funding, genesisTx, changes, receipt };
}

test('expanded bounty feed acknowledges capability and preserves legacy polling and subscriptions', async t => {
  const f = fixture(t);
  const baseline = await f.changes();
  assert.deepEqual(baseline, { tip: f.store.tip(), changes: [], next_cursor: baseline.next_cursor, has_more: false, bounty_deltas: 1 });
  await f.api.dispatch('subscribebounties', {}, f.context);
  const claim = transaction('claim', [native], [{ txid: f.funding.txid, vout: 0 }]);
  f.pool([claim]);
  const expanded = await f.changes(baseline.next_cursor);
  const event = expanded.changes[0];
  assert.deepEqual(event.bounty, f.store.bountyByOutpoint(f.funding.txid, 0));
  assert.deepEqual(event.tip, f.store.tip());
  assert.equal(event.bounty.status, 'pending_spend');
  assert.equal(event.bounty.spending_txid, claim.txid);
  assert.equal(Object.hasOwn(event.bounty, 'hex'), false);
  const { bounty: _row, tip: _tip, ...legacyEvent } = event;
  const legacy = await f.api.dispatch('getbountychanges', { cursor: baseline.next_cursor }, f.context);
  assert.deepEqual(legacy, { tip: f.store.tip(), changes: [legacyEvent], next_cursor: expanded.next_cursor, has_more: false });
  assert.deepEqual(await f.changes(baseline.next_cursor, false), legacy);
  assert.deepEqual(f.notices[0].params.changes, [legacyEvent]);
  assert.deepEqual(f.calls, []);
});

test('expanded bounty params reject malformed flags and extra fields', async t => {
  const f = fixture(t);
  for (const include_bounties of [undefined, null, 0, 1, 'true', [], {}]) {
    await assert.rejects(f.api.dispatch('getbountychanges', { include_bounties }, f.context), { code: -32602 });
  }
  for (const params of [null, [], 1, { include_bounties: true, unknown: 1 }]) {
    await assert.rejects(f.api.dispatch('getbountychanges', params, f.context), { code: -32602 });
  }
  await assert.rejects(f.api.dispatch('getbountychanges', { cursor: 'bad', include_bounties: true }, f.context), { code: -32011 });
});

test('historical pages preserve publication rows and tips after later eviction and confirmation', async t => {
  const f = fixture(t, { pageSize: 1 });
  const baseline = await f.changes();
  const claim = transaction('claim-both', [native], [0, 1].map(vout => ({ txid: f.funding.txid, vout })));
  f.pool([claim]);
  const originalTip = f.store.tip();
  const first = await f.changes(baseline.next_cursor);
  assert.equal(first.has_more, true);
  f.pool([]);
  const confirmed = f.mine('confirmed-claim', [claim]);
  f.store.replaceMempool([]);
  f.publish(confirmed);
  // Pagination must not read the current row, now confirmed and spent.
  f.store.bountyByOutpoint = () => { throw new Error('Late delta lookup'); };
  const second = await f.changes(first.next_cursor);
  assert.equal(second.changes[0].bounty.status, 'pending_spend');
  assert.deepEqual(second.changes[0].tip, originalTip);
  assert.equal(second.tip.height, originalTip.height + 1);
  assert.deepEqual((await f.changes(baseline.next_cursor)).changes, first.changes);
  const restored = await f.changes(second.next_cursor);
  assert.equal(restored.changes[0].type, 'available_again');
  assert.equal(restored.changes[0].bounty.status, 'available');
  assert.equal(restored.changes[0].bounty.spending_txid, null);
  let cursor = restored.next_cursor, last;
  do { last = await f.changes(cursor); cursor = last.next_cursor; } while (last.has_more);
  assert.equal(last.changes[0].bounty.status, 'spent');
  assert.equal(last.changes[0].bounty.spending_txid, claim.txid);
});

test('coalesced event types can precede their final coherent bounty state', async t => {
  const f = fixture(t);
  const baseline = await f.changes();
  const added = transaction('new-bounty', [bounty]);
  const created = f.mine('new-bounty-block', [added]);
  const claimant = transaction('new-claim', [native], [{ txid: added.txid, vout: 0 }]);
  const spent = f.mine('new-claim-block', [claimant]);
  f.publish({ bountyChanges: [...created.bountyChanges, ...spent.bountyChanges] });
  const page = await f.changes(baseline.next_cursor);
  assert.deepEqual(page.changes.map(event => event.type), ['added', 'spent']);
  for (const event of page.changes) {
    assert.equal(event.bounty.status, 'spent');
    assert.equal(event.bounty.spending_txid, claimant.txid);
    assert.deepEqual(event.tip, f.store.tip());
  }
  const pending = transaction('pending-old', [native], [{ txid: f.funding.txid, vout: 0 }]);
  const pendingUpdate = f.store.replaceMempool([pending]);
  const removedUpdate = f.store.replaceMempool([]);
  f.publish({ bountyChanges: [...pendingUpdate.bountyChanges, ...removedUpdate.bountyChanges] });
  const removed = await f.changes(page.next_cursor);
  assert.equal(removed.changes[0].type, 'pending_spend');
  assert.equal(removed.changes[0].spending_txid, pending.txid);
  assert.equal(removed.changes[0].bounty.status, 'available');
  assert.equal(removed.changes[0].bounty.spending_txid, null);
});

test('publication copies its row and tip instead of retaining mutable store objects', async t => {
  const f = fixture(t);
  const baseline = await f.changes();
  const row = f.store.bountyByOutpoint(f.funding.txid, 0), tip = f.store.tip();
  f.store.bountyByOutpoint = () => row;
  f.indexer.emit('update', { tip, bountyChanges: [{ type: 'matured', txid: row.txid, vout: row.vout }] });
  row.amount = '0'; tip.hash = hash('mutated');
  const event = (await f.changes(baseline.next_cursor)).changes[0];
  assert.equal(event.bounty.amount, '10000000001');
  assert.equal(event.tip.hash, f.store.tip().hash);
});

test('missing required outpoints reset the complete feed and explicitly notify subscribers', async t => {
  const f = fixture(t);
  const baseline = await f.changes();
  await f.api.dispatch('subscribebounties', {}, f.context);
  f.publish({ bountyChanges: [
    { type: 'matured', txid: f.funding.txid, vout: 0 },
    { type: 'spent', txid: hash('missing'), vout: 0 },
  ] });
  await assert.rejects(f.changes(baseline.next_cursor), { code: -32011 });
  assert.equal(f.api.journal.length, 0);
  assert.equal(f.api.journalBytes, 0);
  assert.equal(f.notices[0].params.resync_required, true);
  assert.equal((await f.changes()).bounty_deltas, 1);
});

test('window exits have null payloads and an out-of-window non-removal requires resync', async t => {
  const f = fixture(t);
  const baseline = await f.changes();
  let exited;
  for (let height = 2; height <= 601; height++) {
    const update = f.mine(`empty-${height}`);
    if (height === 601) exited = update;
  }
  assert.equal(f.store.bountyByOutpoint(f.funding.txid, 0), null);
  f.publish(exited);
  const page = await f.changes(baseline.next_cursor);
  assert.equal(page.changes.length, 2);
  assert.ok(page.changes.every(event => event.type === 'window_exit' && event.bounty === null && event.tip.height === 601));
  f.publish({ bountyChanges: [{ type: 'matured', txid: f.funding.txid, vout: 0 }] });
  await assert.rejects(f.changes(page.next_cursor), { code: -32011 });
});

test('enriched private payloads count toward byte retention even for legacy polling', async t => {
  const f = fixture(t, { maxJournalBytes: 1_100 });
  const baseline = await f.changes();
  await f.api.dispatch('subscribebounties', {}, f.context);
  f.publish({ bountyChanges: [0, 1].map(vout => ({ type: 'matured', txid: f.funding.txid, vout })) });
  assert.ok(f.api.journalBytes <= 1_100);
  assert.equal(f.api.journalBytes, f.api.journal.reduce((sum, { event, bounty, tip, bytes }) => {
    assert.equal(bytes, Buffer.byteLength(JSON.stringify({ event, bounty, tip })));
    return sum + bytes;
  }, 0));
  await assert.rejects(f.changes(baseline.next_cursor, false), { code: -32011 });
  assert.equal(f.notices[0].params.resync_required, true);
  assert.equal((await f.changes(f.api.changeCursor(f.api.journalFloor))).has_more, false);
});

test('event retention overflow and reorg invalidate expanded cursors without stale rows', async t => {
  const f = fixture(t, { maxJournalEvents: 1 });
  const baseline = await f.changes();
  f.publish({ bountyChanges: [0, 1].map(vout => ({ type: 'matured', txid: f.funding.txid, vout })) });
  await assert.rejects(f.changes(baseline.next_cursor), { code: -32011 });
  const current = await f.changes();
  f.publish(f.store.rollbackTip());
  await assert.rejects(f.changes(current.next_cursor), { code: -32011 });
  assert.equal(f.api.journal.length, 0);
});

test('transaction receipts read only indexed pending or confirmed metadata and normalize txids', async t => {
  const f = fixture(t);
  const pending = transaction('pending-receipt', [native], [{ txid: f.funding.txid, vout: 0 }]);
  f.pool([pending]);
  assert.deepEqual(await f.receipt(pending.txid.toUpperCase()), {
    tip: f.store.tip(), txid: pending.txid, status: 'pending', confirmations: 0,
  });
  const update = f.mine('receipt-confirmation', [pending]);
  f.store.replaceMempool([]); f.publish(update);
  const confirmedTip = f.store.tip();
  assert.deepEqual(await f.receipt(pending.txid), { tip: confirmedTip, txid: pending.txid,
    status: 'confirmed', block_hash: confirmedTip.hash, block_height: confirmedTip.height, confirmations: 1 });
  f.publish(f.mine('receipt-extra-confirmation'));
  assert.equal((await f.receipt(pending.txid)).confirmations, 2);
  assert.equal((await f.receipt(f.genesisTx.txid)).confirmations, f.store.tip().height + 1);
  assert.deepEqual(f.calls, []);
});

test('transaction receipts enforce readiness, disappearance, reorg and mempool return', async t => {
  const f = fixture(t);
  await assert.rejects(f.receipt(hash('absent')), { code: -32004 });
  f.indexer.ready = false;
  await assert.rejects(f.receipt(f.funding.txid), { code: -32001 });
  f.indexer.pendingSync = Promise.resolve();
  const waiting = f.receipt(f.funding.txid);
  f.indexer.ready = true; f.publish({ bountyChanges: [] });
  assert.equal((await waiting).status, 'confirmed');
  f.indexer.pendingSync = null;
  f.publish(f.store.rollbackTip());
  await assert.rejects(f.receipt(f.funding.txid), { code: -32004 });
  f.pool([f.funding]);
  assert.equal((await f.receipt(f.funding.txid)).status, 'pending');
  f.pool([]);
  await assert.rejects(f.receipt(f.funding.txid), { code: -32004 });
  assert.deepEqual(f.calls, []);
});

test('transaction receipts reject malformed params and inconsistent indexed locations', async t => {
  const f = fixture(t);
  for (const params of [null, [], {}, 1, { txid: null }, { txid: 1 }, { txid: '' },
    { txid: 'g'.repeat(64) }, { txid: 'a'.repeat(63) }, { txid: ` ${f.funding.txid}` },
    { txid: f.funding.txid, verbose: true }, { txid: f.funding.txid, block_hash: hash('block') }]) {
    await assert.rejects(f.api.dispatch('gettransactionstatus', params, f.context), { code: -32602 });
  }
  const actual = f.store.transactionLocation(f.funding.txid);
  for (const location of [{ status: 'unknown' }, {}, { status: 'pending', block_height: 1 },
    { status: 'pending', block_hash: actual.block_hash },
    { ...actual, block_height: -1 }, { ...actual, block_height: 1.5 }, { ...actual, block_height: 2 },
    { ...actual, block_hash: hash('disconnected') }, { ...actual, block_hash: actual.block_hash.toUpperCase() }]) {
    f.store.transactionLocation = () => location;
    await assert.rejects(f.receipt(f.funding.txid), { code: -32011 });
  }
  f.store.transactionLocation = () => actual;
  const tip = f.store.tip();
  for (const invalid of [{ ...tip, hash: tip.hash.toUpperCase() }, { ...tip, genesis_hash: null },
    { ...tip, height: -1 }, { ...tip, height: 1.5 }, { ...tip, chain: '' }, { ...tip, hash: hash('unindexed-tip') }]) {
    f.store.tip = () => invalid;
    await assert.rejects(f.receipt(f.funding.txid), { code: -32011 });
  }
  assert.deepEqual(f.calls, []);
});

test('receipt and delta quotas each default to 200 calls per minute and remain independent', () => {
  const limiter = new RateLimiter({ now: () => 0 });
  for (const method of ['gettransactionstatus', 'getbountychanges']) {
    for (let index = 0; index < 200; index++) assert.equal(limiter.consume({ ip: '127.0.0.1', method }).allowed, true);
    assert.equal(limiter.consume({ ip: '::ffff:127.0.0.1', method }).allowed, false);
  }
  assert.equal(limiter.consume({ ip: '127.0.0.1', method: 'gettransaction' }).allowed, true);
});

test('TCP exposes compact receipts and deltas, counts invalid receipt requests and shares quota across sockets', { timeout: 10_000 }, async t => {
  const f = fixture(t);
  const server = createRpcServer({ allowedMethods: METHODS, limiter: new RateLimiter({ now: () => 0 }),
    dispatch: f.api.dispatch });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const sockets = [];
  t.after(async () => {
    for (const socket of sockets) socket.destroy();
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  });
  let id = 0;
  const connect = async () => {
    const socket = net.connect(server.address().port, '127.0.0.1'); sockets.push(socket);
    await once(socket, 'connect');
    return (method, params) => {
      const response = once(socket, 'data').then(([data]) => JSON.parse(data.toString('utf8')));
      socket.write(`${JSON.stringify({ jsonrpc: '2.0', id: ++id, method, params })}\n`);
      return response;
    };
  };
  const first = await connect(), second = await connect();
  assert.equal((await first('gettransactionstatus', { txid: f.funding.txid, verbose: true })).error.code, -32602);
  for (let index = 1; index < 200; index++) {
    const result = (await (index % 2 ? first : second)('gettransactionstatus', { txid: f.funding.txid })).result;
    assert.deepEqual(result, { tip: f.store.tip(), txid: f.funding.txid, status: 'confirmed',
      block_hash: f.store.tip().hash, block_height: 1, confirmations: 1 });
  }
  assert.equal((await second('gettransactionstatus', { txid: f.funding.txid })).error.code, -32029);
  const third = await connect();
  assert.equal((await third('gettransactionstatus', { txid: f.funding.txid })).error.code, -32029);
  const baseline = (await third('getbountychanges', { include_bounties: true })).result;
  const claim = transaction('tcp-claim', [native], [{ txid: f.funding.txid, vout: 0 }]);
  f.pool([claim]);
  const delta = (await third('getbountychanges', { cursor: baseline.next_cursor, include_bounties: true })).result;
  assert.equal(delta.bounty_deltas, 1);
  assert.equal(delta.changes[0].bounty.status, 'pending_spend');
  assert.deepEqual(f.calls, []);
});
