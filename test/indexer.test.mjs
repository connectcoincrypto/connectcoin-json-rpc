import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, basename, resolve } from 'node:path';
import { Store } from '../src/store.mjs';
import { Indexer } from '../src/indexer.mjs';

const hash = text => createHash('sha256').update(String(text)).digest('hex');
const output = (value, address = 'alice') => ({ value, type: 1, scriptPubKey: { address } });
const bounty = (value = '1.0000000000') => ({ value, type: 2, domain: 'example.com',
  connection_work_target: 'f'.repeat(64), root_certificates_version: 1, signature_algorithms_mask: 7, scriptPubKey: {} });
const tx = (name, outputs, inputs = []) => ({ txid: hash(name), vin: inputs.length ? inputs : [{ coinbase: '01' }],
  vout: outputs.map((item, n) => ({ ...item, n })) });
const input = (transaction, vout = 0) => ({ txid: transaction.txid, vout });
function block(name, previous, transactions = []) {
  return { hash: hash(name), previousblockhash: previous?.hash, height: previous ? previous.height + 1 : 0,
    mediantime: 1_700_000_000 + (previous?.height ?? -1) * 10, tx: transactions };
}
class FakeBackend {
  constructor(chain) { this.chain = chain; this.pool = []; this.sequence = 1; this.calls = []; this.pruned = false; }
  setPool(txs) { this.pool = txs; this.sequence++; }
  async call(method, params = []) {
    this.calls.push([method, params]);
    if (method === 'getblockchaininfo') return { chain: 'regtest', blocks: this.chain.length - 1,
      bestblockhash: this.chain.at(-1).hash, pruned: this.pruned, initialblockdownload: false };
    if (method === 'getblockhash') { if (!this.chain[params[0]]) throw new Error('Height out of range'); return this.chain[params[0]].hash; }
    if (method === 'getblock') return this.chain.find(b => b.hash === params[0]);
    if (method === 'getrawmempool') return { txids: this.pool.map(t => t.txid), mempool_sequence: String(this.sequence) };
    throw new Error(`Unexpected backend call ${method}`);
  }
  async transaction(id) {
    const result = this.pool.find(t => t.txid === id);
    if (!result) { const error = new Error('Evicted'); error.code = -5; throw error; }
    return result;
  }
}
function fixture(path = ':memory:') {
  const genesis = block('genesis', null, [tx('genesis-tx', [output('50.0')])]);
  const funding = tx('funding', [output('100.0000000001')]);
  const first = block('first', genesis, [funding]);
  const transfer = tx('transfer', [output('60.0', 'bob'), output('39.0000000000'), bounty('1.0')], [input(funding)]);
  const second = block('second', first, [transfer]);
  const backend = new FakeBackend([genesis, first, second]);
  const store = new Store(path);
  const indexer = new Indexer({ backend, store });
  return { genesis, funding, first, transfer, second, backend, store, indexer };
}

test('indexes confirmed native address activity, exact amounts, and compact bounties', async t => {
  const f = fixture(); t.after(() => f.store.close());
  await f.indexer.syncOnce();
  assert.equal(f.indexer.ready, true);
  assert.equal(f.store.tip().hash, f.second.hash);
  assert.equal(f.store.tip().chain, 'regtest');
  assert.equal(f.store.tip().genesis_hash, f.genesis.hash);
  assert.equal(f.store.balance('alice').confirmed, '390000000000');
  assert.equal(f.store.balance('bob').confirmed, '600000000000');
  const history = f.store.history('alice');
  assert.equal(history.length, 2, 'genesis output must not become an available balance');
  assert.equal(history[0].spent, '1000000000001');
  assert.equal(history[0].balance_delta, '-610000000001');
  assert.equal(f.store.utxos('alice').length, 1);
  assert.equal(f.store.transactionLocation(f.transfer.txid).block_hash, f.second.hash);
  const list = [...f.store.bounties(f.second.hash)];
  assert.equal(list.length, 1);
  assert.equal(list[0].vout, 2);
  assert.equal(list[0].domain, 'example.com');
  assert.equal(list[0].amount, '10000000000');
  assert.equal(list[0].status, 'available');
  assert.equal(Object.hasOwn(list[0], 'hex'), false);
  const revision = f.store.revision();
  await f.indexer.syncOnce();
  assert.equal(f.store.revision(), revision, 'unchanged polls must not invalidate pagination cursors');
});

test('mempool parent/child overlays are order independent and removals restore UTXOs', async t => {
  const f = fixture(); t.after(() => f.store.close());
  await f.indexer.syncOnce();
  const parent = tx('parent', [output('55', 'carol'), output('4', 'bob')], [input(f.transfer)]);
  const child = tx('child', [output('54', 'alice')], [input(parent)]);
  f.backend.setPool([child, parent]);
  await f.indexer.syncOnce();
  assert.equal(f.store.balance('bob').confirmed, '600000000000');
  assert.equal(f.store.balance('bob').pending_spent, '600000000000');
  assert.equal(f.store.balance('bob').pending_received, '40000000000');
  assert.equal(f.store.balance('bob').total, '40000000000');
  assert.equal(f.store.balance('carol').total, '0');
  assert.equal(f.store.balance('alice').total, '930000000000');
  assert.equal(f.store.utxos('bob').length, 1);
  assert.equal(f.store.utxos('bob')[0].txid, parent.txid);
  assert.equal(f.store.history('carol').length, 2);
  assert.deepEqual(f.store.transactionLocation(child.txid), { status: 'pending' });
  f.backend.setPool([]);
  await f.indexer.syncOnce();
  assert.equal(f.store.balance('bob').total, '600000000000');
  assert.equal(f.store.history('carol').length, 0);
  assert.equal(f.store.transactionLocation(child.txid), null);
});

test('pending and confirmed bounty spends update availability without deleting bounty records', async t => {
  const f = fixture(); t.after(() => f.store.close());
  await f.indexer.syncOnce();
  const claim = tx('claim', [output('0.99', 'claimer')], [input(f.transfer, 2)]);
  const updates = []; f.indexer.on('update', update => updates.push(update));
  f.backend.setPool([claim]); await f.indexer.syncOnce();
  assert.equal(f.store.bountyPage(f.second.hash)[0].status, 'pending_spend');
  assert.equal(updates.at(-1).bountyChanges[0].type, 'pending_spend');
  f.backend.setPool([]); await f.indexer.syncOnce();
  assert.equal(f.store.bountyPage(f.second.hash)[0].status, 'available');
  assert.equal(updates.at(-1).bountyChanges[0].type, 'available_again');
  f.backend.chain.push(block('claim-block', f.second, [claim]));
  await f.indexer.syncOnce();
  assert.equal(f.store.bountyPage(f.second.hash)[0].status, 'spent');
  assert.equal(f.store.bountyPage(f.second.hash)[0].spending_txid, claim.txid);
});

test('reorganizations roll back spends and address history and restore state after restart', async t => {
  const folder = mkdtempSync(join(tmpdir(), 'connectcoin-index-test-'));
  let reopened;
  t.after(() => {
    reopened?.close();
    assert.equal(dirname(resolve(folder)), resolve(tmpdir()));
    assert.ok(basename(folder).startsWith('connectcoin-index-test-'));
    rmSync(folder, { recursive: true, force: true });
  });
  const file = join(folder, 'index.sqlite');
  const f = fixture(file);
  await f.indexer.syncOnce();
  f.store.close();
  reopened = new Store(file);
  const indexer = new Indexer({ backend: f.backend, store: reopened });
  const replacementTx = tx('replacement-transfer', [output('99', 'dave')], [input(f.funding)]);
  const replacement = block('replacement', f.first, [replacementTx]);
  f.backend.chain[2] = replacement;
  let event; indexer.on('update', e => { event = e; });
  await indexer.syncOnce();
  assert.equal(indexer.ready, true);
  assert.equal(reopened.tip().hash, replacement.hash);
  assert.equal(reopened.balance('bob').confirmed, '0');
  assert.equal(reopened.balance('dave').confirmed, '990000000000');
  assert.equal(reopened.history('alice').length, 2);
  assert.equal(reopened.transactionLocation(f.transfer.txid), null);
  assert.equal(reopened.isRecentBlock(f.second.hash), false);
  assert.equal(event.reorg, true);
  assert.equal(event.bountyChanges[0].type, 'resync_required');
});

test('600-block window is inclusive, metadata is pruned, and rewind rehydrates its newly included boundary', async t => {
  const genesis = block('window-genesis', null);
  const chain = [genesis];
  const earlyBounty = tx('early-bounty', [bounty()]);
  for (let i = 1; i <= 601; i++) chain.push(block(`height-${i}`, chain.at(-1), i === 1 ? [earlyBounty] : []));
  const backend = new FakeBackend(chain);
  const store = new Store(); t.after(() => store.close());
  const indexer = new Indexer({ backend, store });
  await indexer.syncOnce();
  assert.equal(store.recentBlocks().length, 600);
  assert.equal(store.recentBlocks().at(-1).height, 2);
  assert.equal(store.isRecentBlock(chain[1].hash), false);
  assert.equal(store.db.prepare('SELECT COUNT(*) AS n FROM bounties').get().n, 0);
  backend.chain = chain.slice(0, 601);
  await indexer.syncOnce();
  assert.equal(store.recentBlocks().at(-1).height, 1);
  assert.equal(store.bountyPage(chain[1].hash).length, 1);
  assert.equal(store.bountyPage(chain[1].hash)[0].status, 'available');
});

test('all bounties in a block are retained and available across deterministic pages', async t => {
  const genesis = block('many-genesis', null);
  const many = block('many-block', genesis, [tx('many-bounties', Array.from({ length: 255 }, () => bounty()))]);
  const store = new Store(); t.after(() => store.close());
  const indexer = new Indexer({ store, backend: new FakeBackend([genesis, many]) });
  await indexer.syncOnce();
  assert.equal([...store.bounties(many.hash)].length, 255);
  assert.equal(store.bountyPage(many.hash, 200, 100).length, 55);
  assert.equal(store.bountyPage(many.hash, 200, 100)[0].vout, 200);
  assert.equal(store.bountyPage(many.hash)[0].status, 'immature');
});

test('large-block notification overflow requests resync without truncating indexed bounty results', async t => {
  const store = new Store(); t.after(() => store.close());
  store.pinNetwork('regtest', hash('large-genesis'));
  const genesis = block('large-genesis', null);
  store.applyBlock(genesis);
  const many = block('large-block', genesis, [tx('large-bounty-tx', Array.from({ length: 10002 }, () => bounty()))]);
  const update = store.applyBlock(many);
  assert.equal(update.resync, true);
  assert.equal(update.bountyChanges.length, 0);
  assert.equal(store.db.prepare('SELECT COUNT(*) AS n FROM bounties').get().n, 10002);
  assert.equal(store.bountyPage(many.hash, 10000, 100).length, 2);
  assert.deepEqual(store.bountyPageAfter(many.hash, { after: { txid: many.tx[0].txid, vout: 9999 }, limit: 100 }),
    store.bountyPage(many.hash, 10000, 100));
  let after = null, count = 0, pages = 0;
  while (true) {
    const page = store.bountyPageAfter(many.hash, { after, limit: 100 });
    if (!page.length) break;
    for (const item of page) assert.equal(item.vout, count++, 'keyset must neither skip nor duplicate an output');
    after = { txid: page.at(-1).txid, vout: page.at(-1).vout };
    pages++;
  }
  assert.equal(count, 10002);
  assert.equal(pages, 101);

  const prepare = store.db.prepare.bind(store.db);
  let pageSQL;
  store.db.prepare = sql => {
    if (sql.startsWith('SELECT b.*,s.spender,p.spender AS pending_spender,? AS block_hash')) pageSQL = sql;
    return prepare(sql);
  };
  store.bountyPageAfter(many.hash, { after: { txid: many.tx[0].txid, vout: 9999 }, limit: 100 });
  const plan = prepare(`EXPLAIN QUERY PLAN ${pageSQL}`).all(many.hash, many.height, many.tx[0].txid, 9999, 100)
    .map(row => row.detail).join('\n');
  assert.match(plan, /SEARCH b USING INDEX bounties_height/);
  assert.match(plan, /height=\?.*txid/);
  assert.doesNotMatch(plan, /SCAN b\b|USE TEMP B-TREE/);
});

test('failed block and mempool writes roll back the complete database mutation', async t => {
  const f = fixture(); t.after(() => f.store.close());
  await f.indexer.syncOnce();
  const revision = f.store.revision();
  const badTx = tx('double-spend', [output('1', 'mallory')], [input(f.transfer), input(f.transfer)]);
  const badBlock = block('bad-block', f.second, [badTx]);
  assert.throws(() => f.store.applyBlock(badBlock), /UNIQUE/);
  assert.equal(f.store.tip().hash, f.second.hash);
  assert.equal(f.store.balance('bob').confirmed, '600000000000');
  assert.equal(f.store.balance('mallory').confirmed, '0');
  assert.equal(f.store.revision(), revision);
  assert.throws(() => f.store.replaceMempool([badTx]), /UNIQUE/);
  assert.equal(f.store.transactionLocation(badTx.txid), null);
  assert.equal(f.store.balance('bob').total, '600000000000');
  assert.equal(f.store.revision(), revision);
});

test('a transaction evicted during mempool collection is retried, never published partially', async t => {
  const f = fixture(); t.after(() => f.store.close());
  const pending = tx('evicted', [output('59', 'eve')], [input(f.transfer)]);
  f.backend.setPool([pending]);
  const transaction = f.backend.transaction.bind(f.backend);
  f.backend.transaction = async id => {
    f.backend.setPool([]);
    return transaction(id);
  };
  await f.indexer.syncOnce();
  assert.equal(f.indexer.ready, true);
  assert.equal(f.store.balance('eve').total, '0');
  assert.equal(f.store.balance('bob').total, '600000000000');
  assert.equal(f.store.transactionLocation(pending.txid), null);
});

test('rejects pruned nodes, switched networks, and unstable mempool snapshots without exposing partial state', async t => {
  const f = fixture(); t.after(() => f.store.close());
  f.backend.pruned = true;
  await assert.rejects(f.indexer.syncOnce(), /unpruned/);
  assert.equal(f.indexer.ready, false);
  f.backend.pruned = false;
  await f.indexer.syncOnce();
  assert.throws(() => f.store.pinNetwork('main', hash('another-genesis')), /differs/);
  const original = f.backend.call.bind(f.backend);
  f.backend.call = async (method, params) => {
    const result = await original(method, params);
    if (method === 'getrawmempool') result.mempool_sequence = String(f.backend.sequence++);
    return result;
  };
  await assert.rejects(f.indexer.syncOnce(), /changed repeatedly/);
  assert.equal(f.indexer.ready, false);
  assert.equal(f.store.balance('bob').confirmed, '600000000000');
});

test('stop waits for in-flight synchronization before database shutdown', async t => {
  const f = fixture(); t.after(() => f.store.close());
  let release; let reached;
  const barrier = new Promise(resolve => { release = resolve; });
  const started = new Promise(resolve => { reached = resolve; });
  const original = f.backend.call.bind(f.backend);
  f.backend.call = async (method, params) => { reached(); await barrier; return original(method, params); };
  const syncing = f.indexer.syncOnce();
  await started;
  let stopped = false;
  const stopping = f.indexer.stop().then(() => { stopped = true; });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(stopped, false);
  release();
  await Promise.all([syncing, stopping]);
  assert.equal(f.indexer.ready, false);
});

test('stop during the final coherence check cannot re-enable public readiness', async t => {
  const f = fixture(); t.after(() => f.store.close());
  await f.indexer.syncOnce();
  let release; let reached; let chainInfoCalls = 0;
  const barrier = new Promise(resolve => { release = resolve; });
  const started = new Promise(resolve => { reached = resolve; });
  const original = f.backend.call.bind(f.backend);
  f.backend.call = async (method, params) => {
    const response = await original(method, params);
    if (method === 'getblockchaininfo' && ++chainInfoCalls === 3) { reached(); await barrier; }
    return response;
  };
  const syncing = f.indexer.syncOnce();
  await started;
  const stopping = f.indexer.stop();
  release();
  await Promise.all([syncing, stopping]);
  assert.equal(f.indexer.ready, false);
});

test('live keyset history and UTXOs finish more than 60 pages despite unrelated new blocks', t => {
  const store = new Store(); t.after(() => store.close());
  let tip = block('pagination-genesis', null);
  store.pinNetwork('regtest', tip.hash);
  store.applyBlock(tip);
  const transactions = Array.from({ length: 6101 }, (_, i) =>
    tx(`pagination-transaction-${i}`, [output('1', 'busy-address')], [{ txid: hash(`prior-output-${i}`), vout: 0 }]));
  tip = block('pagination-activity', tip, transactions);
  store.applyBlock(tip);
  const expected = transactions.map(transaction => transaction.txid).sort();
  const historyIds = [], utxoIds = [];
  let historyAfter = null, utxoAfter = null, pages = 0;
  while (true) {
    const history = store.historyPage('busy-address', { after: historyAfter, limit: 100 });
    const utxos = store.utxoPage('busy-address', { after: utxoAfter, limit: 100 });
    if (!history.length && !utxos.length) break;
    historyIds.push(...history.map(row => row.txid));
    utxoIds.push(...utxos.map(row => row.txid));
    historyAfter = history.at(-1)?.txid ?? historyAfter;
    if (utxos.length) utxoAfter = { txid: utxos.at(-1).txid, vout: utxos.at(-1).vout };
    tip = block(`pagination-new-tip-${++pages}`, tip);
    store.applyBlock(tip);
    assert.ok(pages < 100, 'pagination must converge without restarting on each new tip');
  }
  assert.ok(pages > 60);
  assert.deepEqual(historyIds, expected);
  assert.deepEqual(utxoIds, expected);
});

test('deleting UTXOs or pending history before the cursor never skips unchanged following rows', t => {
  const store = new Store(); t.after(() => store.close());
  const genesis = block('keyset-delete-genesis', null);
  store.pinNetwork('regtest', genesis.hash);
  store.applyBlock(genesis);
  const funding = tx('keyset-funding', Array.from({ length: 4 }, () => output('1', 'keyset-address')));
  store.applyBlock(block('keyset-funding-block', genesis, [funding]));
  const first = store.utxoPage('keyset-address', { limit: 2 });
  assert.deepEqual(first.map(row => row.vout), [0, 1]);
  const spending = tx('spend-before-cursor', [output('0.9', 'other-address')], [input(funding, 0)]);
  store.replaceMempool([spending]);
  const next = store.utxoPage('keyset-address', { after: { txid: first[1].txid, vout: first[1].vout }, limit: 2 });
  assert.deepEqual(next.map(row => row.vout), [2, 3]);

  const pending = Array.from({ length: 4 }, (_, i) => tx(`history-keyset-${i}`, [output('0.9', 'pending-history')], [input(funding, i)]))
    .sort((a, b) => a.txid.localeCompare(b.txid));
  store.replaceMempool(pending);
  const firstHistory = store.historyPage('pending-history', { limit: 2 });
  store.replaceMempool(pending.slice(1));
  const nextHistory = store.historyPage('pending-history', { after: firstHistory[1].txid, limit: 2 });
  assert.deepEqual(nextHistory.map(row => row.txid), pending.slice(2).map(row => row.txid));
});

test('keyset queries use address plus immutable-key indexes, not full history/output scans', t => {
  const store = new Store(); t.after(() => store.close());
  const prepare = store.db.prepare.bind(store.db);
  const captured = [];
  store.db.prepare = sql => {
    if (sql.startsWith('SELECT * FROM (')) captured.push(sql);
    return prepare(sql);
  };
  const cursor = '8'.repeat(64);
  store.historyPage('indexed-address', { after: cursor, limit: 100 });
  store.utxoPage('indexed-address', { after: { txid: cursor, vout: 2 }, limit: 100 });
  assert.equal(captured.length, 2);
  const historyPlan = prepare(`EXPLAIN QUERY PLAN ${captured[0]}`).all('indexed-address', cursor, 'indexed-address', cursor, 100).map(row => row.detail).join('\n');
  const utxoPlan = prepare(`EXPLAIN QUERY PLAN ${captured[1]}`).all('indexed-address', cursor, 2, 'indexed-address', cursor, 2, 100).map(row => row.detail).join('\n');
  assert.match(historyPlan, /SEARCH h USING INDEX.*address=\? AND txid>\?/);
  assert.match(historyPlan, /SEARCH pending_history USING INDEX.*address=\? AND txid>\?/);
  assert.match(utxoPlan, /SEARCH o USING INDEX outputs_address_outpoint/);
  assert.match(utxoPlan, /SEARCH o USING INDEX pending_outputs_address_outpoint/);
  assert.doesNotMatch(historyPlan + '\n' + utxoPlan, /SCAN (h|o|history|outputs|pending_history|pending_outputs)\b/);
});
