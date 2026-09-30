import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { Store } from '../src/store.mjs';
import { Indexer } from '../src/indexer.mjs';

const hash = value => createHash('sha256').update(String(value)).digest('hex');
const output = (value, address = 'allocation-owner') => ({ value, type: 1, scriptPubKey: { address } });
const bounty = () => ({ value: '2', type: 2, domain: 'example.com', connection_work_target: 'f'.repeat(64),
  root_certificates_version: 1, signature_algorithms_mask: 7, scriptPubKey: {} });
const tx = (name, outputs, inputs = []) => ({ txid: hash(name), vin: inputs.length ? inputs : [{ coinbase: '01' }],
  vout: outputs.map((item, n) => ({ ...item, n })) });
const input = (transaction, vout = 0) => ({ txid: transaction.txid, vout });
const block = (name, previous, transactions = []) => ({ hash: hash(name), height: previous ? previous.height + 1 : 0,
  previousblockhash: previous?.hash, mediantime: 1_700_000_000 + (previous?.height ?? -1) * 10, tx: transactions });
const amount = '500000000001';

function fixture(t, path = ':memory:') {
  const allocation = tx('allocation', [output('50.0000000001')]);
  const genesis = block('allocation-genesis', null, [allocation]);
  const store = new Store(path);
  t.after(() => store.close());
  store.pinNetwork('regtest', genesis.hash);
  store.applyBlock(genesis);
  const chain = [genesis];
  const append = (name, transactions = []) => {
    const next = block(name, chain.at(-1), transactions);
    chain.push(next);
    store.applyBlock(next);
    return next;
  };
  const mature = () => {
    while (chain.at(-1).height < 99) append(`maturity-${chain.length}`);
  };
  return { store, genesis, allocation, chain, append, mature };
}

// Recreate schema 1's omission, including its debit error, without loading a
// real database or depending on the old implementation remaining in production.
function makeLegacy(store, genesis) {
  store.atomic(() => {
    const genesisOutputs = store.db.prepare('SELECT * FROM outputs WHERE height=0').all();
    for (const out of genesisOutputs) {
      const spending = store.db.prepare('SELECT spender FROM spends WHERE txid=? AND vout=?').get(out.txid, out.vout);
      if (!spending) continue;
      const historical = store.db.prepare('SELECT * FROM history WHERE address=? AND txid=?').get(out.address, spending.spender);
      assert.ok(historical, 'the fixed fixture must contain the original debit');
      const remaining = BigInt(historical.spent) - BigInt(out.amount);
      assert.ok(remaining >= 0n);
      if (remaining === 0n && historical.received === '0') {
        store.db.prepare('DELETE FROM history WHERE address=? AND txid=?').run(out.address, spending.spender);
      } else {
        store.db.prepare('UPDATE history SET spent=? WHERE address=? AND txid=?').run(String(remaining), out.address, spending.spender);
      }
    }
    store.db.exec('DELETE FROM outputs WHERE height=0; DELETE FROM history WHERE height=0; DELETE FROM bounties WHERE height=0;');
    store.setMeta('schema', '1');
    store.balanceCache.clear();
  });
  assert.equal(store.blockAt(0).hash, genesis.hash);
  assert.equal(store.needsGenesisRepair(), true);
}

const tables = ['metadata', 'blocks', 'transactions', 'outputs', 'spends', 'history', 'bounties',
  'pending_transactions', 'pending_outputs', 'pending_spends', 'pending_history'];
function snapshot(store) {
  return Object.fromEntries(tables.map(table => [table, store.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all()]));
}

class Backend {
  constructor(chain, pool = []) { this.chain = chain; this.pool = pool; this.calls = []; }
  async call(method, params = []) {
    this.calls.push([method, params]);
    if (method === 'getblockchaininfo') return { chain: 'regtest', blocks: this.chain.length - 1,
      bestblockhash: this.chain.at(-1).hash, pruned: false, initialblockdownload: false };
    if (method === 'getblockhash') return this.chain[params[0]]?.hash;
    if (method === 'getblock') return this.chain.find(item => item.hash === params[0]);
    if (method === 'getrawmempool') return { txids: this.pool.map(item => item.txid), mempool_sequence: '1' };
    throw new Error(`Unexpected backend method: ${method}`);
  }
  async transaction(id) { return this.pool.find(item => item.txid === id); }
}

test('fresh indexes retain genesis allocations and mature them at 100 confirmations, not 99', t => {
  const f = fixture(t);
  assert.equal(f.store.meta('schema'), '2');
  assert.equal(f.store.needsGenesisRepair(), false);
  assert.equal(f.store.balance('allocation-owner').confirmed, amount);
  assert.equal(f.store.balance('allocation-owner').immature, amount);
  assert.equal(f.store.history('allocation-owner')[0].received, amount);
  assert.equal(f.store.utxoPage('allocation-owner')[0].coinbase, true);
  while (f.chain.at(-1).height < 98) f.append(`pre-maturity-${f.chain.length}`);
  assert.equal(f.store.utxos('allocation-owner')[0].confirmations, 99);
  assert.equal(f.store.utxos('allocation-owner')[0].mature, false);
  assert.equal(f.store.balance('allocation-owner').available_confirmed, '0');
  f.append('maturity-boundary');
  assert.equal(f.store.utxoPage('allocation-owner')[0].confirmations, 100);
  assert.equal(f.store.utxoPage('allocation-owner')[0].mature, true);
  assert.equal(f.store.balance('allocation-owner').immature, '0');
  assert.equal(f.store.balance('allocation-owner').available_confirmed, amount);
});

test('a pending genesis spend subtracts its full input and reports only change as received', t => {
  const f = fixture(t); f.mature();
  const spending = tx('allocation-pending', [output('20', 'recipient'), output('29')], [input(f.allocation)]);
  f.store.replaceMempool([spending]);
  assert.equal(f.store.balance('allocation-owner').pending_spent, amount);
  assert.equal(f.store.balance('allocation-owner').pending_received, '290000000000');
  assert.equal(f.store.balance('allocation-owner').pending_delta, '-210000000001');
  assert.equal(f.store.balance('allocation-owner').total, '290000000000');
  const history = f.store.history('allocation-owner').find(item => item.txid === spending.txid);
  assert.equal(history.spent, amount);
  assert.equal(history.balance_delta, '-210000000001');
  assert.deepEqual(f.store.utxos('allocation-owner').map(item => item.txid), [spending.txid]);
  f.store.replaceMempool([]);
  assert.equal(f.store.balance('allocation-owner').available_confirmed, amount);
});

test('confirmed genesis spends, rollback and reinclusion preserve debits and original coinbase maturity', t => {
  const f = fixture(t); f.mature();
  const spending = tx('allocation-confirmed', [output('20', 'recipient'), output('29')], [input(f.allocation)]);
  const first = f.append('allocation-spend-block', [spending]);
  assert.equal(f.store.balance('allocation-owner').confirmed, '290000000000');
  assert.equal(f.store.history('allocation-owner')[0].spent, amount);
  f.store.rollbackTip();
  assert.equal(f.store.transactionLocation(spending.txid), null);
  assert.equal(f.store.history('allocation-owner').length, 1);
  assert.equal(f.store.balance('allocation-owner').available_confirmed, amount);
  const restored = f.store.utxoPage('allocation-owner')[0];
  assert.equal(restored.txid, f.allocation.txid);
  assert.equal(restored.coinbase, true);
  assert.equal(restored.mature, true);
  const replacement = block('replacement-allocation-spend', f.chain.at(-2), [spending]);
  f.store.applyBlock(replacement);
  assert.notEqual(replacement.hash, first.hash);
  assert.equal(f.store.transactionLocation(spending.txid).block_hash, replacement.hash);
  assert.equal(f.store.history('allocation-owner')[0].spent, amount);
  assert.equal(f.store.balance('allocation-owner').confirmed, '290000000000');
});

test('genesis bounty outputs use the same retention and rehydration rules as other coinbases', t => {
  const store = new Store(); t.after(() => store.close());
  const genesis = block('bounty-genesis', null, [tx('bounty-allocation', [bounty()])]);
  store.pinNetwork('regtest', genesis.hash);
  store.applyBlock(genesis);
  assert.equal(store.bountyPage(genesis.hash)[0].status, 'immature');
  let tip = genesis;
  for (let height = 1; height <= 600; height++) {
    tip = block(`bounty-prune-${height}`, tip);
    store.applyBlock(tip);
  }
  assert.equal(store.bountyPage(genesis.hash).length, 0);
  store.rollbackTip();
  assert.ok(store.missingBountyBlocks().some(item => item.height === 0));
  store.restoreBountyBlock(genesis);
  const restored = store.bountyPage(genesis.hash);
  assert.equal(restored.length, 1);
  assert.equal(restored[0].status, 'available');
  assert.equal(restored[0].coinbase, true);
});

test('legacy migration restores unspent allocations and invalidates cached balances exactly once', t => {
  const f = fixture(t); f.mature(); makeLegacy(f.store, f.genesis);
  assert.equal(f.store.balance('allocation-owner').confirmed, '0');
  const beforeRevision = f.store.revision();
  f.store.repairGenesis(f.genesis);
  assert.equal(f.store.meta('schema'), '2');
  assert.equal(f.store.needsGenesisRepair(), false);
  assert.equal(f.store.balance('allocation-owner').available_confirmed, amount);
  assert.equal(f.store.history('allocation-owner')[0].received, amount);
  assert.equal(f.store.revision(), beforeRevision + 1);
  const after = snapshot(f.store);
  f.store.repairGenesis(f.genesis);
  assert.deepEqual(snapshot(f.store), after, 'repeated repair must not double-credit or bump revision');
});

test('legacy migration adds missing debits to existing change and other-input history', t => {
  const f = fixture(t);
  const funding = tx('non-genesis-funding', [output('20')]);
  f.append('non-genesis-funding-block', [funding]);
  f.mature();
  const spending = tx('combined-spend', [output('40', 'recipient'), output('29')], [input(f.allocation), input(funding)]);
  f.append('combined-spend-block', [spending]);
  makeLegacy(f.store, f.genesis);
  assert.equal(f.store.history('allocation-owner')[0].spent, '200000000000');
  f.store.repairGenesis(f.genesis);
  const repaired = f.store.history('allocation-owner')[0];
  assert.equal(repaired.txid, spending.txid);
  assert.equal(repaired.received, '290000000000');
  assert.equal(repaired.spent, '700000000001');
  assert.equal(repaired.balance_delta, '-410000000001');
  assert.equal(f.store.history('allocation-owner').length, 3);
  assert.equal(f.store.balance('allocation-owner').confirmed, '290000000000');
  f.store.rollbackTip();
  assert.equal(f.store.balance('allocation-owner').confirmed, '700000000001');
  assert.equal(f.store.history('allocation-owner').length, 2);
});

test('legacy migration creates a missing debit-only history row without inventing a receipt', t => {
  const f = fixture(t); f.mature();
  const spending = tx('debit-only-spend', [output('49', 'recipient')], [input(f.allocation)]);
  const spendingBlock = f.append('debit-only-block', [spending]);
  makeLegacy(f.store, f.genesis);
  assert.equal(f.store.history('allocation-owner').length, 0);
  f.store.repairGenesis(f.genesis);
  const debit = f.store.historyPage('allocation-owner').find(item => item.txid === spending.txid);
  assert.equal(debit.received, '0');
  assert.equal(debit.spent, amount);
  assert.equal(debit.balance_delta, `-${amount}`);
  assert.equal(debit.block_hash, spendingBlock.hash);
  assert.equal(debit.block_height, spendingBlock.height);
  assert.equal(f.store.balance('allocation-owner').confirmed, '0');
});

test('repair aggregates multiple genesis outputs per address and multiple inputs of the same spend', t => {
  const store = new Store(); t.after(() => store.close());
  const allocation = tx('multiple-allocation', [output('30'), output('20.0000000001'), output('10', 'other-owner')]);
  const genesis = block('multiple-allocation-genesis', null, [allocation]);
  store.pinNetwork('regtest', genesis.hash);
  store.applyBlock(genesis);
  let tip = genesis;
  for (let height = 1; height < 100; height++) {
    tip = block(`multiple-maturity-${height}`, tip);
    store.applyBlock(tip);
  }
  const spending = tx('multiple-input-allocation-spend', [output('49', 'recipient')], [input(allocation, 0), input(allocation, 1)]);
  store.applyBlock(block('multiple-input-allocation-spend-block', tip, [spending]));
  makeLegacy(store, genesis);
  store.repairGenesis(genesis);
  const history = store.history('allocation-owner');
  assert.equal(history.length, 2);
  assert.equal(history[0].spent, amount);
  assert.equal(history[1].received, amount);
  assert.equal(store.balance('allocation-owner').confirmed, '0');
  assert.equal(store.balance('other-owner').available_confirmed, '100000000000');
  assert.equal(store.history('other-owner').length, 1);
  assert.equal(store.utxoPage('other-owner')[0].vout, 2);
});

test('repair restores old genesis address accounting without resurrecting bounties outside the window', t => {
  const store = new Store(); t.after(() => store.close());
  const genesis = block('old-genesis', null, [tx('old-allocation', [output('50.0000000001'), bounty()])]);
  store.pinNetwork('regtest', genesis.hash);
  store.applyBlock(genesis);
  let tip = genesis;
  for (let height = 1; height <= 600; height++) {
    tip = block(`old-genesis-height-${height}`, tip);
    store.applyBlock(tip);
  }
  makeLegacy(store, genesis);
  store.repairGenesis(genesis);
  assert.equal(store.balance('allocation-owner').available_confirmed, amount);
  assert.equal(store.db.prepare('SELECT COUNT(*) AS n FROM bounties WHERE height=0').get().n, 0);
  assert.equal(store.bountyPage(genesis.hash).length, 0);
  store.rollbackTip();
  store.restoreBountyBlock(genesis);
  assert.equal(store.bountyPage(genesis.hash).length, 1);
});

test('legacy repair clears stale pending accounting and forces an unchanged mempool to rebuild', t => {
  const f = fixture(t); f.mature(); makeLegacy(f.store, f.genesis);
  const pending = tx('legacy-pending-spend', [output('49')], [input(f.allocation)]);
  f.store.replaceMempool([pending]);
  assert.equal(f.store.history('allocation-owner')[0].spent, '0');
  const previousFingerprint = f.store.meta('mempool_fingerprint');
  assert.ok(previousFingerprint);
  f.store.repairGenesis(f.genesis);
  for (const table of tables.filter(name => name.startsWith('pending_'))) {
    assert.equal(f.store.db.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n, 0, `${table} must be invalidated`);
  }
  assert.notEqual(f.store.meta('mempool_fingerprint'), previousFingerprint);
  f.store.replaceMempool([pending]);
  assert.equal(f.store.history('allocation-owner')[0].spent, amount);
  assert.equal(f.store.balance('allocation-owner').total, '490000000000');
});

test('a failure after repair starts rolls back outputs, receipts, metadata and pending invalidation', t => {
  const f = fixture(t); f.mature();
  const spending = tx('atomic-legacy-spend', [output('49', 'recipient')], [input(f.allocation)]);
  f.append('atomic-legacy-spend-block', [spending]);
  makeLegacy(f.store, f.genesis);
  f.store.replaceMempool([]);
  f.store.db.exec(`CREATE TRIGGER reject_repaired_debit BEFORE INSERT ON history
    WHEN NEW.txid='${spending.txid}' BEGIN SELECT RAISE(ABORT, 'injected repair failure'); END;`);
  const before = snapshot(f.store);
  assert.throws(() => f.store.repairGenesis(f.genesis), /injected repair failure/);
  assert.deepEqual(snapshot(f.store), before);
  assert.equal(f.store.needsGenesisRepair(), true);
  assert.equal(f.store.balance('allocation-owner').confirmed, '0');
  f.store.db.exec('DROP TRIGGER reject_repaired_debit');
  f.store.repairGenesis(f.genesis);
  assert.equal(f.store.history('allocation-owner').length, 2);
});

test('repair rejects a different genesis hash or transaction identity without mutation', async t => {
  for (const variant of ['hash', 'transaction', 'missing-transaction', 'height', 'pinned-network']) {
    await t.test(variant, sub => {
      const f = fixture(sub); makeLegacy(f.store, f.genesis);
      const raw = structuredClone(f.genesis);
      if (variant === 'hash') raw.hash = hash('other-genesis');
      if (variant === 'transaction') raw.tx[0].txid = hash('other-allocation');
      if (variant === 'missing-transaction') raw.tx = [];
      if (variant === 'height') raw.height = 1;
      if (variant === 'pinned-network') f.store.setMeta('genesis', hash('wrong-pinned-network'));
      const before = snapshot(f.store);
      assert.throws(() => f.store.repairGenesis(raw));
      assert.deepEqual(snapshot(f.store), before);
    });
  }
});

test('repair fails closed on a partially repaired legacy index instead of double-counting it', async t => {
  for (const retained of ['outputs', 'history']) {
    await t.test(retained, sub => {
      const f = fixture(sub);
      const records = f.store.db.prepare(`SELECT * FROM ${retained} WHERE height=0`).all();
      makeLegacy(f.store, f.genesis);
      const row = records[0];
      if (retained === 'outputs') f.store.db.prepare('INSERT INTO outputs VALUES (?,?,?,?,?,?)')
        .run(row.txid, row.vout, row.address, row.amount, row.height, row.coinbase);
      else f.store.db.prepare('INSERT INTO history VALUES (?,?,?,?,?,?)')
        .run(row.address, row.txid, row.height, row.position, row.received, row.spent);
      const before = snapshot(f.store);
      assert.throws(() => f.store.repairGenesis(f.genesis));
      assert.deepEqual(snapshot(f.store), before);
    });
  }
});

test('schema-1 repair is persistent and startup fetches the genesis body only once', async t => {
  const folder = mkdtempSync(join(tmpdir(), 'connectcoin-genesis-test-'));
  let store;
  t.after(() => {
    store?.close();
    assert.equal(dirname(resolve(folder)), resolve(tmpdir()));
    assert.ok(basename(folder).startsWith('connectcoin-genesis-test-'));
    rmSync(folder, { recursive: true, force: true });
  });
  const file = join(folder, 'index.sqlite');
  const genesis = block('persisted-genesis', null, [tx('persisted-allocation', [output('50.0000000001')])]);
  store = new Store(file);
  store.pinNetwork('regtest', genesis.hash);
  store.applyBlock(genesis);
  makeLegacy(store, genesis);
  store.close();
  store = new Store(file);
  assert.equal(store.needsGenesisRepair(), true);
  const backend = new Backend([genesis]);
  let indexer = new Indexer({ store, backend });
  await indexer.syncOnce();
  assert.equal(indexer.ready, true);
  assert.equal(store.balance('allocation-owner').confirmed, amount);
  assert.equal(backend.calls.filter(([method]) => method === 'getblock').length, 1);
  assert.deepEqual(backend.calls.find(([method]) => method === 'getblock')[1], [genesis.hash, 2]);
  const firstRevision = store.revision();
  await indexer.syncOnce();
  assert.equal(store.revision(), firstRevision);
  store.close();
  store = new Store(file);
  indexer = new Indexer({ store, backend });
  await indexer.syncOnce();
  assert.equal(indexer.ready, true);
  assert.equal(store.meta('schema'), '2');
  assert.equal(store.history('allocation-owner').length, 1);
  assert.equal(backend.calls.filter(([method]) => method === 'getblock').length, 1);
});

test('repair backend failure never exposes legacy state as ready and can recover on retry', async t => {
  const f = fixture(t); makeLegacy(f.store, f.genesis);
  const backend = new Backend(f.chain);
  const original = backend.call.bind(backend);
  const indexer = new Indexer({ store: f.store, backend });
  indexer.ready = true;
  backend.call = async (method, params) => {
    if (method === 'getblock') {
      assert.equal(indexer.ready, false, 'readiness must be cleared before awaiting the repair body');
      throw new Error('genesis temporarily unavailable');
    }
    return original(method, params);
  };
  const before = snapshot(f.store);
  await assert.rejects(indexer.syncOnce(), /genesis temporarily unavailable/);
  assert.equal(indexer.ready, false);
  assert.equal(f.store.needsGenesisRepair(), true);
  assert.deepEqual(snapshot(f.store), before);
  backend.call = original;
  await indexer.syncOnce();
  assert.equal(indexer.ready, true);
  assert.equal(f.store.balance('allocation-owner').confirmed, amount);
});

test('a different backend network leaves a legacy index unrepaired and not ready', async t => {
  const f = fixture(t); makeLegacy(f.store, f.genesis);
  const otherGenesis = block('switched-genesis', null, [tx('switched-allocation', [output('1')])]);
  const indexer = new Indexer({ store: f.store, backend: new Backend([otherGenesis]) });
  const before = snapshot(f.store);
  await assert.rejects(indexer.syncOnce(), /differs/);
  assert.equal(indexer.ready, false);
  assert.deepEqual(snapshot(f.store), before);
});

test('an empty legacy database upgrades atomically when its first genesis block is indexed', t => {
  const store = new Store(); t.after(() => store.close());
  const genesis = block('empty-legacy-genesis', null, [tx('empty-legacy-allocation', [output('50.0000000001')])]);
  store.pinNetwork('regtest', genesis.hash);
  store.setMeta('schema', '1');
  store.applyBlock(genesis);
  assert.equal(store.meta('schema'), '2');
  assert.equal(store.needsGenesisRepair(), false);
  assert.equal(store.balance('allocation-owner').confirmed, amount);
  assert.equal(store.history('allocation-owner').length, 1);
});

test('a nonempty legacy database cannot accept more blocks before its accounting is repaired', t => {
  const f = fixture(t); makeLegacy(f.store, f.genesis);
  const next = block('premature-legacy-extension', f.genesis);
  const before = snapshot(f.store);
  assert.throws(() => f.store.applyBlock(next), /[Rr]epair.*genesis/);
  assert.deepEqual(snapshot(f.store), before);
  f.store.repairGenesis(f.genesis);
  f.store.applyBlock(next);
  assert.equal(f.store.tip().hash, next.hash);
});
