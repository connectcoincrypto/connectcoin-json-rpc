import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { setImmediate as yieldLoop } from 'node:timers/promises';
import { Store } from '../src/store.mjs';
import { Indexer } from '../src/indexer.mjs';
import { PublicAPI } from '../src/api.mjs';

const hash = text => createHash('sha256').update(String(text)).digest('hex');
const output = (address, value = '1') => ({ n: 0, value, type: 1, scriptPubKey: { address } });
const transaction = (name, parent = hash(`funding-${name}`)) => ({
  txid: hash(name), vin: [{ txid: parent, vout: 0 }], vout: [output(name)],
});
const block = (name, previous, tx = []) => ({ hash: hash(name), height: previous ? previous.height + 1 : 0,
  previousblockhash: previous?.hash, mediantime: 1_700_000_000, tx });

class ChurningBackend {
  constructor() {
    this.chain = [block('genesis', null)];
    this.pool = new Map();
    this.sequence = 9_007_199_254_740_999n;
    this.snapshots = 0;
    this.fetched = [];
    this.onSnapshot = null;
    this.onTransaction = null;
  }
  add(tx) { assert.equal(this.pool.has(tx.txid), false); this.pool.set(tx.txid, tx); this.sequence++; }
  remove(id) { assert.equal(this.pool.delete(id), true); this.sequence++; }
  async call(method, params = []) {
    if (method === 'getblockchaininfo') return { chain: 'regtest', blocks: this.chain.length - 1,
      bestblockhash: this.chain.at(-1).hash, pruned: false, initialblockdownload: false };
    if (method === 'getblockhash') return this.chain[params[0]].hash;
    if (method === 'getblock') return this.chain.find(item => item.hash === params[0]);
    if (method === 'getrawmempool') {
      this.snapshots++;
      this.onSnapshot?.(this.snapshots);
      return { txids: [...this.pool.keys()], mempool_sequence: String(this.sequence) };
    }
    throw new Error(`Unexpected backend call ${method}`);
  }
  async transaction(id) {
    this.fetched.push(id);
    await this.onTransaction?.(id);
    const tx = this.pool.get(id);
    if (!tx) throw Object.assign(new Error('Transaction disappeared'), { code: -5 });
    return tx;
  }
}

function fixture(t, options = {}) {
  const backend = new ChurningBackend();
  const store = new Store();
  t.after(() => store.close());
  return { backend, store, indexer: new Indexer({ backend, store, ...options }) };
}

const pendingIds = store => store.db.prepare('SELECT txid FROM pending_transactions ORDER BY txid').all().map(row => row.txid);

test('continuous additive load publishes complete snapshots and catches arrivals on subsequent polls', async t => {
  const { backend, store, indexer } = fixture(t);
  const funding = { txid: hash('load-bounties'), vin: [{ txid: hash('load-funding'), vout: 0 }],
    vout: Array.from({ length: 6000 }, (_, n) => ({ n, value: '1', type: 2, domain: `claim-${n}.example.com`,
      connection_work_target: 'f'.repeat(64), root_certificates_version: 1, signature_algorithms_mask: 7, scriptPubKey: {} })) };
  const bountyBlock = block('load-bounty-block', backend.chain[0], [funding]);
  backend.chain.push(bountyBlock);
  let nextBounty = 0;
  const claim = name => {
    assert.ok(nextBounty < funding.vout.length);
    const tx = transaction(name, funding.txid);
    tx.vin[0].vout = nextBounty++;
    return tx;
  };
  for (let i = 0; i < 1000; i++) backend.add(claim(`initial-${i}`));
  let arrival = 0;
  backend.onTransaction = async () => {
    backend.add(claim(`during-fetch-${arrival++}`));
    await yieldLoop();
  };
  // Every attempted snapshot sees another arrival even when every initial
  // transaction was cached. The old identical-sequence policy never converges.
  backend.onSnapshot = count => { if (count % 2 === 0) backend.add(claim(`during-check-${arrival++}`)); };
  const updates = []; indexer.on('update', update => updates.push(update));
  for (let poll = 0; poll < 3; poll++) {
    const expected = [...backend.pool.keys()].sort();
    const started = performance.now(), snapshots = backend.snapshots;
    try { await indexer.syncOnce(); }
    finally {
      t.diagnostic(JSON.stringify({ poll, initialClaims: expected.length, backendClaims: backend.pool.size,
        attempts: indexer.lastSyncStats?.attempts ?? Math.ceil((backend.snapshots - snapshots) / 2),
        durationMs: Math.round(performance.now() - started), ready: indexer.ready }));
    }
    assert.equal(indexer.ready, true);
    assert.equal(indexer.lastError, null);
    assert.deepEqual(pendingIds(store), expected, 'publish every and only txid from the first complete snapshot');
    assert.equal(backend.snapshots, (poll + 1) * 2, 'additive growth must not consume retries');
    assert.ok(backend.pool.size > expected.length, 'arrivals remain for the next poll');
    assert.equal(store.bountyPage(bountyBlock.hash, 0, 1)[0].status, 'pending_spend');
  }
  backend.onTransaction = null;
  backend.onSnapshot = null;
  await indexer.syncOnce();
  assert.deepEqual(pendingIds(store), [...backend.pool.keys()].sort());
  assert.equal(new Set(backend.fetched).size, backend.fetched.length, 'cached immutable transactions are fetched once');
  assert.equal(updates.length, 4);
});

test('a parent and child arriving during collection are indexed together on the next poll', async t => {
  const { backend, store, indexer } = fixture(t);
  const initial = transaction('initial');
  const parent = transaction('parent');
  const child = transaction('child', parent.txid);
  backend.add(initial);
  backend.onTransaction = () => {
    backend.onTransaction = null;
    backend.add(parent);
    backend.add(child);
  };
  await indexer.syncOnce();
  assert.deepEqual(pendingIds(store), [initial.txid]);
  assert.equal(store.balance('parent').total, '0');
  assert.equal(store.balance('child').total, '0');
  await indexer.syncOnce();
  assert.deepEqual(pendingIds(store), [initial, parent, child].map(tx => tx.txid).sort());
  assert.equal(store.balance('parent').total, '0', 'the child consumes the parent output');
  assert.equal(store.balance('child').total, '10000000000');
});

test('replacement plus net growth retries instead of publishing a removed transaction', async t => {
  const { backend, store, indexer } = fixture(t);
  const old = transaction('old');
  const replacement = transaction('replacement', old.vin[0].txid);
  const extra = transaction('extra');
  backend.add(old);
  backend.onSnapshot = count => {
    if (count !== 2) return;
    backend.remove(old.txid);
    backend.add(replacement);
    backend.add(extra);
  };
  const publications = []; indexer.on('update', () => publications.push(pendingIds(store)));
  await indexer.syncOnce();
  assert.equal(backend.snapshots, 4);
  assert.deepEqual(publications, [[replacement.txid, extra.txid].sort()]);
  assert.equal(store.transactionLocation(old.txid), null);
  assert.equal(indexer.mempoolCache.has(old.txid), false);
});

test('hidden remove/readd churn cannot pass the additive sequence proof', async t => {
  const { backend, store, indexer } = fixture(t);
  const existing = transaction('existing');
  backend.add(existing);
  await indexer.syncOnce();
  const revision = store.revision();
  backend.onSnapshot = count => {
    if (count % 2 !== 0) return;
    backend.remove(existing.txid);
    backend.add(existing);
    backend.add(transaction(`new-${count}`));
  };
  await assert.rejects(indexer.syncOnce(), /changed repeatedly/);
  assert.equal(indexer.ready, false);
  assert.equal(store.revision(), revision);
  assert.deepEqual(pendingIds(store), [existing.txid]);
  assert.equal(backend.snapshots, 10, 'hidden removals exhaust the bounded four retries');
  assert.equal(indexer.lastSyncStats.errorKind, 'churn');
  assert.equal(indexer.lastSyncStats.attempts, 4);
  assert.equal(indexer.lastSyncStats.phase, 'mempool_verify');
});

test('observed removals immediately disable reads until a coherent retry finishes', async t => {
  for (const mode of ['fetch', 'verification']) await t.test(mode, async t => {
    const { backend, store, indexer } = fixture(t);
    await indexer.syncOnce();
    const pending = transaction(`removed-during-${mode}`);
    backend.add(pending);
    if (mode === 'fetch') backend.onTransaction = () => {
      backend.remove(pending.txid);
      backend.onTransaction = null;
    };
    let checked = false;
    backend.onSnapshot = count => {
      if (mode === 'verification' && count === 4) backend.remove(pending.txid);
      if (count === (mode === 'fetch' ? 4 : 5)) {
        assert.equal(indexer.ready, false, 'a known removal must not expose the previous overlay during retry');
        checked = true;
      }
    };
    await indexer.syncOnce();
    assert.equal(checked, true);
    assert.equal(indexer.ready, true);
    assert.deepEqual(pendingIds(store), []);
  });
});

test('readiness recovery wakes a public read even when remove/readd leaves the published pool unchanged', async t => {
  const { backend, store, indexer } = fixture(t);
  const existing = transaction('unchanged-after-retry');
  backend.add(existing);
  await indexer.syncOnce();
  const revision = store.revision();
  const api = new PublicAPI({ backend, store, indexer });
  const controller = new AbortController();
  t.after(() => { controller.abort(); api.close(); });
  const context = { signal: controller.signal };
  const baseline = await api.dispatch('getbountychanges', {}, context);
  let readResult, readFailure, read;
  const updates = []; indexer.on('update', update => updates.push(update));
  backend.onSnapshot = count => {
    if (count === 4) { backend.remove(existing.txid); backend.add(existing); }
    if (count === 5) {
      assert.equal(indexer.ready, false);
      read = api.dispatch('getchaintip', {}, context).then(result => { readResult = result; }, error => { readFailure = error; });
    }
  };
  await indexer.syncOnce();
  await yieldLoop();
  try {
    assert.ok(read, 'the public read must start while synchronization is retrying');
    assert.equal(readFailure, undefined);
    assert.equal(readResult?.hash, store.tip().hash, 'read must wake promptly instead of reaching its timeout');
    assert.equal(store.revision(), revision);
    assert.equal(updates.length, 1);
    assert.equal(updates[0].resync, false);
    assert.deepEqual(updates[0].bountyChanges, []);
    assert.deepEqual((await api.dispatch('getbountychanges', { cursor: baseline.next_cursor }, context)).changes, []);
  } finally {
    controller.abort();
    await read;
  }
});

test('a changed chain tip during additive growth is reconciled before publication', async t => {
  const { backend, store, indexer } = fixture(t);
  const genesis = backend.chain[0];
  const oldBlock = block('old-block', genesis);
  const nextBlock = block('replacement-block', genesis);
  backend.chain.push(oldBlock);
  await indexer.syncOnce();
  const pending = transaction('pending');
  backend.add(pending);
  backend.onSnapshot = count => {
    if (count !== 4) return;
    backend.chain[1] = nextBlock;
    backend.add(transaction('new-arrival'));
  };
  const publications = []; indexer.on('update', event => publications.push(event));
  await indexer.syncOnce();
  assert.equal(backend.snapshots, 6);
  assert.equal(indexer.ready, true);
  assert.equal(store.tip().hash, nextBlock.hash);
  assert.deepEqual(pendingIds(store), [...backend.pool.keys()].sort());
  assert.equal(publications.length, 1);
  assert.equal(publications[0].reorg, true);
  assert.deepEqual(publications[0].bountyChanges, [{ type: 'resync_required', reason: 'reorg' }]);
});

test('fetch concurrency is bounded to three and transaction caching survives a removal race', async t => {
  const { backend, store, indexer } = fixture(t);
  const txs = Array.from({ length: 25 }, (_, i) => transaction(`parallel-${i}`));
  txs.forEach(tx => backend.add(tx));
  let active = 0, maximum = 0;
  backend.onTransaction = async () => {
    maximum = Math.max(maximum, ++active);
    await yieldLoop();
    active--;
  };
  backend.onSnapshot = count => { if (count === 2) backend.remove(txs[0].txid); };
  await indexer.syncOnce();
  assert.equal(maximum, 3);
  assert.equal(active, 0);
  assert.equal(backend.fetched.length, txs.length, 'surviving transactions are not fetched again on retry');
  assert.deepEqual(pendingIds(store), txs.slice(1).map(tx => tx.txid).sort());
});

test('fetch failure and stop await all workers and never publish partial state', async t => {
  for (const mode of ['failure', 'stop']) await t.test(mode, async t => {
    const { backend, store, indexer } = fixture(t);
    Array.from({ length: 12 }, (_, i) => transaction(`${mode}-${i}`)).forEach(tx => backend.add(tx));
    let release;
    const barrier = new Promise(resolve => { release = resolve; });
    let entered = 0, completed = 0;
    backend.onTransaction = async () => {
      const worker = ++entered;
      if (mode === 'failure' && worker === 1) throw new Error('RPC failure');
      await barrier;
      completed++;
    };
    let settled = false;
    const syncing = indexer.syncOnce();
    const result = syncing.then(() => { settled = true; }, error => { settled = true; return error; });
    try {
      // Event-loop turns bound the test without wall-clock sleeps or promises
      // that can hang forever if the implementation stops starting workers.
      for (let turn = 0; turn < 10 && entered < 3 && !settled; turn++) await yieldLoop();
      assert.equal(entered, 3);
      const stopping = mode === 'stop' ? indexer.stop() : null;
      await yieldLoop();
      assert.equal(settled, false, 'the sync must not return with RPC fetches still in flight');
      release();
      const error = await result;
      if (mode === 'failure') assert.match(error.message, /RPC failure/);
      await stopping;
      assert.equal(entered, 3, 'no new work starts after failure or stop');
      assert.equal(completed, mode === 'failure' ? 2 : 3);
      assert.equal(indexer.ready, false);
      assert.deepEqual(pendingIds(store), []);
    } finally {
      release();
      await result;
    }
  });
});

test('growth beyond mempool capacity fails before publishing a smaller snapshot', async t => {
  const { backend, store, indexer } = fixture(t, { maxMempoolTransactions: 1 });
  backend.add(transaction('within-limit'));
  backend.onSnapshot = count => { if (count === 2) backend.add(transaction('over-limit')); };
  await assert.rejects(indexer.syncOnce(), /exceeds configured index capacity/);
  assert.equal(indexer.ready, false);
  assert.deepEqual(pendingIds(store), []);
  assert.equal(indexer.lastSyncStats.errorKind, 'capacity');
});

test('mempool sequences reject lossy or invalid counters and retries cannot mask a reset', async t => {
  for (const sequence of [-1, 1.5, Number.MAX_SAFE_INTEGER + 1, '-1', '1.0', 'bogus', '18446744073709551616']) {
    await t.test(String(sequence), async t => {
      const { backend, indexer } = fixture(t);
      const call = backend.call.bind(backend);
      backend.call = async (method, params) => {
        const result = await call(method, params);
        if (method === 'getrawmempool') result.mempool_sequence = sequence;
        return result;
      };
      await assert.rejects(indexer.syncOnce(), /mempool sequence/);
      assert.equal(indexer.ready, false);
    });
  }
  await t.test('counter reset', async t => {
    const { backend, store, indexer } = fixture(t);
    const existing = transaction('reset-existing');
    backend.add(existing);
    backend.onSnapshot = count => {
      if (count === 2) { backend.sequence = 1n; backend.add(transaction('after-reset')); }
    };
    await indexer.syncOnce();
    assert.equal(backend.snapshots, 4, 'a lower sequence cannot certify the earlier snapshot');
    assert.deepEqual(pendingIds(store), [...backend.pool.keys()].sort());
  });
});

test('sync diagnostics report bounded progress and never include backend or index error text', async t => {
  await t.test('success and catchup', async t => {
    const { backend, indexer } = fixture(t);
    const events = []; indexer.on('syncStats', stats => events.push(stats));
    assert.equal(indexer.lastSyncStats, null);
    await indexer.syncOnce();
    assert.equal(events.length, 1);
    assert.equal(events[0], indexer.lastSyncStats);
    assert.equal(Object.isFrozen(events[0]), true);
    assert.deepEqual({ ...events[0], durationMs: 0 }, {
      durationMs: 0, ready: true, startHeight: null, endHeight: 0, attempts: 1, phase: 'complete', errorKind: null,
    });
    backend.chain.push(block('catchup-block', backend.chain[0]));
    await indexer.syncOnce();
    assert.equal(events.length, 2);
    assert.equal(events[1].startHeight, 0);
    assert.equal(events[1].endHeight, 1);
    assert.ok(Number.isSafeInteger(events[1].durationMs) && events[1].durationMs >= 0);
  });
  for (const kind of ['backend', 'network', 'index']) await t.test(kind, async t => {
    const { backend, store, indexer } = fixture(t);
    const failure = new Error('SENSITIVE cookie=C:/private/node.cookie transaction=PRIVATE wallet=SECRET');
    if (kind === 'backend') backend.call = async () => { throw failure; };
    if (kind === 'network') store.pinNetwork = () => { throw failure; };
    if (kind === 'index') store.replaceMempool = () => { throw failure; };
    const events = []; indexer.on('syncStats', stats => events.push(stats));
    await assert.rejects(indexer.syncOnce(), error => error === failure);
    assert.equal(events.length, 1);
    assert.equal(events[0].errorKind, kind);
    assert.equal(events[0].ready, false);
    assert.equal(events[0].attempts, 1);
    assert.equal(events[0].phase, { backend: 'chain_info', network: 'network', index: 'mempool_publish' }[kind]);
    assert.doesNotMatch(JSON.stringify(events[0]), /SENSITIVE|cookie|private|transaction|wallet|SECRET/);
  });
});
