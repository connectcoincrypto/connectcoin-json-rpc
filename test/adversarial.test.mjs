import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { Store } from '../src/store.mjs';
import { Indexer } from '../src/indexer.mjs';

const hash = text => createHash('sha256').update(text).digest('hex');
const block = (name, previous = null) => ({ hash: hash(name), height: previous ? previous.height + 1 : 0,
  previousblockhash: previous?.hash, mediantime: 1700000000, tx: [] });

class AdversarialBackend {
  constructor(chain) { this.chain = chain; this.ibd = false; this.infoCount = 0; }
  async call(method, params = []) {
    if (method === 'getblockchaininfo') {
      this.infoCount++;
      const result = { chain: 'regtest', blocks: this.chain.length - 1, bestblockhash: this.chain.at(-1).hash,
        initialblockdownload: this.ibd, pruned: false };
      await this.infoHook?.(this.infoCount);
      return result;
    }
    if (method === 'getblockhash') {
      this.hashHook?.(params[0]);
      return this.chain[params[0]].hash;
    }
    if (method === 'getblock') return this.chain.find(item => item.hash === params[0]);
    if (method === 'getrawmempool') {
      this.mempoolHook?.();
      return this.mempoolResult?.() ?? { txids: [], mempool_sequence: '1' };
    }
    throw new Error(`Unexpected method ${method}`);
  }
}

async function fixture(t) {
  const genesis = block('genesis');
  const first = block('old-one', genesis);
  const second = block('old-two', first);
  const store = new Store();
  const backend = new AdversarialBackend([genesis, first, second]);
  const indexer = new Indexer({ backend, store });
  t.after(() => store.close());
  await indexer.syncOnce();
  assert.equal(indexer.ready, true);
  return { genesis, first, second, store, backend, indexer };
}

test('reorg after initial same-tip response never exposes partial rollback as ready', async t => {
  const f = await fixture(t);
  const forkFirst = block('fork-one', f.genesis);
  const forkSecond = block('fork-two', forkFirst);
  let switched = false;
  f.backend.hashHook = height => {
    if (!switched && height === 2) {
      switched = true;
      f.backend.chain = [f.genesis, forkFirst, forkSecond];
    }
  };
  const observed = [];
  for (const method of ['rollbackTip', 'applyBlock', 'restoreBountyBlock']) {
    const original = f.store[method].bind(f.store);
    f.store[method] = (...args) => { observed.push({ method, ready: f.indexer.ready }); return original(...args); };
  }
  await f.indexer.syncOnce();
  assert.equal(f.store.tip().hash, forkSecond.hash);
  assert.ok(observed.some(item => item.method === 'rollbackTip'));
  assert.ok(observed.every(item => item.ready === false), JSON.stringify(observed));
  assert.equal(f.indexer.ready, true);
});

test('same-tip backend re-entering initial block download stops ready queries immediately', async t => {
  const f = await fixture(t);
  f.backend.ibd = true;
  const observed = [];
  f.backend.mempoolHook = () => observed.push(f.indexer.ready);
  await f.indexer.syncOnce();
  assert.ok(observed.length);
  assert.ok(observed.every(ready => ready === false));
  assert.equal(f.indexer.ready, false);
});

test('stop during final backend await cannot republish readiness after shutdown', async t => {
  const f = await fixture(t);
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  let enteredResolve;
  const entered = new Promise(resolve => { enteredResolve = resolve; });
  f.backend.infoCount = 0;
  f.backend.infoHook = async count => {
    if (count === 3) { enteredResolve(); await gate; }
  };
  const syncing = f.indexer.syncOnce();
  await entered;
  const stopping = f.indexer.stop();
  release();
  await Promise.all([syncing, stopping]);
  assert.equal(f.indexer.ready, false);
  assert.equal(f.indexer.running, false);
});

test('immutable mempool transaction cache survives failed coherence checks', async t => {
  const f = await fixture(t);
  const txid = hash('pending-transaction');
  const raw = { txid, vin: [{ txid: hash('prior-input'), vout: 0 }],
    vout: [{ n: 0, value: '1.0', type: 1, scriptPubKey: { address: 'alice' } }] };
  let fetches = 0;
  f.backend.transaction = async id => { assert.equal(id, txid); fetches++; return raw; };
  let sequence = 0;
  f.backend.mempoolResult = () => ({ txids: [txid], mempool_sequence: String(++sequence) });
  await assert.rejects(f.indexer.syncOnce(), /changed repeatedly/);
  assert.equal(fetches, 1, 'retries must reuse immutable data, not refetch the entire cold pool');
  assert.equal(f.indexer.ready, false);
  assert.equal(f.store.transactionLocation(txid), null, 'inconsistent pool must not be published');
  f.backend.mempoolResult = () => ({ txids: [txid], mempool_sequence: '100' });
  await f.indexer.syncOnce();
  assert.equal(fetches, 1);
  assert.equal(f.indexer.ready, true);
  assert.equal(f.store.transactionLocation(txid).status, 'pending');
});
