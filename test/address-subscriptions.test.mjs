import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { Store } from '../src/store.mjs';
import { Indexer } from '../src/indexer.mjs';
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

async function fixture(t, options) {
  const funding = tx('funding', [[ALICE, '10'], [BOB, '20']]);
  const genesis = block('genesis', null, [funding]);
  const chain = [genesis]; let pool = [], sequence = 0;
  const backend = {
    call: async (method, params = []) => {
      if (method === 'getblockchaininfo') return { chain: 'main', blocks: chain.length - 1, bestblockhash: chain.at(-1).hash };
      if (method === 'getblockhash') return chain[params[0]].hash;
      if (method === 'getblock') return chain.find(b => b.hash === params[0]);
      if (method === 'getrawmempool') return { txids: pool.map(transaction => transaction.txid), mempool_sequence: sequence };
      throw new Error(`Unexpected method ${method}`);
    },
    transaction: async id => pool.find(transaction => transaction.txid === id),
  };
  const store = new Store(':memory:', options), indexer = new Indexer({ backend, store });
  const api = new PublicAPI({ store, indexer, backend });
  t.after(() => { api.close(); store.close(); });
  await indexer.syncOnce();
  const notices = [], updates = [];
  indexer.on('update', update => updates.push(update));
  const context = { ip: '127.0.0.1', onClose: () => {}, notify: async (method, params) => notices.push(params) };
  await api.dispatch('subscribeaddress', { address: ALICE, changes_only: true }, context);
  const setPool = async transactions => { pool = transactions; sequence++; await indexer.syncOnce(); };
  const mine = async transactions => {
    chain.push(block(`block-${chain.length}`, chain.at(-1), transactions));
    const included = new Set((transactions ?? []).map(transaction => transaction.txid));
    pool = pool.filter(transaction => !included.has(transaction.txid)); sequence++;
    await indexer.syncOnce();
  };
  return { funding, genesis, chain, store, indexer, api, notices, updates, setPool, mine, context };
}

test('changes-only subscriptions ignore new blocks and unrelated mempool changes while own pending state is unchanged', async t => {
  const f = await fixture(t);
  const own = tx('own-pending', [[ALICE, '9']], [input(f.funding)]);
  const unrelated = tx('unrelated-pending', [[CAROL, '19']], [input(f.funding, 1)]);
  await f.setPool([own]); assert.equal(f.notices.length, 1); f.notices.length = 0;
  await f.setPool([own, unrelated]);
  await f.mine([]);
  await f.setPool([own]);
  await f.mine([]);
  assert.deepEqual(f.notices, []);
  assert.ok(f.updates.slice(1).every(update => !update.addresses.includes(ALICE)));

  const replacement = tx('own-replacement', [[ALICE, '8']], [input(f.funding)]);
  await f.setPool([replacement]); assert.equal(f.notices.length, 1);
  await f.setPool([]); assert.equal(f.notices.length, 2);
  await f.setPool([replacement]); assert.equal(f.notices.length, 3);
  await f.mine([replacement]); assert.equal(f.notices.length, 4);
  await f.mine([]); assert.equal(f.notices.length, 4);
  assert.equal(f.store.balance(ALICE).confirmed, '80000000000');
});

test('confirmed receives and spends notify the owning address but no unrelated address', async t => {
  const f = await fixture(t);
  const receive = tx('receive', [[ALICE, '19']], [input(f.funding, 1)]);
  await f.mine([receive]); assert.equal(f.notices.length, 1);
  const spend = tx('spend', [[CAROL, '18']], [input(receive)]);
  await f.mine([spend]); assert.equal(f.notices.length, 2);
  await f.mine([]); assert.equal(f.notices.length, 2);
});

test('coinbase maturity and its rollback notify once and maturity lookups use the existing height index', async t => {
  const f = await fixture(t);
  for (let height = 1; height < 99; height++) await f.mine([]);
  assert.deepEqual(f.notices, []); assert.equal(f.store.balance(ALICE).immature, '100000000000');
  const cursor = (await f.api.dispatch('getaddresschanges', { addresses: [ALICE] }, f.context)).next_cursor;
  await f.mine([]);
  assert.equal(f.notices.length, 1); assert.equal(f.store.balance(ALICE).immature, '0');
  const delta = await f.api.dispatch('getaddresschanges', { addresses: [ALICE], cursor }, f.context);
  assert.equal(delta.changes.length, 1); assert.equal(delta.changes[0].item.mature, true);
  assert.equal(delta.changes[0].item.confirmations, 100);
  await f.mine([]); assert.equal(f.notices.length, 1);
  f.chain.pop(); f.chain.pop(); await f.indexer.syncOnce();
  assert.equal(f.notices.length, 2); assert.equal(f.notices[1].reorg, true);
  assert.equal(f.store.balance(ALICE).immature, '100000000000');
  assert.ok(f.updates.at(-1).addresses.includes(ALICE));
  const plan = f.store.db.prepare(`EXPLAIN QUERY PLAN SELECT o.address,o.txid,o.vout FROM outputs o
    LEFT JOIN spends s USING(txid,vout) WHERE o.height=? AND o.coinbase=1 AND s.spender IS NULL`).all(0);
  assert.ok(plan.some(row => /SEARCH o USING INDEX outputs_height/.test(row.detail)));
  assert.equal(plan.some(row => /SCAN o\b/.test(row.detail)), false);
});

test('mempool changed-key overflow explicitly invalidates subscriptions without silently losing changes', async t => {
  const f = await fixture(t, { maxAddressMutationKeys: 1 });
  const own = tx('overflow-pending', [[ALICE, '9']], [input(f.funding)]);
  await f.setPool([own]);
  assert.equal(f.notices.length, 1); assert.equal(f.updates.at(-1).resync, true);
  f.notices.length = 0;
  await f.mine([]);
  assert.deepEqual(f.notices, []);
});

test('spent coinbase outputs do not trigger a later maturity refresh', async t => {
  const f = await fixture(t);
  // The store accepts already-validated backend blocks; this synthetic early
  // spend isolates its maturity filter without requiring a second coinbase.
  await f.mine([tx('spent-coinbase', [[CAROL, '9']], [input(f.funding)])]);
  f.notices.length = 0;
  while (f.chain.length <= 100) await f.mine([]);
  assert.deepEqual(f.notices, []);
});
