import { EventEmitter } from 'node:events';
import { setImmediate as yieldLoop } from 'node:timers/promises';
import { normalizeTransaction } from './store.mjs';

const HASH = /^[0-9a-f]{64}$/;
const MEMPOOL_FETCH_CONCURRENCY = 3;
function mempoolSnapshot(value) {
  if (!value || !Array.isArray(value.txids) || value.txids.some(id => !HASH.test(id))
    || value.mempool_sequence === undefined) throw new Error('Backend must support getrawmempool(false,true)');
  if (new Set(value.txids).size !== value.txids.length) throw new Error('Duplicate transaction in backend mempool snapshot');
  const sequence = value.mempool_sequence;
  if ((typeof sequence !== 'string' && !(Number.isSafeInteger(sequence) && sequence >= 0))
    || !/^(0|[1-9][0-9]{0,19})$/.test(String(sequence)) || BigInt(sequence) > 0xffffffffffffffffn) {
    throw new Error('Invalid backend mempool sequence');
  }
  return { ids: [...value.txids].sort(), sequence: BigInt(sequence) };
}
function onlyAdditions(a, b) {
  // ConnectCoin increments its uint64 sequence once for every addition and
  // removal. A superset with exactly one increment per extra txid proves that
  // no removals (including remove/readd races) occurred during collection.
  const added = b.ids.length - a.ids.length;
  if (added < 0 || b.sequence - a.sequence !== BigInt(added)) return false;
  const finalIds = new Set(b.ids);
  return a.ids.every(id => finalIds.has(id));
}

export class Indexer extends EventEmitter {
  constructor({ backend, store, pollIntervalMs = 2000, maxMempoolTransactions = 100_000, maxUpdateItems = 10_000 } = {}) {
    super();
    if (!backend || !store) throw new Error('Indexer requires backend and store');
    if (!Number.isSafeInteger(pollIntervalMs) || pollIntervalMs < 10) throw new Error('Invalid indexer polling interval');
    this.backend = backend;
    this.store = store;
    this.pollIntervalMs = pollIntervalMs;
    this.maxMempoolTransactions = maxMempoolTransactions;
    this.maxUpdateItems = maxUpdateItems;
    this.ready = false;
    this.running = false;
    this.stopping = false;
    this.pendingSync = null;
    this.timer = null;
    this.mempoolCache = new Map();
    this.lastError = null;
    this.lastSyncStats = null;
  }

  syncOnce() {
    if (this.pendingSync) return this.pendingSync;
    const started = performance.now();
    const stats = { durationMs: 0, ready: false, startHeight: null, endHeight: null,
      attempts: 0, phase: 'starting', errorKind: null };
    this.pendingSync = this.synchronize(stats).catch(error => {
      this.ready = false;
      this.lastError = error;
      stats.errorKind ??= 'index';
      throw error;
    }).finally(() => {
      stats.durationMs = Math.round(performance.now() - started);
      stats.ready = this.ready;
      if (this.stopping) stats.phase = 'stopped';
      try { stats.endHeight = this.store.tip()?.height ?? null; } catch { /* Preserve the original index failure. */ }
      this.lastSyncStats = Object.freeze({ ...stats });
      this.pendingSync = null;
      // Diagnostics contain only fixed categories and counters, never backend
      // messages, transaction data, addresses, credentials, or filesystem paths.
      this.emit('syncStats', this.lastSyncStats);
    });
    return this.pendingSync;
  }

  async synchronize(stats) {
    if (this.stopping) return;
    const call = async (...args) => {
      try { return await this.backend.call(...args); }
      catch (error) { stats.errorKind = 'backend'; throw error; }
    };
    const fail = (kind, message) => { stats.errorKind = kind; throw new Error(message); };
    const previousTip = this.store.tip();
    stats.startHeight = previousTip?.height ?? null;
    const previousRevision = this.store.revision();
    let reorg = false;
    let resync = !this.ready;
    const addresses = new Set();
    let bountyChanges = [];
    const merge = update => {
      if (resync) return;
      if (update.resync) { resync = true; addresses.clear(); bountyChanges = []; return; }
      for (const address of update.addresses) addresses.add(address);
      for (const change of update.bountyChanges) bountyChanges.push(change);
      if (addresses.size + bountyChanges.length > this.maxUpdateItems) {
        resync = true;
        addresses.clear();
        bountyChanges = [];
      }
    };
    // A bounded number of retries prevents a continuously-changing backend
    // from monopolizing the event loop. The next poll resumes persisted work.
    for (let attempt = 0; attempt < 4; attempt++) {
      stats.attempts = attempt + 1;
      stats.phase = 'chain_info';
      const info = await call('getblockchaininfo');
      if (info.initialblockdownload) this.ready = false;
      if (info.pruned) fail('network', 'The address index requires an unpruned ConnectCoin node');
      if (!Number.isSafeInteger(info.blocks) || info.blocks < 0 || !HASH.test(info.bestblockhash)) fail('backend', 'Invalid backend chain information');
      stats.phase = 'network';
      const genesis = await call('getblockhash', [0]);
      try { this.store.pinNetwork(info.chain, genesis); }
      catch (error) { stats.errorKind = 'network'; throw error; }
      let tip = this.store.tip();
      if (!tip || tip.hash !== info.bestblockhash) this.ready = false;
      stats.phase = 'rollback';
      while (tip) {
        if (this.stopping) return;
        if (tip.height <= info.blocks && await call('getblockhash', [tip.height]) === tip.hash) break;
        reorg = true;
        // The backend may reorganize after the initial chain-info response.
        // Never expose an intermediate rollback with the old mempool overlay.
        this.ready = false;
        merge(this.store.rollbackTip());
        tip = this.store.tip();
        await yieldLoop();
      }
      stats.phase = 'blocks';
      for (let height = (tip?.height ?? -1) + 1; height <= info.blocks; height++) {
        if (this.stopping) return;
        const hash = await call('getblockhash', [height]);
        const block = await call('getblock', [hash, 2]);
        if (block.hash !== hash || block.height !== height) fail('backend', 'Backend returned a different block than requested');
        this.ready = false;
        merge(this.store.applyBlock(block));
        await yieldLoop();
      }
      // Pruned bounty metadata can re-enter the 600-block window after a
      // rollback. Rehydrate only those rows, not the full address index.
      stats.phase = 'bounty_restore';
      for (const block of this.store.missingBountyBlocks()) {
        if (this.stopping) return;
        this.ready = false;
        this.store.restoreBountyBlock(await call('getblock', [block.hash, 2]));
      }
      const indexedTip = this.store.tip();
      stats.phase = 'mempool_snapshot';
      const beforeMempool = await call('getblockchaininfo');
      if (beforeMempool.initialblockdownload) this.ready = false;
      if (beforeMempool.bestblockhash !== indexedTip.hash) { this.ready = false; continue; }
      const snapshot = async () => {
        const value = await call('getrawmempool', [false, true]);
        try { return mempoolSnapshot(value); }
        catch (error) { stats.errorKind = 'backend'; throw error; }
      };
      const first = await snapshot();
      if (this.stopping) return;
      if (first.ids.length > this.maxMempoolTransactions) fail('capacity', 'Backend mempool exceeds configured index capacity');
      const currentIds = new Set(first.ids);
      for (const id of this.mempoolCache.keys()) if (!currentIds.has(id)) this.mempoolCache.delete(id);
      // These normalized inputs/outputs are immutable non-witness data bound
      // to txid. Keep successfully fetched entries even when a tip/sequence
      // race prevents publication, so a busy mempool does not restart its cold
      // download forever. Only the coherent snapshot below reaches SQLite.
      const cache = this.mempoolCache;
      stats.phase = 'mempool_fetch';
      let next = 0, disappeared = false, fetchError = null;
      const collect = async () => {
        while (!this.stopping && !disappeared && !fetchError && next < first.ids.length) {
          const id = first.ids[next++];
          if (cache.has(id)) continue;
          try {
            const raw = await this.backend.transaction(id);
            if (raw.txid !== id) throw new Error('Backend returned a different transaction than requested');
            cache.set(id, normalizeTransaction(raw));
          } catch (error) {
            if (error.code === -5) { disappeared = true; this.ready = false; }
            else fetchError ??= error;
          }
        }
      };
      // Keep RPC work bounded and leave one of NodeBackend's four connections
      // available for public requests. All workers settle before retry/stop.
      await Promise.all(Array.from({ length: Math.min(MEMPOOL_FETCH_CONCURRENCY, first.ids.length) }, collect));
      if (this.stopping) return;
      if (fetchError) { stats.errorKind = 'backend'; throw fetchError; }
      if (disappeared) continue;
      stats.phase = 'mempool_verify';
      const second = await snapshot();
      if (this.stopping) return;
      if (second.ids.length > this.maxMempoolTransactions) fail('capacity', 'Backend mempool exceeds configured index capacity');
      const afterMempool = await call('getblockchaininfo');
      if (this.stopping) return;
      if (afterMempool.initialblockdownload || afterMempool.bestblockhash !== indexedTip.hash) this.ready = false;
      if (!onlyAdditions(first, second) || afterMempool.bestblockhash !== indexedTip.hash) { this.ready = false; continue; }
      // Publish the complete first snapshot, whose chain and membership stayed
      // valid throughout collection. Later arrivals are included next poll;
      // chasing them here can prevent publication indefinitely under load.
      stats.phase = 'mempool_publish';
      merge(this.store.replaceMempool(first.ids.map(id => cache.get(id)), { normalized: true }));
      this.mempoolCache = cache;
      const recovered = !this.ready;
      this.ready = !Boolean(afterMempool.initialblockdownload);
      this.lastError = null;
      // A failed proof can disable readiness and then converge to the already
      // published data. Wake waiting readers even when no revision changed.
      if (this.ready && (this.store.revision() !== previousRevision || resync || recovered)) {
        if (resync || reorg) bountyChanges = [{ type: 'resync_required', reason: reorg ? 'reorg' : 'index_snapshot' }];
        this.emit('update', { tip: this.store.tip(), previousTip, addresses: [...addresses], bountyChanges, reorg, resync });
      }
      stats.phase = 'complete';
      return;
    }
    fail('churn', 'Backend changed repeatedly while indexing; snapshot will be retried');
  }

  start() {
    if (this.running) return;
    this.running = true;
    this.stopping = false;
    const tick = async () => {
      try { await this.syncOnce(); }
      catch (error) { this.emit('syncError', error); }
      if (this.running && !this.stopping) this.timer = setTimeout(tick, this.pollIntervalMs);
    };
    void tick();
  }

  async stop() {
    this.running = false;
    this.stopping = true;
    this.ready = false;
    clearTimeout(this.timer);
    if (this.pendingSync) await this.pendingSync.catch(() => {});
  }
}
