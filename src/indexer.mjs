import { EventEmitter } from 'node:events';
import { setImmediate as yieldLoop } from 'node:timers/promises';
import { normalizeTransaction } from './store.mjs';

const HASH = /^[0-9a-f]{64}$/;
function mempoolSnapshot(value) {
  if (!value || !Array.isArray(value.txids) || value.txids.some(id => !HASH.test(id))
    || value.mempool_sequence === undefined) throw new Error('Backend must support getrawmempool(false,true)');
  if (new Set(value.txids).size !== value.txids.length) throw new Error('Duplicate transaction in backend mempool snapshot');
  return { ids: [...value.txids].sort(), sequence: String(value.mempool_sequence) };
}
function sameSnapshot(a, b) {
  return a.sequence === b.sequence && a.ids.length === b.ids.length && a.ids.every((id, i) => id === b.ids[i]);
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
  }

  syncOnce() {
    if (this.pendingSync) return this.pendingSync;
    this.pendingSync = this.synchronize().catch(error => {
      this.ready = false;
      this.lastError = error;
      throw error;
    }).finally(() => { this.pendingSync = null; });
    return this.pendingSync;
  }

  async synchronize() {
    if (this.stopping) return;
    const previousTip = this.store.tip();
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
      const info = await this.backend.call('getblockchaininfo');
      if (info.initialblockdownload) this.ready = false;
      if (info.pruned) throw new Error('The address index requires an unpruned ConnectCoin node');
      if (!Number.isSafeInteger(info.blocks) || info.blocks < 0 || !HASH.test(info.bestblockhash)) throw new Error('Invalid backend chain information');
      const genesis = await this.backend.call('getblockhash', [0]);
      this.store.pinNetwork(info.chain, genesis);
      let tip = this.store.tip();
      if (!tip || tip.hash !== info.bestblockhash) this.ready = false;
      while (tip) {
        if (this.stopping) return;
        if (tip.height <= info.blocks && await this.backend.call('getblockhash', [tip.height]) === tip.hash) break;
        reorg = true;
        // The backend may reorganize after the initial chain-info response.
        // Never expose an intermediate rollback with the old mempool overlay.
        this.ready = false;
        merge(this.store.rollbackTip());
        tip = this.store.tip();
        await yieldLoop();
      }
      for (let height = (tip?.height ?? -1) + 1; height <= info.blocks; height++) {
        if (this.stopping) return;
        const hash = await this.backend.call('getblockhash', [height]);
        const block = await this.backend.call('getblock', [hash, 2]);
        if (block.hash !== hash || block.height !== height) throw new Error('Backend returned a different block than requested');
        this.ready = false;
        merge(this.store.applyBlock(block));
        await yieldLoop();
      }
      // Pruned bounty metadata can re-enter the 600-block window after a
      // rollback. Rehydrate only those rows, not the full address index.
      for (const block of this.store.missingBountyBlocks()) {
        if (this.stopping) return;
        this.ready = false;
        this.store.restoreBountyBlock(await this.backend.call('getblock', [block.hash, 2]));
      }
      const indexedTip = this.store.tip();
      const beforeMempool = await this.backend.call('getblockchaininfo');
      if (beforeMempool.initialblockdownload) this.ready = false;
      if (beforeMempool.bestblockhash !== indexedTip.hash) { this.ready = false; continue; }
      const first = mempoolSnapshot(await this.backend.call('getrawmempool', [false, true]));
      if (this.stopping) return;
      if (first.ids.length > this.maxMempoolTransactions) throw new Error('Backend mempool exceeds configured index capacity');
      const currentIds = new Set(first.ids);
      for (const id of this.mempoolCache.keys()) if (!currentIds.has(id)) this.mempoolCache.delete(id);
      // These normalized inputs/outputs are immutable non-witness data bound
      // to txid. Keep successfully fetched entries even when a tip/sequence
      // race prevents publication, so a busy mempool does not restart its cold
      // download forever. Only the coherent snapshot below reaches SQLite.
      const cache = this.mempoolCache;
      let disappeared = false;
      for (const id of first.ids) {
        if (this.stopping) return;
        let tx = this.mempoolCache.get(id);
        if (!tx) {
          try {
            const raw = await this.backend.transaction(id);
            if (raw.txid !== id) throw new Error('Backend returned a different transaction than requested');
            tx = normalizeTransaction(raw);
          } catch (error) {
            if (error.code === -5) { disappeared = true; break; }
            throw error;
          }
        }
        cache.set(id, tx);
      }
      if (this.stopping) return;
      if (disappeared) continue;
      const second = mempoolSnapshot(await this.backend.call('getrawmempool', [false, true]));
      if (this.stopping) return;
      const afterMempool = await this.backend.call('getblockchaininfo');
      if (this.stopping) return;
      if (afterMempool.initialblockdownload || afterMempool.bestblockhash !== indexedTip.hash) this.ready = false;
      if (!sameSnapshot(first, second) || afterMempool.bestblockhash !== indexedTip.hash) continue;
      merge(this.store.replaceMempool([...cache.values()], { normalized: true }));
      this.mempoolCache = cache;
      this.ready = !Boolean(afterMempool.initialblockdownload);
      this.lastError = null;
      if (this.ready && (this.store.revision() !== previousRevision || resync)) {
        if (resync || reorg) bountyChanges = [{ type: 'resync_required', reason: reorg ? 'reorg' : 'index_snapshot' }];
        this.emit('update', { tip: this.store.tip(), previousTip, addresses: [...addresses], bountyChanges, reorg, resync });
      }
      return;
    }
    throw new Error('Backend changed repeatedly while indexing; snapshot will be retried');
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
