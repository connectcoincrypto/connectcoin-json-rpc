import { randomUUID } from 'node:crypto';
import { setImmediate as yieldLoop } from 'node:timers/promises';
import { RpcError } from './errors.mjs';
import { Cursors } from './cursor.mjs';
import { normalizeAddress } from './address.mjs';

export const METHODS = Object.freeze([
  'getchaintip', 'getrecentblockhashes', 'getblockbounties',
  'getaddressbalance', 'getaddressutxos', 'getaddresshistory',
  'gettransaction', 'sendrawtransaction', 'getbountychanges',
  'subscribebounties', 'subscribeaddress', 'subscribetip', 'unsubscribe',
]);

export function hash(value) {
  if (typeof value !== 'string' || !/^[0-9a-fA-F]{64}$/.test(value)) throw new RpcError(-32602, 'Expected a 64-character hexadecimal hash.');
  return value.toLowerCase();
}

function paramsOnly(params, names) {
  if (!params || typeof params !== 'object' || Array.isArray(params) || Object.keys(params).some(k => !names.includes(k))) {
    throw new RpcError(-32602, 'Unexpected parameters.');
  }
}

export class PublicAPI {
  constructor({ store, indexer, backend, options = {} }) {
    this.store = store;
    this.indexer = indexer;
    this.backend = backend;
    this.options = { pageSize: 100, maxSubscriptionsPerIP: 100, maxSubscriptions: 10000,
      maxJournalEvents: 10000, maxJournalBytes: 8 * 1024 * 1024, maxTransactionBytes: 400000, ...options };
    this.cursors = new Cursors();
    this.subscriptions = new Map();
    this.contexts = new WeakSet();
    this.journal = [];
    this.journalBytes = 0;
    this.sequence = 0;
    this.journalFloor = 0;
    this.onUpdate = update => this.updated(update);
    this.indexer.on('update', this.onUpdate);
  }

  close() { this.indexer.off('update', this.onUpdate); this.subscriptions.clear(); }
  revision() { return this.store.revision(); }
  tip() { return this.store.tip(); }
  ready() {
    if (!this.indexer.ready || !this.tip()) throw new RpcError(-32001, 'Index is synchronizing or backend is unavailable; retry later.');
  }
  async broadcast(params, context) {
    paramsOnly(params, ['transaction_hex']);
    const hex = params.transaction_hex;
    if (typeof hex !== 'string' || hex.length < 20 || hex.length % 2 || hex.length > this.options.maxTransactionBytes * 2 || !/^[0-9a-fA-F]+$/.test(hex)) {
      throw new RpcError(-32602, `Expected transaction hex of at most ${this.options.maxTransactionBytes} bytes.`);
    }
    // Broadcasting needs node validation, not the address/mempool index. Keep
    // network pinning even during catchup, failure recovery or a backend switch.
    const pinned = this.tip();
    if (!pinned?.chain || !/^[0-9a-f]{64}$/.test(pinned.genesis_hash ?? '')) {
      throw new RpcError(-32001, 'Backend network identity is not initialized; retry after synchronization.');
    }
    const checkCancelled = () => {
      if (context.signal?.aborted || this.indexer.stopping) throw new RpcError(-32001, 'Broadcast cancelled before submission.');
    };
    checkCancelled();
    let info, genesis;
    try {
      info = await this.backend.call('getblockchaininfo');
      genesis = await this.backend.call('getblockhash', [0]);
    } catch {
      throw new RpcError(-32002, 'Backend unavailable; transaction was not submitted.');
    }
    checkCancelled();
    if (info?.chain !== pinned.chain || genesis !== pinned.genesis_hash) {
      throw new RpcError(-32001, 'Backend network identity differs from this index; transaction was not submitted.');
    }
    try { return { txid: await this.backend.call('sendrawtransaction', [hex]) }; }
    catch (error) {
      // A failed/unknown broadcast must never be retried automatically here.
      if ([-22, -25, -26, -27, -8].includes(error.code)) throw new RpcError(-32020, 'Node rejected the transaction.', { node_code: error.code });
      throw new RpcError(-32002, 'Backend unavailable or broadcast outcome unknown; check the txid before retrying.');
    }
  }
  async waitForReady(context, timeoutMs = 30000) {
    if (context.signal?.aborted) throw new RpcError(-32001, 'Transfer cancelled.');
    if (this.indexer.ready) return;
    // A short normal catchup must not restart a long stream. Wait for a fully
    // published index, never read half-applied multi-block/reorg state.
    await new Promise((resolve, reject) => {
      const finish = error => {
        clearTimeout(timer);
        this.indexer.off('update', updated);
        this.indexer.off('syncError', failed);
        context.signal?.removeEventListener('abort', aborted);
        if (error) reject(error); else resolve();
      };
      const updated = () => { if (this.indexer.ready) finish(); };
      const failed = () => finish(new RpcError(-32001, 'Backend synchronization failed during transfer; retry later.'));
      const aborted = () => finish(new RpcError(-32001, 'Transfer cancelled.'));
      const timer = setTimeout(failed, timeoutMs);
      this.indexer.on('update', updated);
      this.indexer.on('syncError', failed);
      context.signal?.addEventListener('abort', aborted, { once: true });
      updated();
    });
  }
  classifyBountyHash = params => {
    try { const h = hash(params?.block_hash); return this.store.isRecentBlock(h) ? h : null; }
    catch { return null; }
  };
  changeCursor(sequence = this.sequence) { return this.cursors.sign({ kind: 'changes', sequence }); }

  updated(update) {
    const start = this.sequence;
    const reset = (update.bountyChanges ?? []).some(c => c.type === 'resync_required');
    if (reset) {
      this.journal = [];
      this.journalBytes = 0;
      this.journalFloor = ++this.sequence;
    }
    for (const change of reset ? [] : update.bountyChanges ?? []) {
      const event = { sequence: ++this.sequence, ...change };
      const bytes = Buffer.byteLength(JSON.stringify(event));
      this.journal.push({ event, bytes });
      this.journalBytes += bytes;
      while (this.journal.length > this.options.maxJournalEvents || this.journalBytes > this.options.maxJournalBytes) {
        const removed = this.journal.shift();
        this.journalBytes -= removed.bytes;
        this.journalFloor = removed.event.sequence;
      }
    }
    const touched = new Set(update.addresses ?? []);
    for (const sub of this.subscriptions.values()) {
      const notify = payload => {
        Promise.resolve(sub.context.notify('subscription', { subscription_id: sub.id, ...payload })).catch(() => {});
      };
      if (sub.kind === 'tip' && (sub.lastTip !== update.tip?.hash)) {
        sub.lastTip = update.tip?.hash;
        notify({ kind: 'tip', tip: update.tip, reorg: Boolean(update.reorg) });
      } else if (sub.kind === 'address' && (update.reorg || reset || touched.has(sub.address) || sub.lastTip !== update.tip?.hash)) {
        sub.lastTip = update.tip?.hash;
        notify({ kind: 'address', address: sub.address, tip: update.tip, reorg: Boolean(update.reorg), refresh: true });
      } else if (sub.kind === 'bounties' && this.sequence !== start) {
        // Bounded journal notifications preserve order. Overflow explicitly requires resync.
        if (reset || start < this.journalFloor) {
          notify({ kind: 'bounties', resync_required: true, tip: update.tip, cursor: this.changeCursor() });
        } else {
          let batch = [];
          for (const { event } of this.journal) {
            if (event.sequence <= start) continue;
            batch.push(event);
            if (batch.length === this.options.pageSize) {
              notify({ kind: 'bounties', changes: batch, tip: update.tip, cursor: this.changeCursor(event.sequence) });
              batch = [];
            }
          }
          if (batch.length) notify({ kind: 'bounties', changes: batch, tip: update.tip, cursor: this.changeCursor() });
        }
      }
    }
  }

  subscribe(kind, params, context) {
    const address = kind === 'address' ? normalizeAddress(params.address, this.tip().chain) : undefined;
    const existing = [...this.subscriptions.values()].find(s => s.context === context && s.kind === kind && s.address === address);
    if (existing) return { subscription_id: existing.id, tip: this.tip(), cursor: this.changeCursor() };
    let perIP = 0;
    for (const sub of this.subscriptions.values()) if (sub.context.ip === context.ip) perIP++;
    if (perIP >= this.options.maxSubscriptionsPerIP || this.subscriptions.size >= this.options.maxSubscriptions) {
      throw new RpcError(-32005, 'Subscription capacity reached.');
    }
    const id = randomUUID();
    this.subscriptions.set(id, { id, kind, address, context, lastTip: this.tip().hash });
    if (!this.contexts.has(context)) {
      this.contexts.add(context);
      context.onClose(() => { for (const [key, sub] of this.subscriptions) if (sub.context === context) this.subscriptions.delete(key); });
    }
    return { subscription_id: id, tip: this.tip(), cursor: this.changeCursor() };
  }

  page(kind, params) {
    paramsOnly(params, ['address', 'cursor']);
    const address = normalizeAddress(params.address, this.tip().chain);
    let anchor = { height: this.tip().height, hash: this.tip().hash };
    let after = null;
    if (params.cursor != null) {
      const decoded = this.cursors.read(params.cursor);
      if (decoded.kind !== kind || decoded.address !== address || !decoded.anchor ||
          this.store.blockAt(decoded.anchor.height)?.hash !== decoded.anchor.hash) {
        throw new RpcError(-32011, 'Chain reorganized or cursor does not match; restart this query.');
      }
      anchor = decoded.anchor;
      after = decoded.after;
    }
    const reader = kind === 'history' ? 'historyPage' : 'utxoPage';
    const rows = this.store[reader](address, { after, limit: this.options.pageSize + 1 });
    const more = rows.length > this.options.pageSize;
    const items = rows.slice(0, this.options.pageSize);
    const last = items.at(-1);
    const nextKey = kind === 'history' ? last?.txid : last && { txid: last.txid, vout: last.vout };
    return { address, tip: this.tip(), unit: 'connects', live: true, items,
      next_cursor: more ? this.cursors.sign({ kind, address, anchor, after: nextKey }) : null };
  }

  async *streamBounties(blockHash, context) {
    this.ready();
    if (!this.store.isRecentBlock(blockHash)) throw new RpcError(-32004, 'Block is not in the active last-600-block window.');
    yield { type: 'snapshot', block_hash: blockHash, tip: this.tip(), cursor: this.changeCursor(), unit: 'connects', live: true };
    let after = null;
    for (;;) {
      if (context.signal?.aborted) return;
      while (!this.indexer.ready) await this.waitForReady(context);
      if (!this.store.isRecentBlock(blockHash)) throw new RpcError(-32011, 'Block left the active recent window during transfer; refresh the block list.');
      const items = this.store.bountyPageAfter(blockHash, { after, limit: this.options.pageSize });
      if (!items.length) break;
      const last = items.at(-1);
      after = { txid: last.txid, vout: last.vout };
      yield { type: 'bounties', tip: this.tip(), items };
      await yieldLoop();
    }
    while (!this.indexer.ready) await this.waitForReady(context);
    if (!this.store.isRecentBlock(blockHash)) throw new RpcError(-32011, 'Block left the active recent window during transfer; refresh the block list.');
    yield { type: 'state', tip: this.tip(), cursor: this.changeCursor() };
  }

  dispatch = async (method, params, context) => {
    if (!METHODS.includes(method)) throw new RpcError(-32601, 'Method not found.');
    if (method === 'unsubscribe') {
      paramsOnly(params, ['subscription_id']);
      if (typeof params.subscription_id !== 'string' || params.subscription_id.length > 100) throw new RpcError(-32602, 'Invalid subscription id.');
      const sub = this.subscriptions.get(params.subscription_id);
      const removed = Boolean(sub && sub.context === context && this.subscriptions.delete(sub.id));
      return { removed };
    }
    if (method === 'sendrawtransaction') return this.broadcast(params, context);
    // A request can arrive between applying a new block and publishing its
    // coherent mempool overlay. Absorb short, healthy catchup, but do not serve
    // partially indexed data or turn backend failures into unbounded waits.
    if (!this.indexer.ready && this.indexer.pendingSync && !this.indexer.lastError && !this.indexer.stopping) {
      await this.waitForReady(context, 2000);
    }
    this.ready();
    switch (method) {
      case 'getchaintip': paramsOnly(params, []); return this.tip();
      case 'getrecentblockhashes':
        paramsOnly(params, []);
        return { tip: this.tip(), blocks: this.store.recentBlocks(), window: 600 };
      case 'getblockbounties': {
        paramsOnly(params, ['block_hash']);
        const blockHash = hash(params.block_hash);
        if (!this.store.isRecentBlock(blockHash)) throw new RpcError(-32004, 'Block is not in the active last-600-block window.');
        return this.streamBounties(blockHash, context);
      }
      case 'getaddressbalance': {
        paramsOnly(params, ['address']);
        const address = normalizeAddress(params.address, this.tip().chain);
        return { address, tip: this.tip(), unit: 'connects', ...this.store.balance(address) };
      }
      case 'getaddressutxos': return this.page('utxos', params);
      case 'getaddresshistory': return this.page('history', params);
      case 'gettransaction': {
        paramsOnly(params, ['txid']);
        const txid = hash(params.txid);
        const location = this.store.transactionLocation(txid);
        if (!location) throw new RpcError(-32004, 'Transaction not found in the current index or mempool.');
        let transaction;
        try { transaction = await this.backend.transaction(txid, location.block_hash); }
        catch { throw new RpcError(-32002, 'Transaction unavailable from the backend; retry after synchronization.'); }
        const currentLocation = this.store.transactionLocation(txid);
        if (!this.indexer.ready || currentLocation?.status !== location.status || currentLocation?.block_hash !== location.block_hash) {
          throw new RpcError(-32011, 'Transaction state changed; retry this query.');
        }
        return { tip: this.tip(), ...location, transaction };
      }
      case 'getbountychanges': {
        paramsOnly(params, ['cursor']);
        if (params.cursor == null) return { tip: this.tip(), changes: [], next_cursor: this.changeCursor(), has_more: false };
        const decoded = this.cursors.read(params.cursor);
        if (decoded.kind !== 'changes' || !Number.isSafeInteger(decoded.sequence) || decoded.sequence < this.journalFloor || decoded.sequence > this.sequence) {
          throw new RpcError(-32011, 'Change cursor expired; reload the recent-block snapshot.');
        }
        const changes = this.journal.filter(e => e.event.sequence > decoded.sequence).slice(0, this.options.pageSize).map(e => e.event);
        const seq = changes.at(-1)?.sequence ?? decoded.sequence;
        return { tip: this.tip(), changes, next_cursor: this.changeCursor(seq), has_more: seq < this.sequence };
      }
      case 'subscribebounties': paramsOnly(params, []); return this.subscribe('bounties', params, context);
      case 'subscribeaddress': paramsOnly(params, ['address']); return this.subscribe('address', params, context);
      case 'subscribetip': paramsOnly(params, []); return this.subscribe('tip', params, context);
    }
  };
}
