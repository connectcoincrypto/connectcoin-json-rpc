import { DatabaseSync } from 'node:sqlite';
import { createHash, randomBytes } from 'node:crypto';
import { toConnects } from './backend.mjs';

const HASH = /^[0-9a-f]{64}$/;
const key = (txid, vout) => `${txid}:${vout}`;
const zeroTotals = () => ({ received: 0n, spent: 0n });
function validHash(hash) { if (!HASH.test(hash)) throw new Error('Invalid hash from backend'); return hash; }
function pageLimit(limit) {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 10001) throw new Error('Invalid page limit');
}
function outpointAfter(after) {
  if (after === null) return ['', -1];
  if (!after || typeof after !== 'object') throw new Error('Invalid outpoint cursor');
  validHash(after.txid);
  if (!Number.isSafeInteger(after.vout) || after.vout < 0 || after.vout > 0xffffffff) throw new Error('Invalid output cursor index');
  return [after.txid, after.vout];
}

export function normalizeTransaction(tx) {
  validHash(tx.txid);
  if (!Array.isArray(tx.vin) || !Array.isArray(tx.vout)) throw new Error('Backend omitted transaction fields');
  const coinbase = tx.vin.length === 1 && Object.hasOwn(tx.vin[0], 'coinbase');
  return {
    txid: tx.txid, coinbase,
    inputs: coinbase ? [] : tx.vin.map(input => {
      validHash(input.txid);
      if (!Number.isSafeInteger(input.vout) || input.vout < 0) throw new Error('Invalid backend input index');
      return { txid: input.txid, vout: input.vout };
    }),
    outputs: tx.vout.map((output, index) => {
      if (output.n !== index) throw new Error('Backend output indexes are not contiguous');
      const amount = toConnects(output.value);
      const normalized = { vout: index, amount, address: output.scriptPubKey?.address ?? null, p2c: null };
      if (output.type === 2) {
        if (typeof output.domain !== 'string' || !HASH.test(output.connection_work_target)
          || !Number.isSafeInteger(output.root_certificates_version)
          || !Number.isSafeInteger(output.signature_algorithms_mask)) throw new Error('Invalid backend P2C output');
        normalized.address = null;
        normalized.p2c = { domain: output.domain, connection_work_target: output.connection_work_target,
          root_certificates_version: output.root_certificates_version,
          signature_algorithms_mask: output.signature_algorithms_mask };
      } else if (output.type !== 1 && normalized.address) {
        throw new Error('Unsupported addressed output type; expected native ConnectCoin P2PK');
      }
      return normalized;
    }),
  };
}

export class Store {
  constructor(path = ':memory:', { window = 600, maxAddressEvents = 100000, maxAddressBytes = 32 * 1024 * 1024,
    maxAddressMutationKeys = 10000 } = {}) {
    if (window !== 600) throw new Error('The public bounty window is fixed at 600 blocks');
    for (const value of [maxAddressEvents, maxAddressBytes, maxAddressMutationKeys]) {
      if (!Number.isSafeInteger(value) || value < 1) throw new Error('Invalid address journal bound');
    }
    this.window = window;
    Object.assign(this, { maxAddressEvents, maxAddressBytes, maxAddressMutationKeys });
    this.balanceCache = new Map();
    this.db = new DatabaseSync(path);
    this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;
      CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS blocks (height INTEGER PRIMARY KEY, hash TEXT UNIQUE NOT NULL,
        mediantime INTEGER NOT NULL, bounty_indexed INTEGER NOT NULL DEFAULT 1);
      CREATE INDEX IF NOT EXISTS blocks_bounty_indexed ON blocks(height) WHERE bounty_indexed=1;
      CREATE TABLE IF NOT EXISTS transactions (txid TEXT PRIMARY KEY, block_height INTEGER NOT NULL,
        position INTEGER NOT NULL, FOREIGN KEY(block_height) REFERENCES blocks(height));
      CREATE INDEX IF NOT EXISTS transactions_height ON transactions(block_height);
      CREATE TABLE IF NOT EXISTS outputs (txid TEXT NOT NULL, vout INTEGER NOT NULL, address TEXT NOT NULL,
        amount TEXT NOT NULL, height INTEGER NOT NULL, coinbase INTEGER NOT NULL, PRIMARY KEY(txid,vout));
      CREATE INDEX IF NOT EXISTS outputs_address ON outputs(address,height,txid,vout);
      CREATE INDEX IF NOT EXISTS outputs_address_outpoint ON outputs(address,txid,vout);
      CREATE INDEX IF NOT EXISTS outputs_height ON outputs(height);
      CREATE TABLE IF NOT EXISTS spends (txid TEXT NOT NULL, vout INTEGER NOT NULL,
        spender TEXT NOT NULL, height INTEGER NOT NULL, PRIMARY KEY(txid,vout));
      CREATE INDEX IF NOT EXISTS spends_height ON spends(height);
      CREATE TABLE IF NOT EXISTS history (address TEXT NOT NULL, txid TEXT NOT NULL, height INTEGER NOT NULL,
        position INTEGER NOT NULL, received TEXT NOT NULL, spent TEXT NOT NULL, PRIMARY KEY(address,txid));
      CREATE INDEX IF NOT EXISTS history_address ON history(address,height,position,txid);
      CREATE INDEX IF NOT EXISTS history_height ON history(height);
      CREATE TABLE IF NOT EXISTS bounties (txid TEXT NOT NULL, vout INTEGER NOT NULL, height INTEGER NOT NULL,
        amount TEXT NOT NULL, coinbase INTEGER NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(txid,vout));
      CREATE INDEX IF NOT EXISTS bounties_height ON bounties(height,txid,vout);
      CREATE TABLE IF NOT EXISTS pending_transactions (txid TEXT PRIMARY KEY, payload TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS pending_outputs (txid TEXT NOT NULL, vout INTEGER NOT NULL,
        address TEXT, amount TEXT NOT NULL, PRIMARY KEY(txid,vout));
      CREATE INDEX IF NOT EXISTS pending_outputs_address ON pending_outputs(address);
      CREATE INDEX IF NOT EXISTS pending_outputs_address_outpoint ON pending_outputs(address,txid,vout);
      CREATE TABLE IF NOT EXISTS pending_spends (txid TEXT NOT NULL, vout INTEGER NOT NULL,
        spender TEXT NOT NULL, PRIMARY KEY(txid,vout));
      CREATE TABLE IF NOT EXISTS pending_history (address TEXT NOT NULL, txid TEXT NOT NULL,
        received TEXT NOT NULL, spent TEXT NOT NULL, PRIMARY KEY(address,txid));
      CREATE TABLE IF NOT EXISTS address_journal (sequence INTEGER PRIMARY KEY, address TEXT NOT NULL,
        payload TEXT NOT NULL, bytes INTEGER NOT NULL);
      CREATE INDEX IF NOT EXISTS address_journal_address_sequence ON address_journal(address,sequence);
      INSERT OR IGNORE INTO metadata VALUES ('schema','2');
      INSERT OR IGNORE INTO metadata VALUES ('revision','0');
      INSERT OR IGNORE INTO metadata VALUES ('address_sequence','0');
      INSERT OR IGNORE INTO metadata VALUES ('address_floor','0');
      INSERT OR IGNORE INTO metadata VALUES ('address_epoch','0');
      INSERT OR IGNORE INTO metadata VALUES ('address_journal_bytes','0');`);
    if (!['1', '2'].includes(this.meta('schema'))) {
      this.db.close();
      throw new Error('Unsupported index database version');
    }
    // SQLite transactions below are synchronous. Callers never observe half of
    // one block/mempool replacement; Indexer.ready guards multi-block catchup.
    if (!this.meta('address_cursor_key')) this.setMeta('address_cursor_key', randomBytes(32).toString('hex'));
    // An older service binary can still understand schema-2 accounting, but
    // cannot maintain this journal. Detect intervening writes on upgrade again
    // rather than accepting persisted cursors with an unrecorded gap.
    if (this.meta('address_journal_revision') !== String(this.revision())) {
      this.atomic(() => { this.resetAddressJournal(); this.setMeta('address_journal_revision', this.revision()); });
    }
  }

  meta(name) { return this.db.prepare('SELECT value FROM metadata WHERE key=?').get(name)?.value ?? null; }
  setMeta(name, value) { this.db.prepare('INSERT OR REPLACE INTO metadata VALUES (?,?)').run(name, String(value)); }
  revision() { return Number(this.meta('revision')); }
  bump() {
    const revision = this.revision() + 1;
    this.setMeta('revision', revision); this.setMeta('address_journal_revision', revision); this.balanceCache.clear();
  }
  addressChangeState() {
    return { sequence: Number(this.meta('address_sequence')), floor: Number(this.meta('address_floor')),
      epoch: Number(this.meta('address_epoch')) };
  }
  resetAddressJournal() {
    const state = this.addressChangeState();
    if (!Number.isSafeInteger(state.sequence + 1) || !Number.isSafeInteger(state.epoch + 1)) throw new Error('Address journal counter exhausted');
    this.db.exec('DELETE FROM address_journal');
    this.setMeta('address_sequence', state.sequence + 1);
    this.setMeta('address_floor', state.sequence + 1);
    this.setMeta('address_epoch', state.epoch + 1);
    this.setMeta('address_journal_bytes', 0);
  }
  newAddressTouches() { return { keys: new Map(), overflow: false }; }
  touchAddress(touches, address, kind, txid, vout) {
    if (!address || touches.overflow) return;
    const id = `${address}:${kind}:${txid}:${vout ?? ''}`;
    touches.keys.set(id, { address, kind, txid, ...(kind === 'utxo' ? { vout } : {}) });
    if (touches.keys.size > this.maxAddressMutationKeys) { touches.overflow = true; touches.keys.clear(); }
  }
  pendingMutationTouches(transactions) {
    // Compare immutable transaction membership/payloads, then journal ONLY the
    // affected keys. A large unchanged overlay must not repeatedly exhaust the
    // mutation bound or force clients to reload their entire address history.
    const touches = this.newAddressTouches(), current = new Map(transactions.map(tx => [tx.txid, tx]));
    const seen = new Set();
    const confirmed = this.db.prepare('SELECT address FROM outputs WHERE txid=? AND vout=?');
    const pending = this.db.prepare('SELECT address FROM pending_outputs WHERE txid=? AND vout=?');
    const collect = (tx, next) => {
      for (const output of tx.outputs) {
        this.touchAddress(touches, output.address, 'utxo', tx.txid, output.vout);
        this.touchAddress(touches, output.address, 'history', tx.txid);
      }
      for (const input of tx.inputs) {
        const source = next ? current.get(input.txid)?.outputs[input.vout] ?? confirmed.get(input.txid, input.vout)
          : pending.get(input.txid, input.vout) ?? confirmed.get(input.txid, input.vout);
        this.touchAddress(touches, source?.address, 'utxo', input.txid, input.vout);
        this.touchAddress(touches, source?.address, 'history', tx.txid);
      }
    };
    for (const row of this.db.prepare('SELECT txid,payload FROM pending_transactions').iterate()) {
      const next = current.get(row.txid); seen.add(row.txid);
      if (!next || row.payload !== JSON.stringify(next)) {
        collect(JSON.parse(row.payload), false);
        if (next) collect(next, true);
      }
      if (touches.overflow) return touches;
    }
    for (const tx of transactions) {
      if (!seen.has(tx.txid)) collect(tx, true);
      if (touches.overflow) return touches;
    }
    return touches;
  }
  addressItem({ address, kind, txid, vout }) {
    if (kind === 'history') {
      const row = this.db.prepare(`SELECT h.received,h.spent,h.height,b.hash AS block_hash FROM history h
        JOIN blocks b ON b.height=h.height WHERE h.address=? AND h.txid=?`).get(address, txid);
      const pending = row ? null : this.db.prepare('SELECT received,spent FROM pending_history WHERE address=? AND txid=?').get(address, txid);
      const value = row ?? pending;
      return value ? { txid, status: row ? 'confirmed' : 'pending', block_height: row?.height ?? null,
        block_hash: row?.block_hash ?? null, received: value.received, spent: value.spent,
        balance_delta: String(BigInt(value.received) - BigInt(value.spent)) } : null;
    }
    const row = this.db.prepare(`SELECT o.amount,o.height,o.coinbase,s.spender,p.spender AS pending_spent_by
      FROM outputs o LEFT JOIN spends s USING(txid,vout) LEFT JOIN pending_spends p USING(txid,vout)
      WHERE o.address=? AND o.txid=? AND o.vout=?`).get(address, txid, vout);
    if (row?.spender) return null;
    const pending = row ? null : this.db.prepare(`SELECT o.amount,p.spender AS pending_spent_by FROM pending_outputs o
      LEFT JOIN pending_spends p USING(txid,vout) WHERE o.address=? AND o.txid=? AND o.vout=?`).get(address, txid, vout);
    const value = row ?? pending;
    return value ? { txid, vout, amount: value.amount, block_height: row?.height ?? null,
      status: row ? 'confirmed' : 'pending', coinbase: Boolean(row?.coinbase), pending_spent_by: value.pending_spent_by ?? null } : null;
  }
  appendAddressChanges(touches, before = null) {
    if (touches.overflow) { this.resetAddressJournal(); return null; }
    const addresses = new Set();
    let { sequence } = this.addressChangeState();
    let bytes = Number(this.meta('address_journal_bytes'));
    const insert = this.db.prepare('INSERT INTO address_journal VALUES (?,?,?,?)');
    for (const [id, entry] of touches.keys) {
      const item = this.addressItem(entry);
      if (before && (before.get(id) ?? null) === (item ? JSON.stringify(item) : null)) continue;
      addresses.add(entry.address);
      if (!Number.isSafeInteger(++sequence)) throw new Error('Address journal counter exhausted');
      const event = { sequence, ...entry, action: item ? 'upsert' : 'remove', ...(item ? { item } : {}) };
      const payload = JSON.stringify(event), size = Buffer.byteLength(payload);
      insert.run(sequence, entry.address, payload, size); bytes += size;
    }
    this.setMeta('address_sequence', sequence);
    let count = this.db.prepare('SELECT COUNT(*) AS count FROM address_journal').get().count;
    let floor = this.addressChangeState().floor;
    // Retain a contiguous suffix under both event-count and byte bounds.
    if (count > this.maxAddressEvents || bytes > this.maxAddressBytes) {
      for (const row of this.db.prepare('SELECT sequence,bytes FROM address_journal ORDER BY sequence').iterate()) {
        if (count <= this.maxAddressEvents && bytes <= this.maxAddressBytes) break;
        floor = row.sequence; bytes -= row.bytes; count--;
      }
      this.db.prepare('DELETE FROM address_journal WHERE sequence<=?').run(floor);
      this.setMeta('address_floor', floor);
    }
    this.setMeta('address_journal_bytes', bytes);
    return addresses;
  }
  addressChangesPage(addresses, { after, through, limit, tip }) {
    pageLimit(limit);
    const placeholders = addresses.map(() => '?').join(',');
    const rows = this.db.prepare(`SELECT payload FROM address_journal WHERE address IN (${placeholders})
      AND sequence>? AND sequence<=? ORDER BY sequence LIMIT ?`).all(...addresses, after, through, limit);
    return rows.map(row => {
      const event = JSON.parse(row.payload);
      if (event.item) {
        event.item.confirmations = event.item.status === 'pending' ? 0 : tip.height - event.item.block_height + 1;
        if (event.kind === 'utxo') event.item.mature = !event.item.coinbase || event.item.confirmations >= 100;
      }
      return event;
    });
  }
  atomic(fn) {
    this.db.exec('BEGIN IMMEDIATE');
    try { const result = fn(); this.db.exec('COMMIT'); return result; }
    catch (error) { this.db.exec('ROLLBACK'); throw error; }
  }
  pinNetwork(chain, genesis) {
    validHash(genesis);
    const prior = this.meta('genesis');
    if (prior && (prior !== genesis || this.meta('chain') !== chain)) throw new Error('Backend network/genesis differs from this index');
    if (!prior) this.atomic(() => { this.setMeta('genesis', genesis); this.setMeta('chain', chain); });
  }
  tip() {
    const row = this.db.prepare('SELECT height,hash,mediantime FROM blocks ORDER BY height DESC LIMIT 1').get();
    return row ? { ...row, chain: this.meta('chain'), genesis_hash: this.meta('genesis') } : null;
  }
  recentBlocks() {
    return this.db.prepare('SELECT height,hash FROM blocks ORDER BY height DESC LIMIT ?').all(this.window).map(row => ({ ...row }));
  }
  isRecentBlock(hash) {
    return Boolean(this.db.prepare('SELECT 1 FROM blocks WHERE hash=? AND height >= (SELECT MAX(height)-599 FROM blocks)').get(hash));
  }
  blockAt(height) { return this.db.prepare('SELECT height,hash,mediantime FROM blocks WHERE height=?').get(height) ?? null; }
  missingBountyBlocks() {
    return this.db.prepare('SELECT height,hash FROM blocks WHERE bounty_indexed=0 AND height >= (SELECT MAX(height)-599 FROM blocks) ORDER BY height').all();
  }

  needsGenesisRepair() { return this.meta('schema') === '1'; }
  repairGenesis(block) {
    if (!this.needsGenesisRepair()) return false;
    const indexed = this.blockAt(0);
    if (!indexed || block?.height !== 0 || block.hash !== indexed.hash
        || block.hash !== this.meta('genesis')) throw new Error('Genesis repair requires the pinned indexed genesis block');
    if (!Array.isArray(block.tx) || block.tx.length !== 1) throw new Error('Invalid genesis transaction list for index repair');
    const tx = normalizeTransaction(block.tx[0]);
    const stored = this.db.prepare('SELECT txid,position FROM transactions WHERE block_height=0').all();
    if (!tx.coinbase || stored.length !== 1 || stored[0].txid !== tx.txid || stored[0].position !== 0) {
      throw new Error('Genesis repair transaction does not match the existing index');
    }
    this.atomic(() => {
      // Schema 1 skipped every genesis coinbase output and receipt. Refuse a
      // manually/partially repaired index rather than risk applying debits twice.
      for (const table of ['outputs', 'history', 'bounties']) {
        if (this.db.prepare(`SELECT 1 FROM ${table} WHERE height=0 LIMIT 1`).get()) {
          throw new Error('Genesis repair found unexpected existing genesis accounting');
        }
      }
      const received = new Map();
      const insertOutput = this.db.prepare('INSERT INTO outputs VALUES (?,?,?,?,?,?)');
      const findSpend = this.db.prepare('SELECT spender,height FROM spends WHERE txid=? AND vout=?');
      const findTransaction = this.db.prepare('SELECT block_height,position FROM transactions WHERE txid=?');
      const findHistory = this.db.prepare('SELECT received,spent,height,position FROM history WHERE address=? AND txid=?');
      const insertHistory = this.db.prepare('INSERT INTO history VALUES (?,?,?,?,?,?)');
      const updateSpent = this.db.prepare('UPDATE history SET spent=? WHERE address=? AND txid=?');
      for (const output of tx.outputs) {
        if (output.address) {
          insertOutput.run(tx.txid, output.vout, output.address, output.amount, 0, 1);
          received.set(output.address, (received.get(output.address) ?? 0n) + BigInt(output.amount));
          // Old indexes retained spends even when their source output was
          // missing. Repair only the omitted debit, preserving other inputs
          // and self-change. BigInt keeps ConnectCoin's ten decimals exact.
          const spend = findSpend.get(tx.txid, output.vout);
          if (spend) {
            const location = findTransaction.get(spend.spender);
            if (!location || location.block_height !== spend.height || spend.height <= 0) {
              throw new Error('Genesis repair found an inconsistent spending transaction');
            }
            const history = findHistory.get(output.address, spend.spender);
            if (history) {
              if (history.height !== location.block_height || history.position !== location.position) {
                throw new Error('Genesis repair found inconsistent spending history');
              }
              updateSpent.run(String(BigInt(history.spent) + BigInt(output.amount)), output.address, spend.spender);
            } else {
              insertHistory.run(output.address, spend.spender, location.block_height, location.position, '0', output.amount);
            }
          }
        }
        if (output.p2c && this.isRecentBlock(block.hash)) this.insertBounty(tx, output, 0);
      }
      for (const [address, amount] of received) insertHistory.run(address, tx.txid, 0, 0, String(amount), '0');
      // Rebuild the overlay from a coherent backend snapshot before readers
      // become ready. A restart/failure cannot preserve an old, wrong debit
      // through the unchanged-mempool fingerprint shortcut.
      this.db.exec(`DELETE FROM pending_history; DELETE FROM pending_spends;
        DELETE FROM pending_outputs; DELETE FROM pending_transactions;
        DELETE FROM metadata WHERE key IN ('mempool_fingerprint','mempool_tip');`);
      // Transactional semantic-version bump also prevents old binaries from
      // reopening a repaired index and reverting to the old genesis rules.
      this.setMeta('schema', '2');
      this.resetAddressJournal();
      this.bump();
    });
    return true;
  }

  applyBlock(block) {
    const before = this.tip();
    validHash(block.hash);
    if (!Number.isSafeInteger(block.height) || block.height !== (before?.height ?? -1) + 1
      || (before && block.previousblockhash !== before.hash)) throw new Error('Block does not extend indexed chain');
    if (!Array.isArray(block.tx) || !Number.isSafeInteger(block.mediantime)) throw new Error('Incomplete backend block');
    if (block.height > 0 && this.needsGenesisRepair()) throw new Error('Repair genesis accounting before extending this index');
    const transactions = block.tx.map(normalizeTransaction);
    const addressTouches = this.newAddressTouches();
    const addresses = new Set();
    const changes = [];
    let resync = false;
    const track = (address, change) => {
      if (resync) return;
      if (address) addresses.add(address);
      if (change) changes.push(change);
      if (addresses.size + changes.length > 10_000) {
        resync = true; addresses.clear(); changes.length = 0;
      }
    };
    this.atomic(() => {
      this.db.prepare('INSERT INTO blocks(height,hash,mediantime) VALUES (?,?,?)').run(block.height, block.hash, block.mediantime);
      const findOutput = this.db.prepare('SELECT address,amount FROM outputs WHERE txid=? AND vout=?');
      const findBounty = this.db.prepare('SELECT 1 FROM bounties WHERE txid=? AND vout=?');
      const insertSpend = this.db.prepare('INSERT INTO spends VALUES (?,?,?,?)');
      const insertOutput = this.db.prepare('INSERT INTO outputs VALUES (?,?,?,?,?,?)');
      const insertHistory = this.db.prepare('INSERT INTO history VALUES (?,?,?,?,?,?)');
      for (const [position, tx] of transactions.entries()) {
        this.db.prepare('INSERT INTO transactions VALUES (?,?,?)').run(tx.txid, block.height, position);
        const totals = new Map();
        const totalFor = address => { if (!totals.has(address)) totals.set(address, zeroTotals()); track(address); return totals.get(address); };
        for (const input of tx.inputs) {
          const previous = findOutput.get(input.txid, input.vout);
          if (previous) {
            totalFor(previous.address).spent += BigInt(previous.amount);
            this.touchAddress(addressTouches, previous.address, 'utxo', input.txid, input.vout);
          }
          insertSpend.run(input.txid, input.vout, tx.txid, block.height);
          if (findBounty.get(input.txid, input.vout)) track(null, { type: 'spent', txid: input.txid, vout: input.vout, spending_txid: tx.txid });
        }
        for (const output of tx.outputs) {
          // ConnectCoin includes its genesis allocation in the UTXO set; it
          // follows the same coinbase maturity/accounting rules as later blocks.
          if (output.address) {
            insertOutput.run(tx.txid, output.vout, output.address, output.amount, block.height, Number(tx.coinbase));
            totalFor(output.address).received += BigInt(output.amount);
            this.touchAddress(addressTouches, output.address, 'utxo', tx.txid, output.vout);
          }
          if (output.p2c) {
            this.insertBounty(tx, output, block.height);
            track(null, { type: 'added', txid: tx.txid, vout: output.vout, block_hash: block.hash, block_height: block.height });
          }
        }
        for (const [address, total] of totals) {
          insertHistory.run(address, tx.txid, block.height, position, String(total.received), String(total.spent));
          this.touchAddress(addressTouches, address, 'history', tx.txid);
        }
      }
      const minimum = block.height - this.window + 1;
      for (const row of this.db.prepare('SELECT txid,vout FROM bounties WHERE height<?').iterate(minimum)) track(null, { type: 'window_exit', ...row });
      this.db.prepare('DELETE FROM bounties WHERE height<?').run(minimum);
      this.db.prepare('UPDATE blocks SET bounty_indexed=0 WHERE height<? AND bounty_indexed=1').run(minimum);
      for (const row of this.db.prepare('SELECT txid,vout FROM bounties WHERE coinbase=1 AND height=?').iterate(block.height - 99)) track(null, { type: 'matured', ...row });
      // Maturity changes spendable address funds exactly once, even when this
      // block has no transaction involving that address. Seek outputs_height;
      // never scan a subscriber's full history on ordinary tip advancement.
      for (const row of this.db.prepare(`SELECT o.address,o.txid,o.vout FROM outputs o
        LEFT JOIN spends s USING(txid,vout) WHERE o.height=? AND o.coinbase=1 AND s.spender IS NULL`).iterate(block.height - 99)) {
        track(row.address);
        this.touchAddress(addressTouches, row.address, 'utxo', row.txid, row.vout);
      }
      if (block.height === 0) this.setMeta('schema', '2');
      this.appendAddressChanges(addressTouches);
      this.bump();
    });
    return { addresses: [...addresses], bountyChanges: changes, resync };
  }
  insertBounty(tx, output, height) {
    this.db.prepare('INSERT OR REPLACE INTO bounties VALUES (?,?,?,?,?,?)').run(tx.txid, output.vout, height, output.amount, Number(tx.coinbase), JSON.stringify(output.p2c));
  }
  restoreBountyBlock(block) {
    const row = this.blockAt(block.height);
    if (!row || row.hash !== block.hash || !this.isRecentBlock(block.hash)) throw new Error('Cannot restore bounty metadata from noncanonical block');
    const txs = block.tx.map(normalizeTransaction);
    this.atomic(() => {
      this.db.prepare('DELETE FROM bounties WHERE height=?').run(block.height);
      for (const tx of txs) for (const output of tx.outputs) {
        if (output.p2c) this.insertBounty(tx, output, block.height);
      }
      this.db.prepare('UPDATE blocks SET bounty_indexed=1 WHERE height=?').run(block.height);
      this.bump();
    });
  }
  rollbackTip() {
    const tip = this.tip();
    if (!tip) return { addresses: [], bountyChanges: [] };
    const addresses = new Set();
    for (const row of this.db.prepare(`SELECT address FROM history WHERE height=?
      UNION ALL SELECT o.address FROM outputs o LEFT JOIN spends s USING(txid,vout)
      WHERE o.height=? AND o.coinbase=1 AND s.spender IS NULL`).iterate(tip.height, tip.height - 99)) {
      addresses.add(row.address);
      if (addresses.size > 10000) break;
    }
    this.atomic(() => {
      this.db.prepare('DELETE FROM spends WHERE height=?').run(tip.height);
      this.db.prepare('DELETE FROM outputs WHERE height=?').run(tip.height);
      this.db.prepare('DELETE FROM history WHERE height=?').run(tip.height);
      this.db.prepare('DELETE FROM bounties WHERE height=?').run(tip.height);
      this.db.prepare('DELETE FROM transactions WHERE block_height=?').run(tip.height);
      this.db.prepare('DELETE FROM blocks WHERE height=?').run(tip.height);
      // Conservative explicit resynchronization: never replay obsolete-chain
      // events as canonical, including after rollback/restart mid-catchup.
      this.resetAddressJournal();
      this.bump();
    });
    return { addresses: [...addresses].slice(0, 10000), resync: addresses.size > 10000,
      bountyChanges: [{ type: 'reorg', removed_block_hash: tip.hash, removed_block_height: tip.height }] };
  }

  replaceMempool(rawTransactions, { normalized = false } = {}) {
    const txs = (normalized ? [...rawTransactions] : rawTransactions.map(normalizeTransaction)).sort((a, b) => a.txid.localeCompare(b.txid));
    const fingerprint = createHash('sha256').update(JSON.stringify(txs)).digest('hex');
    if (this.meta('mempool_fingerprint') === fingerprint && this.meta('mempool_tip') === this.tip()?.hash) return { addresses: [], bountyChanges: [] };
    const addressTouches = this.pendingMutationTouches(txs);
    const before = new Map([...addressTouches.keys].map(([id, entry]) => {
      const item = this.addressItem(entry);
      return [id, item ? JSON.stringify(item) : null];
    }));
    const addresses = new Set();
    const oldBountySpends = new Map(this.db.prepare('SELECT p.txid,p.vout,p.spender FROM pending_spends p JOIN bounties b USING(txid,vout) LIMIT 10001').all().map(row => [key(row.txid, row.vout), row]));
    const changes = [];
    let resync = oldBountySpends.size > 10000;
    const track = (address, change) => {
      if (resync) return;
      if (address) addresses.add(address);
      if (change) changes.push(change);
      if (addresses.size + changes.length > 10000) { resync = true; addresses.clear(); changes.length = 0; }
    };
    this.atomic(() => {
      this.db.exec('DELETE FROM pending_history; DELETE FROM pending_spends; DELETE FROM pending_outputs; DELETE FROM pending_transactions;');
      const insertOutput = this.db.prepare('INSERT INTO pending_outputs VALUES (?,?,?,?)');
      for (const tx of txs) {
        if (tx.coinbase) throw new Error('Coinbase cannot appear in mempool');
        this.db.prepare('INSERT INTO pending_transactions VALUES (?,?)').run(tx.txid, JSON.stringify(tx));
        for (const output of tx.outputs) insertOutput.run(tx.txid, output.vout, output.address, output.amount);
      }
      const confirmed = this.db.prepare('SELECT address,amount FROM outputs WHERE txid=? AND vout=?');
      const pending = this.db.prepare('SELECT address,amount FROM pending_outputs WHERE txid=? AND vout=?');
      const alreadySpent = this.db.prepare('SELECT 1 FROM spends WHERE txid=? AND vout=?');
      for (const tx of txs) {
        const totals = new Map();
        const totalFor = address => { if (!totals.has(address)) totals.set(address, zeroTotals()); return totals.get(address); };
        for (const input of tx.inputs) {
          if (alreadySpent.get(input.txid, input.vout)) throw new Error('Mempool snapshot conflicts with indexed chain');
          const previous = pending.get(input.txid, input.vout) ?? confirmed.get(input.txid, input.vout);
          if (previous?.address) totalFor(previous.address).spent += BigInt(previous.amount);
          this.db.prepare('INSERT INTO pending_spends VALUES (?,?,?)').run(input.txid, input.vout, tx.txid);
        }
        for (const output of tx.outputs) if (output.address) totalFor(output.address).received += BigInt(output.amount);
        for (const [address, total] of totals) this.db.prepare('INSERT INTO pending_history VALUES (?,?,?,?)').run(address, tx.txid, String(total.received), String(total.spent));
      }
      const current = new Map(this.db.prepare('SELECT p.txid,p.vout,p.spender FROM pending_spends p JOIN bounties b USING(txid,vout) LIMIT 10001').all().map(row => [key(row.txid, row.vout), row]));
      if (current.size > 10000) { resync = true; addresses.clear(); changes.length = 0; }
      for (const [id, row] of current) if (oldBountySpends.get(id)?.spender !== row.spender) track(null, { type: 'pending_spend', txid: row.txid, vout: row.vout, spending_txid: row.spender });
      for (const [id, row] of oldBountySpends) if (!current.has(id)) {
        if (!alreadySpent.get(row.txid, row.vout)) track(null, { type: 'available_again', txid: row.txid, vout: row.vout });
      }
      this.setMeta('mempool_fingerprint', fingerprint);
      this.setMeta('mempool_tip', this.tip()?.hash ?? '');
      const changedAddresses = this.appendAddressChanges(addressTouches, before);
      if (changedAddresses === null) { resync = true; addresses.clear(); changes.length = 0; }
      else for (const address of changedAddresses) track(address);
      this.bump();
    });
    return { addresses: [...addresses], bountyChanges: changes, resync };
  }

  bountyPage(hash, offset = 0, limit = 100) {
    if (!this.isRecentBlock(hash)) return [];
    const rows = this.db.prepare(`SELECT b.*,s.spender,p.spender AS pending_spender,blocks.hash AS block_hash
      FROM bounties b JOIN blocks ON blocks.height=b.height
      LEFT JOIN spends s ON s.txid=b.txid AND s.vout=b.vout
      LEFT JOIN pending_spends p ON p.txid=b.txid AND p.vout=b.vout
      WHERE blocks.hash=? ORDER BY b.txid,b.vout LIMIT ? OFFSET ?`).all(hash, limit, offset);
    return this.formatBounties(rows);
  }
  bountyPageAfter(hash, { after = null, limit = 100 } = {}) {
    pageLimit(limit);
    const [lastTxid, lastVout] = outpointAfter(after);
    const block = this.db.prepare('SELECT height FROM blocks WHERE hash=? AND height >= (SELECT MAX(height)-599 FROM blocks)').get(hash);
    if (!block) return [];
    // Seek directly into the per-height/outpoint index instead of rescanning
    // earlier pages. The funding outpoints never change within a canonical block.
    const rows = this.db.prepare(`SELECT b.*,s.spender,p.spender AS pending_spender,? AS block_hash
      FROM bounties b LEFT JOIN spends s ON s.txid=b.txid AND s.vout=b.vout
      LEFT JOIN pending_spends p ON p.txid=b.txid AND p.vout=b.vout
      WHERE b.height=? AND (b.txid,b.vout)>(?,?) ORDER BY b.txid,b.vout LIMIT ?`)
      .all(hash, block.height, lastTxid, lastVout, limit);
    return this.formatBounties(rows);
  }
  formatBounties(rows) {
    const tip = this.tip();
    return rows.map(row => {
      const confirmations = tip.height - row.height + 1;
      return { txid: row.txid, vout: row.vout, amount: row.amount, ...JSON.parse(row.payload),
        block_height: row.height, block_hash: row.block_hash, coinbase: Boolean(row.coinbase), confirmations,
        status: row.spender ? 'spent' : row.pending_spender ? 'pending_spend' : row.coinbase && confirmations < 100 ? 'immature' : 'available',
        spending_txid: row.spender ?? row.pending_spender ?? null };
    });
  }
  *bounties(hash) {
    let after = null;
    while (true) {
      const rows = this.bountyPageAfter(hash, { after, limit: 100 });
      yield* rows;
      if (rows.length < 100) return;
      after = { txid: rows.at(-1).txid, vout: rows.at(-1).vout };
    }
  }
  balance(address) {
    if (this.balanceCache.has(address)) return { ...this.balanceCache.get(address) };
    let confirmed = 0n, immature = 0n, pendingSpent = 0n, pendingReceived = 0n;
    const height = this.tip()?.height ?? -1;
    for (const row of this.db.prepare(`SELECT o.amount,o.coinbase,o.height,p.spender FROM outputs o
      LEFT JOIN spends s ON s.txid=o.txid AND s.vout=o.vout
      LEFT JOIN pending_spends p ON p.txid=o.txid AND p.vout=o.vout WHERE o.address=? AND s.spender IS NULL`).iterate(address)) {
      const amount = BigInt(row.amount);
      confirmed += amount;
      if (row.coinbase && height - row.height + 1 < 100) immature += amount;
      if (row.spender) pendingSpent += amount;
    }
    for (const row of this.db.prepare(`SELECT o.amount FROM pending_outputs o LEFT JOIN pending_spends s
      ON s.txid=o.txid AND s.vout=o.vout WHERE o.address=? AND s.spender IS NULL`).iterate(address)) pendingReceived += BigInt(row.amount);
    const result = { confirmed: String(confirmed), immature: String(immature), available_confirmed: String(confirmed - immature - pendingSpent),
      pending_received: String(pendingReceived), pending_spent: String(pendingSpent),
      pending_delta: String(pendingReceived - pendingSpent), total: String(confirmed + pendingReceived - pendingSpent) };
    if (this.balanceCache.size >= 1000) this.balanceCache.delete(this.balanceCache.keys().next().value);
    this.balanceCache.set(address, result);
    return { ...result };
  }
  history(address, offset = 0, limit = 100) {
    const rows = this.db.prepare(`SELECT * FROM (
      SELECT h.txid,h.received,h.spent,h.height,h.position,b.hash AS block_hash,0 AS pending FROM history h JOIN blocks b ON h.height=b.height WHERE address=?
      UNION ALL SELECT txid,received,spent,NULL,NULL,NULL,1 FROM pending_history WHERE address=?
      ) ORDER BY pending DESC,height DESC,position DESC,txid LIMIT ? OFFSET ?`).all(address, address, limit, offset);
    const height = this.tip()?.height ?? -1;
    return rows.map(row => ({ txid: row.txid, status: row.pending ? 'pending' : 'confirmed',
      block_height: row.height, block_hash: row.block_hash, confirmations: row.pending ? 0 : height - row.height + 1,
      received: row.received, spent: row.spent, balance_delta: String(BigInt(row.received) - BigInt(row.spent)) }));
  }
  // Live keyset traversal, not a frozen snapshot. The immutable txid order is
  // independent of confirmation count, mining and mempool removal. New records
  // inserted behind `after` require an address refresh/subscription; deletions
  // never shift an OFFSET and silently skip the following unchanged records.
  historyPage(address, { after = null, limit = 100 } = {}) {
    pageLimit(limit);
    if (after !== null) validHash(after);
    const lastTxid = after ?? '';
    const rows = this.db.prepare(`SELECT * FROM (
      SELECT h.txid,h.received,h.spent,h.height,b.hash AS block_hash,0 AS pending
        FROM history h JOIN blocks b ON h.height=b.height WHERE h.address=? AND h.txid>?
      UNION ALL SELECT txid,received,spent,NULL,NULL,1 FROM pending_history WHERE address=? AND txid>?
      ) ORDER BY txid LIMIT ?`).all(address, lastTxid, address, lastTxid, limit);
    const height = this.tip()?.height ?? -1;
    return rows.map(row => ({ txid: row.txid, status: row.pending ? 'pending' : 'confirmed',
      block_height: row.height, block_hash: row.block_hash, confirmations: row.pending ? 0 : height - row.height + 1,
      received: row.received, spent: row.spent, balance_delta: String(BigInt(row.received) - BigInt(row.spent)) }));
  }
  utxos(address, offset = 0, limit = 100) {
    const rows = this.db.prepare(`SELECT * FROM (
      SELECT o.txid,o.vout,o.amount,o.height,o.coinbase,0 AS pending FROM outputs o
        LEFT JOIN spends s ON s.txid=o.txid AND s.vout=o.vout
        LEFT JOIN pending_spends p ON p.txid=o.txid AND p.vout=o.vout
        WHERE o.address=? AND s.spender IS NULL AND p.spender IS NULL
      UNION ALL SELECT o.txid,o.vout,o.amount,NULL,0,1 FROM pending_outputs o
        LEFT JOIN pending_spends s ON s.txid=o.txid AND s.vout=o.vout WHERE o.address=? AND s.spender IS NULL
      ) ORDER BY pending DESC,height DESC,txid,vout LIMIT ? OFFSET ?`).all(address, address, limit, offset);
    const height = this.tip()?.height ?? -1;
    return rows.map(row => ({ txid: row.txid, vout: row.vout, amount: row.amount, block_height: row.height,
      status: row.pending ? 'pending' : 'confirmed', confirmations: row.pending ? 0 : height - row.height + 1,
      coinbase: Boolean(row.coinbase), mature: !row.coinbase || height - row.height + 1 >= 100 }));
  }
  utxoPage(address, { after = null, limit = 100, includePendingSpent = false } = {}) {
    pageLimit(limit);
    const [lastTxid, lastVout] = outpointAfter(after);
    const rows = this.db.prepare(`SELECT * FROM (
      SELECT o.txid,o.vout,o.amount,o.height,o.coinbase,0 AS pending,p.spender AS pending_spent_by FROM outputs o
        LEFT JOIN spends s ON s.txid=o.txid AND s.vout=o.vout
        LEFT JOIN pending_spends p ON p.txid=o.txid AND p.vout=o.vout
        WHERE o.address=? AND (o.txid,o.vout)>(?,?) AND s.spender IS NULL AND (? OR p.spender IS NULL)
      UNION ALL SELECT o.txid,o.vout,o.amount,NULL,0,1,s.spender FROM pending_outputs o
        LEFT JOIN pending_spends s ON s.txid=o.txid AND s.vout=o.vout
        WHERE o.address=? AND (o.txid,o.vout)>(?,?) AND (? OR s.spender IS NULL)
      ) ORDER BY txid,vout LIMIT ?`).all(address, lastTxid, lastVout, Number(includePendingSpent), address, lastTxid, lastVout, Number(includePendingSpent), limit);
    const height = this.tip()?.height ?? -1;
    return rows.map(row => ({ txid: row.txid, vout: row.vout, amount: row.amount, block_height: row.height,
      status: row.pending ? 'pending' : 'confirmed', confirmations: row.pending ? 0 : height - row.height + 1,
      coinbase: Boolean(row.coinbase), mature: !row.coinbase || height - row.height + 1 >= 100,
      ...(includePendingSpent ? { pending_spent_by: row.pending_spent_by ?? null } : {}) }));
  }
  transactionLocation(txid) {
    const row = this.db.prepare('SELECT b.hash AS block_hash,t.block_height FROM transactions t JOIN blocks b ON t.block_height=b.height WHERE txid=?').get(txid);
    if (row) return { ...row, status: 'confirmed' };
    if (this.db.prepare('SELECT 1 FROM pending_transactions WHERE txid=?').get(txid)) return { status: 'pending' };
    return null;
  }
  close() { this.db.close(); }
}
