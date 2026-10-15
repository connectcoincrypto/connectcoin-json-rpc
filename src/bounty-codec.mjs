import { createHash } from 'node:crypto';
import { RpcError } from './errors.mjs';

const digest = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');

// Versioned positional encodings are shared with clients. Property order in
// input objects is irrelevant; strings and monetary amounts are not coerced.
export function bountyStateDigest(tip, cursor) {
  return digest(['bounty-state-v1', tip.chain, tip.genesis_hash, tip.height,
    tip.hash, tip.mediantime, cursor]);
}

export function bountyImmutableDigest(bounty) {
  return digest(['bounty-immutable-v1', bounty.txid, bounty.vout,
    bounty.block_hash, bounty.block_height, bounty.amount, bounty.domain,
    bounty.connection_work_target, bounty.root_certificates_version,
    bounty.signature_algorithms_mask, bounty.coinbase]);
}

const STATE_EVENTS = new Set(['pending_spend', 'spent', 'available_again', 'matured']);
const sameTip = (left, right) => left && right && ['height', 'hash', 'mediantime', 'chain', 'genesis_hash']
  .every(field => left[field] === right[field]);

export function compactBountyEvents(entries, { version = 1, currentTip, baseTip } = {}) {
  const tips = [], tipIndexes = new Map();
  const changes = entries.map(({ event, bounty, tip }) => {
    const current = version >= 2 && sameTip(tip, currentTip);
    // baseTip is supplied only after API cursor/full-state authentication and a
    // complete canonical suffix. A historical tip must match all five fields.
    const base = version === 3 && sameTip(tip, baseTip);
    let tip_ref = -1;
    if (!current && base) tip_ref = -2;
    else if (!current) {
      const key = JSON.stringify(tip);
      if (!tipIndexes.has(key)) { tipIndexes.set(key, tips.length); tips.push(tip); }
      tip_ref = tipIndexes.get(key);
    }
    // The legacy event's spender may describe an earlier coalesced mutation.
    // Never reconstruct it from the final coherent bounty state.
    if (STATE_EVENTS.has(event.type) && bounty) {
      const encoded = { ...event, tip_ref, bounty_ref: bountyImmutableDigest(bounty),
        bounty_delta: { status: bounty.status, spending_txid: bounty.spending_txid,
          confirmations: bounty.confirmations } };
      if (version >= 2 && Object.hasOwn(event, 'spending_txid') && event.spending_txid === bounty.spending_txid) {
        delete encoded.spending_txid;
        encoded.spending_txid_ref = true;
      }
      if (version === 3) {
        // Derivation never repairs bad source data. In particular this is the
        // event's publication height, not a later response/page tip.
        const confirmations = tip.height - bounty.block_height + 1;
        if (!Number.isSafeInteger(tip.height) || tip.height < 0 ||
            !Number.isSafeInteger(bounty.block_height) || bounty.block_height < 0 ||
            bounty.block_height > tip.height || !Number.isSafeInteger(confirmations) ||
            !Number.isSafeInteger(bounty.confirmations) || bounty.confirmations !== confirmations) {
          throw new RpcError(-32011, 'Bounty confirmation metadata is inconsistent; retry after synchronization.');
        }
        delete encoded.bounty_delta.confirmations;
      }
      return encoded;
    }
    if (version >= 2 && event.type === 'added') {
      const identity = ['txid', 'vout', 'block_hash', 'block_height'];
      if (!bounty || identity.some(field => !Object.hasOwn(event, field) || event[field] !== bounty[field])) {
        throw new RpcError(-32011, 'Bounty event identity is inconsistent; retry after synchronization.');
      }
      const encoded = { ...event, bounty, tip_ref };
      for (const field of identity) delete encoded[field];
      return encoded;
    }
    return { ...event, bounty, tip_ref };
  });
  return { changes, tips };
}
