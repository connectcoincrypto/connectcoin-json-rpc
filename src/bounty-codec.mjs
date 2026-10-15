import { createHash } from 'node:crypto';

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

export function compactBountyEvents(entries) {
  const tips = [], tipIndexes = new Map();
  const changes = entries.map(({ event, bounty, tip }) => {
    const key = JSON.stringify(tip);
    if (!tipIndexes.has(key)) { tipIndexes.set(key, tips.length); tips.push(tip); }
    const tip_ref = tipIndexes.get(key);
    // The legacy event's spender may describe an earlier coalesced mutation.
    // Never reconstruct it from the final coherent bounty state.
    if (STATE_EVENTS.has(event.type) && bounty) return { ...event, tip_ref,
      bounty_ref: bountyImmutableDigest(bounty),
      bounty_delta: { status: bounty.status, spending_txid: bounty.spending_txid,
        confirmations: bounty.confirmations } };
    return { ...event, bounty, tip_ref };
  });
  return { changes, tips };
}
