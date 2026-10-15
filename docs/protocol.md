# TCP protocol, version 0.1

This document specifies this repository's API, not ConnectCoin's private node RPC. The transport is raw plaintext TCP: one UTF-8 JSON object followed by a newline. A browser cannot directly use a native TCP socket. Native clients can keep one connection open and correlate responses by `id`.

Only JSON-RPC `"2.0"` requests with an explicit string/safe-integer `id`, a listed method and named object `params` are accepted. `params` may be omitted for `{}`. Extra parameters are errors. Client notifications (missing/null id), JSON-RPC batch arrays and arbitrary RPC forwarding are not supported. The bounded `gettransactions` method is one ordinary request. Subscriptions and streaming use server-originated JSON-RPC notifications.

```json
{"jsonrpc":"2.0","id":1,"method":"getrecentblockhashes","params":{}}
```

```json
{"jsonrpc":"2.0","id":1,"result":{"tip":{"height":700,"hash":"<64 hex>","mediantime":1700000000,"chain":"testnet4","genesis_hash":"<64 hex>"},"window":600,"blocks":[{"height":700,"hash":"<64 hex>"}]}}
```

The example omits other block entries for readability. With `params:{}`, the response remains unchanged: every available active-chain block from `max(0,H-599)` through `H`, newest first. An optional integer `count` between 1 and 600 requests only the newest `min(count,H+1)` entries. For example, `params:{"count":5}` returns heights `H` down through `max(0,H-4)`. `window:600` always describes the bounty eligibility window, **not** the number of returned entries. No new response field is required. Null, booleans, strings, fractions, out-of-range counts and extra parameters are rejected with `-32602`; omitting `count` defaults to 600.

Incremental clients can request 5 entries, then 100 if no returned height/hash matches their cached active chain, and finally 600 if needed. Each request is a separate indexed snapshot: verify the response tip, count, contiguous descending heights and hashes before joining it to cached metadata, and restart reconciliation if the tip changes during the operation. Matching a height alone is insufficient; require the hash at that height too. A short suffix is not a complete 600-block snapshot; if the cache cannot supply the remainder of the current window, refresh with 600. This optimization does not replace bounty-change cursor replay or the full bounty resynchronization required when a cursor expires or a reorganization invalidates it. Older servers reject `count` with `-32602`; clients can retry with `{}` and validate the complete legacy response.

A block hash is a displayed hexadecimal hash, normalized to lowercase. The server never accepts arbitrary heights or old-chain hashes in `getblockbounties`. The optional count does not expand the eligible window, change validation or change per-method rate limits.

Optional boolean `include_bounty_counts:true` adds `bounty_counts:1` to this response and `bounty_count` to **every** returned block. It counts all P2C creation outputs in that canonical block, including spent, pending-spent and immature outputs, exactly like the complete block snapshot. The tip, descending contiguous block list and counts are obtained synchronously from one ready indexed state. Missing metadata, an indexing gap, an unindexed block or an inconsistent tip returns `-32011`, never an inferred zero. An indexed zero authorizes an empty snapshot only for that exact hash. Positive counts require a complete validated block set, obtained from the full stream or the proven-complete enriched-event procedure below; clients validate its unique output count and retain the ordinary initial watermark, replay and final publication fences. The `count` range and inclusive 600-block window are unchanged. Omitting the flag or using `false` preserves the original payload; nonboolean flags return `-32602`. An older server can reject the flag, requiring ordinary full streams instead of assuming missing counts mean zero.

## Amounts and address queries

Every indexed amount is a **base-10 integer string in connects**. One CONN is **10,000,000,000 connects**, not Bitcoin's 100,000,000 satoshis. Use arbitrary-precision integers in clients. Strings prevent JavaScript/JSON's 53-bit precision limit from silently changing values.

`getaddressbalance` returns:

- `confirmed`: all unspent confirmed outputs, including immature coinbase outputs and outputs currently spent by a pending transaction.
- `immature`: the immature part of `confirmed`.
- `pending_spent`: confirmed outputs consumed by transactions in the indexed mempool.
- `pending_received`: currently unspent mempool outputs received by the address (including pending change).
- `available_confirmed`: `confirmed - immature - pending_spent`.
- `pending_delta`: `pending_received - pending_spent`.
- `total`: `confirmed + pending_delta`, including immature amounts.

History entries contain `txid`, `status` (`confirmed`/`pending`), `block_height`, `block_hash`, `confirmations`, `received`, `spent` and `balance_delta`. Pending block fields are `null`. `spent` means address-owned input value, not necessarily value paid to another person; change is included in `received`. Confirmations are evaluated at the response's `tip`.

UTXO entries include `txid`, `vout`, `amount`, `block_height`, `status`, `confirmations`, `coinbase`, `mature`. By default, outputs consumed by indexed mempool transactions are excluded. Unconfirmed outputs can themselves be spent by descendants; only terminal unspent outputs are returned. The optional boolean `include_pending_spent:true` also returns confirmed and pending outputs consumed only by indexed mempool transactions, adding `pending_spent_by` (spending txid or `null`) to every item. It never returns outputs spent in the active chain. This opt-in is bound into pagination cursors; do not change it between pages. Clients must exclude marked outputs from automatic coin selection and ordinary available balances. Their presence is information, not evidence that a conflicting replacement will be accepted.

History and UTXO responses have `items`, `tip`, `unit`, `address`, `live:true` and `next_cursor`. Return `next_cursor` unchanged to request the next page. `null` means no further records at that moment. Each page request uses the ordinary method quota. The default page size is 100, configurable up to 500. Cursors are opaque authenticated keyset markers bound to the query, address and a canonical chain anchor. History is ordered by txid ascending; UTXOs by txid then vout ascending, **not by timestamp**. The UI can reorder downloaded history by block height.

Pagination is a **live view**, not a frozen point-in-time database snapshot. New blocks and unrelated mempool activity do not invalidate it. Deleting a preceding row does not skip the next result, as offset pagination would. New records inserted behind the cursor, and updates/removals of already-read records, require a refresh; use address subscriptions, deduplicate by txid/outpoint and refresh on notifications. Do not assume concatenated pages represent an atomic spendable balance. A reorganization removing the cursor's anchor or a service restart invalidates the cursor explicitly (`-32011`); restart the query. This avoids indefinite restarts of large histories under the 200/minute per-method/IP quota and avoids holding database snapshots open for slow clients.

### Incremental address synchronization

`getaddresschanges` accepts `{ "addresses": ["<address>", "<address>"], "cursor": "<opaque cursor>" }`. Supply 1–100 unique native addresses for the indexed network; the order and uniform letter case do not matter. Duplicate normalized addresses and additional fields are rejected. Omit `cursor` (or use `null`) for a **watermark only**, not a historical snapshot. The response is:

```json
{"tip":{"height":700,"hash":"<64 hex>","mediantime":1700000000,"chain":"main","genesis_hash":"<64 hex>"},"unit":"connects","through_sequence":123,"journal_epoch":1,"changes":[{"sequence":123,"address":"<address>","kind":"utxo","action":"upsert","txid":"<64 hex>","vout":0,"item":{"txid":"<64 hex>","vout":0,"amount":"10000000000","block_height":699,"status":"confirmed","coinbase":false,"pending_spent_by":null,"confirmations":2,"mature":true}}],"next_cursor":"<opaque cursor>","has_more":false}
```

Every event has a safe-integer global `sequence`, `address`, `kind` (`history` or `utxo`), `action` (`upsert` or `remove`) and `txid`. UTXO events additionally have `vout`. An upsert contains the complete standard history/UTXO record in `item`; a remove omits `item` and deletes that address's txid/outpoint. UTXO upserts always include `pending_spent_by`, including for a pending output consumed by a pending child. Confirmations and maturity are evaluated at the response tip, not the current wall clock. Event sequences are ordered but can have gaps for unrelated addresses. Apply events idempotently in order, recompute confirmation/maturity-dependent values at the supplied tip, and advance the stored cursor only after applying the page successfully. No raw transaction download is needed to apply these indexed deltas.

Race-safe initial synchronization:

1. Get an initial watermark for the complete address set **before** reading any baseline pages.
2. Read all history pages and UTXO pages with `include_pending_spent:true` into a staging cache. Deduplicate by address+txid and address+outpoint. These baseline pages remain live views.
3. Replay `getaddresschanges` from the starting watermark until `has_more:false`, applying every upsert and removal. This reconciles mutations that occurred anywhere in the baseline, including behind an already-read keyset marker.
4. Publish the reconciled cache. On later tips/address notifications, fetch only changes from the saved cursor; do not download all historical pages again. Derive balances from the full pending-aware UTXO set. Outputs with `pending_spent_by` are not ordinary spend candidates.

Each paginated delta drain freezes an upper event sequence and a canonical tip; event payloads are persisted at mutation time, so a later eviction or confirmation cannot rewrite an earlier page. After `has_more:false`, a new call starts the next drain and includes newer mutations. Cursors bind the normalized address set, persistent journal epoch, sequence and canonical chain anchor. Adding/removing an address requires a new baseline for that set. Reorganizations, mutation overflow, incompatible writer gaps or expired retention return `-32011`: discard an incomplete baseline/delta staging pass and resynchronize. An older server returns `-32601`, allowing a client to fall back to legacy refresh behavior. Cursor tokens are opaque and are never authorization or unspentness proofs.

Every response, including initial watermarks and empty deltas, includes safe-integer `through_sequence` (the frozen global upper sequence) and `journal_epoch`. For wallets split across multiple address batches, **all completed batches must have the same tip hash, `journal_epoch` and `through_sequence` before publishing their combined balances**. A matching block hash alone is insufficient: mempool changes can occur between batches without a new block and otherwise double-count a self-transfer. Converge lagging batches with further delta drains, preserving the private staging cache; do not replace ordinary same-epoch drift with full historical rereads. If continuous churn prevents bounded convergence, retain the last coherent published snapshot and retry later instead of publishing a mixture.

The SQLite address journal and its separate signing key persist across normal service restarts. It retains at most 100,000 events or 32 MiB of serialized payloads by default; a single mutation exceeding 10,000 tracked keys explicitly resets the journal instead of dropping partial changes silently. These bounds apply globally, not per address. Pages use the configured address page size (default 100, maximum 500). `getaddresschanges` has its own ordinary **200 calls per sliding minute per IP** quota, regardless of how many addresses are in the request. The existing bounty journal and legacy pagination cursors retain their separate lifetime rules.

## Bounty streams: no result-count cutoff

Request:

```json
{"jsonrpc":"2.0","id":2,"method":"getblockbounties","params":{"block_hash":"<64 hex>"}}
```

The request gets one normal result:

```json
{"jsonrpc":"2.0","id":2,"result":{"stream_id":"<opaque id>"}}
```

Then server notifications carry the snapshot and consecutive chunks:

```json
{"jsonrpc":"2.0","method":"stream.chunk","params":{"stream_id":"<opaque id>","sequence":0,"items":{"type":"snapshot","block_hash":"<64 hex>","tip":{},"cursor":"<change watermark>","unit":"connects","live":true}}}
{"jsonrpc":"2.0","method":"stream.chunk","params":{"stream_id":"<opaque id>","sequence":1,"items":{"type":"bounties","tip":{},"items":[{"txid":"<64 hex>","vout":2,"amount":"10000000000","domain":"example.com","connection_work_target":"<64 hex>","root_certificates_version":1,"signature_algorithms_mask":7,"block_height":699,"block_hash":"<64 hex>","coinbase":false,"confirmations":2,"status":"available","spending_txid":null}]}}}
{"jsonrpc":"2.0","method":"stream.chunk","params":{"stream_id":"<opaque id>","sequence":2,"items":{"type":"state","tip":{},"cursor":"<end watermark>"}}}
{"jsonrpc":"2.0","method":"stream.end","params":{"stream_id":"<opaque id>","complete":true,"chunks":3}}
```

Tip fields in this example are abbreviated. The funding outpoint is `txid` plus `vout`; do not confuse this output index with a claim's input index. No funding transaction bytes or proof witness are included in the bounty list.

The server sends all chunks automatically without another request. Default chunk size is 100 bounties, **not an overall limit**. A block without bounties still sends snapshot metadata and an explicit successful end. Bounty status is `available`, `immature`, `pending_spend` or `spent`. A spent record includes `spending_txid`; a pending spend can later disappear or be replaced.

The **membership** of this list is fixed by the requested block hash; bounty availability is a **live view**, observed at the indexed tip on each chunk. A normal tip advance or unrelated mempool update does not restart a long transfer. Replay changes from the **starting** watermark after downloading (not just the end watermark), to reconcile status changes during transfer. The last `state` chunk supplies the ending tip/watermark for reference. A creation block leaving the active recent window during transfer makes the stream end with `complete:false`; resource/timeout failures can close the connection. In either case discard the partial block list and retry as appropriate. **A missing successful end always means incomplete**, never an empty or truncated successful result. Slow clients cannot retain an unbounded SQLite snapshot or queue. A progressing stream is not cut off merely because its total duration exceeds the request timeout.

If the index is temporarily catching up between chunks, the stream waits for a coherent published state (at most 30 seconds, also subject to connection deadlines). A synchronization error or timeout marks it incomplete; partially applied reorganizations are never read as a ready index.

## Changes and subscriptions

Suggested race-safe synchronization:

1. Call `getbountychanges` without a cursor to obtain the current `next_cursor` watermark. This returns no historical events.
2. Fetch `getrecentblockhashes` and the blocks you do not already have. A reorg/out-of-window error requires refreshing the hash list.
3. Replay `getbountychanges` starting at the watermark from step 1. Process pages until `has_more:false`. Use each page's `next_cursor` for the next request.
4. Continue polling changes, or use `subscribebounties`. To switch safely, subscribe first, then replay from the last cursor to close the gap; deduplicate events by `sequence`.

Changes include `added` (outpoint and creation block), `spent`/`pending_spend` (outpoint and spending txid), `available_again`, `matured`, and `window_exit`. These events tell clients what to refresh; an `added` event does not contain the whole block's bounty list. Query the indicated block for its authoritative current records. Batch changes for the same block rather than issuing one request per outpoint. A window exit is discovery removal, not consensus expiry.

To apply changes directly, pass the optional boolean `include_bounties:true` to `getbountychanges`, including the initial watermark call. Every opted-in result adds `bounty_deltas:1`; clients must check this acknowledgement. Each returned event keeps the legacy fields and adds `bounty` (the complete standard bounty row) and `tip` (that event's original publication tip):

```json
{"tip":{"height":701,"hash":"<current hash>","mediantime":1700000010,"chain":"main","genesis_hash":"<genesis hash>"},"bounty_deltas":1,"changes":[{"sequence":123,"type":"pending_spend","txid":"<funding txid>","vout":2,"spending_txid":"<claim txid>","tip":{"height":700,"hash":"<publication hash>","mediantime":1700000000,"chain":"main","genesis_hash":"<genesis hash>"},"bounty":{"txid":"<funding txid>","vout":2,"amount":"10000000000","domain":"example.com","connection_work_target":"<64 hex>","root_certificates_version":1,"signature_algorithms_mask":7,"block_height":699,"block_hash":"<creation hash>","coinbase":false,"confirmations":2,"status":"pending_spend","spending_txid":"<claim txid>"}}],"next_cursor":"<opaque cursor>","has_more":false}
```

Rows and event tips are copied when the index publishes a coherent update. They are never fetched again from current state while reading a historical page. Several changes can be coalesced in one publication: each event then carries the **final coherent row at that publication**, even when its legacy `type` or `spending_txid` describes an earlier mutation. For example, an `added` event can carry a row already `spent`, or a `pending_spend` event a row already `available` after eviction. Apply the full `bounty` row by outpoint, in sequence order; do not infer its current status from the event type. `bounty:null` is used only for `window_exit`, which removes that outpoint. Missing required rows cause explicit resynchronization, never silent omission.

The outer response `tip` is current at the page request; each event's `tip` and confirmation count remain historical. Later pages can include newer publications, and there is no frozen upper watermark for a whole bounty drain. Continue until `has_more:false`, then advance confirmations and coinbase maturity against the current canonical tip. The starting watermark and full initial block snapshots are still required; this option eliminates full-block refreshes for ordinary subsequent changes. Omitting `include_bounties` or setting it to `false` preserves the exact legacy response/event shapes, and subscription notifications remain unchanged. Nonboolean flags and extra parameters return `-32602`. Older servers reject the new flag; clients can deliberately use the legacy refresh flow, but must not treat a response lacking `bounty_deltas:1` as complete row deltas.

With `include_bounties:true`, a client with an already validated canonical window may additionally send `known_tip:{"height":700,"hash":"<known tip hash>"}`. Both fields are required; height must be a nonnegative safe integer and hash must contain exactly 64 hexadecimal characters (normalized to lowercase). Extra nested fields, malformed anchors, or `known_tip` without `include_bounties:true` return `-32602`. Only requests containing `known_tip` add the following fields:

```json
{"block_deltas":1,"block_delta":{"base_height":700,"base_hash":"<known tip hash>","blocks":[{"height":701,"hash":"<current tip hash>","bounty_count":0}]}}
```

`block_delta.blocks` is an **ascending**, contiguous suffix from `base_height+1` through the outer response tip, with at most five entries. The base must still be canonical. An unchanged canonical tip returns `blocks:[]`. A different hash at the base height, a client height ahead of the server, or a gap greater than five returns `block_delta:null`; this is an explicit fallback to the existing recent-block reconciliation, not permission to skip blocks or discard the old window. Invalid/expired change cursors still require resynchronization (`-32011`) regardless of the suffix. Clients must check `block_deltas:1`, the echoed base, heights, hashes, suffix length and final tip before using the optimization; an older server can reject `known_tip`, in which case clients may retry without it and keep the normal window flow.

Each `bounty_count` counts **all P2C creation outputs** of that canonical block, including spent, pending-spent and immature outputs, exactly as its complete `getblockbounties` snapshot would. It is not a count of currently eligible claims. Only a completely indexed block can report zero: missing block metadata, a sequence gap or an unindexed anchor/suffix returns `-32011`, never an inferred zero. Counts and hashes are bounded synchronous indexed reads made together with the page's current tip, journal entries and cursor, with no backend fetch or asynchronous interleaving. No database migration is needed.

A validated zero-count entry permits installing an empty block snapshot **for that exact hash** without requesting the empty stream. Positive counts require the complete normal block stream or the proven-complete enriched-event procedure below, retaining all row validations. Counts are trusted RPC index reports, not independently verifiable inclusion or absence proofs. Initial synchronization, retained-window ancestry, journal replay, invalidation and resource limits still apply. In particular, an advertised empty block cannot also contain an added/full-row bounty in the same coherent response. The client must replay pages through `has_more:false`; if it performs a block stream, recent-hash query or other asynchronous index read after that final page, it must fence/replay again before publishing its catalog. A final coherent page can serve as the publication fence only when no such intervening read occurs.

The in-memory shared change journal defaults to 10,000 events or 8 MiB, whichever is reached first. The byte bound includes the private saved bounty rows and publication tips even when only legacy clients are connected. Old cursors, service restarts, missing required rows, large catchup and reorganizations require full resynchronization; they cannot silently skip events. A poll receives error `-32011`; a subscriber receives `resync_required:true`. Reorganizations discard the old journal before any further delta response, so disconnected-chain events are not replayed as current. If the connection is lost, resume from the last processed cursor or take a fresh snapshot if it expired. `getbountychanges` keeps its ordinary independent 200 calls per sliding minute per IP quota, with either flag value.

### Compact bounty feed

Optional `compact:true` requires `include_bounties:true`. Without `compact_version`, the version-1 contract in this section applies unchanged. Full compact responses preserve `tip`, `next_cursor`, `has_more`, `bounty_deltas:1` and any requested `block_deltas`/`block_delta`, and add `compact:1` and `tips:[]`. `tips` is a page-local table of exact historical publication tips, deduplicated in first-occurrence order. Each event replaces its `tip` with an integer zero-based `tip_ref` into that table. The outer current tip is not implicitly table entry zero; it appears there only if an event was published at that tip. The empty event list has an empty table.

Every event preserves its legacy fields, sequence and outpoint. An `added` event carries its complete `bounty` row, even when the final coherent row is already spent. A `window_exit` event retains `bounty:null`. The status events `pending_spend`, `spent`, `available_again` and `matured` instead omit `bounty` and carry:

```json
{"bounty_ref":"<64 hex SHA-256>","bounty_delta":{"status":"pending_spend","spending_txid":"<claim txid>","confirmations":12},"tip_ref":0}
```

`bounty_ref` is the lowercase hexadecimal SHA-256 of the UTF-8 bytes of `JSON.stringify` applied to this **exact positional array**, with no numeric/string coercion:

```javascript
['bounty-immutable-v1', bounty.txid, bounty.vout,
 bounty.block_hash, bounty.block_height, bounty.amount, bounty.domain,
 bounty.connection_work_target, bounty.root_certificates_version,
 bounty.signature_algorithms_mask, bounty.coinbase]
```

The immutable record must already have been fully validated for that outpoint and canonical block. A client checks the digest, overlays only the three mutable fields, resolves `tip_ref`, and performs all ordinary full-row, confirmation, membership and publication checks. If the referenced outpoint is unknown, replay from the **same unadvanced cursor** without `compact` and `known_state` to obtain the full rows. A mismatch against a known validated immutable record is an error, not permission to replace it. Never infer the final status from the event type or reconstruct the root event's `spending_txid` from `bounty_delta.spending_txid`: coalesced publications can legitimately differ. The server retains full historical rows and tips internally; compact encoding does not change journal retention, ordering, invalidation, subscriptions or quotas.

To avoid repeating an unchanged checkpoint, a compact request with a non-null string `cursor` and `known_tip` may also include `known_state`, a 64-character hexadecimal SHA-256 (letter case is accepted and normalized). Compute it from the **complete previously validated tip and that exact cursor**:

```javascript
SHA256(JSON.stringify(['bounty-state-v1', tip.chain, tip.genesis_hash,
 tip.height, tip.hash, tip.mediantime, cursor])) // UTF-8 input, hex output
```

Only after checking readiness, cursor authentication/type/range/floor, the indexed canonical anchor, equality to the current sequence, equality of `known_tip` to the current tip, and equality of this full-state digest may the server return the exact result:

```json
{"compact":1,"unchanged":true}
```

This is the **only** successful feed result allowed to omit tip/cursor/events. The authenticated cursor also binds the server instance's signing key and journal lifetime; a restart, reset, reorg or expired cursor still requires normal resynchronization. Any new event prevents the ACK, even if the bounty later returns to its former state. A changed median time or network identity under an otherwise identical height/hash cannot be hidden by this ACK. A digest mismatch returns the complete compact response, preserving the client's usual metadata checks.

The client may accept the ACK only for the exact outstanding request and unchanged validated cache generation from which it derived the digest. It preserves that complete snapshot and cursor; it does not synthesize a missing count, create a new empty block or advance a watermark. If there was intervening asynchronous snapshot work, the ordinary replay/fence requirement still applies. No cursor or cache may be advanced on an unknown or malformed compact response. `known_state` without `compact:true`, `include_bounties:true`, `known_tip` and a string cursor is invalid (`-32602`). Omitting `compact` or setting it to `false` retains the old full/legacy shape. On an older server's `-32602`, clients may remove `compact` and `known_state` together and retry the same cursor with the earlier capability negotiation.

Canonical digest test vectors:

- `['bounty-state-v1','main','a'.repeat(64),123,'b'.repeat(64),1700000000,'opaque-cursor-v1']` hashes to `a028924ae24da57f10b87198d8ae90f660bcf67ad7e09e80985e744f42d8a5a7`.
- `['bounty-immutable-v1','c'.repeat(64),2,'d'.repeat(64),120,'100000000','example.com','e'.repeat(64),1,7,false]` hashes to `49ef00b7fa42a74c1576f41c0e4d9fbc1cc08025b0785bdd12e0736a8f8d3b73`.

### Compact bounty feed version 2

Add **exactly** `compact_version:2` to a request with `compact:true` and `include_bounties:true`. Version 3 is a separate opt-in documented below; other version values and incompatible flags are invalid (`-32602`). Omitting the version keeps every version-1 field and meaning unchanged; this is not a server-wide setting. Every v2 response explicitly acknowledges `compact:2`. The v1 digest algorithms above remain unchanged: the digest binds the reconstructed full tip and actual opaque cursor, never an abbreviated wire representation.

V2 non-ACK pages retain `tip`, `changes`, `tips`, `next_cursor` and `has_more`. They omit the redundant `bounty_deltas` and `block_deltas` capability fields: `compact:2` implies enriched bounty deltas, and the presence of `block_delta` acknowledges the requested canonical suffix. If no `known_tip` was requested, `block_delta` is absent. This does not permit treating an absent suffix as an empty block list. The exact unchanged ACK is `{ "compact": 2, "unchanged": true }`, with all v1 authentication, snapshot-generation and publication-fence requirements retained.

**Self-contained versus request-bound pages.** Initial requests, requests without an authenticated matching full-state digest, and requests whose canonical suffix is unavailable return a full tip object, a string cursor, and (when requested) the ordinary `block_delta` object or `null`. No network identity or block ancestry may be inferred from the client's cache for this fallback.

A page is request-bound only after authenticating and range-checking the request cursor, obtaining a complete non-null suffix of at most five canonical indexed blocks from `known_tip`, and reconstructing the base tip from `store.blockAt(known_tip.height)` plus the current pinned chain/genesis identity. The base hash must equal `known_tip.hash`; its median time and the opaque request cursor must produce exactly `known_state`. A cursor by itself is insufficient. These checks and response construction occur without an intervening await, against the same ready publication. A reorg, expired cursor, missing index data or false zero count must never be compressed into apparent continuity.

Only a bound page uses these representations, **together**:

- `tip: [height, hash, mediantime]`. Inherit only `chain` and `genesis_hash` from the fully validated, privately owned state used for this exact outstanding request. Validate the resulting complete tip normally.
- `block_delta: [[hash, bounty_count], ...]`. Heights are consecutive from `request.known_tip.height + 1`. The list has zero to five pairs, and its endpoint must equal the response height. An empty list is valid only when the base and response tip are the same canonical block. Each count remains a nonnegative safe integer covering **all** bounty creation outputs, not only available ones. `null` is allowed instead of a hash only in the **last** pair whose inferred height equals the response tip height; it references `response.tip.hash`. It never means unknown, missing or empty. All intermediate hashes must be present.
- `next_cursor: null` only when there are no events, `has_more:false`, and the response cursor is exactly the request cursor. It means preserve that authenticated cursor, not initialize or advance it. All other pages carry a string cursor, including every page containing events.

For example, an otherwise unchanged journal advancing one empty block can return:

```json
{"tip":[702,"<new block hash>",1700000020],"compact":2,"changes":[],"tips":[],"next_cursor":null,"has_more":false,"block_delta":[[null,0]]}
```

The zero still has the same authoritative indexed-count requirements as an ordinary suffix. A client must validate the complete array shapes, safe integers, ancestry, endpoint, counts and cache generation before applying aliases. Mixed bound/unbound tip and suffix forms, unknown versions, unsupported null references, and stale request generations are errors, not signals to accept cached data or advance the cursor.

**Event representation, in both bound and self-contained v2 pages:**

- `tip_ref:-1` means the complete response tip, but only when the event's exact historical publication tip matches it in height, hash, median time, chain and genesis. Otherwise the nonnegative index references `tips[]`, whose entries remain full publication tip objects. The table excludes entries represented by `-1`; historical tips are not replaced with the current tip.
- An `added` event carries the complete nested `bounty` row and omits the four duplicate outer fields `txid`, `vout`, `block_hash`, and `block_height`. Before encoding, the server checks that all four original outer fields exist and equal their nested counterparts. The client validates the complete row and reconstructs those identities before the ordinary event/canonical-count checks. It must reject unexpected mixed duplicate identities instead of silently discarding contradictory values. New rows still carry every monetary, target, certificate, algorithm, coinbase and mutable state field.
- A state event may replace its outer `spending_txid` with **exactly** `spending_txid_ref:true` only if the original outer property existed and equaled `bounty_delta.spending_txid`. Resolve it to that validated delta value. An absent original outer property remains absent; differing outer and final spenders remain separate values. The boolean alias and an explicit outer spender may not coexist, and false/nonboolean aliases are invalid. Neither the event type nor an absent alias authorizes inferring a spender.
- State events still carry explicit `txid`/`vout`, the same `bounty_ref` digest and all three delta fields. `window_exit` still carries its explicit outpoint and `bounty:null`. Sequence ordering, cache identity, row validity, confirmation arithmetic, canonical membership, immutable counts and window bounds remain mandatory.

When a v2 request gets an older server's `-32602`, a client may retry the **same unadvanced cursor** with only `compact_version` removed to negotiate v1. A malformed or contradictory successful v2 result must never trigger a permissive downgrade. If an unknown immutable bounty reference requires the existing full-row replay, remove `compact_version` along with `compact` and `known_state`. Keep the original cursor and all snapshot/publication fences. No v2 encoding changes journal storage, retention, quotas, subscription formats, reconciliation or broadcast validation.

### Compact bounty feed version 3

Add **exactly** `compact_version:3` with `compact:true` and `include_bounties:true`. Every response acknowledges `compact:3`; omitted version and explicit version 2 preserve their existing representations. V3 uses the same digests, cursor authentication, canonical suffix bounds, immutable references, pagination, resource limits and full-row replay as v2. The exact unchanged ACK is `{ "compact": 3, "unchanged": true }`.

**Defined defaults.** In non-ACK pages, absent `changes` and `tips` mean empty arrays, and absent `has_more` means false. An absent `next_cursor` means the exact opaque cursor of this outstanding request **only** under the v2 unchanged-cursor conditions: a bound page, no events, no further page, and an unchanged sequence. It does not initialize a cursor or signal completion of an incomplete replay. Initial and unbound pages always include a string cursor. Explicit defaults remain valid only under the same checks as v2. Clients must use property-presence checks, not null-coalescing or truthiness: explicit null/wrong-type arrays, invalid booleans and contradictory cursors remain errors. The server omits these defaults canonically; no other absent member receives a default.

**Request-base aliases.** The bound-page predicate is exactly the one in v2: authenticated/range-checked cursor, coherent ready publication, complete canonical indexed suffix and a `known_state` digest matching all five fields of the canonical base tip plus that exact cursor. Only such a page may use:

- `tip:null`, meaning the fully validated and privately owned tip used for this exact request. This requires `block_delta:[]` and exact equality in height, hash, median time, chain and genesis between the canonical base and response tips. A null tip without that binding is invalid. Non-null tip arrays and block pairs retain the full v2 validation rules.
- `tip_ref:-2`, meaning that same full request-base tip **as the event's historical publication tip**. It is valid only in a bound page (tip array or null, with block-delta array), and the server checks exact equality of all five publication/base fields before encoding it. It cannot be inferred merely from equal height/hash. The existing `-1` response-tip alias takes priority when both tips match. Other historical tips remain explicit full objects in `tips[]`; their median time and network identity are never replaced by current values.

**Derived state confirmations.** For `pending_spend`, `spent`, `available_again` and `matured`, the server omits `bounty_delta.confirmations` only after checking that the stored value is an integer exactly equal to `event_publication_tip.height - bounty.block_height + 1`, with nonnegative safe integer heights and a creation height no later than publication. An inconsistent original value returns `-32011`, never a repaired success. The client resolves the correct publication tip, validates explicit outpoint identity and the immutable digest against its owned cached row, then derives the omitted confirmations from that row's creation height. It must run every existing row, maturity, status/spender, window and canonical-membership validation on the reconstructed row. If confirmations are explicitly present, they remain subject to those exact checks and may not be overwritten. An unknown immutable reference still requires full-row replay at the same unadvanced cursor; a conflicting known reference remains an error.

The full `added` row keeps its confirmations and all other metadata. State events keep explicit outpoints, immutable digest, status and final spender; event type does not determine final status, and a divergent historical outer spender must not be discarded. `window_exit` still has its explicit outpoint and `bounty:null`. Neither aliases nor omitted defaults permit skipping a malformed sibling event, advancing past a partially processed page, or overwriting shared cache state from a stale outstanding request.

For example, the v2 one-empty-block response above becomes:

```json
{"tip":[702,"<new block hash>",1700000020],"compact":3,"block_delta":[[null,0]]}
```

On a genuine `-32602` version negotiation failure, retry the same cursor with version 2, then the existing v1 request if necessary; remember negotiated capabilities only for the current RPC connection/epoch. A malformed successful response, identity mismatch, lost connection or cursor expiration is not permission to accept weaker validation. Full-row replay removes the compact version and state-reference options as before. No v3 mode changes subscriptions, persistence schema, transaction lookup, broadcasting, proof validation or counters.

### Complete new blocks from enriched events

The existing opt-in `include_bounties:true` feed (acknowledged by `bounty_deltas:1` or compact versions 1–3) carries the complete creation row in each `added` event, including its actual coherent publication state if already spent. The canonical indexed `bounty_count` counts **all creation outputs**, not just available bounties, and remains immutable for that canonical block. These are the same rows/counts used by the block stream, not a new zero-count inference.

A client may avoid downloading a second full stream for a new block only after staging a complete validated creation set: every expected unique outpoint must have a full validated indexed row (from an `added` event or an expanded state-event replay), the set size must equal the independently supplied authoritative count, and its block hash/height must match the validated canonical window. It must drain all pages through the publication fence, preserving sequence continuity and applying subsequent state/removal events to the staged rows. Duplicated or contradictory identities/counts, missing rows, incomplete pagination, unresolved references, reorg/resync or a mismatched snapshot generation must not become a complete block. Fall back to the ordinary full stream or coherent recovery as appropriate. No cache or cursor may be advanced using a partial set, and pending proofs/journals remain preserved. Initial/recovery windows and legacy responses without counts cannot acquire completeness from absence of events.

### Subscriptions

Subscription methods return `subscription_id`, `tip` and the current change `cursor`. An address subscription requested with `changes_only:true` additionally returns `changes_only:true` as an explicit capability acknowledgement. The legacy request (omitting the flag or setting it to `false`) keeps the original three-field result. Repeating the same subscription and mode on the same connection reuses it; legacy and changes-only address subscriptions are distinct. Subscriptions belong to that connection and are deleted on disconnect. IDs cannot cancel another connection's subscriptions.

`subscribeaddress` accepts `{ "address": "<address>", "changes_only": true }` to receive notifications for that address's confirmed receives/spends, changed mempool state (including eviction/replacement), and unspent coinbase outputs reaching 100 confirmations. Ordinary new blocks and unrelated mempool changes do not notify this mode, even when the address already has pending transactions. Confirmation counts can be advanced locally from a separately obtained tip when needed. With the flag omitted or `false`, notifications still include every new tip for legacy confirmation tracking. Other flag types return `-32602`. A client requiring changes-only behavior must check for the `changes_only:true` acknowledgement; older servers reject the extra parameter and must not be silently retried in legacy mode.

Both modes retain explicit refreshes on chain reorganization and bounded-index resynchronization, because journal/cursor invalidation can require a new snapshot even when a complete set of affected addresses is unavailable. These exceptional invalidations do not turn ordinary tip advancement into a refresh. The address notification payload is unchanged in both modes:

```json
{"jsonrpc":"2.0","method":"subscription","params":{"subscription_id":"<id>","kind":"address","address":"<address>","tip":{},"reorg":false,"refresh":true}}
```

Address notifications tell the client to update cached balance/history/UTXOs and confirmations; incremental clients should replay `getaddresschanges`, while legacy clients refresh their pages. They are not full transaction downloads. Tip notifications contain `kind:"tip"`, `tip`, `reorg`. Bounty notifications contain `kind:"bounties"`, `changes`, `tip`, `cursor`, or an explicit `resync_required:true`. Notifications do not consume request quotas, but subscriptions and output queues are bounded.

## Indexed transaction status

`gettransactionstatus` accepts exactly `{ "txid": "<64 hex>" }`. It normalizes the requested txid to lowercase and returns a compact receipt from the coherent active-chain/mempool index:

```json
{"tip":{"height":701,"hash":"<current hash>","mediantime":1700000010,"chain":"main","genesis_hash":"<genesis hash>"},"txid":"<requested txid>","status":"confirmed","block_hash":"<confirmation block>","block_height":700,"confirmations":2}
```

A pending receipt contains `tip`, `txid`, `status:"pending"` and `confirmations:0`, and omits `block_hash` and `block_height`. A confirmed receipt includes both block fields, with `confirmations = tip.height - block_height + 1`. The block location must still belong to the active indexed chain. Unknown, evicted and disconnected transactions return `-32004` unless they are present again in the current mempool. Index synchronization uses the same readiness gate as other indexed methods (`-32001`); inconsistent stored locations return `-32011` instead of a successful receipt. Invalid hashes, non-object parameters and extra fields return `-32602`.

This method performs no backend transaction lookup and returns no serialized transaction, decoded inputs/outputs, proof or witness. It uses its own ordinary 200 calls per sliding minute per actual peer IP quota, including malformed requests, independently of `gettransaction` and `gettransactions`. Poll it to track a txid already known locally. The receipt is a server-reported indexed observation, not an inclusion proof or verification of transaction contents. An older server returns `-32601`; clients requiring compact receipts must not silently download a full transaction as confirmation polling fallback.

## Full transaction lookup and broadcast

`gettransaction` looks up the requested txid in the local active-chain/mempool index, then asks the node for that transaction with its known block hash. No `txindex` is required. Unknown/disconnected transactions are not searched by scanning the chain.

Its `transaction` field preserves ConnectCoin node RPC's decoded schema, including `hex`. **Exception to indexed units:** monetary fields inside this raw-node transaction object (`value`, `fee`, etc.) are exact decimal strings in **CONN**, because this is the node's schema. The envelope supplies indexed confirmation/location metadata. Clients must independently parse/hash transactions if they intend to verify the data.

Optional boolean `raw_only:true` retains that same indexed envelope but returns `transaction:{"hex":"<serialized transaction>"}` only. The backend request uses `getrawtransaction` with verbosity `false`, so unused decoded inputs/outputs and duplicated witness fields are neither downloaded nor forwarded. The server validates nonempty even-length hexadecimal encoding and rechecks readiness and the transaction's indexed status, block hash and block height after the fetch. A changed location returns `-32011`; malformed bytes or backend failures return sanitized `-32002`. The ordinary backend and response-frame limits still apply, and disconnect cancellation is forwarded to the backend; the broadcast-only 400,000-byte cap is not a new read limit. Clients must still parse the full bytes, verify txid and P2C fields, and perform every existing funding validation. Omission or `false` preserves the verbose response, nonboolean flags return `-32602`, and an older server may be retried without the flag. This mode uses the same independent 200/minute/IP `gettransaction` quota, not the lower batch quota.

`sendrawtransaction` accepts only `transaction_hex`; it cannot forward RPC options that disable fee safeguards. Default submitted transaction cap is 400,000 bytes. Requests remain subject to the node's standardness, fee and consensus rules. A timeout can have an unknown broadcast outcome; check the locally known txid before retrying. The server does not sign, alter recipient outputs, generate proofs or reserve bounties.

Broadcast is independent of temporary index readiness. An initialized, pinned network identity is required; before forwarding a transaction, the server checks the local node's chain and genesis against that identity. It does not require the address/mempool index to be caught up. Input validation, per-IP quotas, concurrency limits and node fee/consensus checks still apply. A preflight failure explicitly reports that the transaction was not submitted; an error after forwarding may have an unknown outcome and is never automatically rebroadcast by this service.

### Compact transaction batches

`gettransactions` accepts exactly `{ "txids": ["<64 hex>", "<64 hex>"] }`: between 1 and 32 transaction IDs, unique ignoring letter case. Extra fields, duplicate IDs, invalid hashes and empty/oversized arrays return `-32602`. The server normalizes hashes to lowercase and preserves request order. Every ID must exist in the active-chain/mempool index (`-32004` otherwise); no old-chain search, arbitrary block hash, verbose flag or private-node option can be supplied.

The result has exactly the following shape:

```json
{"tip":{"height":700,"hash":"<64 hex>","mediantime":1700000000,"chain":"main","genesis_hash":"<64 hex>"},"transactions":[{"txid":"<first requested hash>","hex":"<serialized transaction>"}],"remaining":["<next requested hash>"]}
```

`transactions` is a nonempty ordered prefix of the requested IDs; `remaining` is its exact unreturned suffix. An empty `remaining` completes the group. The complete JSON **result** is at most **1,572,864 bytes (1.5 MiB)**, leaving room for the request ID and JSON-RPC envelope within the default 2 MiB frame. The service fetches each parent with private-node `getrawtransaction` using `verbose:false` and its indexed block hash. It stops at the first item that would exceed the result budget, without fetching the later suffix. Clients can issue another bounded request for `remaining`, subject to the same quota. No decoded input/output history is returned. Clients must independently parse and hash each transaction and verify ownership and amounts before spending it.

If the first transaction cannot fit alone (or its bounded backend response exceeds the limit), the method returns **`-32021`** with `data:{"txid":"<that ID>","max_result_bytes":1572864}`. The client may fall back to existing `gettransaction` for that single ID, whose ordinary response-frame limits still apply, then continue the remaining IDs. A client connected to an older server receives `-32601` and can use individual lookups. Backend failures or invalid hex return `-32002`; the server never returns a successful partial prefix after these failures.

The quota is **eight calls per sliding 60 seconds per actual peer IP**, independent of individual transaction lookups; invalid batch parameters also consume it. Each call performs at most 32 backend transaction reads. Only **two batches globally** can execute; additional calls return `-32030` with `retry_after_ms:1000`. Backend reads run sequentially inside each batch, each with a **five-second deadline** and response-byte cap. A **30-second total batch deadline**, disconnect or service shutdown aborts pending HTTP requests and releases the batch slot. Per-call limits cannot raise the operator's configured backend limits.

Every returned transaction's indexed status, block hash and height are checked after its fetch and again before publication. The starting chain anchor must remain active, and the indexed network identity must remain unchanged. A changed location, reorganized anchor or unavailable coherent index causes `-32011`, with no usable result prefix. Normal chain growth and unrelated mempool changes are allowed when all these checks still pass. As with individual lookups, this is an indexed live view, not an inclusion or unspentness proof.

## Errors and resource limits

| Code | Meaning |
|---|---|
| `-32700` | Invalid JSON or UTF-8 |
| `-32600` | Invalid request/profile (including batch or missing id) |
| `-32601` | Method not exposed |
| `-32602` | Invalid/extra parameters |
| `-32603` | Sanitized internal error |
| `-32001` | Index not ready / synchronization unavailable |
| `-32002` | Backend unavailable or transaction lookup/broadcast outcome uncertain |
| `-32004` | Transaction/block not found in the permitted index/window |
| `-32005` | Subscription capacity reached |
| `-32011` | Expired cursor or invalidated snapshot; resynchronize |
| `-32020` | Transaction rejected by the node (`data.node_code` identifies its error class) |
| `-32021` | First transaction exceeds compact batch size limit; try an individual lookup |
| `-32029` | Rate limit, with `data.retry_after_ms` and `reason` |
| `-32030` | Server concurrency/queue capacity reached |

Some framing, oversized-output, slow-client and connection-cap failures close the TCP connection rather than send an error that would exceed the same bounds. Client code must handle EOF at any point.

Default limits: 128 connections total, 8 per real peer IP, 32 active requests total, 16 queued requests per connection, 1 MiB request frame, 2 MiB individual response frame, 2 MiB queued input, 4 MiB queued output, 100 subscriptions per IP and 10,000 globally. Idle timeout is 120 seconds; incomplete-frame timeout is 15 seconds; stalled request/stream timeout is 60 seconds. See `config.example.json` and exported transport defaults. No header or parameter can override the actual peer IP. This implementation has no distributed/shared rate-limit database.

The indexer caps a mempool snapshot at 100,000 transactions and verifies the active chain tip and exact 64-bit mempool sequence around acquisition. Identical snapshots are accepted. Pure growth is also accepted when the second snapshot contains every original txid and the sequence increase equals the number of additions: ConnectCoin increments this counter once per addition or removal, so this establishes that no removals occurred. In that case the complete **first** snapshot is published; later arrivals appear on subsequent polls. Replacements, removals, counter resets and chain changes require reconciliation. A busy or unavailable backend can still temporarily return not-ready rather than pretend an incomplete state is authoritative. Indexed requests wait at most two seconds for an already-running healthy synchronization; stream waits retain their separate 30-second limit. The initial full-chain address index and long-term disk usage are not a 600-block rolling database.

Verbose node responses can be much larger than serialized blocks. A legitimate unusually large block can exceed `backend.maxResponseBytes` (default 256 MiB) and make synchronization unavailable. Review host memory and workload before changing this cap; defaults are not a universal capacity guarantee. Backend HTTP concurrency is bounded to four sockets, with at most three mempool-transaction fetches in flight. Immutable normalized mempool transactions are reused across acquisition retries, but only a coherent full snapshot is published.
