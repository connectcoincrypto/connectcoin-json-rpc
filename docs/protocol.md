# TCP protocol, version 0.1

This document specifies this repository's API, not ConnectCoin's private node RPC. The transport is raw plaintext TCP: one UTF-8 JSON object followed by a newline. A browser cannot directly use a native TCP socket. Native clients can keep one connection open and correlate responses by `id`.

Only JSON-RPC `"2.0"` requests with an explicit string/safe-integer `id`, a listed method and named object `params` are accepted. `params` may be omitted for `{}`. Extra parameters are errors. Client notifications (missing/null id), JSON-RPC batch arrays and arbitrary RPC forwarding are not supported. The bounded `gettransactions` method is one ordinary request. Subscriptions and streaming use server-originated JSON-RPC notifications.

```json
{"jsonrpc":"2.0","id":1,"method":"getrecentblockhashes","params":{}}
```

```json
{"jsonrpc":"2.0","id":1,"result":{"tip":{"height":700,"hash":"<64 hex>","mediantime":1700000000,"chain":"testnet4","genesis_hash":"<64 hex>"},"window":600,"blocks":[{"height":700,"hash":"<64 hex>"}]}}
```

The example omits other block entries for readability; the actual response contains every available active-chain block from `max(0,H-599)` through `H`, newest first. A block hash is a displayed hexadecimal hash, normalized to lowercase. The server never accepts arbitrary heights or old-chain hashes in `getblockbounties`.

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

Pagination is a **live view**, not a frozen point-in-time database snapshot. New blocks and unrelated mempool activity do not invalidate it. Deleting a preceding row does not skip the next result, as offset pagination would. New records inserted behind the cursor, and updates/removals of already-read records, require a refresh; use address subscriptions, deduplicate by txid/outpoint and refresh on notifications. Do not assume concatenated pages represent an atomic spendable balance. A reorganization removing the cursor's anchor or a service restart invalidates the cursor explicitly (`-32011`); restart the query. This avoids indefinite restarts of large histories under the 60/minute quota and avoids holding database snapshots open for slow clients.

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

The SQLite address journal and its separate signing key persist across normal service restarts. It retains at most 100,000 events or 32 MiB of serialized payloads by default; a single mutation exceeding 10,000 tracked keys explicitly resets the journal instead of dropping partial changes silently. These bounds apply globally, not per address. Pages use the configured address page size (default 100, maximum 500). `getaddresschanges` has its own ordinary **60 calls per sliding minute per IP** quota, regardless of how many addresses are in the request. The existing bounty journal and legacy pagination cursors retain their separate lifetime rules.

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

The in-memory shared change journal defaults to 10,000 events or 8 MiB, whichever is reached first. Old cursors, service restarts, large catchup and reorganizations require full resynchronization; they cannot silently skip events. A poll receives error `-32011`; a subscriber receives `resync_required:true`. If the connection is lost, resume from the last processed cursor or take a fresh snapshot if it expired.

Subscription methods return `subscription_id`, `tip` and the current change `cursor`. Repeating the same subscription on the same connection reuses it. Subscriptions belong to that connection and are deleted on disconnect. IDs cannot cancel another connection's subscriptions.

```json
{"jsonrpc":"2.0","method":"subscription","params":{"subscription_id":"<id>","kind":"address","address":"<address>","tip":{},"reorg":false,"refresh":true}}
```

Address notifications tell the client to update cached balance/history/UTXOs and confirmations; incremental clients should replay `getaddresschanges`, while legacy clients refresh their pages. They are not full transaction downloads. Tip notifications contain `kind:"tip"`, `tip`, `reorg`. Bounty notifications contain `kind:"bounties"`, `changes`, `tip`, `cursor`, or an explicit `resync_required:true`. Notifications do not consume request quotas, but subscriptions and output queues are bounded.

## Full transaction lookup and broadcast

`gettransaction` looks up the requested txid in the local active-chain/mempool index, then asks the node for that transaction with its known block hash. No `txindex` is required. Unknown/disconnected transactions are not searched by scanning the chain.

Its `transaction` field preserves ConnectCoin node RPC's decoded schema, including `hex`. **Exception to indexed units:** monetary fields inside this raw-node transaction object (`value`, `fee`, etc.) are exact decimal strings in **CONN**, because this is the node's schema. The envelope supplies indexed confirmation/location metadata. Clients must independently parse/hash transactions if they intend to verify the data.

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
