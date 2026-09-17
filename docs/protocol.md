# TCP protocol, version 0.1

This document specifies this repository's API, not ConnectCoin's private node RPC. The transport is raw plaintext TCP: one UTF-8 JSON object followed by a newline. A browser cannot directly use a native TCP socket. Native clients can keep one connection open and correlate responses by `id`.

Only JSON-RPC `"2.0"` requests with an explicit string/safe-integer `id`, a listed method and named object `params` are accepted. `params` may be omitted for `{}`. Extra parameters are errors. Client notifications (missing/null id), batches and arbitrary RPC forwarding are not supported. Subscriptions and streaming use server-originated JSON-RPC notifications.

```json
{"jsonrpc":"2.0","id":1,"method":"getrecentblockhashes","params":{}}
```

```json
{"jsonrpc":"2.0","id":1,"result":{"tip":{"height":700,"hash":"<64 hex>","mediantime":1700000000,"chain":"testnet4","genesis_hash":"<64 hex>"},"window":600,"blocks":[{"height":700,"hash":"<64 hex>"}]}}
```

The example omits other block entries for readability; the actual response contains every available active-chain block from `max(0,H-599)` through `H`, newest first. A block hash is a displayed hexadecimal hash, normalized to lowercase. The server never accepts arbitrary heights or old-chain hashes in `getblockbounties`.

## Amounts and address queries

Every indexed amount is a **base-10 integer string in connects**. One CC is **10,000,000,000 connects**, not Bitcoin's 100,000,000 satoshis. Use arbitrary-precision integers in clients. Strings prevent JavaScript/JSON's 53-bit precision limit from silently changing values.

`getaddressbalance` returns:

- `confirmed`: all unspent confirmed outputs, including immature coinbase outputs and outputs currently spent by a pending transaction.
- `immature`: the immature part of `confirmed`.
- `pending_spent`: confirmed outputs consumed by transactions in the indexed mempool.
- `pending_received`: currently unspent mempool outputs received by the address (including pending change).
- `available_confirmed`: `confirmed - immature - pending_spent`.
- `pending_delta`: `pending_received - pending_spent`.
- `total`: `confirmed + pending_delta`, including immature amounts.

History entries contain `txid`, `status` (`confirmed`/`pending`), `block_height`, `block_hash`, `confirmations`, `received`, `spent` and `balance_delta`. Pending block fields are `null`. `spent` means address-owned input value, not necessarily value paid to another person; change is included in `received`. Confirmations are evaluated at the response's `tip`.

UTXO entries include `txid`, `vout`, `amount`, `block_height`, `status`, `confirmations`, `coinbase`, `mature`. Outputs consumed by indexed mempool transactions are excluded. Unconfirmed outputs can themselves be spent by descendants; only terminal unspent outputs are returned.

History and UTXO responses have `items`, `tip`, `unit`, `address`, `live:true` and `next_cursor`. Return `next_cursor` unchanged to request the next page. `null` means no further records at that moment. Each page request uses the ordinary method quota. The default page size is 100, configurable up to 500. Cursors are opaque authenticated keyset markers bound to the query, address and a canonical chain anchor. History is ordered by txid ascending; UTXOs by txid then vout ascending, **not by timestamp**. The UI can reorder downloaded history by block height.

Pagination is a **live view**, not a frozen point-in-time database snapshot. New blocks and unrelated mempool activity do not invalidate it. Deleting a preceding row does not skip the next result, as offset pagination would. New records inserted behind the cursor, and updates/removals of already-read records, require a refresh; use address subscriptions, deduplicate by txid/outpoint and refresh on notifications. Do not assume concatenated pages represent an atomic spendable balance. A reorganization removing the cursor's anchor or a service restart invalidates the cursor explicitly (`-32011`); restart the query. This avoids indefinite restarts of large histories under the 60/minute quota and avoids holding database snapshots open for slow clients.

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

Address notifications tell the client to refresh cached balance/history/UTXOs and confirmations; they are not full transaction downloads. Tip notifications contain `kind:"tip"`, `tip`, `reorg`. Bounty notifications contain `kind:"bounties"`, `changes`, `tip`, `cursor`, or an explicit `resync_required:true`. Notifications do not consume request quotas, but subscriptions and output queues are bounded.

## Full transaction lookup and broadcast

`gettransaction` looks up the requested txid in the local active-chain/mempool index, then asks the node for that transaction with its known block hash. No `txindex` is required. Unknown/disconnected transactions are not searched by scanning the chain.

Its `transaction` field preserves ConnectCoin node RPC's decoded schema, including `hex`. **Exception to indexed units:** monetary fields inside this raw-node transaction object (`value`, `fee`, etc.) are exact decimal strings in **CC**, because this is the node's schema. The envelope supplies indexed confirmation/location metadata. Clients must independently parse/hash transactions if they intend to verify the data.

`sendrawtransaction` accepts only `transaction_hex`; it cannot forward RPC options that disable fee safeguards. Default submitted transaction cap is 400,000 bytes. Requests remain subject to the node's standardness, fee and consensus rules. A timeout can have an unknown broadcast outcome; check the locally known txid before retrying. The server does not sign, alter recipient outputs, generate proofs or reserve bounties.

Broadcast is independent of temporary index readiness. An initialized, pinned network identity is required; before forwarding a transaction, the server checks the local node's chain and genesis against that identity. It does not require the address/mempool index to be caught up. Input validation, per-IP quotas, concurrency limits and node fee/consensus checks still apply. A preflight failure explicitly reports that the transaction was not submitted; an error after forwarding may have an unknown outcome and is never automatically rebroadcast by this service.

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
| `-32029` | Rate limit, with `data.retry_after_ms` and `reason` |
| `-32030` | Server concurrency/queue capacity reached |

Some framing, oversized-output, slow-client and connection-cap failures close the TCP connection rather than send an error that would exceed the same bounds. Client code must handle EOF at any point.

Default limits: 128 connections total, 8 per real peer IP, 32 active requests total, 16 queued requests per connection, 1 MiB request frame, 2 MiB individual response frame, 2 MiB queued input, 4 MiB queued output, 100 subscriptions per IP and 10,000 globally. Idle timeout is 120 seconds; incomplete-frame timeout is 15 seconds; stalled request/stream timeout is 60 seconds. See `config.example.json` and exported transport defaults. No header or parameter can override the actual peer IP. This implementation has no distributed/shared rate-limit database.

The indexer caps a mempool snapshot at 100,000 transactions and verifies the active chain tip and exact 64-bit mempool sequence around acquisition. Identical snapshots are accepted. Pure growth is also accepted when the second snapshot contains every original txid and the sequence increase equals the number of additions: ConnectCoin increments this counter once per addition or removal, so this establishes that no removals occurred. In that case the complete **first** snapshot is published; later arrivals appear on subsequent polls. Replacements, removals, counter resets and chain changes require reconciliation. A busy or unavailable backend can still temporarily return not-ready rather than pretend an incomplete state is authoritative. Indexed requests wait at most two seconds for an already-running healthy synchronization; stream waits retain their separate 30-second limit. The initial full-chain address index and long-term disk usage are not a 600-block rolling database.

Verbose node responses can be much larger than serialized blocks. A legitimate unusually large block can exceed `backend.maxResponseBytes` (default 256 MiB) and make synchronization unavailable. Review host memory and workload before changing this cap; defaults are not a universal capacity guarantee. Backend HTTP concurrency is bounded to four sockets, with at most three mempool-transaction fetches in flight. Immutable normalized mempool transactions are reused across acquisition retries, but only a coherent full snapshot is published.
