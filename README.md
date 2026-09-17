# ConnectCoin JSON-RPC

A restricted, indexed API for ConnectCoin light wallets and Pay-to-Connect clients. It runs separately from the node and speaks **newline-delimited JSON-RPC 2.0 over persistent, unencrypted TCP**. No HTTPS, TLS, wallet secrets, mining RPCs, block downloads, or arbitrary RPC proxying are exposed.

- Address balances, history, UTXOs, transaction lookup and broadcast.
- All bounties in any requested active-chain block within the latest **600 blocks**, without a result-count cutoff.
- Incremental bounty updates and address/tip subscriptions.
- Persistent SQLite indexing, exact amounts, reorganization recovery and bounded network queues.
- No npm runtime dependencies. Requires **Node.js 24 or newer** with `node:sqlite`.

This is an initial implementation, not an independent security audit. A public listener is an abuse target. Operate it with host-level resource limits and monitoring. Plaintext traffic reveals queries and can be intercepted or altered; clients still trust the server for chain state and availability. This service does **not** provide SPV proofs, sign transactions or generate TLS proofs.

## Run

```sh
git clone https://github.com/connectcoincrypto/connectcoin-json-rpc.git
cd connectcoin-json-rpc
npm ci --ignore-scripts
```

Copy `config.example.json` to `config.json`, then edit it for your machine:

```sh
cp config.example.json config.json
npm start
```

In PowerShell, use `Copy-Item config.example.json config.json`. If PowerShell prevents `npm.ps1` execution, use `npm.cmd`.

The existing ConnectCoin node must have RPC enabled (`server=1`), retain all blocks (`prune=0`), and have completed its initial sync. **No wallet, `txindex`, block filter index, or spender index is required.** The first run indexes the chain from genesis; subsequent runs resume the database. Address history is chain-wide; **only bounty metadata/discovery is restricted to 600 blocks**. Indexing a large chain takes time and disk space. Indexed queries briefly wait (at most two seconds) for healthy synchronization already in progress, then return an explicit not-ready error if necessary rather than serving a partial index. Backend failures remain explicit. Transaction broadcast does not depend on index readiness: once the index has pinned a network identity, the service checks that the local node still matches it and forwards the transaction for normal node validation.

The default backend is the local testnet4 RPC at `http://127.0.0.1:48178/`. Configure `cookieFile` with the actual cookie path, for example:

- Linux: `/home/your-user/.connectcoin/testnet4/.cookie`
- Windows: use the node's configured data directory, with JSON forward slashes or escaped backslashes.

Cookie authentication is re-read on requests, so node restarts do not require copying credentials. Alternatively, remove `cookieFile` and supply `CONNECTCOIN_RPC_USER` and `CONNECTCOIN_RPC_PASSWORD` in the environment. Never commit real credentials. Relative file paths resolve against the configuration file's directory.

Local service logs include safe synchronization diagnostics: duration, attempt count, start/end heights, readiness and a fixed phase/error category. Failed synchronizations and successful cycles taking at least one second are logged with separate 30-second throttles. Backend messages, transaction payloads and credentials are not logged. A not-ready response during brief catchup is not evidence of a node crash or a rate-limit violation.

The public listener defaults to `127.0.0.1:48190`. To deliberately make it reachable from other machines, set `host` to `0.0.0.0` (IPv4) or the appropriate interface and open **only this service's port** in the firewall. Keep the node's administrative RPC bound to loopback. The backend client rejects non-loopback URLs and redirects. Do not put this behind a proxy that conceals client IP addresses; forwarded IP fields and PROXY protocol are not accepted.

Run it as a dedicated OS user. Prefer a dedicated, RPC-whitelisted backend account rather than the node's unrestricted cookie for a public deployment. The only backend methods needed are:

```text
getblockchaininfo,getblockhash,getblock,getrawmempool,getrawtransaction,sendrawtransaction
```

The service uses internal `getblock` calls for indexing once, **not a public `getblock` endpoint or a block scan per user query**. ConnectCoin's RPC allowlist is defense in depth, not an isolation boundary. Do not host funded wallet keys in this service account.

## Commands

All parameters are named objects. Each request must have a string or safe-integer `id`. Batches and client notifications are intentionally rejected, preventing uncounted calls. See [the protocol](docs/protocol.md) for streaming, cursors, units, errors and examples.

| Method | Parameters | Result |
|---|---|---|
| `getchaintip` | `{}` | Indexed active tip: height, hash, median time, chain and genesis identity |
| `getrecentblockhashes` | `{}` | Height/hash pairs, newest first, latest 600 including the tip |
| `getblockbounties` | `block_hash` | Streams **every** bounty created in that active recent block, with current state, without funding transaction bytes |
| `getaddressbalance` | `address` | Confirmed, immature, pending and available balance fields |
| `getaddressutxos` | `address`, optional `cursor` | Paginated outputs not spent in the chain or current indexed mempool |
| `getaddresshistory` | `address`, optional `cursor` | Paginated per-address received/spent/net summaries and confirmation status |
| `gettransaction` | `txid` | Full decoded transaction and hex from the node, with indexed location |
| `sendrawtransaction` | `transaction_hex` | Broadcast an already-completed transaction; normal node validation/fee policy applies |
| `getbountychanges` | optional `cursor` | Bounded, replayable incremental changes; no cursor obtains the starting watermark |
| `subscribebounties` | `{}` | Bounty-change notifications, or explicit resync notices |
| `subscribeaddress` | `address` | Notifications to refresh that address's data/confirmations |
| `subscribetip` | `{}` | Tip-change notifications (optional alternative to polling `getchaintip`) |
| `unsubscribe` | `subscription_id` | Cancel this connection's subscription |

Only native ConnectCoin P2PK Bech32m addresses are supported. The domain in a P2C output is not an owning wallet address. Claims paying an address appear in that address's ordinary history.

## Rate limits

| Scope | Sliding 60-second quota |
|---|---|
| Every method except `getblockbounties` | **60 per method per IP** |
| `getblockbounties` for a valid eligible block | **10 per block hash per IP** |
| Malformed, unknown, stale or out-of-window bounty block requests | **60 combined per IP** |

Counters combine all connections from an IP. Reconnecting does not reset them. Hash letter case and IPv4-mapped IPv6 spellings cannot create extra quotas. An initial request for each of 600 different blocks uses one call from each block's independent quota. Unknown/malformed general requests share a separate 60/minute/IP bucket; they cannot allocate arbitrary method keys. Server notifications do not count as client calls. Rate-limit state is in memory and resets on service restart; use one listener process per public endpoint, or an external shared enforcement layer for multiple replicas. Users behind the same NAT share a quota.

The limiter's storage is bounded and fails closed at capacity; additional connection/queue/subscription limits can reject requests even below their method quota. These protect memory and concurrent work, and **never silently shorten a successful bounty list**. A client must receive `stream.end` with `complete: true` before treating a block snapshot as complete.

## Tests

```sh
npm run check
npm test
```

These run local unit and real-TCP tests without using your wallet, VPS, blockchain node or public network. CI runs them on Linux, Windows and macOS with Node 24.

An additional real-node integration test is available:

```sh
CONNECTCOIND=/path/to/connectcoind npm run test:regtest
```

PowerShell:

```powershell
$env:CONNECTCOIND = 'C:\path\to\connectcoind.exe'
npm.cmd run test:regtest
```

It launches its **own temporary, network-disabled regtest node**, exercises actual RPC serialization, wallet-created transactions, the recent-block window and reorganizations, and stops its own node afterward. It uses ConnectCoin's test-only mock proof-of-work for speed; it does not test RandomX performance or successful external TLS proof generation. No production node is modified. This optional test requires a compatible locally built ConnectCoin binary and is not downloaded or run by default in CI.

## Limits and trust

- The node remains the consensus validator. This index follows it; it is not a second consensus implementation.
- Bounties older than 600 blocks can remain consensus-valid even though this API will not list them.
- All bounties created in a requested recent block are returned, including spent ones with explicit status. Filter `status: "available"` for candidates; competing claims can still race.
- Automatic updates are polled from the local node (default two seconds), not instantaneous reservations. Reorganizations and change-log overflow require a fresh snapshot.
- Continuous mempool arrivals do not force endless acquisition retries. The indexer may publish the complete snapshot captured at the start of collection when the node's exact sequence counter proves that only additions occurred and the chain tip is unchanged. Those later arrivals are picked up on subsequent polls. Removals, replacements or chain changes still require reconciliation; partial snapshots are never published.
- `gettransaction` can help bind output fields to a txid if the client independently validates its serialization/hash. This API supplies no inclusion or unspentness proof. A metadata-only client must not mistake server-reported amounts for authenticated UTXO values.
- Broadcast is not transaction construction, signing, mining, confirmation, or a promise of relay. Never send private keys or seeds.
- Broad wallet-address indexing needs an unpruned node. The server does not repeatedly call `scantxoutset`, scan historical blocks on address requests, or expose private wallet RPCs.
- Response/frame size, mempool/index capacity, subscription queues and journal retention are bounded. Inspect configuration and [protocol limits](docs/protocol.md) before increasing them.

## Project

- [ConnectCoin](https://connectcoincrypto.com/)
- [Node source](https://github.com/connectcoincrypto/connectcoin)
- [Whitepaper](https://connectcoincrypto.com/whitepaper.pdf)
- [Community](https://discord.gg/JYWbz5PsPp)

MIT license. See [LICENSE](LICENSE).
