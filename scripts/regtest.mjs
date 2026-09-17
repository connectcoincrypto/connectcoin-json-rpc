// Integration smoke test against an isolated ConnectCoin regtest node.
// No live node, real wallet, production network, or external TLS server is used.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createWriteStream } from 'node:fs';
import { access, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import net from 'node:net';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { NodeBackend, parseNodeJson } from '../src/backend.mjs';
import { PublicAPI, METHODS } from '../src/api.mjs';
import { Store } from '../src/store.mjs';
import { Indexer } from '../src/indexer.mjs';
import { createRpcServer } from '../src/transport.mjs';

const project = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const defaultBinary = path.resolve(project, '..', 'connectcoin', 'build', 'bin',
  ...(process.platform === 'win32' ? ['Release', 'connectcoind.exe'] : ['connectcoind']));
const binary = path.resolve(process.env.CONNECTCOIND ?? defaultBinary);
try { await access(binary); }
catch { throw new Error(`ConnectCoin daemon not found: ${binary}. Set CONNECTCOIND to a wallet-enabled connectcoind binary. This test is not skipped.`); }

async function freePort() {
  const listener = net.createServer();
  listener.listen(0, '127.0.0.1');
  await once(listener, 'listening');
  const port = listener.address().port;
  await new Promise((resolve, reject) => listener.close(error => error ? reject(error) : resolve()));
  return port;
}

class Client {
  constructor(socket) {
    this.socket = socket;
    this.nextId = 1;
    this.buffer = '';
    this.messages = [];
    this.waiters = [];
    this.failure = null;
    socket.setEncoding('utf8');
    socket.on('data', data => {
      this.buffer += data;
      for (;;) {
        const newline = this.buffer.indexOf('\n');
        if (newline < 0) break;
        const message = JSON.parse(this.buffer.slice(0, newline));
        this.buffer = this.buffer.slice(newline + 1);
        const index = this.waiters.findIndex(waiter => waiter.predicate(message));
        if (index < 0) this.messages.push(message);
        else this.waiters.splice(index, 1)[0].resolve(message);
      }
    });
    socket.on('error', error => this.fail(error));
    socket.on('close', () => this.fail(new Error('Public API connection closed')));
  }
  fail(error) {
    this.failure = error;
    for (const waiter of this.waiters.splice(0)) waiter.reject(error);
  }
  wait(predicate, timeoutMs = 15000) {
    const index = this.messages.findIndex(predicate);
    if (index >= 0) return Promise.resolve(this.messages.splice(index, 1)[0]);
    if (this.failure) return Promise.reject(this.failure);
    return new Promise((resolve, reject) => {
      const waiter = { predicate, resolve: value => { clearTimeout(timer); resolve(value); },
        reject: error => { clearTimeout(timer); reject(error); } };
      const timer = setTimeout(() => {
        const at = this.waiters.indexOf(waiter);
        if (at >= 0) this.waiters.splice(at, 1);
        reject(new Error('Timed out waiting for public API response'));
      }, timeoutMs);
      this.waiters.push(waiter);
    });
  }
  async request(method, params = {}) {
    const id = this.nextId++;
    this.socket.write(`${JSON.stringify({ jsonrpc: '2.0', id, method, params })}\n`);
    const message = await this.wait(value => value.id === id);
    if (message.error) throw Object.assign(new Error(message.error.message), message.error);
    return message.result;
  }
  async bounties(block_hash) {
    const result = await this.request('getblockbounties', { block_hash });
    assert.equal(typeof result.stream_id, 'string');
    const chunks = [];
    for (;;) {
      const message = await this.wait(value => value.params?.stream_id === result.stream_id);
      if (message.method === 'stream.end') {
        assert.equal(message.params.complete, true, JSON.stringify(message.params.error));
        assert.equal(message.params.chunks, chunks.length);
        assert.equal(chunks[0].type, 'snapshot');
        assert.equal(chunks.at(-1).type, 'state');
        return chunks.filter(chunk => chunk.type === 'bounties').flatMap(chunk => chunk.items);
      }
      assert.equal(message.method, 'stream.chunk');
      assert.equal(message.params.sequence, chunks.length);
      chunks.push(message.params.items);
    }
  }
  close() { this.socket.destroy(); }
}

const temp = await mkdtemp(path.join(tmpdir(), 'connectcoin-json-rpc-regtest-'));
const cookieFile = path.join(temp, 'regtest', '.cookie');
const rpcPort = await freePort();
const url = `http://127.0.0.1:${rpcPort}/`;
const output = createWriteStream(path.join(temp, 'daemon-stdout.log'));
let daemon;
let daemonExit;
let daemonClosed = false;
let backend;
let store;
let api;
let server;
let client;
let cli;
let cliExit;
let cliClosed = true;
let cliClient;
let success = false;
let rpcId = 1;
const started = Date.now();

// This unrestricted RPC helper is only for the newly spawned, isolated test
// node. The production NodeBackend still has its strict method allowlist.
async function rpc(method, params = [], wallet = '') {
  const credentials = (await readFile(cookieFile, 'utf8')).trim();
  const response = await fetch(`${url}${wallet ? `wallet/${encodeURIComponent(wallet)}` : ''}`, {
    method: 'POST', headers: { 'content-type': 'application/json',
      authorization: `Basic ${Buffer.from(credentials).toString('base64')}` },
    body: JSON.stringify({ jsonrpc: '2.0', id: rpcId++, method, params }),
    signal: AbortSignal.timeout(120000),
  });
  const body = parseNodeJson(await response.text());
  if (body.error) throw Object.assign(new Error(`${method}: ${body.error.message}`), body.error);
  if (!response.ok) throw new Error(`${method}: HTTP ${response.status}`);
  return body.result;
}

function progress(message) { console.log(`[regtest +${((Date.now() - started) / 1000).toFixed(1)}s] ${message}`); }

async function startDaemon() {
  daemonClosed = false;
  daemon = spawn(binary, [`-datadir=${temp}`, '-regtest', '-server=1', '-listen=0', '-networkactive=0',
    '-dnsseed=0', '-fixedseeds=0', '-discover=0', '-listenonion=0', '-natpmp=0',
    '-test=randomx_mock_pow', '-fallbackfee=0.001', '-printtoconsole=1',
    '-rpcbind=127.0.0.1', '-rpcallowip=127.0.0.1', `-rpcport=${rpcPort}`],
  { windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
  daemon.stdout.pipe(output, { end: false });
  daemon.stderr.pipe(output, { end: false });
  let spawnError;
  daemon.on('error', error => { spawnError = error; });
  daemonExit = new Promise(resolve => daemon.once('close', code => { daemonClosed = true; resolve(code); }));
  let ready = false;
  for (let attempt = 0; attempt < 240; attempt++) {
    if (spawnError) throw spawnError;
    if (daemon.exitCode !== null) throw new Error(`Daemon exited during startup (${daemon.exitCode}); inspect ${temp}`);
    try { await rpc('getblockchaininfo'); ready = true; break; } catch { await delay(250); }
  }
  if (!ready) throw new Error(`Daemon did not become ready; inspect ${temp}`);
  assert.equal((await rpc('getblockchaininfo')).chain, 'regtest');
  assert.equal((await rpc('getnetworkinfo')).networkactive, false);
}

async function connectClient(port) {
  const socket = net.createConnection({ host: '127.0.0.1', port });
  await once(socket, 'connect');
  return new Client(socket);
}

async function pollUntil(check, description, timeoutMs = 15000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await delay(250);
  }
  throw new Error(`Timed out: ${description}`);
}

try {
  progress(`Starting isolated node; data/logs: ${temp}`);
  await startDaemon();
  await rpc('createwallet', ['api-smoke']);
  await rpc('createwallet', ['api-recipient']);
  const miner = await rpc('getnewaddress', [], 'api-smoke');
  const receiver = await rpc('getnewaddress', [], 'api-recipient');
  const receiver2 = await rpc('getnewaddress', [], 'api-recipient');
  await rpc('generatetoaddress', [101, miner]);
  backend = new NodeBackend({ url, cookieFile });
  const database = path.join(temp, 'index.sqlite');
  store = new Store(database);
  const indexer = new Indexer({ backend, store });
  await indexer.syncOnce();
  assert.equal(indexer.ready, true);
  assert.equal(store.tip().height, 101);
  api = new PublicAPI({ store, indexer, backend });
  server = createRpcServer({ dispatch: api.dispatch, allowedMethods: METHODS, classifyBountyHash: api.classifyBountyHash });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const socket = net.createConnection({ host: '127.0.0.1', port: server.address().port });
  await once(socket, 'connect');
  client = new Client(socket);
  assert.equal((await client.request('getchaintip')).height, 101);
  const tipSub = await client.request('subscribetip');
  const addressSub = await client.request('subscribeaddress', { address: receiver });
  const bountySub = await client.request('subscribebounties');
  const changesStart = (await client.request('getbountychanges')).next_cursor;
  progress('Node, initial index and native TCP API ready. Creating typed P2PK/P2C transactions.');

  const amount = '1.2345678901';
  const amountConnects = '12345678901';
  const paymentTxid = await rpc('sendtoaddress', [receiver, amount], 'api-smoke');
  const bountyTxid = (await rpc('sendtop2c', ['example.com', 1, { work_bits: 0 }], 'api-smoke')).txids[0];
  const [fundingBlock] = await rpc('generatetoaddress', [1, miner]);
  await indexer.syncOnce();
  const tip = await client.request('getchaintip');
  assert.equal(tip.hash, fundingBlock);
  assert.equal(tip.height, 102);
  for (const sub of [tipSub, addressSub, bountySub]) {
    const notification = await client.wait(message => message.method === 'subscription' && message.params.subscription_id === sub.subscription_id);
    assert.equal(notification.params.tip.hash, fundingBlock);
  }
  assert.equal((await client.request('getaddressbalance', { address: receiver })).confirmed, amountConnects);
  const history = await client.request('getaddresshistory', { address: receiver });
  assert.equal(history.items.length, 1);
  assert.equal(history.items[0].txid, paymentTxid);
  assert.equal(history.items[0].received, amountConnects);
  assert.equal(history.items[0].status, 'confirmed');
  const utxos = (await client.request('getaddressutxos', { address: receiver })).items;
  assert.equal(utxos.length, 1);
  assert.equal(utxos[0].amount, amountConnects);
  const full = await client.request('gettransaction', { txid: paymentTxid });
  assert.equal(full.transaction.txid, paymentTxid);
  assert.equal(full.transaction.vout[utxos[0].vout].value, amount);
  assert.equal(full.transaction.vout[utxos[0].vout].type, 1);
  const bounties = await client.bounties(fundingBlock);
  assert.equal(bounties.length, 1);
  assert.equal(bounties[0].txid, bountyTxid);
  assert.equal(bounties[0].amount, '10000000000');
  assert.equal(bounties[0].domain, 'example.com');
  assert.equal(bounties[0].status, 'available');
  const originalBounty = await backend.transaction(bountyTxid, fundingBlock);
  const outputBounty = originalBounty.vout[bounties[0].vout];
  assert.equal(outputBounty.type, 2);
  for (const field of ['connection_work_target', 'root_certificates_version', 'signature_algorithms_mask']) {
    assert.equal(bounties[0][field], outputBounty[field]);
  }
  const changes = await client.request('getbountychanges', { cursor: changesStart });
  assert.ok(changes.changes.length > 0);
  const emptyBountyBlock = (await client.request('getrecentblockhashes')).blocks.find(block => block.height === 101).hash;
  assert.deepEqual(await client.bounties(emptyBountyBlock), []);

  progress('Exact 10-decimal amounts and bounty metadata match real typed RPC outputs. Testing signed broadcast and mempool.');
  const unsigned = await rpc('createrawtransaction', [[{ txid: paymentTxid, vout: utxos[0].vout }], [{ [receiver2]: '1.2300000001' }]]);
  const signed = await rpc('signrawtransactionwithwallet', [unsigned], 'api-recipient');
  assert.equal(signed.complete, true);
  const { txid: spendingTxid } = await client.request('sendrawtransaction', { transaction_hex: signed.hex });
  assert.ok((await rpc('getrawmempool')).includes(spendingTxid));
  await indexer.syncOnce();
  const pending = await client.request('getaddressbalance', { address: receiver });
  assert.equal(pending.confirmed, amountConnects);
  assert.equal(pending.pending_spent, amountConnects);
  assert.equal(pending.available_confirmed, '0');
  assert.equal((await client.request('getaddressutxos', { address: receiver })).items.length, 0);
  assert.equal((await client.request('getaddresshistory', { address: receiver })).items.find(item => item.txid === spendingTxid).status, 'pending');
  assert.equal((await client.request('gettransaction', { txid: spendingTxid })).status, 'pending');
  assert.equal((await client.request('getaddressbalance', { address: receiver2 })).pending_received, '12300000001');
  const [spendingBlock] = await rpc('generatetoaddress', [1, miner]);
  await indexer.syncOnce();
  assert.equal((await client.request('getaddressbalance', { address: receiver })).confirmed, '0');
  assert.equal((await client.request('getaddressbalance', { address: receiver2 })).confirmed, '12300000001');
  assert.equal((await client.request('getaddresshistory', { address: receiver })).items.find(item => item.txid === spendingTxid).status, 'confirmed');

  progress('Broadcast/mempool/confirmation passed. Testing invalidate/reconsider recovery.');
  await rpc('invalidateblock', [spendingBlock]);
  await indexer.syncOnce();
  assert.equal((await client.request('getchaintip')).hash, fundingBlock);
  assert.equal((await client.request('getaddresshistory', { address: receiver })).items.find(item => item.txid === spendingTxid).status, 'pending');
  await rpc('reconsiderblock', [spendingBlock]);
  await indexer.syncOnce();
  assert.equal((await client.request('getchaintip')).hash, spendingBlock);
  assert.equal((await client.request('getaddressbalance', { address: receiver2 })).confirmed, '12300000001');

  progress('Reorg recovery passed. Mining to exercise the exact last-600-block boundary.');
  await rpc('generatetoaddress', [598, miner]);
  await indexer.syncOnce();
  const boundary = await client.request('getrecentblockhashes');
  assert.equal(boundary.blocks.length, 600);
  assert.equal(boundary.blocks.at(-1).hash, fundingBlock);
  assert.equal((await client.bounties(fundingBlock)).length, 1);
  const [boundaryBlock] = await rpc('generatetoaddress', [1, miner]);
  await indexer.syncOnce();
  await assert.rejects(client.bounties(fundingBlock), error => error.code === -32004);
  assert.equal((await client.request('getrecentblockhashes')).blocks.at(-1).hash, spendingBlock);
  // Ordinary address history is not truncated when the bounty window moves.
  assert.equal((await client.request('getaddresshistory', { address: receiver })).items.length, 2);
  assert.equal((await client.request('gettransaction', { txid: paymentTxid })).transaction.txid, paymentTxid);
  // Reorg back across the retention boundary must restore the dropped bounty.
  await rpc('invalidateblock', [boundaryBlock]);
  await indexer.syncOnce();
  assert.equal((await client.bounties(fundingBlock)).length, 1);
  await rpc('reconsiderblock', [boundaryBlock]);
  await indexer.syncOnce();
  await assert.rejects(client.bounties(fundingBlock), error => error.code === -32004);
  assert.equal((await client.request('unsubscribe', { subscription_id: bountySub.subscription_id })).removed, true);
  await assert.rejects(client.request('getblocktemplate'), error => error.code === -32601);
  await assert.rejects(client.request('stop'), error => error.code === -32601);

  progress('600-block expiry/restoration and private-RPC isolation passed. Reopening persisted index.');
  const lastTip = store.tip();
  client.close(); client = null;
  server.closeAllConnections();
  await new Promise(resolve => server.close(resolve)); server = null;
  api.close(); api = null;
  store.close(); store = new Store(database);
  assert.deepEqual(store.tip(), lastTip);
  assert.equal(store.balance(receiver2).confirmed, '12300000001');
  const restarted = new Indexer({ backend, store });
  await restarted.syncOnce();
  assert.equal(restarted.ready, true);
  assert.deepEqual(store.tip(), lastTip);
  await restarted.stop();
  store.close(); store = null;
  backend.close(); backend = null;

  progress('Starting actual CLI entrypoint against persisted index; testing backend outage and recovery.');
  const cliPort = await freePort();
  const configPath = path.join(temp, 'cli-config.json');
  await writeFile(configPath, JSON.stringify({ host: '127.0.0.1', port: cliPort,
    database, backend: { url, cookieFile }, pollIntervalMs: 250 }));
  const cliArgs = [];
  if (process.platform === 'win32') {
    // Windows child.kill('SIGTERM') forcibly terminates a process instead of
    // delivering the Node.js signal event. Exercise the actual installed
    // shutdown handler via a test-only IPC shim, without altering main.mjs.
    const shim = path.join(temp, 'windows-test-signal.mjs');
    await writeFile(shim, "process.once('message', value => { if (value === 'regtest-sigterm') { process.disconnect(); process.emit('SIGTERM'); } });\n");
    cliArgs.push('--import', pathToFileURL(shim).href);
  }
  cliArgs.push(path.join(project, 'src', 'main.mjs'), '--config', configPath);
  let cliOutput = '';
  let cliSpawnError;
  cliClosed = false;
  cli = spawn(process.execPath, cliArgs, { windowsHide: true,
    stdio: ['ignore', 'pipe', 'pipe', ...(process.platform === 'win32' ? ['ipc'] : [])] });
  const cliLog = createWriteStream(path.join(temp, 'cli.log'));
  cli.stdout.pipe(cliLog, { end: false }); cli.stderr.pipe(cliLog, { end: false });
  const collect = chunk => { cliOutput += chunk.toString(); };
  cli.stdout.on('data', collect); cli.stderr.on('data', collect);
  cli.on('error', error => { cliSpawnError = error; });
  cliExit = new Promise(resolve => cli.once('close', code => { cliClosed = true; cliLog.end(); resolve(code); }));
  await pollUntil(async () => {
    if (cliSpawnError) throw cliSpawnError;
    if (cliClosed) throw new Error(`CLI exited unexpectedly: ${cliOutput}`);
    try { cliClient = await connectClient(cliPort); return true; } catch { return false; }
  }, 'CLI listener startup');
  await pollUntil(async () => {
    try { return (await cliClient.request('getchaintip')).hash === lastTip.hash; }
    catch (error) { if (error.code !== -32001) throw error; return false; }
  }, 'CLI index ready');
  assert.match(cliOutput, /Index ready/);
  const cookieBeforeStop = (await readFile(cookieFile, 'utf8')).trim();
  await rpc('stop');
  await daemonExit;
  await pollUntil(async () => {
    try { await cliClient.request('getchaintip'); return false; }
    catch (error) { if (error.code !== -32001) throw error; return true; }
  }, 'CLI marks backend unavailable');
  assert.equal(cliClosed, false);
  const errorPrefix = 'Index sync failed; indexed queries are unavailable until recovery. ';
  await pollUntil(() => cliOutput.includes(errorPrefix), 'safe syncError log');
  const failureStats = JSON.parse(cliOutput.split('\n').find(line => line.startsWith(errorPrefix)).slice(errorPrefix.length));
  assert.equal(failureStats.errorKind, 'backend');
  assert.equal(failureStats.ready, false);
  assert.ok(Number.isSafeInteger(failureStats.durationMs));
  assert.deepEqual(Object.keys(failureStats).sort(), ['attempts', 'durationMs', 'endHeight', 'errorKind', 'phase', 'ready', 'startHeight']);
  assert.ok(!cliOutput.includes(cookieBeforeStop), 'CLI must not log backend credentials');
  assert.doesNotMatch(cliOutput, /Unhandled|uncaughtException|ERR_UNHANDLED_ERROR/);
  await startDaemon();
  await pollUntil(async () => {
    try { return (await cliClient.request('getchaintip')).hash === lastTip.hash; }
    catch (error) { if (error.code !== -32001) throw error; return false; }
  }, 'CLI backend recovery');
  cliClient.close(); cliClient = null;
  if (process.platform === 'win32') cli.send('regtest-sigterm');
  else cli.kill('SIGTERM');
  const cliCode = await Promise.race([cliExit, delay(15000, null, { ref: false }).then(() => { throw new Error('CLI graceful shutdown timed out'); })]);
  assert.equal(cliCode, 0, cliOutput);
  const verified = new Store(database);
  assert.deepEqual(verified.tip(), lastTip);
  verified.close();
  progress('CLI startup, safe unavailable state, automatic backend recovery, and graceful shutdown handler passed.');
  success = true;
  progress('PASS: real-node integration smoke test completed (mock PoW; no external TLS proof generation).');
} finally {
  client?.close();
  cliClient?.close();
  if (cli && !cliClosed) {
    cli.kill();
    await Promise.race([cliExit, delay(5000, null, { ref: false })]);
  }
  if (server) { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
  api?.close();
  store?.close();
  backend?.close();
  if (daemon && daemon.exitCode === null && !daemon.killed) {
    try { await rpc('stop'); } catch { /* Starting node may not have created the cookie yet. */ }
    const graceful = await Promise.race([daemonExit.then(() => true), delay(15000, null, { ref: false }).then(() => false)]);
    if (!graceful) {
      // Only the child spawned by this script is terminated; never search for
      // process names or interact with a pre-existing daemon/wallet.
      daemon.kill();
      await Promise.race([daemonExit, delay(5000, null, { ref: false })]);
    }
  }
  await new Promise(resolve => output.end(resolve));
  if (success && daemonClosed && cliClosed && process.env.KEEP_REGTEST !== '1') {
    // Validate the exact owned mkdtemp target before any recursive removal.
    const resolved = path.resolve(temp);
    assert.equal(path.dirname(resolved), path.resolve(tmpdir()));
    assert.ok(path.basename(resolved).startsWith('connectcoin-json-rpc-regtest-'));
    await rm(resolved, { recursive: true });
    progress('Removed only this successful run\'s temporary regtest directory.');
  } else progress(`Preserved test data and logs: ${temp}`);
}
