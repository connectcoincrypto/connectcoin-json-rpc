import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { loadConfig } from './config.mjs';
import { NodeBackend } from './backend.mjs';
import { Store } from './store.mjs';
import { Indexer } from './indexer.mjs';
import { PublicAPI, METHODS } from './api.mjs';
import { createRpcServer } from './transport.mjs';

if (process.argv.includes('--help')) {
  console.log('Usage: node src/main.mjs [--config path/to/config.json]\nRequires Node.js 24+. See config.example.json. No node is started or modified.');
  process.exit(0);
}
const args = process.argv.slice(2);
if (args.length && !(args.length === 2 && args[0] === '--config')) {
  console.error('Usage: node src/main.mjs [--config path/to/config.json]');
  process.exit(1);
}

let backend, store, indexer, api, server;
let shuttingDown = false;
async function shutdown(code = 0) {
  if (shuttingDown) return;
  shuttingDown = true;
  if (server) { server.closeAllConnections(); server.close(); }
  api?.close();
  await indexer?.stop();
  backend?.close();
  store?.close();
  process.exitCode = code;
}

try {
  const config = loadConfig(args[1]);
  backend = new NodeBackend(config.backend);
  mkdirSync(dirname(config.database), { recursive: true, mode: 0o700 });
  store = new Store(config.database);
  indexer = new Indexer({ backend, store, pollIntervalMs: config.pollIntervalMs });
  api = new PublicAPI({ backend, store, indexer, options: config.api });
  server = createRpcServer({ dispatch: api.dispatch, classifyBountyHash: api.classifyBountyHash, allowedMethods: METHODS, options: config.transport });
  // Each admitted streaming request may wait on a coherent index publication.
  // Listener capacity follows the already-validated concurrent-work bound.
  indexer.setMaxListeners(Math.max(indexer.getMaxListeners(), (config.transport.maxConcurrentRequests ?? 32) + 4));
  server.on('error', () => { console.error('TCP listener failed. Check bind address and port.'); void shutdown(1); });
  let wasReady = false;
  indexer.on('update', ({ tip }) => {
    if (!wasReady) console.log(`Index ready at ${tip.height} (${tip.chain}).`);
    wasReady = true;
  });
  // Do not log arbitrary node responses or RPC credentials.
  let lastError = 0;
  indexer.on('syncError', () => {
    wasReady = false;
    if (Date.now() - lastError > 30000) {
      console.error('Index sync failed; queries are unavailable until recovery. Check local node, authentication, unpruned history, and network identity.');
      lastError = Date.now();
    }
  });
  server.listen(config.port, config.host, () => console.log(`Plaintext TCP JSON-RPC listening on ${config.host}:${config.port}; indexing locally.`));
  indexer.start();
  process.on('SIGINT', () => { void shutdown(); });
  process.on('SIGTERM', () => { void shutdown(); });
} catch (error) {
  // Config errors are local; never print file contents or a full credential-bearing URL.
  console.error(`Startup failed (${error.name ?? 'Error'}). Check config paths, supported options and Node.js version.`);
  await shutdown(1);
}
