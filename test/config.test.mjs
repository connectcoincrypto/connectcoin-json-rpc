import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, resolve, basename } from 'node:path';
import { loadConfig } from '../src/config.mjs';

test('configuration resolves files and environment credentials without altering globals', t => {
  const dir = mkdtempSync(join(tmpdir(), 'connectcoin-rpc-config-'));
  t.after(() => {
    assert.equal(dirname(resolve(dir)), resolve(tmpdir()));
    assert.ok(basename(dir).startsWith('connectcoin-rpc-config-'));
    rmSync(dir, { recursive: true });
  });
  const file = join(dir, 'config.json');
  writeFileSync(file, JSON.stringify({ backend: { cookieFile: 'cookie' } }));
  const config = loadConfig(file, { CONNECTCOIN_RPC_USER: 'test-user', CONNECTCOIN_RPC_PASSWORD: 'test-only' });
  assert.equal(config.host, '127.0.0.1');
  assert.equal(config.port, 48190);
  assert.equal(config.backend.cookieFile, join(dir, 'cookie'));
  assert.equal(config.database, join(dir, 'data', 'index.sqlite'));
  assert.equal(config.backend.username, 'test-user');
  assert.equal(config.backend.password, 'test-only');
  for (const value of [
    { backend: {}, transport: true }, { backend: {}, transport: [] },
    { backend: {}, api: true }, { backend: {}, api: { pageSize: 0 } },
    { backend: {}, api: { maxSubscriptions: '100' } },
    { backend: {}, port: 65536 }, { backend: {}, pollIntervalMs: -1 },
    { backend: { arbitrary: true } }, { backend: {}, mystery: true },
    { backend: null }, [], null,
  ]) {
    writeFileSync(file, JSON.stringify(value));
    assert.throws(() => loadConfig(file, {}));
  }
});
