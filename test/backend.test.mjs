import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, basename, resolve } from 'node:path';
import { NodeBackend, parseNodeJson, toConnects } from '../src/backend.mjs';

test('ConnectCoin decimal RPC amounts never round through binary floating point', () => {
  const values = parseNodeJson('{"value":900719.9254740993,"fee":0.0000000001,"height":600,"mempool_sequence":9007199254740993}');
  assert.equal(values.value, '900719.9254740993');
  assert.equal(values.fee, '0.0000000001');
  assert.equal(values.mempool_sequence, '9007199254740993');
  assert.equal(values.height, 600);
  assert.equal(toConnects(values.value), '9007199254740993');
  assert.equal(toConnects('100000000.0000000000'), '1000000000000000000');
  assert.equal(toConnects('1e-10'), '1');
  assert.equal(toConnects('1.234e2'), '1234000000000');
  assert.throws(() => toConnects(0.1), /lossless/);
  assert.throws(() => toConnects('0.00000000001'), /sub-connect/);
  assert.throws(() => toConnects('-1'), /MoneyRange/);
  assert.throws(() => toConnects('100000001'), /MoneyRange/);
});

test('backend is loopback-only, validates method allowlist, and uses exact JSON', async t => {
  const seen = [];
  const server = http.createServer((request, response) => {
    let body = '';
    request.on('data', chunk => { body += chunk; });
    request.on('end', () => {
      const rpc = JSON.parse(body); seen.push(rpc);
      assert.equal(request.headers.authorization, `Basic ${Buffer.from('user:secret').toString('base64')}`);
      response.setHeader('Content-Type', 'application/json');
      response.end(`{"jsonrpc":"2.0","id":${rpc.id},"result":{"value":900719.9254740993}}`);
    });
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const backend = new NodeBackend({ url: `http://127.0.0.1:${server.address().port}/`, username: 'user', password: 'secret' });
  t.after(() => { backend.close(); server.close(); });
  assert.equal((await backend.call('getblock', ['a'.repeat(64), 2])).value, '900719.9254740993');
  await assert.rejects(backend.call('stop'), /allowlisted/);
  assert.equal(seen.length, 1);
  for (const url of ['http://example.com/', 'http://localhost/', 'https://127.0.0.1/', 'http://user:pass@127.0.0.1/', 'http://127.0.0.1/wallet/a']) {
    assert.throws(() => new NodeBackend({ url, username: 'x', password: 'x' }), /loopback/);
  }
});

test('cookie credentials are reread, response size/time are bounded, errors do not leak diagnostics', async t => {
  const directory = mkdtempSync(join(tmpdir(), 'connectcoin-backend-test-'));
  const cookie = join(directory, '.cookie');
  writeFileSync(cookie, 'first:secret');
  let mode = 'ok'; const authentication = [];
  const server = http.createServer((request, response) => {
    authentication.push(request.headers.authorization);
    let body = '';
    request.on('data', chunk => { body += chunk; });
    request.on('end', () => {
      const { id } = JSON.parse(body);
      if (mode === 'timeout') return;
      if (mode === 'large') { response.end('x'.repeat(2048)); return; }
      if (mode === 'error') { response.end(JSON.stringify({ id, error: { code: -26, message: 'SECRET PATH PRIVATE' } })); return; }
      response.end(JSON.stringify({ id, result: true }));
    });
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const backend = new NodeBackend({ url: `http://127.0.0.1:${server.address().port}/`, cookieFile: cookie, timeoutMs: 5000, maxResponseBytes: 1024 });
  t.after(() => {
    backend.close(); server.closeAllConnections(); server.close();
    assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
    assert.ok(basename(directory).startsWith('connectcoin-backend-test-'));
    rmSync(directory, { recursive: true, force: true });
  });
  assert.equal(await backend.call('getblockchaininfo'), true);
  writeFileSync(cookie, 'second:secret');
  await backend.call('getblockchaininfo');
  assert.notEqual(authentication[0], authentication[1]);
  mode = 'large'; await assert.rejects(backend.call('getblockchaininfo'), /size limit/);
  mode = 'error'; await assert.rejects(backend.call('sendrawtransaction', ['00']), error => error.code === -26 && !error.message.includes('SECRET'));
  mode = 'timeout'; backend.timeoutMs = 100;
  await assert.rejects(backend.call('getblockchaininfo'), /timed out/);
});
