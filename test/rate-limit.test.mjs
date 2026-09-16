import test from 'node:test';
import assert from 'node:assert/strict';
import { RateLimiter, canonicalBlockHash, normalizeIp } from '../src/rate-limit.mjs';

test('canonicalizes IPv4, equivalent IPv6 and IPv4-mapped IPv6 addresses', () => {
  assert.equal(normalizeIp('192.0.2.1'), '192.0.2.1');
  assert.equal(normalizeIp('::ffff:192.0.2.1'), '192.0.2.1');
  assert.equal(normalizeIp('0:0:0:0:0:FFFF:C000:0201'), '192.0.2.1');
  assert.equal(normalizeIp('2001:0db8:0000:0000:0000:0000:0000:0001'), '2001:db8::1');
  assert.equal(normalizeIp('fe80::1%eth0'), 'fe80::1');
  assert.throws(() => normalizeIp('not-an-ip'));
  assert.equal(canonicalBlockHash('A'.repeat(64)), 'a'.repeat(64));
  assert.equal(canonicalBlockHash('a'.repeat(63)), null);
});

test('60 calls per method per canonical IP in a sliding window', () => {
  let now = 0;
  const rate = new RateLimiter({ now: () => now });
  for (let n = 0; n < 60; n++) {
    now = n * 500;
    assert.equal(rate.consume({ ip: '127.0.0.1', method: 'getchaintip' }).allowed, true);
  }
  assert.equal(rate.consume({ ip: '::ffff:127.0.0.1', method: 'getchaintip' }).allowed, false);
  assert.equal(rate.consume({ ip: '127.0.0.1', method: 'getaddresshistory' }).allowed, true);
  assert.equal(rate.consume({ ip: '127.0.0.2', method: 'getchaintip' }).allowed, true);
  now = 59_999;
  assert.equal(rate.consume({ ip: '127.0.0.1', method: 'getchaintip' }).retryAfterMs, 1);
  now = 60_000;
  assert.equal(rate.consume({ ip: '127.0.0.1', method: 'getchaintip' }).allowed, true);
  assert.equal(rate.consume({ ip: '127.0.0.1', method: 'getchaintip' }).allowed, false);
});

test('10 requests per block; all 600 distinct blocks fit; invalid blocks share 60', () => {
  const rate = new RateLimiter();
  const consume = (blockHash) => rate.consume({ ip: '127.0.0.1', method: 'getblockbounties', blockHash });
  for (let n = 0; n < 600; n++) assert.equal(consume(n.toString(16).padStart(64, '0')).allowed, true);
  const hash = 'a'.repeat(64);
  for (let n = 0; n < 10; n++) assert.equal(consume(hash).allowed, true);
  assert.equal(consume(hash.toUpperCase()).allowed, false);
  for (let n = 0; n < 60; n++) assert.equal(consume(`bad-${n}`).allowed, true);
  assert.equal(consume(null).allowed, false);
});

test('storage limits reject new keys without evicting quota history, then expire', () => {
  let now = 0;
  const rate = new RateLimiter({ now: () => now, maxIps: 1, maxKeysPerIp: 2, maxKeys: 2 });
  assert.equal(rate.consume({ ip: '127.0.0.1', method: 'a' }).allowed, true);
  assert.equal(rate.consume({ ip: '127.0.0.1', method: 'b' }).allowed, true);
  assert.equal(rate.consume({ ip: '127.0.0.1', method: 'c' }).reason, 'capacity');
  assert.equal(rate.consume({ ip: '127.0.0.2', method: 'a' }).reason, 'capacity');
  assert.deepEqual(rate.stats, { ips: 1, keys: 2 });
  now = 60_000;
  rate.prune();
  assert.deepEqual(rate.stats, { ips: 0, keys: 0 });
  assert.equal(rate.consume({ ip: '127.0.0.2', method: 'a' }).allowed, true);
});

test('invalid limiter options fail closed', () => {
  for (const options of [{ maxIps: 0 }, { windowMs: -1 }, { methodLimit: 1.5 }, { maxKeys: Infinity }]) {
    assert.throws(() => new RateLimiter(options));
  }
});
