import { createHmac, randomBytes, timingSafeEqual } from 'node:crypto';
import { RpcError } from './errors.mjs';

export class Cursors {
  constructor(key = randomBytes(32)) {
    if (!Buffer.isBuffer(key) || key.length !== 32) throw new Error('Invalid cursor signing key');
    this.key = Buffer.from(key);
  }
  sign(value) {
    const body = Buffer.from(JSON.stringify(value)).toString('base64url');
    return `${body}.${createHmac('sha256', this.key).update(body).digest('base64url')}`;
  }
  read(cursor) {
    const fail = () => { throw new RpcError(-32011, 'Invalid or expired cursor; restart this query.'); };
    if (typeof cursor !== 'string' || cursor.length > 1024) return fail();
    const parts = cursor.split('.');
    if (parts.length !== 2 || !parts.every(s => /^[A-Za-z0-9_-]+$/.test(s))) return fail();
    const sig = Buffer.from(parts[1], 'base64url');
    const expected = createHmac('sha256', this.key).update(parts[0]).digest();
    if (sig.length !== expected.length || !timingSafeEqual(sig, expected)) return fail();
    try { return JSON.parse(Buffer.from(parts[0], 'base64url').toString('utf8')); }
    catch { return fail(); }
  }
}
