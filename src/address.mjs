import { RpcError } from './errors.mjs';

const ALPHABET = 'qpzry9x8gf2tvdw0s3jn54khce6mua7l';
const GENERATORS = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3];

function polymod(values) {
  let chk = 1;
  for (const value of values) {
    const top = chk >>> 25;
    chk = ((chk & 0x1ffffff) << 5) ^ value;
    for (let i = 0; i < 5; i++) if ((top >>> i) & 1) chk ^= GENERATORS[i];
  }
  return chk >>> 0;
}

/** Native ConnectCoin P2PK addresses: witness-v1, 32-byte key, Bech32m. */
export function normalizeAddress(value, chain) {
  const bad = () => { throw new RpcError(-32602, 'Expected a native ConnectCoin P2PK Bech32m address.'); };
  if (typeof value !== 'string' || value.length > 90 || value.length < 8) return bad();
  if (value !== value.toLowerCase() && value !== value.toUpperCase()) return bad();
  const addr = value.toLowerCase();
  const split = addr.lastIndexOf('1');
  const hrp = addr.slice(0, split);
  const expected = chain === 'main' ? 'cc' : chain === 'regtest' ? 'ccrt' : 'tcc';
  if (split < 1 || hrp !== expected) return bad();
  const data = [...addr.slice(split + 1)].map(c => ALPHABET.indexOf(c));
  if (data.some(n => n < 0) || data.length !== 59 || data[0] !== 1) return bad();
  const expanded = [...hrp].map(c => c.charCodeAt(0) >>> 5).concat(0, [...hrp].map(c => c.charCodeAt(0) & 31));
  if (polymod([...expanded, ...data]) !== 0x2bc830a3) return bad();
  // 32 bytes occupy 52 base32 words; the final four padding bits must be zero.
  if ((data[52] & 15) !== 0) return bad();
  return addr;
}
