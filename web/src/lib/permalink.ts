/**
 * Hash-encoded permalinks for share-by-URL.
 *
 * The whole pipeline is stateless, so we can round-trip a user's input
 * (pasted coordinates + analytics options) through the URL fragment
 * (``#s=…``). Fragments are not sent to the backend, keep the URL short
 * in server logs, and can exceed the ~2KB query-string limit imposed by
 * some reverse proxies.
 *
 * We deliberately DO NOT encrypt or obfuscate the payload — sharing a
 * link is an explicit, opt-in user action. The fragment is just a
 * convenience wrapper over ``btoa(JSON.stringify(state))``, with a
 * short ``v1.`` prefix so we can migrate encodings later without
 * breaking old links.
 *
 * Large inputs (e.g. a 100kB GPX text dump) would blow the URL-length
 * budget, so :func:`encodeShareState` refuses anything over
 * ``MAX_FRAGMENT_BYTES`` — the UI reflects this by disabling the
 * Share button with an explanatory tooltip.
 */

export const MAX_FRAGMENT_BYTES = 8 * 1024;

export interface ShareState {
  tab: "insights" | "plot";
  text: string;
  // Analytics options — only present when tab === "insights".
  options?: Record<string, unknown>;
}

function utf8Bytes(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function utf8Decode(bytes: Uint8Array): string {
  return new TextDecoder().decode(bytes);
}

/** Base64url (RFC 4648 §5) encoder — URL-safe, no padding. */
function b64UrlEncode(bytes: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < bytes.byteLength; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  // Use split+join instead of String.prototype.replaceAll so the bundle
  // works on the ES2020 target configured in tsconfig.
  return btoa(binary).split("+").join("-").split("/").join("_").split("=").join("");
}

function b64UrlDecode(s: string): Uint8Array {
  const pad = s.length % 4 === 0 ? "" : "=".repeat(4 - (s.length % 4));
  const b64 = s.split("-").join("+").split("_").join("/") + pad;
  const binary = atob(b64);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    out[i] = binary.charCodeAt(i);
  }
  return out;
}

/**
 * Encode ``state`` into a ``#s=v1.<base64url>`` fragment string.
 *
 * Returns ``null`` when the encoded payload would exceed
 * :data:`MAX_FRAGMENT_BYTES` — callers should surface a "paste too large
 * to share" hint in the UI rather than producing a link that truncates
 * in mail clients or browser address bars.
 */
export function encodeShareState(state: ShareState): string | null {
  const json = JSON.stringify(state);
  const encoded = b64UrlEncode(utf8Bytes(json));
  if (encoded.length > MAX_FRAGMENT_BYTES) return null;
  return `#s=v1.${encoded}`;
}

/**
 * Try to decode the current window's location fragment into a
 * :class:`ShareState`. Returns ``null`` for any parse failure (missing
 * fragment, unknown version prefix, bad base64, bad JSON) — the caller
 * renders its default state in that case.
 */
export function decodeShareFragment(fragment: string): ShareState | null {
  const cleaned = fragment.startsWith("#") ? fragment.slice(1) : fragment;
  if (!cleaned.startsWith("s=")) return null;
  const payload = cleaned.slice(2);
  if (!payload.startsWith("v1.")) return null;
  const b64 = payload.slice(3);
  try {
    const bytes = b64UrlDecode(b64);
    const parsed = JSON.parse(utf8Decode(bytes));
    if (
      parsed &&
      typeof parsed === "object" &&
      (parsed.tab === "insights" || parsed.tab === "plot") &&
      typeof parsed.text === "string"
    ) {
      return parsed as ShareState;
    }
    return null;
  } catch {
    return null;
  }
}

/** Build an absolute URL for ``state`` anchored at the current origin + path. */
export function buildShareUrl(state: ShareState): string | null {
  const fragment = encodeShareState(state);
  if (fragment === null) return null;
  const { origin, pathname } = window.location;
  return `${origin}${pathname}${fragment}`;
}
