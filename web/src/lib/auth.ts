/**
 * Session-token storage + magic-link URL parsing.
 *
 * The token lives in ``localStorage`` so it survives reloads. We keep the
 * surface minimal — all React state lives in :class:`AuthProvider`; this
 * file just has the pure helpers.
 */

const TOKEN_KEY = "bhulan.session_token";

export function getStoredToken(): string | null {
  try {
    return localStorage.getItem(TOKEN_KEY);
  } catch {
    return null;
  }
}

export function setStoredToken(token: string): void {
  try {
    localStorage.setItem(TOKEN_KEY, token);
  } catch {
    // ignore (private mode etc)
  }
}

export function clearStoredToken(): void {
  try {
    localStorage.removeItem(TOKEN_KEY);
  } catch {
    // ignore
  }
}

/**
 * Extract a magic-link token from the current URL's fragment, e.g.
 * ``#magic=abc...`` → ``"abc..."``. The fragment is then cleared from
 * the address bar so a copied URL can't be re-used.
 */
export function extractAndClearMagicTokenFromHash(): string | null {
  const hash = window.location.hash || "";
  if (!hash) return null;
  // ``#magic=<token>`` or ``?magic=<token>`` — tolerate both.
  const stripped = hash.replace(/^[#?]/, "");
  const params = new URLSearchParams(stripped);
  const token = params.get("magic");
  if (!token) return null;
  params.delete("magic");
  const remaining = params.toString();
  const newHash = remaining ? `#${remaining}` : "";
  const url = `${window.location.pathname}${window.location.search}${newHash}`;
  window.history.replaceState(null, "", url);
  return token;
}
