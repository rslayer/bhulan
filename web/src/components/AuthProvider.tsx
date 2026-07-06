import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  authMe,
  authLogout,
  authVerifyLink,
  getCapabilities,
  registerAuthHeaderProvider,
  type Capabilities,
  type CurrentUser,
} from "@/lib/api";
import {
  clearStoredToken,
  extractAndClearMagicTokenFromHash,
  getStoredToken,
  setStoredToken,
} from "@/lib/auth";

interface AuthContextValue {
  user: CurrentUser | null;
  capabilities: Capabilities;
  loading: boolean;
  /** Non-null while the AuthProvider is processing a magic link from the URL. */
  verifying: boolean;
  /** One-shot error message from the last magic-link verify attempt. */
  verifyError: string | null;
  dismissVerifyError: () => void;
  /** Apply a session token the UI already obtained (e.g. after manual paste). */
  setSession: (token: string, user: CurrentUser) => void;
  logout: () => Promise<void>;
}

const AuthContext = createContext<AuthContextValue | null>(null);

const DEFAULT_CAPABILITIES: Capabilities = {
  auth_enabled: false,
  history_enabled: false,
  public_demo: true,
};

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within <AuthProvider>");
  return ctx;
}

interface Props {
  children: React.ReactNode;
}

export function AuthProvider({ children }: Props) {
  const tokenRef = useRef<string | null>(getStoredToken());
  const [user, setUser] = useState<CurrentUser | null>(null);
  const [capabilities, setCapabilities] =
    useState<Capabilities>(DEFAULT_CAPABILITIES);
  const [loading, setLoading] = useState<boolean>(true);
  const [verifying, setVerifying] = useState<boolean>(false);
  const [verifyError, setVerifyError] = useState<string | null>(null);

  // Register the auth-header provider exactly once. The api.ts module
  // pulls the current token on every request.
  useEffect(() => {
    registerAuthHeaderProvider(() => {
      const t = tokenRef.current;
      const headers: Record<string, string> = {};
      if (t) headers.Authorization = `Bearer ${t}`;
      return headers;
    });
  }, []);

  // On mount: (1) discover server capabilities, (2) consume a magic-link
  // token if auth is enabled, (3) otherwise rehydrate the existing session
  // by calling /v1/auth/me.
  useEffect(() => {
    let cancelled = false;

    async function boot() {
      let caps = DEFAULT_CAPABILITIES;
      try {
        caps = await getCapabilities();
      } catch {
        // Fail closed into anonymous public-demo mode. The core /v1
        // analytics endpoints do not need auth, and this avoids showing
        // sign-in controls when capability discovery is unavailable.
      }
      if (!cancelled) setCapabilities(caps);

      if (!caps.auth_enabled) {
        extractAndClearMagicTokenFromHash();
        tokenRef.current = null;
        clearStoredToken();
        if (!cancelled) {
          setUser(null);
          setLoading(false);
        }
        return;
      }

      const magic = extractAndClearMagicTokenFromHash();
      if (magic) {
        setVerifying(true);
        try {
          const resp = await authVerifyLink(magic);
          // Persist the session regardless of ``cancelled``: the server
          // already consumed the magic link (single-use) and created the
          // session, so bailing client-side would strand the user with no
          // way to retry. We only guard React state setters below — those
          // are safely idempotent when re-run after a StrictMode
          // dev-mode remount because the token is now in localStorage
          // and the next boot() hits the /auth/me rehydrate branch.
          tokenRef.current = resp.session_token;
          setStoredToken(resp.session_token);
          if (!cancelled) setUser(resp.user);
        } catch (e) {
          if (!cancelled)
            setVerifyError(
              e instanceof Error ? e.message : "Could not verify magic link",
            );
        } finally {
          // Always clear the verifying flag. Previously this was guarded
          // by !cancelled, which meant a StrictMode-triggered cleanup
          // between request and response left the UI stuck on
          // "Verifying link…" forever. React tolerates setState after
          // unmount; in StrictMode's dev double-invoke the component
          // isn't really unmounted, just remounted, so this is safe.
          setVerifying(false);
        }
      }

      // Only rehydrate via /auth/me when we DIDN'T just consume a magic
      // link — verify() already returned the authoritative user. Calling
      // /auth/me here would make a single transient network blip (server
      // restart, cold start, etc.) wipe the session we just created, and
      // the magic link is single-use so the user couldn't retry.
      if (!magic && tokenRef.current && !cancelled) {
        try {
          const me = await authMe();
          if (!cancelled) setUser(me);
        } catch {
          // Stale token — drop it silently.
          tokenRef.current = null;
          clearStoredToken();
          if (!cancelled) setUser(null);
        }
      }
      if (!cancelled) setLoading(false);
    }

    boot();
    return () => {
      cancelled = true;
    };
  }, []);

  const setSession = useCallback((token: string, nextUser: CurrentUser) => {
    tokenRef.current = token;
    setStoredToken(token);
    setUser(nextUser);
  }, []);

  const logout = useCallback(async () => {
    try {
      if (tokenRef.current) await authLogout();
    } catch {
      // ignore
    }
    tokenRef.current = null;
    clearStoredToken();
    setUser(null);
  }, []);

  const dismissVerifyError = useCallback(() => setVerifyError(null), []);

  const value = useMemo<AuthContextValue>(
    () => ({
      user,
      capabilities,
      loading,
      verifying,
      verifyError,
      dismissVerifyError,
      setSession,
      logout,
    }),
    [
      user,
      capabilities,
      loading,
      verifying,
      verifyError,
      dismissVerifyError,
      setSession,
      logout,
    ],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}
