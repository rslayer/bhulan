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
  registerAuthHeaderProvider,
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

  // On mount: (1) consume a magic-link token if present in the URL hash,
  // (2) otherwise rehydrate the existing session by calling /v1/auth/me.
  useEffect(() => {
    let cancelled = false;

    async function boot() {
      const magic = extractAndClearMagicTokenFromHash();
      if (magic) {
        setVerifying(true);
        try {
          const resp = await authVerifyLink(magic);
          if (cancelled) return;
          tokenRef.current = resp.session_token;
          setStoredToken(resp.session_token);
          setUser(resp.user);
        } catch (e) {
          if (!cancelled)
            setVerifyError(
              e instanceof Error ? e.message : "Could not verify magic link",
            );
        } finally {
          if (!cancelled) setVerifying(false);
        }
      }

      if (tokenRef.current && !cancelled) {
        try {
          const me = await authMe();
          if (!cancelled) setUser(me);
        } catch {
          // Stale token \u2014 drop it silently.
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
      loading,
      verifying,
      verifyError,
      dismissVerifyError,
      setSession,
      logout,
    }),
    [user, loading, verifying, verifyError, dismissVerifyError, setSession, logout],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}
