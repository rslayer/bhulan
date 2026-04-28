import { useState } from "react";
import { LogIn, LogOut, Mail, User as UserIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useAuth } from "@/components/AuthProvider";
import { authRequestLink } from "@/lib/api";

export function UserMenu() {
  const { user, loading, verifying, verifyError, dismissVerifyError, logout } =
    useAuth();
  const [open, setOpen] = useState(false);

  if (loading || verifying) {
    return (
      <div className="text-xs text-slate-500">
        {verifying ? "Verifying link…" : "…"}
      </div>
    );
  }

  return (
    <div className="relative flex items-center gap-2">
      {verifyError && (
        <div className="absolute right-0 top-full z-50 mt-1 flex items-center gap-2 rounded-md border border-rose-200 bg-rose-50 px-2 py-1 text-xs text-rose-800">
          <span>{verifyError}</span>
          <button
            className="text-rose-900 underline"
            onClick={dismissVerifyError}
          >
            dismiss
          </button>
        </div>
      )}
      {user ? (
        <>
          <div className="flex items-center gap-1.5 text-xs text-slate-600">
            <UserIcon className="h-3.5 w-3.5" />
            <span className="max-w-[180px] truncate">{user.email}</span>
          </div>
          <Button size="sm" variant="ghost" onClick={() => void logout()}>
            <LogOut className="mr-1 h-3.5 w-3.5" />
            Sign out
          </Button>
        </>
      ) : (
        <>
          <Button size="sm" variant="default" onClick={() => setOpen(true)}>
            <LogIn className="mr-1 h-3.5 w-3.5" />
            Sign in
          </Button>
          {open && <LoginDialog onClose={() => setOpen(false)} />}
        </>
      )}
    </div>
  );
}

interface LoginDialogProps {
  onClose: () => void;
}

function LoginDialog({ onClose }: LoginDialogProps) {
  const [email, setEmail] = useState("");
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [devLink, setDevLink] = useState<string | null>(null);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true);
    setErr(null);
    setMsg(null);
    setDevLink(null);
    try {
      const resp = await authRequestLink(email.trim());
      setMsg(
        resp.dev_magic_link
          ? "Dev mode — use the link below."
          : `We sent a sign-in link to ${resp.email}. Check your inbox.`,
      );
      setDevLink(resp.dev_magic_link ?? null);
    } catch (e2) {
      setErr(e2 instanceof Error ? e2.message : String(e2));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div
      role="dialog"
      aria-modal="true"
      className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-4"
      onClick={onClose}
    >
      <div
        className="w-full max-w-sm rounded-lg border border-slate-200 bg-white p-4 shadow-lg"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="mb-3 flex items-center gap-2">
          <Mail className="h-4 w-4" />
          <h2 className="text-sm font-semibold">Sign in with email</h2>
        </div>
        <p className="mb-3 text-xs text-slate-500">
          We&rsquo;ll email you a one-time sign-in link. No passwords, no signup
          form — your first link creates the account.
        </p>
        <form onSubmit={onSubmit} className="flex flex-col gap-2">
          <input
            type="email"
            required
            autoFocus
            placeholder="you@example.com"
            className="rounded-md border border-slate-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-slate-400"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            disabled={busy}
          />
          <div className="flex items-center justify-end gap-2">
            <Button
              type="button"
              size="sm"
              variant="ghost"
              onClick={onClose}
              disabled={busy}
            >
              Cancel
            </Button>
            <Button type="submit" size="sm" disabled={busy || !email.trim()}>
              {busy ? "Sending…" : "Send link"}
            </Button>
          </div>
        </form>
        {msg && (
          <div className="mt-3 rounded-md border border-emerald-200 bg-emerald-50 px-2 py-1.5 text-xs text-emerald-800">
            {msg}
          </div>
        )}
        {devLink && (
          <div className="mt-2 space-y-1 rounded-md border border-amber-200 bg-amber-50 px-2 py-1.5 text-xs text-amber-900">
            <div className="font-medium">Dev magic link</div>
            <a
              href={devLink}
              className="break-all underline"
              onClick={onClose}
            >
              {devLink}
            </a>
          </div>
        )}
        {err && (
          <div className="mt-3 rounded-md border border-rose-200 bg-rose-50 px-2 py-1.5 text-xs text-rose-800">
            {err}
          </div>
        )}
      </div>
    </div>
  );
}
