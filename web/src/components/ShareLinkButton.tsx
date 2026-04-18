import { useEffect, useState } from "react";
import { Check, Link as LinkIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { buildShareUrl, type ShareState } from "@/lib/permalink";

interface Props {
  state: ShareState;
  disabled?: boolean;
}

/**
 * Copy-to-clipboard button that serializes the current tab's state into a
 * ``#s=v1.…`` URL fragment. We recompute the URL eagerly on every render
 * (cheap — it's just JSON + base64) so the button stays in sync with live
 * edits. The "Copied!" affordance auto-resets after 2s.
 *
 * When the payload would exceed :data:`MAX_FRAGMENT_BYTES`
 * (:mod:`@/lib/permalink`), the button is disabled with an explanatory
 * tooltip — producing a 20kB URL that gets truncated by mail clients is
 * a footgun worse than silently doing nothing.
 */
export function ShareLinkButton({ state, disabled }: Props) {
  const url = buildShareUrl(state);
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    if (!copied) return;
    const t = window.setTimeout(() => setCopied(false), 2000);
    return () => window.clearTimeout(t);
  }, [copied]);

  const unsupported = url === null;
  const canShare = !disabled && !unsupported;

  async function onClick() {
    if (!url) return;
    try {
      await navigator.clipboard.writeText(url);
      // Reflect in the address bar so the user can also bookmark directly.
      window.history.replaceState(null, "", url);
      setCopied(true);
    } catch {
      // Clipboard API can fail under http:// or in insecure contexts;
      // fall back to updating the hash so the URL bar still changes.
      window.history.replaceState(null, "", url);
    }
  }

  return (
    <Button
      type="button"
      variant="ghost"
      size="sm"
      onClick={onClick}
      disabled={!canShare}
      title={
        unsupported
          ? "Input is too large to encode in a URL fragment (>8 kB)."
          : disabled
            ? "Paste or upload coordinates first."
            : "Copy a shareable permalink for this input"
      }
    >
      {copied ? <Check className="h-4 w-4" /> : <LinkIcon className="h-4 w-4" />}
      {copied ? "Copied!" : "Share link"}
    </Button>
  );
}
