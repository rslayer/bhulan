import { ShieldCheck } from "lucide-react";
import { Button } from "@/components/ui/button";

interface Props {
  onClose: () => void;
}

/**
 * Plain-text privacy dialog summarising what bhulan does and doesn't
 * store. Linked from the footer so first-time visitors can confirm
 * before pasting GPS data. Kept content-only — no analytics, no
 * external calls — so it stays trustworthy.
 */
export function PrivacyDialog({ onClose }: Props) {
  return (
    <div
      role="dialog"
      aria-modal="true"
      // ``z-[1000]`` (not ``z-50``): Leaflet's marker/tooltip/control panes
      // run from z-index 400 to 800. A z-50 dialog would render *below* the
      // map on overlapping viewports.
      className="fixed inset-0 z-[1000] flex items-center justify-center bg-slate-900/40 p-4"
      onClick={onClose}
    >
      <div
        className="w-full max-w-md rounded-lg border border-slate-200 bg-white p-5 text-sm shadow-lg"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="mb-3 flex items-center gap-2">
          <ShieldCheck className="h-4 w-4" />
          <h2 className="text-sm font-semibold">Your data, in plain words</h2>
        </div>
        <ul className="flex list-disc flex-col gap-2 pl-5 text-xs leading-relaxed text-slate-700">
          <li>
            <span className="font-medium text-slate-900">Anonymous runs aren&rsquo;t stored.</span>
            {" "}If you compute insights without signing in, your coordinates are
            processed in memory and discarded as soon as the response is sent.
          </li>
          <li>
            <span className="font-medium text-slate-900">Signed-in runs are saved to your history</span>
            {" "}so you can replay or delete them. Only you can see them. Delete
            any entry from the History tab; it is removed permanently.
          </li>
          <li>
            <span className="font-medium text-slate-900">Sign-in uses one-time email links.</span>
            {" "}No passwords. We hash the link with HMAC-SHA256 keyed on a server
            secret so a database leak can&rsquo;t replay your link.
          </li>
          <li>
            <span className="font-medium text-slate-900">Reverse geocoding is opt-in.</span>
            {" "}Stop coordinates are only sent to OpenStreetMap (Nominatim) when
            you tick &ldquo;Resolve place names&rdquo; in advanced options.
          </li>
          <li>
            <span className="font-medium text-slate-900">Rate-limited per IP</span>
            {" "}so the public instance stays available. Self-host with
            &ldquo;BHULAN_AUTH_ENABLED=true&rdquo; for a private deployment.
          </li>
        </ul>
        <div className="mt-4 flex justify-end">
          <Button size="sm" variant="default" onClick={onClose}>
            Got it
          </Button>
        </div>
      </div>
    </div>
  );
}
