import { useEffect, useRef, useState } from "react";
import { CoordinateInput } from "@/components/CoordinateInput";
import { MapView } from "@/components/MapView";
import { ShareLinkButton } from "@/components/ShareLinkButton";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { validatePlot, type Point } from "@/lib/api";
import { decodeShareFragment } from "@/lib/permalink";

export function PlotPage() {
  const [text, setText] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [points, setPoints] = useState<Point[]>([]);
  const [accepted, setAccepted] = useState(0);
  const [rejected, setRejected] = useState(0);
  const [issues, setIssues] = useState<string[]>([]);
  const hydratedRef = useRef(false);

  // Hydrate from a share fragment on first mount — see InsightsPage for
  // the full rationale.
  useEffect(() => {
    if (hydratedRef.current) return;
    hydratedRef.current = true;
    const state = decodeShareFragment(window.location.hash);
    if (state && state.tab === "plot") {
      setText(state.text);
    }
  }, []);

  async function run() {
    setLoading(true);
    setError(null);
    try {
      const res = await validatePlot({ text });
      setPoints(res.points);
      setAccepted(res.accepted);
      setRejected(res.rejected);
      setIssues(res.issues);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setPoints([]);
      setAccepted(0);
      setRejected(0);
      setIssues([]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="grid grid-cols-1 gap-6 lg:grid-cols-5">
      <div className="lg:col-span-2">
        <Card>
          <CardHeader>
            <CardTitle>Input</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-4">
            <CoordinateInput
              value={text}
              onChange={setText}
              onSubmit={run}
              loading={loading}
              submitLabel="Plot on map"
            />
            {error && (
              <div className="rounded-md border border-red-300 bg-red-50 p-3 text-sm text-red-900">
                {error}
              </div>
            )}
            {(accepted > 0 || rejected > 0) && (
              <div className="rounded-md border border-slate-200 bg-slate-50 p-3 text-sm">
                <div>
                  <span className="font-semibold">{accepted}</span> accepted,{" "}
                  <span className="font-semibold">{rejected}</span> rejected
                </div>
                {issues.length > 0 && (
                  <ul className="mt-2 list-disc pl-5 text-xs text-slate-600">
                    {issues.slice(0, 5).map((i, idx) => (
                      <li key={idx}>{i}</li>
                    ))}
                    {issues.length > 5 && <li>…and {issues.length - 5} more</li>}
                  </ul>
                )}
              </div>
            )}
            <div className="flex justify-end">
              <ShareLinkButton state={{ tab: "plot", text }} disabled={!text.trim()} />
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="lg:col-span-3">
        <Card>
          <CardHeader>
            <CardTitle>Map</CardTitle>
          </CardHeader>
          <CardContent>
            <MapView points={points} />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
