import { useState } from "react";
import { Textarea } from "@/components/ui/textarea";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Download, FileUp, Trash2 } from "lucide-react";
import { parseFile, pointsToCsv } from "@/lib/api";
import { SAMPLE_FILES, SAMPLES, type Sample } from "@/lib/samples";

const PLACEHOLDER = `Paste coordinates — any of these formats work:

12.971,77.594,2025-01-01T09:00:00Z
12.972,77.595,2025-01-01T09:00:05Z
...

# or just lat,lon per line:
12.971,77.594
12.972,77.595

# or CSV with headers:
lat,lon,ts,speed
12.971,77.594,2025-01-01T09:00:00Z,3.1
...`;

interface Props {
  value: string;
  onChange: (v: string) => void;
  onSubmit: () => void;
  submitLabel?: string;
  loading?: boolean;
  /**
   * Optional callback fired when the user picks a preset sample. When
   * provided, the parent can react synchronously (e.g. auto-compute
   * once the textarea has been populated). When omitted, picking a
   * sample only updates the textarea.
   */
  onLoadSample?: (sample: Sample) => void;
}

export function CoordinateInput({ value, onChange, onSubmit, submitLabel = "Compute", loading, onLoadSample }: Props) {
  const [dragging, setDragging] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [uploadNote, setUploadNote] = useState<string | null>(null);
  const [uploadError, setUploadError] = useState<string | null>(null);

  async function handleFile(file: File) {
    setUploadError(null);
    setUploadNote(null);
    const lower = (file.name || "").toLowerCase();
    // Plain-text formats round-trip through the textarea directly — no
    // need to burn a backend call to re-serialize them.
    if (
      lower.endsWith(".csv") ||
      lower.endsWith(".txt") ||
      lower.endsWith(".json") ||
      lower.endsWith(".geojson")
    ) {
      const text = await file.text();
      onChange(text);
      setUploadNote(`Loaded ${file.name}`);
      return;
    }
    // Binary formats (GPX/KML/FIT) are parsed server-side; we replace the
    // textarea contents with a CSV view of the parsed points so the rest
    // of the pipeline (the /v1/insights and /v1/plot/validate endpoints)
    // doesn't need format-specific handling.
    setUploading(true);
    try {
      const res = await parseFile(file);
      if (res.points.length === 0) {
        setUploadError(
          res.issues[0] ?? `Could not parse any coordinates from ${file.name}.`,
        );
        return;
      }
      onChange(pointsToCsv(res.points));
      const noteParts: string[] = [`Loaded ${res.accepted} points from ${res.filename}`];
      if (res.issues.length > 0) noteParts.push(res.issues[0]);
      setUploadNote(noteParts.join(" · "));
    } catch (e) {
      setUploadError(e instanceof Error ? e.message : String(e));
    } finally {
      setUploading(false);
    }
  }

  function onDrop(e: React.DragEvent<HTMLDivElement>) {
    e.preventDefault();
    setDragging(false);
    const f = e.dataTransfer.files?.[0];
    if (f) void handleFile(f);
  }

  function pickSample(sample: Sample) {
    onChange(sample.text);
    if (onLoadSample) onLoadSample(sample);
  }

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <Label htmlFor="coords">GPS coordinates</Label>
        <div className="flex flex-wrap items-center gap-1">
          <span className="hidden text-xs text-slate-500 sm:inline">Try a sample:</span>
          {SAMPLES.map((s) => (
            <Button
              key={s.id}
              type="button"
              variant="outline"
              size="sm"
              onClick={() => pickSample(s)}
              disabled={loading || uploading}
              title={s.description}
            >
              <span aria-hidden className="mr-1">{s.emoji}</span>
              {s.label}
            </Button>
          ))}
          <Button
            type="button"
            variant="ghost"
            size="sm"
            onClick={() => onChange("")}
            disabled={loading || !value}
          >
            <Trash2 className="h-4 w-4" />
            Clear
          </Button>
          <label
            className="inline-flex h-8 cursor-pointer items-center gap-1 rounded-md px-3 text-xs hover:bg-slate-100 aria-disabled:pointer-events-none aria-disabled:opacity-50"
            aria-label="Upload file"
            aria-disabled={uploading || loading}
          >
            <FileUp className="h-4 w-4" />
            {uploading ? "Parsing…" : "Upload"}
            <input
              type="file"
              accept=".csv,.txt,.json,.geojson,.gpx,.kml,.fit,text/plain"
              className="hidden"
              disabled={uploading || loading}
              onChange={(e) => {
                const f = e.target.files?.[0];
                if (f) void handleFile(f);
                // Reset so re-selecting the same file re-fires onChange.
                e.target.value = "";
              }}
            />
          </label>
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-1 rounded-md border border-slate-200 bg-slate-50 px-2 py-1.5">
        <span className="mr-1 text-xs text-slate-500">Download sample files:</span>
        {SAMPLE_FILES.map((s) => (
          <Button key={s.fileUrl} asChild type="button" variant="ghost" size="sm">
            <a href={s.fileUrl} download title={s.description}>
              <Download className="h-3.5 w-3.5" />
              <span aria-hidden>{s.emoji}</span>
              {s.fileLabel}
            </a>
          </Button>
        ))}
      </div>

      <div
        onDragOver={(e) => {
          e.preventDefault();
          setDragging(true);
        }}
        onDragLeave={() => setDragging(false)}
        onDrop={onDrop}
        className={`rounded-md border ${
          dragging ? "border-slate-900 ring-2 ring-slate-900" : "border-transparent"
        }`}
      >
        <Textarea
          id="coords"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder={PLACEHOLDER}
          className="min-h-[240px]"
          spellCheck={false}
        />
      </div>

      {uploadNote && !uploadError && (
        <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-700">
          {uploadNote}
        </div>
      )}
      {uploadError && (
        <div className="rounded-md border border-red-300 bg-red-50 px-3 py-2 text-xs text-red-900">
          {uploadError}
        </div>
      )}

      <div className="flex justify-end">
        <Button onClick={onSubmit} disabled={loading || uploading || !value.trim()}>
          {loading ? "Working…" : submitLabel}
        </Button>
      </div>
    </div>
  );
}
