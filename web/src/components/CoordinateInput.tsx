import { useState } from "react";
import { Textarea } from "@/components/ui/textarea";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { FileUp, Trash2 } from "lucide-react";

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

const SAMPLE = `# Short walk around a block (meters-scale loop)
12.9710,77.5940,2025-01-01T09:00:00Z
12.9712,77.5942,2025-01-01T09:00:30Z
12.9715,77.5945,2025-01-01T09:01:00Z
12.9718,77.5948,2025-01-01T09:01:30Z
12.9720,77.5950,2025-01-01T09:02:00Z
# pause at a shop for 8 minutes
12.9720,77.5950,2025-01-01T09:02:30Z
12.9721,77.5950,2025-01-01T09:04:00Z
12.9720,77.5951,2025-01-01T09:06:00Z
12.9720,77.5950,2025-01-01T09:08:00Z
12.9721,77.5951,2025-01-01T09:10:00Z
# walk home
12.9718,77.5948,2025-01-01T09:10:30Z
12.9715,77.5945,2025-01-01T09:11:00Z
12.9712,77.5942,2025-01-01T09:11:30Z
12.9710,77.5940,2025-01-01T09:12:00Z`;

interface Props {
  value: string;
  onChange: (v: string) => void;
  onSubmit: () => void;
  submitLabel?: string;
  loading?: boolean;
}

export function CoordinateInput({ value, onChange, onSubmit, submitLabel = "Compute", loading }: Props) {
  const [dragging, setDragging] = useState(false);

  async function handleFile(file: File) {
    const text = await file.text();
    onChange(text);
  }

  function onDrop(e: React.DragEvent<HTMLDivElement>) {
    e.preventDefault();
    setDragging(false);
    const f = e.dataTransfer.files?.[0];
    if (f) void handleFile(f);
  }

  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center justify-between">
        <Label htmlFor="coords">GPS coordinates</Label>
        <div className="flex items-center gap-2">
          <Button
            type="button"
            variant="ghost"
            size="sm"
            onClick={() => onChange(SAMPLE)}
            disabled={loading}
          >
            Load sample
          </Button>
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
            className="inline-flex h-8 cursor-pointer items-center gap-1 rounded-md px-3 text-xs hover:bg-slate-100"
            aria-label="Upload file"
          >
            <FileUp className="h-4 w-4" />
            Upload
            <input
              type="file"
              accept=".csv,.txt,.json,.geojson,text/plain"
              className="hidden"
              onChange={(e) => {
                const f = e.target.files?.[0];
                if (f) void handleFile(f);
              }}
            />
          </label>
        </div>
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

      <div className="flex justify-end">
        <Button onClick={onSubmit} disabled={loading || !value.trim()}>
          {loading ? "Working…" : submitLabel}
        </Button>
      </div>
    </div>
  );
}
