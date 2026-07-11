import { Button } from "@/components/ui/button";
import { Circle, Flame, Layers } from "lucide-react";

export type MapLayerMode = "markers" | "heatmap" | "both";

interface Props {
  value: MapLayerMode;
  onChange: (mode: MapLayerMode) => void;
  disabled?: boolean;
}

const MODES: Array<{ mode: MapLayerMode; label: string; Icon: typeof Circle }> = [
  { mode: "markers", label: "Markers", Icon: Circle },
  { mode: "heatmap", label: "Heatmap", Icon: Flame },
  { mode: "both", label: "Both", Icon: Layers },
];

/**
 * Small segmented-button group for picking the map rendering mode. Used
 * on map-heavy workflows so users can flip to a heatmap when
 * there are more markers than a browser can usefully paint.
 */
export function MapLayerToggle({ value, onChange, disabled }: Props) {
  return (
    <div
      role="group"
      aria-label="Map layer"
      className="inline-flex rounded-md border border-slate-200 bg-slate-50/90 p-0.5 text-xs"
    >
      {MODES.map(({ mode, label, Icon }) => {
        const active = value === mode;
        return (
          <Button
            key={mode}
            type="button"
            size="sm"
            variant={active ? "default" : "ghost"}
            className="h-7 gap-1 px-2"
            disabled={disabled}
            onClick={() => onChange(mode)}
            aria-pressed={active}
          >
            <Icon className="h-3.5 w-3.5" />
            {label}
          </Button>
        );
      })}
    </div>
  );
}
