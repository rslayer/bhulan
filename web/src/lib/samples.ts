/**
 * Curated sample tracks used by the "Try a sample" preset buttons. Each
 * one is hand-tuned to produce a visually interesting result on the map
 * and at least one stop / trip / hotspot, so a brand-new visitor sees a
 * meaningful first run with one click.
 *
 * All coordinates are in Bangalore (IST timestamps converted to UTC) so
 * the OSM tiles cache nicely and the basemap is recognisable to most
 * demo viewers.
 */
export interface Sample {
  id: "walk" | "drive" | "hike";
  label: string;
  emoji: string;
  description: string;
  fileLabel: string;
  fileUrl: string;
  text: string;
}

const WALK = `# Short walk around a block (meters-scale loop)
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

const DRIVE = `# Outbound drive on the highway
12.9716,77.5946,2025-01-01T09:00:00Z
12.9700,77.5900,2025-01-01T09:00:30Z
12.9684,77.5854,2025-01-01T09:01:00Z
12.9668,77.5808,2025-01-01T09:01:30Z
12.9652,77.5762,2025-01-01T09:02:00Z
12.9636,77.5716,2025-01-01T09:02:30Z
12.9620,77.5670,2025-01-01T09:03:00Z
12.9604,77.5624,2025-01-01T09:03:30Z
12.9588,77.5578,2025-01-01T09:04:00Z
12.9572,77.5532,2025-01-01T09:04:30Z
# fuel stop
12.9568,77.5526,2025-01-01T09:05:00Z
12.9568,77.5526,2025-01-01T09:08:00Z
12.9568,77.5526,2025-01-01T09:11:00Z
# resume
12.9554,77.5482,2025-01-01T09:11:30Z
12.9540,77.5438,2025-01-01T09:12:00Z
12.9526,77.5394,2025-01-01T09:12:30Z
12.9512,77.5350,2025-01-01T09:13:00Z
12.9498,77.5306,2025-01-01T09:13:30Z
12.9484,77.5262,2025-01-01T09:14:00Z
12.9470,77.5218,2025-01-01T09:14:30Z`;

const HIKE = `# Out-and-back trail with a viewpoint rest
13.0000,77.6000,2025-01-01T08:00:00Z
13.0003,77.6003,2025-01-01T08:00:30Z
13.0006,77.6006,2025-01-01T08:01:00Z
13.0009,77.6009,2025-01-01T08:01:30Z
13.0012,77.6012,2025-01-01T08:02:00Z
13.0015,77.6015,2025-01-01T08:02:30Z
13.0018,77.6018,2025-01-01T08:03:00Z
13.0021,77.6021,2025-01-01T08:03:30Z
13.0024,77.6024,2025-01-01T08:04:00Z
# rest at the viewpoint
13.0026,77.6026,2025-01-01T08:04:30Z
13.0026,77.6026,2025-01-01T08:08:00Z
13.0026,77.6026,2025-01-01T08:11:00Z
# descend
13.0023,77.6023,2025-01-01T08:11:30Z
13.0020,77.6020,2025-01-01T08:12:00Z
13.0017,77.6017,2025-01-01T08:12:30Z
13.0014,77.6014,2025-01-01T08:13:00Z
13.0011,77.6011,2025-01-01T08:13:30Z
13.0008,77.6008,2025-01-01T08:14:00Z
13.0005,77.6005,2025-01-01T08:14:30Z
13.0002,77.6002,2025-01-01T08:15:00Z`;

export const SAMPLES: Sample[] = [
  {
    id: "walk",
    label: "Walk",
    emoji: "🚶",
    description: "Short loop with a coffee-shop pause",
    fileLabel: "CSV",
    fileUrl: "/samples/city-walk.csv",
    text: WALK,
  },
  {
    id: "drive",
    label: "Drive",
    emoji: "🚗",
    description: "Highway drive with a fuel stop",
    fileLabel: "JSON",
    fileUrl: "/samples/commute-with-stop.json",
    text: DRIVE,
  },
  {
    id: "hike",
    label: "Hike",
    emoji: "🥾",
    description: "Out-and-back trail with a viewpoint rest",
    fileLabel: "GPX",
    fileUrl: "/samples/trail-hike.gpx",
    text: HIKE,
  },
];

export const SAMPLE_FILES = [
  ...SAMPLES,
  {
    id: "delivery-route",
    label: "Delivery route",
    emoji: "📦",
    description: "Multi-stop delivery route with dwell points",
    fileLabel: "GeoJSON",
    fileUrl: "/samples/delivery-route.geojson",
  },
] as const;
