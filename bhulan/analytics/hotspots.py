"""
Spatial hotspot detection over a GPS track.

A "hotspot" is a region of high sample density — distinct from a stop in
that a hotspot can include moving samples. For a single track, hotspots
often coincide with stops (parked car, long meeting) but also catch
slow-moving recurring locations (walking in a park, stuck-in-traffic on
the morning commute). For multi-track inputs (the ``/v1/compare``
endpoint) hotspots are the primary way to identify recurring places
across days.

Algorithm: local-tangent-plane projection → grid binning → connected
components on the grid. This is a hand-rolled DBSCAN-lite that needs no
external dependency beyond numpy. It's O(n + g) where n is the number of
samples and g the number of occupied grid cells.

We intentionally avoid scipy / sklearn — the repo already has numpy for
geodesy, and the parameters that matter for this UX (cell size,
min-samples-per-cell, max-hotspots) are trivial to reason about without
DBSCAN's eps/minPts knobs.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np

from bhulan.analytics.geodesy import latlon_to_xy_m
from bhulan.analytics.mobility import TrackSample

DEFAULT_HOTSPOT_GRID_M = 100.0
DEFAULT_HOTSPOT_MIN_SAMPLES = 5
DEFAULT_HOTSPOT_MAX_RESULTS = 10


@dataclass(frozen=True)
class Hotspot:
    """A detected hotspot cluster."""

    lat: float
    lon: float
    sample_count: int
    # Sum of inter-sample durations for consecutive same-cluster samples;
    # None when the track carries no timestamps.
    time_spent_s: Optional[float]
    first_ts: Optional[datetime]
    last_ts: Optional[datetime]
    # How many distinct visits to this cluster (a "visit" is a contiguous
    # run of samples falling in this cluster). Useful for multi-track
    # compare where 5 visits across days >> 5 samples back-to-back in
    # one track.
    visit_count: int


def _bin_points(
    xs: np.ndarray, ys: np.ndarray, grid_m: float
) -> Dict[Tuple[int, int], List[int]]:
    """Group point indices by ``(i, j)`` grid cell."""
    cells: Dict[Tuple[int, int], List[int]] = {}
    if xs.size == 0:
        return cells
    ii = np.floor(xs / grid_m).astype(np.int64)
    jj = np.floor(ys / grid_m).astype(np.int64)
    for idx in range(xs.size):
        key = (int(ii[idx]), int(jj[idx]))
        cells.setdefault(key, []).append(idx)
    return cells


def _cluster_cells(
    cells: Dict[Tuple[int, int], List[int]], min_samples: int
) -> List[List[Tuple[int, int]]]:
    """Group occupied cells (with >= ``min_samples``) into connected
    components using 8-neighbour adjacency on the integer grid.
    """
    qualifying = {k for k, v in cells.items() if len(v) >= min_samples}
    clusters: List[List[Tuple[int, int]]] = []
    seen: set[Tuple[int, int]] = set()
    for key in qualifying:
        if key in seen:
            continue
        # BFS. 8-neighbourhood: the user's path typically runs along
        # cell diagonals (commute corridors), so 4-neighbour connectivity
        # under-merges.
        cluster: List[Tuple[int, int]] = []
        q: deque[Tuple[int, int]] = deque([key])
        seen.add(key)
        while q:
            cx, cy = q.popleft()
            cluster.append((cx, cy))
            for dx in (-1, 0, 1):
                for dy in (-1, 0, 1):
                    if dx == 0 and dy == 0:
                        continue
                    nb = (cx + dx, cy + dy)
                    if nb in qualifying and nb not in seen:
                        seen.add(nb)
                        q.append(nb)
        clusters.append(cluster)
    return clusters


def _visit_count(sorted_indices: Sequence[int]) -> int:
    """Count distinct runs of consecutive indices in a sorted sequence.

    ``[0, 1, 2, 10, 11, 50]`` → 3 visits. Single-index visits are still
    visits; this is what the UX wants ("visited this cell on 3 separate
    occasions").
    """
    visits = 0
    prev: Optional[int] = None
    for i in sorted_indices:
        if prev is None or i != prev + 1:
            visits += 1
        prev = i
    return visits


def _time_spent_s(
    sorted_indices: Sequence[int], points: Sequence[TrackSample]
) -> Optional[float]:
    """Sum inter-sample durations inside a cluster. ``None`` if no
    timestamps are available.

    Only consecutive sample pairs that BOTH belong to the cluster count —
    otherwise the last sample of a cluster would bleed its ``dt`` into
    the next (often far-away) non-cluster sample, over-counting by hours
    on multi-day pooled compare inputs.
    """
    index_set = set(sorted_indices)
    total = 0.0
    saw_ts = False
    for i in sorted_indices:
        if (i + 1) not in index_set:
            continue
        a = points[i].ts_utc
        b = points[i + 1].ts_utc
        if a is None or b is None:
            continue
        saw_ts = True
        dt = (b - a).total_seconds()
        if dt > 0:
            total += dt
    return total if saw_ts else None


def detect_hotspots(
    points: Sequence[TrackSample],
    grid_m: float = DEFAULT_HOTSPOT_GRID_M,
    min_samples: int = DEFAULT_HOTSPOT_MIN_SAMPLES,
    max_results: int = DEFAULT_HOTSPOT_MAX_RESULTS,
) -> List[Hotspot]:
    """Return the top-``max_results`` hotspot clusters, largest first.

    ``grid_m`` controls cluster granularity — 100 m is a good default for
    city-scale tracks (a typical lane change won't create a new hotspot,
    a different building will). ``min_samples`` suppresses noise; with
    1-Hz GPS it's comfortable to require 5 samples (≈5 s dwell).
    """
    n = len(points)
    if n == 0:
        return []

    lats = [p.lat for p in points]
    lons = [p.lon for p in points]
    xs, ys = latlon_to_xy_m(lats, lons)

    cells = _bin_points(xs, ys, grid_m)
    clusters = _cluster_cells(cells, min_samples)

    hotspots: List[Hotspot] = []
    for cluster in clusters:
        idxs: List[int] = []
        for cell in cluster:
            idxs.extend(cells[cell])
        idxs.sort()

        centroid_lat = float(np.mean([lats[i] for i in idxs]))
        centroid_lon = float(np.mean([lons[i] for i in idxs]))

        ts_samples = [points[i].ts_utc for i in idxs if points[i].ts_utc is not None]
        first_ts = min(ts_samples) if ts_samples else None  # type: ignore[type-var]
        last_ts = max(ts_samples) if ts_samples else None  # type: ignore[type-var]

        hotspots.append(
            Hotspot(
                lat=centroid_lat,
                lon=centroid_lon,
                sample_count=len(idxs),
                time_spent_s=_time_spent_s(idxs, points),
                first_ts=first_ts,
                last_ts=last_ts,
                visit_count=_visit_count(idxs),
            )
        )

    # Sort by sample count desc; tie-break on time spent desc then visit
    # count desc for deterministic ordering across runs.
    hotspots.sort(
        key=lambda h: (
            -h.sample_count,
            -(h.time_spent_s or 0.0),
            -h.visit_count,
        )
    )
    return hotspots[:max_results]


__all__ = [
    "DEFAULT_HOTSPOT_GRID_M",
    "DEFAULT_HOTSPOT_MIN_SAMPLES",
    "DEFAULT_HOTSPOT_MAX_RESULTS",
    "Hotspot",
    "detect_hotspots",
]
