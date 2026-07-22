"""
Stop detection over a time-ordered GPS track.

A "stop" is a contiguous run of samples whose spread (the maximum distance
from the run's centroid) stays within ``radius_m`` for at least
``min_duration_s``. The sliding window grows one sample at a time; to keep a
single long cluster from costing O(n^2) — the naive version recomputes the
centroid spread over the *entire* window on every growth step — it carries a
cheap running upper bound on the spread (the centroid can only shift by
~1/window_size per added point) and falls back to an exact recompute only
when that bound crosses ``radius_m``. The accept/reject decision is identical
to a full recompute every step, so results are unchanged; the bound merely
skips provably-safe work, bringing the common tight-cluster case down to
~O(n).
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Sequence, Tuple

import numpy as np

from bhulan.analytics.geodesy import circular_mean_lon, latlon_to_xy_m
from bhulan.analytics.mobility import TrackSample

DEFAULT_RADIUS_M = 50.0
DEFAULT_MIN_DURATION_S = 300.0  # 5 minutes — general-audience default
# A gap between consecutive samples longer than this ends the current stop:
# the device was off or the data is missing, so the two sides are distinct
# real-world visits, not one continuous dwell. Mirrors
# ``trips.DEFAULT_TRIP_SPLIT_GAP_S`` (kept numerically in sync, but defined
# here to avoid a circular import — trips imports :class:`Stop`).
DEFAULT_SPLIT_GAP_S = 60 * 60.0  # 60 minutes

# ``detect_stops`` re-grows a spatial cluster from each starting sample whenever
# the previous cluster was *rejected* (too short in time, or progressive
# movement). For a real track — where dwells are accepted and the scan jumps
# past them — that is ~O(n). But a crafted single giant cluster (a very slow
# drift, or thousands of samples sharing one timestamp) is never accepted, so
# the scan re-grows an O(n)-sized cluster from every sample: O(n·cluster_size),
# which at the public 100k-point cap ties up a worker for ~70s (an
# unauthenticated availability/DoS hole). This budget caps the total scan work
# at a multiple of the sample count; a real track (dwells accepted, the scan
# jumps past them → ~1× n work) stays far under it, while a pathological one is
# bounded to a few seconds. This is an ABSOLUTE cap (not per-point): a realistic
# track does well under a million work units regardless of size, so it is never
# touched, while any pathological input — the slow drift, or a dense mass of
# just-below-``min_duration`` dwells that are rejected and re-grown — is bounded
# to ~5s of stop scanning before it degrades gracefully (no stops + a quality
# note), rather than tying up a worker for ~70s. ``work`` counts grown samples
# plus the window size of every exact-radius recompute (the real time driver, at
# ~2-3M units/sec). The same-timestamp variant is additionally handled exactly
# (a zero-duration cluster can never be a stop, so it is skipped wholesale with
# no re-growth). See ADR 0016.
_MAX_STOP_SCAN_WORK = 12_000_000


class StopScanBudgetExceeded(Exception):  # noqa: N818 — "Exceeded" reads clearly; renaming would churn 5 call sites
    """detect_stops' scan work exceeded its budget — the input is a pathological
    single giant cluster (a slow drift or same-timestamp mass), not a real
    track. Callers degrade gracefully (report no stops + a quality note) rather
    than hang."""


@dataclass(frozen=True)
class Stop:
    """A detected stop with its centroid, duration, and sample range."""

    lat: float
    lon: float
    start_ts: datetime
    end_ts: datetime
    duration_s: float
    radius_m: float
    start_index: int
    end_index: int  # inclusive
    sample_count: int


def _cluster_radius_m(xs: np.ndarray, ys: np.ndarray) -> float:
    cx = float(np.mean(xs))
    cy = float(np.mean(ys))
    d = np.sqrt((xs - cx) ** 2 + (ys - cy) ** 2)
    return float(np.max(d)) if d.size else 0.0


def _cluster_end(
    xs: np.ndarray,
    ys: np.ndarray,
    ts: Sequence[Optional[datetime]],
    i: int,
    n: int,
    radius_m: float,
    split_gap_s: float,
) -> Tuple[int, int]:
    """
    Return ``(end, work)`` where ``end`` is the largest ``end >= i`` such that
    the window ``[i..end]`` has centroid-spread ``<= radius_m`` *and* no gap
    between consecutive samples reaches ``split_gap_s`` (growing one sample at a
    time), and ``work`` is a cost estimate for this grow — one unit per grown
    sample plus the window size of every exact-spread recompute, which is what
    actually dominates the running time (the O(window) ``_cluster_radius_m``
    numpy pass). Callers use ``work`` to enforce a scan budget so a pathological
    single giant cluster can't run away.

    Equivalent to calling :func:`_cluster_radius_m` on every prefix window, but
    avoids the O(k^2) blow-up on a long single cluster: it maintains a running
    upper bound ``r_est`` on the current window's spread and only pays for an
    exact recompute when that bound crosses ``radius_m``. Because a decision to
    *extend* is only taken when the spread is provably (bound) or exactly
    ``<= radius_m``, and a *break* only when the exact spread exceeds it, the
    result is identical to the naive per-step recompute.

    The gap check mirrors :func:`bhulan.analytics.trips._trip_bounds`: a device
    parked at a spot, switched off for a week, then parked at the *same* spot
    again would otherwise cluster into one stop spanning the whole calendar
    gap. Splitting on ``split_gap_s`` reports the two distinct visits instead.
    """
    sx = float(xs[i])
    sy = float(ys[i])
    m = 1
    r_est = 0.0
    work = 0
    j = i
    while j + 1 < n:
        work += 1
        a = ts[j]
        b = ts[j + 1]
        if a is not None and b is not None and (b - a).total_seconds() >= split_gap_s:
            return j, work  # a real-world absence ends the cluster at j
        x = float(xs[j + 1])
        y = float(ys[j + 1])
        cox, coy = sx / m, sy / m
        sx += x
        sy += y
        m += 1
        cnx, cny = sx / m, sy / m
        # The centroid moves by `shift`; every existing point's distance to the
        # new centroid grows by at most `shift`, so this stays a valid bound.
        shift = math.hypot(cnx - cox, cny - coy)
        d_new = math.hypot(x - cnx, y - cny)
        r_est = max(r_est + shift, d_new)
        if r_est > radius_m:
            # Exact recompute is an O(window) numpy pass — the real cost driver;
            # charge its size so the scan budget tracks wall-clock work.
            work += j + 2 - i
            r_exact = _cluster_radius_m(xs[i : j + 2], ys[i : j + 2])
            if r_exact > radius_m:
                return j, work  # adding j+1 breaks the cluster; window ends at j
            r_est = r_exact  # bound was loose — reset it tight and keep going
        j += 1
    return j, work


# A cluster is a *stop* only if its points are clustered around a common centre,
# not progressively translating through it. A dwell's two consecutive-in-time
# halves share a centroid (random GPS jitter averages out); a slow walk or crawl
# drifts, so its first-half centroid sits behind its second-half centroid by
# roughly the distance travelled. Reject a cluster whose half-to-half centroid
# drift exceeds this fraction of the stop radius as *movement*, not a stop.
# Tunable: 0.5 cleanly separates a directional fill of the radius (drift ≈
# radius) from random jitter (drift → 0). See ADR 0014.
_PROGRESSIVE_DRIFT_FRACTION = 0.5


def _is_progressive_translation(
    xs: np.ndarray,
    ys: np.ndarray,
    i: int,
    end: int,
    radius_m: float,
    drift_fraction: float = _PROGRESSIVE_DRIFT_FRACTION,
) -> bool:
    """True if samples ``i..end`` progressively translate (a walk/drive) rather
    than dwell around a fixed centre.

    Measured by the distance between the centroid of the cluster's first (in
    time) half and that of its second half: a dwell's halves coincide because
    random jitter averages out; a translating cluster's halves drift apart by
    ~the distance travelled between them. Without this check a slow walk that
    lasts longer than ``min_duration_s`` is chopped into ``radius_m``-sized
    chunks and each is reported as a phantom stop. A cluster of fewer than four
    samples is too short to judge direction and is treated as a dwell.

    ``drift_fraction`` is the sensitivity: a cluster is movement when its
    half-to-half drift exceeds ``drift_fraction * radius_m``. Lower rejects more
    aggressively; a large value effectively disables the check.
    """
    m = end - i + 1
    if m < 4:
        return False
    mid = i + m // 2
    fx1, fy1 = float(np.mean(xs[i:mid])), float(np.mean(ys[i:mid]))
    fx2, fy2 = float(np.mean(xs[mid : end + 1])), float(np.mean(ys[mid : end + 1]))
    drift = math.hypot(fx2 - fx1, fy2 - fy1)
    return drift > drift_fraction * radius_m


def detect_stops(
    points: Sequence[TrackSample],
    radius_m: float = DEFAULT_RADIUS_M,
    min_duration_s: float = DEFAULT_MIN_DURATION_S,
    split_gap_s: float = DEFAULT_SPLIT_GAP_S,
    max_scan_work: Optional[int] = None,
    progressive_drift_fraction: float = _PROGRESSIVE_DRIFT_FRACTION,
) -> List[Stop]:
    """
    Return chronologically ordered stops found in the track.

    Samples without timestamps are ignored — a stop needs a duration. If fewer
    than two timestamped samples remain, no stops are reported.

    Args:
        points: Time-ordered samples. Call :func:`prepare_track` first if
            you're not sure the input is sorted/deduped.
        radius_m: Maximum spread of the samples making up a stop, in meters.
        min_duration_s: Minimum duration for a cluster to count as a stop.
        split_gap_s: A gap between consecutive samples of at least this many
            seconds ends the current stop, so two visits to the same place
            separated by a real-world absence are reported as two stops rather
            than one stop spanning the calendar gap. Reuses the split-on-gap
            approach of :func:`bhulan.analytics.trips._trip_bounds`.
        max_scan_work: Optional cap on the total cluster-scan work (summed
            grown-cluster sizes). When exceeded, :class:`StopScanBudgetExceeded`
            is raised — the input is a pathological single giant cluster, not a
            real track. ``None`` (the default) leaves the scan uncapped; the
            public API passes ``_MAX_STOP_SCAN_WORK_PER_POINT * n`` so a crafted
            input can't tie up a worker.
    """
    ts_points: List[TrackSample] = [p for p in points if p.ts_utc is not None]
    n = len(ts_points)
    if n < 2:
        return []

    lats = [p.lat for p in ts_points]
    lons = [p.lon for p in ts_points]
    xs, ys = latlon_to_xy_m(lats, lons)
    ts = [p.ts_utc for p in ts_points]

    stops: List[Stop] = []
    scan_work = 0
    i = 0
    while i < n:
        end, work = _cluster_end(xs, ys, ts, i, n, radius_m, split_gap_s)
        # Charge this grow's cost (samples scanned + exact-recompute window
        # sizes) and bail if the cumulative work blows past the budget — a real
        # track stays far under, a pathological single giant cluster (re-grown
        # from every sample) would otherwise run O(n·cluster_size).
        if max_scan_work is not None:
            scan_work += work
            if scan_work > max_scan_work:
                raise StopScanBudgetExceeded(
                    f"stop scan exceeded {max_scan_work} work units at sample {i}"
                )
        if end > i:
            duration = (
                ts_points[end].ts_utc - ts_points[i].ts_utc  # type: ignore[union-attr, operator]
            ).total_seconds()
            if duration <= 0.0:
                # Every sample in [i..end] shares one timestamp, so no sub-window
                # can meet ``min_duration_s`` — the whole cluster is skipped
                # rather than re-grown from i+1. This makes a same-timestamp mass
                # O(n) instead of O(n·cluster_size); real tracks never hit it.
                i = end + 1
                continue
            if duration >= min_duration_s and not _is_progressive_translation(
                xs, ys, i, end, radius_m, progressive_drift_fraction
            ):
                xs_c = xs[i : end + 1]
                ys_c = ys[i : end + 1]
                lat_c = float(np.mean([p.lat for p in ts_points[i : end + 1]]))
                lon_c = circular_mean_lon([p.lon for p in ts_points[i : end + 1]])
                stops.append(
                    Stop(
                        lat=lat_c,
                        lon=lon_c,
                        start_ts=ts_points[i].ts_utc,  # type: ignore[arg-type]
                        end_ts=ts_points[end].ts_utc,  # type: ignore[arg-type]
                        duration_s=float(duration),
                        radius_m=_cluster_radius_m(xs_c, ys_c),
                        start_index=i,
                        end_index=end,
                        sample_count=end - i + 1,
                    )
                )
                i = end + 1
                continue
        i += 1

    return stops


def merge_nearby_stops(
    stops: Sequence[Stop],
    merge_radius_m: Optional[float] = None,
    split_gap_s: float = DEFAULT_SPLIT_GAP_S,
    stop_radius_m: float = DEFAULT_RADIUS_M,
) -> List[Stop]:
    """
    Merge consecutive stops whose centroids are within ``merge_radius_m`` *and*
    that are not separated by a real-world absence — but never beyond one stop's
    worth of ground (``stop_radius_m``).

    A stop is one *place*: points clustered around a common centre, all within
    ``stop_radius_m`` of it. Merging only reunites GPS-jitter fragments of one
    such place — so **the stop radius bounds the merge**. A stop joins the
    current merged group only while it stays within ``stop_radius_m`` of the
    group's *anchor* (its first member's centroid); when the chain has wandered
    beyond one stop's radius (a slow walk or drift, not a dwell), the next stop
    starts a new group instead of extending an ever-widening blob. This bounds
    every merged group to a ``stop_radius_m`` disk around its anchor, so a
    reported "stop" is never larger than one stop. ``merge_radius_m`` still
    gates *whether* two neighbours are close enough to consider at all (and
    whether merging is enabled), but the ``stop_radius_m`` cap dominates: a
    ``merge_radius_m`` looser than ``stop_radius_m`` cannot produce a stop wider
    than ``stop_radius_m``. See ADR 0013.

    Useful when GPS jitter splits what is semantically a single visit into two
    back-to-back stops separated by a handful of moving samples.

    Two stops close in space but separated by a gap of at least ``split_gap_s``
    seconds (the device was off, or the data is missing) are two distinct
    visits, not one dwell — exactly as :func:`detect_stops` established when it
    became gap-aware. Merging them would recombine what ``detect_stops`` had
    correctly split, folding a week-long absence back into a single stop, so we
    leave them separate. ``split_gap_s`` is the *same* notion threaded through
    :func:`detect_stops`; callers must pass the same value (see
    :func:`bhulan.analytics.insights.compute_insights`) rather than introduce a
    second, divergent gap.

    When two stops *are* legitimately merged, the reported duration is the sum
    of the two real dwells, never ``combined_end - combined_start`` — the latter
    would count the (jitter-sized, but still non-zero) time spent moving between
    them as dwell, and for a chained merge it would silently span any gap.
    """
    if merge_radius_m is None or not stops:
        return list(stops)

    from bhulan.analytics.geodesy import haversine_m

    # The member stops accumulated into each blob. The merged geometry
    # (centroid + ``radius_m``) is computed from these *real* members — not by
    # repeated pairwise midpoint — because for a 3+ chain the midpoint drifts
    # toward the tail (can land outside ``merge_radius_m`` of an early member)
    # and its last-hop radius understates the true spread. See ADR 0008.
    #
    # Group *membership* is decided in this single pass, but each group's merged
    # ``Stop`` is built exactly ONCE, after the loop (see ADR 0011): calling
    # ``_merge_members`` on every append made a length-n chain O(n²) — its
    # per-blob recompute is O(group size) — a CPU-exhaustion DoS reachable via
    # ``/v1/insights``. Deferring the geometry to a single post-loop map keeps
    # the accumulation linear while leaving the decision untouched.
    groups: List[List[Stop]] = []
    # The stop that immediately preceded ``s`` in the *original* input. The
    # spatial and time-gap merge decisions are made against this original
    # neighbour, not a blob's drifted centroid, so a chain of consecutive
    # pairwise-close stops collapses fully (single-linkage over consecutive
    # stops). See ADR 0007. This cycle changes ONLY performance and the emitted
    # centroid longitude, never the decision or which stops merge.
    prev_original: Optional[Stop] = None
    for s in stops:
        if not groups:
            groups.append([s])
            prev_original = s
            continue

        # ``groups`` is non-empty only after the first iteration set
        # ``prev_original``, so it is never None here. The assert both documents
        # that invariant and lets the type checker narrow ``Optional[Stop]``.
        assert prev_original is not None

        # Decide against the immediately preceding *original* stop. Its
        # ``end_ts`` equals the blob's ``end_ts`` (the blob always ends at its
        # last member), so gap-awareness is the original A–B boundary either
        # way; the spatial basis is what must not drift with the blob.
        gap_s = (
            (s.start_ts - prev_original.end_ts).total_seconds()
            if prev_original.end_ts is not None and s.start_ts is not None
            else 0.0
        )
        centroid_dist_m = haversine_m(
            prev_original.lat, prev_original.lon, s.lat, s.lon
        )
        close_in_space = centroid_dist_m <= merge_radius_m
        close_in_time = gap_s < split_gap_s

        # Cap: a merged stop is never larger than one stop. ``s`` may extend the
        # current group only while it stays within ``stop_radius_m`` of the
        # group's fixed anchor (its first member's centroid), so every member
        # sits inside one ``stop_radius_m`` disk around a common reference —
        # "clustered around a common centre." This is O(1) per stop (a single
        # anchor comparison, no per-member recompute — that would reintroduce
        # the cycle-9 O(n²) blow-up), and it bounds the group without letting a
        # slow walk/drift chain into one implausibly wide "stop." When the cap
        # is tripped, ``s`` begins a new group. The anchor is the *first*
        # member and never moves, so the guarantee (every member within
        # ``stop_radius_m`` of the anchor) is exact, not centroid-drift-prone.
        anchor = groups[-1][0]
        within_stop_radius = (
            haversine_m(anchor.lat, anchor.lon, s.lat, s.lon) <= stop_radius_m
        )

        if close_in_space and close_in_time and within_stop_radius:
            groups[-1].append(s)
        else:
            groups.append([s])
        prev_original = s

    # A group of one never merged: pass the original stop through byte-for-byte
    # (recomputing its geometry would round-trip the lon through trig and shift
    # the last bit). Only real multi-member blobs get merged geometry.
    return [g[0] if len(g) == 1 else _merge_members(g) for g in groups]


def _merge_members(members: List[Stop]) -> Stop:
    """Emit one merged :class:`Stop` from the ordered member stops of a blob.

    The centroid and ``radius_m`` are computed from the *real* members, not by
    repeated pairwise midpoint, so a chain of 3+ stops reports a truthful
    location and spread (ADR 0008):

    - ``lat`` = the ``sample_count``-weighted mean of the member centroid lats.
    - ``lon`` = the ``sample_count``-weighted (wraparound-aware) circular mean
      of the member centroid lons, so a chain straddling ±180° still centres
      inside the cluster. Longitude uses the *same* weighting as latitude
      (``wᵢ = sample_count``): weighting one axis but not the other pulled the
      centroid off the true member line for unequal-weight chains, landing it
      outside ``merge_radius_m`` of a member (ADR 0011 / AC1). The weighted
      circular mean is inlined here; the module-level ``circular_mean_lon``
      (unweighted, used elsewhere) is left unchanged.
    - ``radius_m`` = the true enclosing radius: the ``max`` over members of
      ``haversine_m(centroid, member) + member.radius_m``. This bounds the
      distance from the reported centroid to every member's own samples.

    For a two-member group of equal ``sample_count`` this reduces to the prior
    pairwise formula (midpoint centroid, ``dist/2 + max(radii)`` radius), so
    the single-merge path is unchanged.
    """
    from bhulan.analytics.geodesy import haversine_m

    total_w = sum(m.sample_count for m in members)
    lat = sum(m.lat * m.sample_count for m in members) / total_w
    # Weighted circular mean of the longitudes, wᵢ = sample_count — the same
    # weighting as ``lat`` — so both axes agree and the centroid stays inside
    # ``merge_radius_m`` of every member (ADR 0011).
    sin_sum = sum(m.sample_count * math.sin(math.radians(m.lon)) for m in members)
    cos_sum = sum(m.sample_count * math.cos(math.radians(m.lon)) for m in members)
    lon = math.degrees(math.atan2(sin_sum, cos_sum))
    radius_m = max(
        haversine_m(lat, lon, m.lat, m.lon) + m.radius_m for m in members
    )
    return Stop(
        lat=lat,
        lon=lon,
        start_ts=members[0].start_ts,
        end_ts=members[-1].end_ts,
        # Sum of the real dwells, not the calendar span: even a small
        # inter-stop gap is travel, not presence.
        duration_s=sum(m.duration_s for m in members),
        radius_m=radius_m,
        start_index=members[0].start_index,
        end_index=members[-1].end_index,
        sample_count=total_w,
    )
