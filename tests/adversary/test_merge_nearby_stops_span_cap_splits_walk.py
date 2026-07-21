"""
Acceptance test for the merge span cap (ADR 0013): a merged stop is one place —
points clustered within ``stop_radius_m`` of a centre — never a smear along a
path.

Exercised at the unit level against ``merge_nearby_stops`` directly. A run of
detected stops, each within ``merge_stops_within_m`` of the next (so cycle 7's
single-linkage would chain them all) but marching past one ``stop_radius_m`` end
to end, must NOT collapse into one wide "stop". The cap splits the run into
groups each bounded to one ``stop_radius_m`` disk.

(At the endpoint, a continuous progression like this is rejected by
``detect_stops`` as movement — ADR 0014 — before the merge ever sees it; the cap
is the defence-in-depth that bounds a chain of genuine near-neighbour stops, so
it is verified by handing the merger the stops directly.)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from bhulan.analytics.stops import Stop, merge_nearby_stops

_T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
_LON = -73.0
_STOP_RADIUS_M = 50.0
_MERGE_RADIUS_M = 45.0   # each hop is mergeable (single-linkage would chain)...
_STEP_M = 40.0           # ...but the run marches _STEP_M each time
_N = 8                   # ~280 m end to end — far more than one stop
_METERS_PER_DEG_LAT = 111_320.0


def _run() -> list[Stop]:
    stops = []
    t = _T0
    north = 0.0
    for i in range(_N):
        lat = 40.0 + north / _METERS_PER_DEG_LAT
        stops.append(
            Stop(
                lat=lat,
                lon=_LON,
                start_ts=t,
                end_ts=t + timedelta(seconds=120),
                duration_s=120.0,
                radius_m=1.0,
                start_index=i,
                end_index=i,
                sample_count=4,
            )
        )
        t += timedelta(seconds=121)
        north += _STEP_M
    return stops


def test_progressive_run_splits_into_bounded_stops():
    total_span_m = _STEP_M * (_N - 1)  # ~280 m
    merged = merge_nearby_stops(
        _run(), merge_radius_m=_MERGE_RADIUS_M, stop_radius_m=_STOP_RADIUS_M
    )

    # It must NOT all merge into one stop (the whole point of the cap).
    assert len(merged) > 1, (
        f"a {total_span_m:.0f} m run of near-neighbour stops collapsed into a "
        f"single 'stop' despite stop_radius_m={_STOP_RADIUS_M} m — merge not capped"
    )

    # But single-linkage still did *some* merging (fewer stops than inputs),
    # proving the cap bounds rather than disables merging.
    assert len(merged) < _N, "the cap must still merge near-neighbours, not disable merging"

    # Every reported stop is bounded to one stop's radius.
    eps = 1e-6
    for st in merged:
        assert st.radius_m <= _STOP_RADIUS_M + eps, (
            f"reported stop radius_m={st.radius_m:.1f} m exceeds one "
            f"stop_radius_m={_STOP_RADIUS_M} m (ADR 0013)"
        )
