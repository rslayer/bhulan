# tests/adversary/

Home for tests written by the **robustness harness**
(`.github/workflows/robustness.yml`). These are not hand-written — a
different-model adversary agent generates them when it finds a robustness
defect in the public `/v1` API.

**A failing test here is a feature, not a bug.** Each one reproduces a real
weakness the adversary found (a crash, an unhandled 500, a silently-wrong
answer, a hang, or clearly-invalid input accepted as valid). They arrive via
a `robustness/run-N` PR alongside a report in `reports/adversary/`.

Workflow when a robustness PR lands:
1. Read `reports/adversary/robustness-N.md` for the defect write-ups.
2. Decide which defects to fix. Fixing lives in `bhulan/` (product code) —
   the harness never touches it.
3. Once fixed, the corresponding test flips from red to green and becomes a
   permanent regression guard. Keep it.

These tests use the in-process `TestClient(app)` pattern from
`tests/integration/test_insights_api.py` (no MongoDB / network required).
