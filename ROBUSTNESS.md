# Robustness harness

An on-demand **adversarial testing** harness for bhulan. A different-model
agent attacks the public `/v1` API, writes reproducing failing tests for any
weakness it finds, and opens a PR with a findings report. It is a *testing*
tool — it never changes product code.

## What it does (one run)

```
Adversary probes /v1  ──►  writes failing pytest tests + a report
        │                          │
        ▼                          ▼
  bhulan/ is read-only      guard fails the run if bhulan/ changed
        │                          │
        └──────────►  runs tests/adversary/  ──►  opens a findings PR
                       (failures = defects found)      you review + fix
```

- **Adversary** (`anthropics/claude-code-action`, a different model) reads
  product code for context but may only write under `tests/adversary/` and
  `reports/adversary/`.
- **Guard** (`Assert product code untouched`) fails the whole run if anything
  under `bhulan/` was modified — the mechanical guarantee that this never
  edits your product.
- **Findings** land as a `robustness/run-N` PR. Each **failing** test under
  `tests/adversary/` documents one real defect; `reports/adversary/` explains
  each in prose (endpoint, input, expected vs actual).

## Setup (once)

Add the Claude token secret to this repo — the harness needs it to run the
adversary:

- **Settings → Secrets and variables → Actions → New repository secret**
  - Name: `CLAUDE_CODE_OAUTH_TOKEN`
  - Value: your Claude Code OAuth token
- …or run `/install-github-app` from Claude Code inside this repo, which
  installs the app and adds the secret for you.

## Run it

- **GitHub UI:** Actions → **robustness** → **Run workflow** (optionally set a
  `focus`, e.g. `/v1/plot` or `timestamp handling`).
- **CLI:** `gh workflow run robustness.yml` (add `-f focus='/v1/compare'` to
  focus it).

It is `workflow_dispatch`-only on purpose — `claude-code-action` does not
support the `push` event, so there is no auto-trigger. Run it whenever you
want a fresh robustness pass (before a release, after touching validation,
etc.).

## Acting on findings

1. Read `reports/adversary/robustness-N.md`.
2. Fix the defects you care about in `bhulan/` (the harness won't).
3. The fixed test flips red → green and stays as a regression guard.

Note: the adversary's generated tests may not perfectly match `ruff`/`mypy`
style, so the existing CI on the findings PR can flag lint/type nits — clean
those up when you fix or accept a test.
