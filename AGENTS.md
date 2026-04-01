# AGENTS.md

This repository requires a review step for any non-trivial change.

## Required Change Flow

1. Read the relevant code and current runtime context before editing.
2. Implement the change.
3. Run focused tests for the touched area. Do not skip this before review.
4. Run sub-agent review before claiming the change is complete.

## Sub-Agent Review Rule

- For any multi-file change, behavior change, retry/recovery logic change, UI status change, process lifecycle change, or deployment/restart change:
  - Spawn at least 1 review sub-agent.
- For high-risk changes such as:
  - `plunger.py`
  - `plunger_ui.py`
  - `run.py`
  - watchdog / supervisor / restart scripts
  - live restart or deploy logic
  - health payload / status semantics
  - retry / timeout / recovery policy
  - use 2 parallel review sub-agents:
    - one reviews behavior / UX / protocol semantics
    - one reviews process lifecycle / deployment / restart safety

## What The Review Must Check

- Regressions and hidden behavior changes
- Contract changes in `/health` or UI-visible status semantics
- Retry / recovery misfires
- Process kill / restart safety
- Backward compatibility with mixed-version runtime states
- Missing or misleading tests

## If Review Finds Problems

1. Fix the issues first.
2. Re-run the relevant tests.
3. Run sub-agent review again on the updated diff if the original finding was medium or high severity.

## Before Live Restart / Deploy

- Tests must pass.
- Sub-agent review must be completed.
- If the port is serving live traffic, wait for a quiet window or use a no-interrupt handoff strategy.

## Final Response Expectation

- Mention that sub-agent review was run.
- Summarize the important findings.
- State what was fixed and what residual risk remains.
