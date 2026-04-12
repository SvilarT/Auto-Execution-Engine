# Remaining Production Steps

## Current Baseline

The repository now has a stronger durable coordination substrate than the earlier foundation review reflected. The following milestones are already completed and committed on `main`, including the newly finished event-log projection milestone:

| Step | Commit | Outcome |
|---|---|---|
| **1** | `13e3857` | Durable SQLite-backed account lease ownership, durable broker submission deduplication, startup wiring, and restart-safety tests |
| **2** | `7dd9576` | Crash-gap repair from durable submission records during execution retry and startup reconciliation |
| **3** | `608f818` | Explicit lease release semantics with immediate ownership handoff coverage |
| **4** | `d0918bb` | Durable reconciliation-cycle journal with persisted broker/internal truth, drift sets, and replayable outcomes |
| **5** | `54a11f2` | Recurring reconciliation runner with fail-closed account handling and duplicate-run protection |
| **6** | `cf1a402` | Explicit broker submission outcome taxonomy with retry-safe rejection, timeout, and unknown-result handling |
| **7** | `b14f07a` | Durable fill ingestion, deterministic post-submit order progression, replay-safe duplicate fill handling, and restart-safe fill recovery tests |
| **8** | `da00b00` | Deterministic event-log position and cash projections, reconciliation integration for internal versus broker exposure checks, durable persistence of projection inputs, and restart-safe tests |

The system is still **not paper-ready or live-ready**, but it now has materially better restart safety for execution coordination.

## Prioritized Remaining Steps

The remaining work should be executed in the following order. Each step is intentionally framed as a **commit-sized milestone** so progress can continue to be validated and preserved incrementally.

| Priority | Step | Why it comes next | Expected output |
|---|---|---|---|
| **9** | **Strengthen the risk layer with aggregated exposure and policy versioning** | Now that order, fill, position, and cash truth can be reconstructed, the next safety gap is enforcing richer account-level limits consistently | Versioned policy evaluation, concentration and notional limits, account-level exposure aggregation, audit logging, and tests |
| **10** | **Add observability and operator control surfaces** | Safe runtime operation requires explainability under failure, not just correct code paths | Structured logs, metrics, health summaries, quarantine/operator action history, and clear runtime diagnostics |
| **11** | **Create promotion gates from simulation to paper and paper to live** | A trading engine becomes dangerous when modes exist without disciplined acceptance criteria | Checklists, automated validation gates, drill scenarios, and mode-promotion tests/documentation |
| **12** | **Integrate a real broker adapter behind the existing boundary** | Only after durable coordination, reconciliation cadence, recovery, observability, and promotion discipline are in place should external capital-facing connectivity be attempted | Real adapter implementation, credential/config plumbing, sandbox/paper verification flows, and end-to-end tests |

## Definition of Done for the Next Several Steps

### Step 4: Persist reconciliation inputs and outcomes

This step should add a durable journal of reconciliation observations rather than storing only the latest derived report. At minimum, each reconciliation cycle should persist the account, timestamp, internal snapshot set, broker snapshot set, drift set, and chosen action. The implementation should make it possible to answer the operational questions, **"What did the engine believe? What did the broker report? What action did the engine take?"** after a restart.

### Step 5: Add a recurring reconciliation runner

This step should turn reconciliation from a callable function into an operational loop. It should support deterministic periodic execution, avoid overlapping work on the same account, and persist enough metadata to make failed or skipped runs explainable. Quarantine-triggering drift should remain fail-closed.

### Step 6: Model broker failures and retries explicitly

This step should separate **safe retry**, **unsafe retry**, **terminal rejection**, and **submission state unknown** outcomes. The goal is to ensure the engine never retries blindly in a way that could duplicate exposure, while still allowing deterministic recovery when the external result is ambiguous.

## Working Rules for Future Steps

Each remaining step should continue to follow the same operating discipline used in the recent coordination work.

| Rule | Requirement |
|---|---|
| **Small coherent increments** | Each step should be large enough to matter but small enough to validate thoroughly and commit cleanly |
| **Tests before commit** | Every milestone should leave the repository in a green state before commit |
| **Restart-safe semantics first** | When choosing between convenience and deterministic recovery, prioritize deterministic recovery |
| **Broker truth over optimistic assumptions** | Any ambiguous runtime condition should resolve conservatively and be visible to reconciliation or quarantine logic |
| **No mode shortcuts** | Paper and live paths should remain fail-closed and explicitly gated |

## Immediate Next Target

The next highest-value build target is **Step 9: strengthen the risk layer with aggregated exposure and policy versioning**. Now that durable submission outcomes, fill ingestion, and event-log reconstruction of position and cash state are in place, the next gap is enforcing richer account-level safety policy on top of that reconstructed truth. Exposure-aware risk controls are the missing bridge between restart-safe state recovery and serious paper-ready trading discipline.
