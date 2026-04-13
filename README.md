## Executive Summary

**Auto-Execution-Engine** is a **production-track execution foundation** for a future live-capital trading system. It is not yet paper-ready and it is not yet live-ready. The purpose of this repository is to build the execution, persistence, reconciliation, and control-plane substrate that a real trading engine would require before any claim of deployment readiness could be credible.

The governing principle of the repository is straightforward.

> A trading system is not readiness-capable until every order, fill, position mutation, balance mutation, risk decision, and operator action is durable, attributable, and reconcilable against broker truth.

The repository therefore prioritizes **correctness before sophistication**, **risk control before strategy complexity**, and **truthful system behavior before performance claims**.

## Current Status

The current truthful description of the codebase is that it is a **strong production-track foundation**. It already contains important structural elements such as execution-mode isolation, durable order-state persistence, restart-oriented submission recovery, broker-facing submission flows, startup safety validation, and reconciliation scaffolding.

That said, the repository should not be mistaken for a paper-trading-ready engine or a live-capable platform. Several mandatory milestones still remain before those claims would be justified, including broker-state synchronization, Decimal-based financial correctness, cancel-and-replace truth, market-data integrity enforcement, closed-loop exposure truth, and deeper operational controls.

## What This Repository Is For

This repository exists to develop the safety-critical execution foundation for the following capabilities.

| Capability | Purpose |
|---|---|
| **Execution mode isolation** | Hard separation between simulation, paper, and live trading paths |
| **Durable financial state** | Immutable event recording, deterministic order lifecycle, and restart-safe recovery |
| **Broker reconciliation** | Continuous comparison between internal system truth and broker truth |
| **Independent risk engine** | Policy-based capital protection that cannot be bypassed by strategy code |
| **Operational control-plane** | Safe operator actions, approvals, observability, and auditability |
| **Promotion gating** | Explicit progression from simulation to paper to controlled live deployment |

## What This Repository Is Not For

This repository is **not** a marketing wrapper for an unfinished autonomous bot. It is not intended to preserve unsafe prototype shortcuts, ambiguous execution semantics, non-durable state handling, synthetic fallback behavior in non-simulation paths, or readiness claims that the code cannot yet support.

Any logic reintroduced from legacy systems must satisfy the architectural constraints of this repository before it can be considered part of the production track.

## Readiness Gates

The repository should only be described as **paper-ready** or **live-ready** after explicit gates have been satisfied. Those gates are intentionally stricter than feature-completeness claims.

| Readiness target | Minimum gates that must be satisfied before the claim is credible |
|---|---|
| **Paper-ready** | Broker-state synchronization is complete, Decimal financial correctness is in place, cancel-and-replace truth exists, market-data freshness and integrity checks are active, exposure and cash truth are closed loop, startup validation is fail-closed, and end-to-end restart recovery is proven |
| **Live-ready** | All paper gates are satisfied, promotion gates are evidenced, reconciliation and quarantine controls are operational, operator approvals are enforced, observability and runtime health reporting are production-usable, and failure drills have been executed successfully |

Until those gates are satisfied, the correct maturity framing remains **production-track foundation**.

## Near-Term Milestone Framing

The near-term roadmap is ordered around safety and truth preservation rather than feature breadth.

| Sequence | Near-term milestone | Why it comes next |
|---|---|---|
| **Phase 0** | Foundation stabilization | Establish packaging truth, startup hardening, maturity framing, precision policy, and explicit boundaries |
| **Step 13** | Broker-state synchronization | Internal order truth must converge with broker order truth before richer execution features are added |
| **Step 14** | Decimal financial correctness | Financial state must stop depending on float semantics before the system becomes larger and harder to migrate |
| **Next execution milestones** | Cancel/replace, market-data truth, exposure closure, and stronger operational controls | These depend on broker truth and financial precision already being trustworthy |

## Repository Structure

The repository is organized around domain correctness and operational separation.

| Path | Responsibility |
|---|---|
| `docs/` | Architecture, design decisions, runbooks, readiness policy, and operational specifications |
| `src/auto_execution_engine/domain/` | Core financial domain models and invariants |
| `src/auto_execution_engine/application/` | Use cases, command handlers, and orchestration services |
| `src/auto_execution_engine/adapters/` | Broker, market-data, and persistence integrations |
| `src/auto_execution_engine/control_plane/` | Operator-facing APIs, approvals, configuration, and governance |
| `src/auto_execution_engine/trading_plane/` | Strategy workers, execution workers, and account-scoped runtime control |
| `src/auto_execution_engine/reconciliation/` | Broker-truth comparison and quarantine logic |
| `src/auto_execution_engine/observability/` | Metrics, logging, tracing, and alerts |
| `tests/` | Unit, integration, and acceptance tests |

## Build Standard

The standard for this repository is **production-grade trading infrastructure**, not demo quality. Every subsystem should be designed with restart safety, idempotency, explicit failure handling, observability, auditability, and mode-safe behavior in mind.

Performance will matter, but only after correctness, safety, and trustworthiness are established.
