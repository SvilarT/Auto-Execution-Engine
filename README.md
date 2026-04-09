# Auto-Execution-Engine

## Executive Summary

**Auto-Execution-Engine** is the clean rebuild of the prior autonomous trading prototype into a production-grade trading platform. The purpose of this repository is not to market an unfinished bot as "autonomous" or "institutional." The purpose is to build a trading system that can eventually handle real capital without ambiguity about execution mode, financial state, reconciliation status, or risk authority.

The foundational principle of this project is simple:

> A trading system is not live-ready until every order, fill, position mutation, balance mutation, risk decision, and operator action is durable, attributable, and reconcilable against broker truth.

This repository therefore prioritizes **correctness before sophistication**, **risk control before strategy complexity**, and **truthful system behavior before performance claims**.

## What This Repository Is For

This repository is the production-track codebase for the following capabilities:

| Capability | Purpose |
|---|---|
| **Execution mode isolation** | Hard separation between simulation, paper, and live trading paths |
| **Durable financial state** | Immutable event recording, deterministic order lifecycle, and restart-safe recovery |
| **Broker reconciliation** | Continuous comparison between internal system truth and broker truth |
| **Independent risk engine** | Policy-based capital protection that cannot be bypassed by strategy code |
| **Operational control-plane** | Safe operator actions, approvals, observability, and auditability |
| **Promotion gating** | Explicit progression from simulation to paper to controlled live deployment |

## What This Repository Is Not For

This repository is **not** intended to preserve prototype shortcuts that are unsafe for live trading. It should not inherit ambiguous execution semantics, non-durable state handling, unsafe secrets management, synthetic fallback behavior in non-simulation paths, or route-coupled runtime control patterns.

Any logic salvaged from legacy systems must be reintroduced only after it satisfies the architectural constraints defined in this repository.

## Initial Build Priorities

The first implementation priorities are ordered around live-trading safety rather than feature breadth.

| Priority | Workstream | Rationale |
|---|---|---|
| **1** | Hard mode separation | Prevent accidental semantic crossover between simulation, paper, and live |
| **2** | Financial core | Establish event ledger, order state machine, and durable projections |
| **3** | Reconciliation engine | Ensure internal truth is continuously validated against broker truth |
| **4** | Independent risk engine | Protect capital with fail-closed policy enforcement |
| **5** | Control-plane separation | Prevent UI or API lifecycle failures from destabilizing trading execution |
| **6** | Security hardening | Protect privileged operations, sessions, and broker credentials |
| **7** | Verification and drills | Prove failure behavior before any real capital is exposed |

## Repository Structure

The repository is organized around domain correctness and operational separation.

| Path | Responsibility |
|---|---|
| `docs/` | Architecture, design decisions, runbooks, and operational specifications |
| `src/auto_execution_engine/domain/` | Core financial domain models and invariants |
| `src/auto_execution_engine/application/` | Use cases, command handlers, and orchestration services |
| `src/auto_execution_engine/adapters/` | Broker, market-data, and persistence integrations |
| `src/auto_execution_engine/control_plane/` | Operator-facing APIs, approvals, configuration, and governance |
| `src/auto_execution_engine/trading_plane/` | Strategy workers, execution workers, and account-scoped runtime control |
| `src/auto_execution_engine/reconciliation/` | Broker-truth comparison and quarantine logic |
| `src/auto_execution_engine/observability/` | Metrics, logging, tracing, and alerts |
| `tests/` | Unit, integration, and acceptance tests |

## Build Standard

The standard for this repository is **production-grade trading infrastructure**, not demo quality. Every subsystem should be designed with restart safety, idempotency, explicit failure handling, observability, and auditability in mind.

Performance will matter, but only after correctness, safety, and trustworthiness are established.
