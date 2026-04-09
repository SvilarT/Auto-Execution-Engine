# Auto-Execution-Engine Architecture Specification

## Purpose

This document defines the initial target architecture for **Auto-Execution-Engine**. Its purpose is to prevent the new system from inheriting the semantic ambiguity and unsafe runtime assumptions identified in the legacy prototype. The architecture is designed around one non-negotiable principle: **the system must remain truthful under failure**.

## System Objective

The objective of the platform is to transform strategy intent into broker-executed orders through a controlled pipeline in which every significant event is durable, every execution path is mode-explicit, every financial projection is reconstructable, and every live-capital action is constrained by independent risk policy.

## Architectural Principles

| Principle | Meaning |
|---|---|
| **Truth over convenience** | The platform must halt rather than improvise when market data, broker state, or internal state becomes unreliable |
| **Mode isolation** | Simulation, paper, and live must be separate execution domains, not flags attached to shared unsafe code paths |
| **Durability first** | Financially meaningful state transitions must be persisted before they are considered real |
| **Reconciliation as a core service** | Broker-truth comparison is a primary runtime function, not a support utility |
| **Risk independence** | Strategy logic may request permission to trade, but it may not define or bypass policy authority |
| **Fail-closed behavior** | Ambiguity, stale data, lease conflicts, or reconciliation drift must stop trading rather than degrade silently |
| **Operational separation** | Control-plane failures must not directly cause trading-plane instability or duplicate execution |

## Top-Level System Decomposition

The platform is divided into a **control plane**, a **trading plane**, and a **shared durable substrate**.

| Plane | Responsibility |
|---|---|
| **Control plane** | Operator authentication, approvals, configuration, dashboards, governance actions, and observability endpoints |
| **Trading plane** | Strategy evaluation, market-data ingestion, risk requests, order lifecycle management, broker submission, and reconciliation triggers |
| **Shared substrate** | Persistent database, event store, message transport, secrets handling, metrics, structured logs, and lease ownership records |

## Execution Modes

The platform supports three isolated execution domains.

| Mode | Purpose | Non-negotiable requirements |
|---|---|---|
| **Simulation** | Research, replay, and synthetic execution | No external broker effects; all data sources explicitly labeled as simulated or replayed |
| **Paper** | Broker-adapter validation without capital exposure | Uses paper broker credentials and isolated persistence namespace |
| **Live** | Real-capital execution | Requires explicit activation gates, strict risk policy, reconciled state, and live-only credentials |

Mode selection must occur at process boot and must determine provider wiring, persistence namespace, broker credentials, and safety assertions. Runtime semantic switching is prohibited.

## Core Runtime Flow

A valid trade lifecycle follows a deterministic sequence.

| Step | Description |
|---|---|
| **1. Market-data ingest** | A mode-specific market-data adapter produces normalized quotes, trades, and metadata with freshness timestamps |
| **2. Strategy evaluation** | Strategy logic generates a candidate intent, not an executable broker order |
| **3. Risk request** | The candidate intent is submitted to the independent risk engine for policy evaluation |
| **4. Order creation** | On approval, an internal order aggregate is created and persisted |
| **5. Broker submission** | An execution adapter submits the order to the broker with idempotent client identifiers |
| **6. Broker state updates** | Acknowledgements, fills, rejections, cancels, and corrections are persisted as immutable events |
| **7. Projection update** | Positions, balances, and PnL are derived from recorded events rather than mutable ad hoc state |
| **8. Reconciliation** | Internal projections are compared with broker truth on schedule and on critical transitions |
| **9. Quarantine on drift** | Any unexplained delta or stale dependency halts further trading until resolved |

## Domain Components

### Event Ledger

The event ledger records every economically meaningful fact as an immutable event.

| Event class | Examples |
|---|---|
| **Strategy events** | Signal generated, signal expired, strategy decision version |
| **Risk events** | Policy evaluated, approval granted, rejection issued, kill-switch activated |
| **Order events** | Order created, submitted, acknowledged, partially filled, filled, rejected, cancelled |
| **Reconciliation events** | Broker snapshot received, drift detected, correction applied, account quarantined |
| **Governance events** | Live enablement approved, manual halt triggered, operator override executed |

### Order Aggregate

The order aggregate owns the deterministic order lifecycle.

| Required state | Meaning |
|---|---|
| `CREATED` | Internal order exists but has not yet passed risk approval |
| `RISK_APPROVED` | Risk engine approved the order request under a specific policy version |
| `SUBMITTED` | Order was transmitted toward the broker boundary |
| `ACKNOWLEDGED` | Broker accepted the order for working status |
| `PARTIALLY_FILLED` | One or more partial fills have been recorded |
| `FILLED` | The order is fully complete |
| `REJECTED` | Broker or policy rejected execution |
| `CANCEL_PENDING` | Cancel request was sent but final cancel state is unresolved |
| `CANCELLED` | Broker confirmed cancellation |
| `RECONCILED` | Final order state has been validated against broker truth |

No transition is valid unless it is persisted. No duplicate client order identifier may exist within a mode and account namespace.

### Risk Engine

The risk engine is an independent policy authority.

| Layer | Example controls |
|---|---|
| **Pre-trade** | Maximum order notional, leverage caps, concentration limits, spread checks, volatility gates |
| **In-flight** | Maximum active orders, stale quote rejection, order-age controls, cancel-on-disconnect |
| **Post-trade** | Daily loss cap, drawdown halt, margin stress halt, reconciliation drift halt |
| **Governance** | Dual approval for live enablement, policy versioning, emergency stop, break-glass workflow |

All risk decisions must return durable reason codes and policy version identifiers.

### Reconciliation Service

The reconciliation service compares internal projections to broker truth at startup, after reconnect, after fill activity, and on a scheduled basis. If the service detects unexplained order, fill, cash, or position drift, it must quarantine the account and refuse new order flow until the discrepancy is resolved or formally overridden.

## Ownership and Process Safety

Trading workers must use **per-account lease ownership** with heartbeats. Only one active worker may control a given live account at a time. Lease loss, duplicate ownership, or uncertain worker status must cause the account to halt rather than permit concurrent order submission.

## Initial Code Organization

| Module | Intended responsibility |
|---|---|
| `domain/` | Entities, value objects, invariants, and event definitions |
| `application/` | Use-case orchestration, command handling, and service coordination |
| `adapters/` | Broker adapters, market-data adapters, and persistence implementations |
| `control_plane/` | Operator-facing APIs, auth integration, approvals, and governance commands |
| `trading_plane/` | Strategy runners, execution loops, and worker lifecycle management |
| `reconciliation/` | Drift detection, broker snapshot comparison, and quarantine management |
| `observability/` | Metrics, logs, traces, alert definitions, and incident context generation |

## First Implementation Sequence

| Order | Workstream | Outcome |
|---|---|---|
| **1** | Bootstrap configuration and execution mode model | Enforce explicit startup semantics and prevent ambiguous runtime behavior |
| **2** | Domain event model and order aggregate skeleton | Establish durable financial primitives |
| **3** | Risk decision interface and kill-switch primitives | Create independent policy enforcement points |
| **4** | Reconciliation service contracts | Make broker-truth comparison a first-class design constraint |
| **5** | Worker ownership and account lease model | Prevent duplicate live execution across processes |

## Immediate Decision

The first code to be written in this repository should implement **hard execution mode separation and startup safety gates**. No broker submission path, strategy runner, or dashboard control logic should be treated as production-capable until that boundary is in place.
