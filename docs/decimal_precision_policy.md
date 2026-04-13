# Decimal Precision Policy

## Purpose

This document defines the single **Decimal precision contract** for the Auto-Execution-Engine repository. It exists so that future financial correctness work follows one shared standard rather than ad hoc local conventions.

The repository is still in its **production-track foundation** stage. This policy is therefore a prerequisite for the later Decimal migration milestone rather than evidence that the migration is already complete.

## Scope

This policy applies to every production-path value that represents financial or market-execution semantics, including **money**, **price**, **quantity**, **notional**, **balances**, **position sizes**, **fees**, and **broker-reported execution values**.

Until the full migration is complete, existing float-based production code remains transitional. New financial correctness work must converge toward this document rather than introducing new local precision rules.

## Canonical Parsing Rules

All external numeric inputs that represent financial values must eventually enter the system through **canonical Decimal parsing**.

The canonical parser must follow these rules:

| Rule | Requirement |
|---|---|
| **Accepted inputs** | `Decimal`, `str`, `int`, and finite `float` values may be accepted during the transition period |
| **Float handling** | Floats are transitional inputs only and must be converted through their string form, never through binary float arithmetic |
| **Whitespace** | Leading and trailing whitespace may be trimmed before parsing |
| **Invalid inputs** | Empty strings, NaN, Infinity, and malformed numeric strings must fail fast |
| **Normalization** | Parsed values must preserve numeric truth and avoid silent rounding during parsing |

## Precision and Quantization Rules

The repository distinguishes between **parsing**, **storage**, and **business-rule quantization**.

Parsing should preserve the exact declared numeric value. Quantization should happen only when a boundary requires a specific precision contract.

| Value class | Default rule |
|---|---|
| **Money / cash / balances / notionals** | Quantize to `0.01` unless an asset-specific rule overrides it |
| **Prices** | Quantize to `0.0001` unless venue or asset metadata requires a different tick size |
| **Quantities** | Quantize to `0.00000001` unless asset metadata requires a different lot size |
| **Ratios / percentages** | Preserve as parsed and quantize only at the consuming policy boundary |

When a future venue or asset requires a tighter rule, the asset-specific rule should override the default instead of creating a new local convention.

## Storage Format

Persisted Decimal values must be stored in a **canonical exact string representation**. Storage must never require a float round trip.

The storage contract is as follows:

| Boundary | Required format |
|---|---|
| **SQLite JSON payloads** | Canonical Decimal string |
| **Structured persistence fields** | Text or equivalent exact string representation |
| **Logs and audit payloads** | Canonical Decimal string when financial truth matters |
| **External API payloads** | String format expected by the downstream API, derived from the canonical Decimal value |

## Comparison Rules

Financial comparisons must not rely on binary floating-point tolerances once Decimal migration begins.

The comparison contract is:

| Comparison case | Rule |
|---|---|
| **Exact equality** | Compare canonical Decimal values directly |
| **Boundary checks** | Compare after applying the same quantization rule on both sides |
| **Sorted ordering** | Compare Decimals after canonical parsing |
| **Cross-source reconciliation** | Parse both sides canonically, then compare under the same boundary precision |

## Asset-Specific Rules

The default rules above are repository-wide fallbacks. Asset-specific precision may later be introduced for cases such as equities, crypto, FX, fractional shares, or venue-specific tick sizes.

When asset-specific rules are added, they must be implemented through shared policy helpers rather than scattered conditionals in domain or adapter code.

## Migration Boundaries

The migration to Decimal must proceed in explicit stages.

| Boundary | Required migration behavior |
|---|---|
| **Domain models** | Financial fields move from float semantics to Decimal semantics |
| **Broker adapters** | Incoming broker quantities, prices, and notionals parse to Decimal at the adapter boundary |
| **Persistence** | Decimal values serialize canonically and restore without loss |
| **Reconciliation** | Broker truth and internal truth compare using Decimal-aware rules |
| **Risk and exposure** | Limits, exposures, and balances operate on Decimal values |

## Transitional Guardrails

During the transition period before the full Decimal migration milestone:

1. New shared helpers should be used instead of local formatting or parsing helpers when Decimal-related work is necessary.
2. New production-path financial logic should avoid introducing additional float-specific comparison hacks.
3. Any unavoidable float boundary should be clearly identified as transitional and should terminate in a shared conversion helper.

## Shared Helper Contract

The repository should expose shared helpers for:

| Helper responsibility | Purpose |
|---|---|
| **Canonical parsing** | Convert transitional numeric inputs into a validated Decimal |
| **Money quantization** | Apply the default money precision contract |
| **Price quantization** | Apply the default price precision contract |
| **Quantity quantization** | Apply the default quantity precision contract |
| **Canonical serialization** | Emit a stable string representation for persistence and API boundaries |

## Implementation Note

This document defines the **authoritative policy**. It does not by itself complete the broader Decimal migration. Later milestones must align code, persistence, broker normalization, reconciliation, and tests with this policy.
