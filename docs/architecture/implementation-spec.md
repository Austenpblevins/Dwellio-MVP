# Dwellio Implementation Specification (Final Reconciled)

## Canonical precedence
1. `docs/source_of_truth/CANONICAL_CONTEXT.md`
2. `docs/source_of_truth/PLATFORM_IMPLEMENTATION_SPEC.md`
3. `docs/source_of_truth/QUOTE_ENGINE_PRODUCT_SPEC.md`
4. `docs/source_of_truth/DWELLIO_BUILD_PLAN.md`
5. `docs/source_of_truth/AGENT_RULES.md`
6. `docs/source_of_truth/DWELLIO_CODEX_CONTEXT.md`
7. `docs/source_of_truth/DWELLIO_SCHEMA_REFERENCE.md`

`docs/source_of_truth/DWELLIO_MASTER_SPEC.md` is a compatibility entry point to this source-of-truth set.

## Core rule
For each parcel-year:
- compute a market-value model
- compute an unequal-appraisal model
- set `defensible_value_point = min(market_value_point, equity_value_point)`

## Public quote path
1. normalize address
2. resolve parcel via `v_search_read_model`
3. fetch latest precomputed quote-safe row via `v_quote_read_model`
4. return quote + explanation + recommendation
