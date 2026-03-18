# DWELLIO_MASTER_SPEC.md

## Purpose

This file exists as a compatibility entry point for older prompts, docs, and workflows
that still refer to `DWELLIO_MASTER_SPEC.md`.

It should not be treated as a separate parallel spec.
Instead, references to `DWELLIO_MASTER_SPEC.md` map to the current combined
source-of-truth document set listed below.

## Canonical Mapping

When a prompt, runbook, or doc references `DWELLIO_MASTER_SPEC.md`, use the combined
authority of:

1. `CANONICAL_CONTEXT.md`
2. `PLATFORM_IMPLEMENTATION_SPEC.md`
3. `QUOTE_ENGINE_PRODUCT_SPEC.md`
4. `DWELLIO_BUILD_PLAN.md`
5. `DWELLIO_SCHEMA_REFERENCE.md`
6. `AGENT_RULES.md`
7. `DWELLIO_CODEX_CONTEXT.md`

## Authority Notes

- `CANONICAL_CONTEXT.md` is the controlling authority map for document precedence.
- `PLATFORM_IMPLEMENTATION_SPEC.md` controls platform, schema, ingestion, and backbone architecture.
- `QUOTE_ENGINE_PRODUCT_SPEC.md` controls quote, valuation, savings, and recommendation logic.
- `DWELLIO_BUILD_PLAN.md` controls execution order.
- `DWELLIO_SCHEMA_REFERENCE.md` is the schema navigation reference.
- `AGENT_RULES.md` contains non-negotiable implementation rules.
- `DWELLIO_CODEX_CONTEXT.md` is the compact implementation context.

## Canonical Architecture Reminder

Dwellio is Python-first for backend/API and ETL.
Frontend may use React/Next.js, but frontend choices do not override Python backend authority
unless the source-of-truth docs are explicitly changed.

Public APIs must use precomputed read models.
Restricted MLS/listing data must never appear in public APIs.
Defensible value remains:

`defensible_value = min(market_model_output, unequal_appraisal_output)`

## Final Instruction

Do not create a second canonical architecture around this file.
Use it only as a stable compatibility pointer into the current source-of-truth set.
