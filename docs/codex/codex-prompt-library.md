# Codex Prompt Library

Use this file as the reusable prompt library for Dwellio implementation work.

Before major work, Codex should read:
- `docs/codex/codex-system-context.md`
- `docs/codex/codex-guardrails.md`
- `docs/runbooks/CANONICAL_PRECEDENCE.md`
- `docs/source_of_truth/*`
- `docs/runbooks/ARCHITECTURE_STATE.md`
- `docs/runbooks/ARCHITECTURE_MAP.md`

## Session anchor prompt

```text
You are working inside the Dwellio repository.

Before making any changes, read:
- docs/codex/codex-system-context.md
- docs/codex/codex-guardrails.md
- docs/runbooks/CANONICAL_PRECEDENCE.md
- docs/source_of_truth/AGENT_RULES.md
- docs/source_of_truth/CANONICAL_CONTEXT.md
- docs/source_of_truth/PLATFORM_IMPLEMENTATION_SPEC.md
- docs/source_of_truth/QUOTE_ENGINE_PRODUCT_SPEC.md
- docs/source_of_truth/DWELLIO_BUILD_PLAN.md
- docs/source_of_truth/DWELLIO_CODEX_CONTEXT.md
- docs/source_of_truth/DWELLIO_SCHEMA_REFERENCE.md
- docs/runbooks/ARCHITECTURE_STATE.md
- docs/runbooks/ARCHITECTURE_MAP.md

Treat the source-of-truth docs as highest precedence.
Treat the actual repo schema and code as higher authority than architecture summaries or imported planning docs.

Hard rules:
1. Dwellio is Python-first for backend and ETL.
2. Frontend may use React/Next.js, but backend authority remains Python unless explicitly changed in source-of-truth docs.
3. Public quote APIs must use precomputed read models.
4. Restricted MLS/listing data must never appear in public APIs.
5. defensible_value = min(market_value_model_output, unequal_appraisal_model_output).
6. Preserve parcel-year centric design.
7. Preserve valuation history.
8. Do not silently create duplicate canonical tables or parallel search systems.

Work only within the scope I provide next.
If you see a safe improvement, suggest it.
If the improvement changes architecture, explain it before making the change.
```

## Database audit prompt

```text
Focus only on:
- app/db/migrations/
- app/db/views/
- sql/dwellio_full_schema.sql

Tasks:
1. validate migration ordering
2. validate foreign keys and indexes
3. validate v_search_read_model and v_quote_read_model
4. identify schema drift against docs/source_of_truth/DWELLIO_SCHEMA_REFERENCE.md

Do not edit models, jobs, services, or APIs yet.
Return mismatches and recommended fixes.
```

## ETL audit prompt

```text
Focus only on app/jobs/ and current schema.

Verify that jobs align to:
- parcel_sales
- neighborhood_stats
- comp_candidates
- valuation_runs
- parcel_savings_estimates
- decision_tree_results

Use docs/runbooks/TAX_DATA_PULL_REPO_MAPPING.md when reconciling imported tax-data-pull concepts.
Return mismatches and exact read/write expectations.
```

## API audit prompt

```text
Focus only on:
- app/api/
- app/models/
- app/db/views/
- docs/architecture/api-contracts.md

Verify:
- GET /search?address={query} uses v_search_read_model
- GET /quote/{county_id}/{tax_year}/{account_number} uses v_quote_read_model
- GET /quote/{county_id}/{tax_year}/{account_number}/explanation uses quote-safe explanation fields only
- POST /lead preserves parcel-year context

Return mismatches and recommended fixes.
```
