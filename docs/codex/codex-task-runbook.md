# Codex Task Runbook

Use this runbook as the recommended prompt sequence for Dwellio.

## Session start prompt

```text
Read:
- docs/codex/codex-system-context.md
- docs/codex/codex-guardrails.md
- docs/codex/codex-prompt-library.md
- docs/runbooks/CANONICAL_PRECEDENCE.md
- docs/source_of_truth/*
- docs/architecture/*
- docs/runbooks/*

Use source-of-truth docs as highest authority.
Use actual repo schema/code as higher authority than imported planning docs.
Stay inside the scope I give next.
```

## Step 1 — repository audit

```text
Inspect repository structure and return:
1. current architecture state
2. missing components
3. stale or duplicate files
4. best next implementation step

Do not modify files.
```

## Step 2 — schema audit

```text
Review:
- app/db/migrations/
- app/db/views/
- sql/dwellio_full_schema.sql

Identify mismatches with docs/source_of_truth/DWELLIO_SCHEMA_REFERENCE.md.
Return issues but do not edit yet.
```

## Step 3 — schema reconciliation

```text
Fix schema mismatches.
Maintain migration ordering.
Add indexes if beneficial.
Do not recreate a second canonical search or quote system.
```

## Step 4 — model audit

```text
Compare Pydantic models against schema and read models.
Return mismatches.
```

## Step 5 — model reconciliation

```text
Update models to match schema and read models.
```

## Step 6 — ETL audit

```text
Review ETL jobs and verify alignment with:
- parcel_sales
- neighborhood_stats
- comp_candidates
- valuation_runs
- parcel_savings_estimates
- decision_tree_results

Use docs/runbooks/TAX_DATA_PULL_REPO_MAPPING.md for any imported tax-data-pull naming differences.
```

## Step 7 — ETL reconciliation

```text
Update ETL jobs to match schema and architecture.
Preserve county adapter separation.
Preserve parcel-year centric design.
```

## Step 8 — API audit

```text
Review API endpoints against docs/architecture/api-contracts.md.

Verify:
- GET /search?address={query}
- GET /quote/{county_id}/{tax_year}/{account_number}
- GET /quote/{county_id}/{tax_year}/{account_number}/explanation
- POST /lead
```

## Step 9 — API implementation

```text
Implement or tighten missing API endpoints using read models.
Do not expose restricted data publicly.
```

## Step 10 — testing

```text
Create unit tests for:
- comp scoring
- valuation engine
- decision tree
- savings engine

Create integration tests for:
- search endpoint
- quote endpoint
- lead creation
```

## Step 11 — documentation sync

```text
Ensure docs/architecture and docs/runbooks reflect the current architecture.
Do not change docs/source_of_truth.
```
