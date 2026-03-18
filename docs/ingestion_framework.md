# Ingestion Framework

This document describes the current Stage 2 ingestion framework built on the approved Stage 1 schema.

## Lifecycle

The ingestion flow is intentionally stage-based:

1. `job_fetch_sources`
2. `job_load_staging`
3. `job_normalize`
4. `job_rollback_publish`

These stages are centered on:

- `job_runs`
- `import_batches`
- `raw_files`
- `validation_results`
- `lineage_records`

## Raw archive behavior

Stage 2 uses a filesystem-backed raw archive rooted at `DWELLIO_RAW_ARCHIVE_ROOT`.
`raw_files.storage_path` stores the relative archive path, while the raw payload itself is written to disk.

This keeps the Stage 1 schema intact and avoids inventing a temporary blob table just to move fixture data between jobs.

## Dry-run semantics

Dry-run mode is supported on:

- `job_fetch_sources`
- `job_load_staging`
- `job_normalize`

Dry-runs execute the adapter logic, validation, and structured stage logging, but the database transaction is rolled back before completion.
`job_fetch_sources --dry-run` also skips raw archive persistence, so no filesystem residue is left behind.

## Rollback semantics

Stage 2 rollback is implemented for the Harris `property_roll` dataset through `job_rollback_publish`.
Normalization captures a rollback manifest in the normalize `job_runs.metadata_json`, and rollback uses that manifest to remove the batch's canonical parcel-year effects and restore any prior canonical state that existed before the publish.

This keeps rollback bounded to the current Stage 2 canonical targets without introducing duplicate history tables or alternate publish systems.

## Adapter contract

All county adapters implement the same contract in [base.py](/Users/nblevins/Desktop/Dwellio/app/county_adapters/common/base.py):

- source discovery
- raw acquisition
- file-format detection
- staging parsing
- dataset validation
- canonical normalization
- publish metadata
- rollback hook
- adapter metadata

## Current county support

`HarrisCountyAdapter` is the first concrete Stage 2 adapter.
It uses fixture-driven `property_roll` data to prove the lifecycle without pretending to parse real Harris source files yet.

`FortBendCountyAdapter` is kept as a contract-complete scaffold and intentionally raises for county-specific acquisition/parsing work that belongs in a later stage.

## Verification

Use:

```bash
python -m infra.scripts.verify_stage2_ingestion --database-url postgresql://postgres:postgres@localhost:55433/postgres
```

The verifier applies pending migrations if needed, runs dry-run and committed fetch/staging/normalize passes for the Harris fixture dataset, then executes rollback and asserts that raw, staging, validation, lineage, canonical, dry-run, and rollback behavior all work as expected.
