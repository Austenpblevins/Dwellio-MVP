# Ingestion Framework

This document describes the shared ingestion framework established in Stage 2 and extended through Stage 5 on top of the approved Stage 1 schema.

## Lifecycle

The ingestion flow is intentionally stage-based:

1. `job_fetch_sources`
2. `job_load_staging`
3. `job_normalize`
4. `job_rollback_publish`

Stage 4-5 add two operator-facing entrypoints on top of the same framework:

5. `job_run_ingestion`
6. `job_inspect_ingestion`

These stages are centered on:

- `job_runs`
- `import_batches`
- `raw_files`
- `validation_results`
- `lineage_records`

## Raw archive behavior

The framework uses a filesystem-backed raw archive rooted at `DWELLIO_RAW_ARCHIVE_ROOT`.
`raw_files.storage_path` stores the relative archive path, while the raw payload itself is written to disk and later reconstructed with the stored file metadata needed by county parsers.

This keeps the Stage 1 schema intact and avoids inventing a temporary blob table just to move fixture data between jobs.

## Dry-run semantics

Dry-run mode is supported on:

- `job_fetch_sources`
- `job_load_staging`
- `job_normalize`

Dry-runs execute the adapter logic, validation, and structured stage logging, but the database transaction is rolled back before completion.
`job_fetch_sources --dry-run` also skips raw archive persistence, so no filesystem residue is left behind.

## Rollback semantics

Rollback is implemented for the shared `property_roll` publish path through `job_rollback_publish`.
Normalization captures a rollback manifest in the normalize `job_runs.metadata_json`, and rollback uses that manifest to remove the batch's canonical parcel-year effects and restore any prior canonical state that existed before the publish.

This keeps rollback bounded to the current parcel-year canonical targets without introducing duplicate history tables or alternate publish systems.

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

`HarrisCountyAdapter` is the first complete adapter on the framework.
Stage 4 extends it into a complete Harris `property_roll` workflow with config-backed source registry entries, fixture acquisition hooks, staging parsing, validation rules, canonical normalization, rerun support, and import-batch inspection.

`FortBendCountyAdapter` becomes the second complete county implementation in Stage 5.
It uses the same ingestion framework, but handles county-specific CSV parsing and exemption column expansion entirely inside Fort Bend adapter/config files.

## Verification

Use:

```bash
python -m infra.scripts.verify_stage2_ingestion --database-url postgresql://postgres:postgres@localhost:55433/postgres
```

The verifier applies pending migrations if needed, runs dry-run and committed fetch/staging/normalize passes for the Harris fixture dataset, then executes rollback and asserts that raw, staging, validation, lineage, canonical, dry-run, and rollback behavior all work as expected.

For the Stage 4 Harris-specific verifier, use:

```bash
python -m infra.scripts.verify_stage4_harris --database-url postgresql://postgres:postgres@localhost:55436/postgres
```

That verifier runs the full Harris lifecycle twice on a clean PostGIS database, confirms rerun behavior creates a new import batch, inspects persisted row counts and validation state, and checks that invalid Harris rows surface failed-record details through adapter validation.

For the Stage 5 Fort Bend verifier, use:

```bash
python -m infra.scripts.verify_stage5_fort_bend --database-url postgresql://postgres:postgres@localhost:55439/postgres
```

That verifier runs the full Fort Bend lifecycle twice on a clean PostGIS database, confirms rerun behavior creates a new import batch, inspects persisted row counts and validation state, checks invalid Fort Bend rows for failed-record details, and confirms Harris and Fort Bend can both publish through the shared framework without schema redesign.
