# Admin Ingestion Ops UI

This internal-only surface gives operators one place to review ingestion status, validation findings, raw source files, manual fallback registration, publish/rollback controls, parcel completeness, tax assignment QA, and county-year readiness.

## Access

- The admin API is protected by `DWELLIO_ADMIN_API_TOKEN`.
- Operators sign in through `/admin/login`.
- The web app stores a temporary internal cookie and forwards the token only to admin routes.
- Public parcel, search, and quote routes do not expose admin-only blocker or QA details.

## Main Pages

- `/admin/ops`
  - overview with county-year readiness, QA counts, and recent import batches
- `/admin/ops/jobs`
  - import batch dashboard by county-year and dataset
- `/admin/ops/jobs/{importBatchId}`
  - batch detail, validation summary, raw files, job runs, maintenance step runs, publish control, rollback control, maintenance retry control
- `/admin/ops/validation?import_batch_id=...`
  - validation findings for a selected import batch
- `/admin/ops/source-files`
  - archived raw source files, storage paths, and checksums
- `/admin/ops/manual-upload`
  - manual import registration form for fallback/backfill workflows
- `/admin/ops/completeness`
  - parcel completeness review using `parcel_data_completeness_view`
- `/admin/ops/tax-assignment`
  - tax assignment QA using `v_parcel_tax_assignment_qa`
- `/admin/readiness`
  - county-year readiness and blockers across ingestion and derived layers

## Operator Workflow

1. Start in `/admin/ops` or `/admin/readiness` and choose the target county-year.
2. Open `/admin/ops/jobs` to inspect recent import batches.
3. Open a batch detail page to review validation findings, source files, and job runs.
4. If canonical publish succeeded but maintenance failed, use the batch detail `step_runs` and maintenance retry action before rerunning the full pipeline.
5. If county automation is weak or a fuller year is needed, use `/admin/ops/manual-upload` to register a manual import.
6. Publish only after validation results and QA surfaces are acceptable.
7. Use rollback only when a published batch needs to be reversed.
8. Review parcel completeness and tax assignment issues before downstream valuation or quote QA.

## Sparse-Year Guidance

- Prefer fuller historical years such as `2025` when `2026` is sparse or incomplete.
- Readiness blockers and low QA counts are signals to switch validation focus to a stronger year, not reasons to fabricate data.

## Boundaries

- This UI extends the existing ingestion backbone.
- It does not create a second ingestion, validation, or quote system.
- Admin-only diagnostics stay in admin routes and pages.
