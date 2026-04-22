# MVP V1 Stage 2 County Data Ops Playbook

This playbook is the single operator-facing contract for Stage 2.

It consolidates the repo's existing county onboarding, readiness, manual prep, and recovery guidance into one execution path for Harris and Fort Bend county-years.

Stage 2 tickets covered here:

- `S2-T1` Finalize Harris/Fort Bend county-year onboarding checklist
- `S2-T2` Define the raw -> ready -> register -> ingest workflow contract
- `S2-T3` Clarify readiness dashboard status meanings
- `S2-T4` Document publish / rollback / retry-maintenance rules
- `S2-T5` Create schema-drift, malformed-file, and historical-validation SOP

Supporting evidence lives in:

- [COUNTY_ONBOARDING_CONTRACT.md](./COUNTY_ONBOARDING_CONTRACT.md)
- [MANUAL_COUNTY_FILE_PREP.md](./MANUAL_COUNTY_FILE_PREP.md)
- [ADMIN_COUNTY_YEAR_READINESS.md](./ADMIN_COUNTY_YEAR_READINESS.md)
- [HISTORICAL_BACKFILL_AND_READINESS.md](./HISTORICAL_BACKFILL_AND_READINESS.md)
- [HISTORICAL_VALIDATION_AND_TRENDS.md](./HISTORICAL_VALIDATION_AND_TRENDS.md)
- [OPS_AND_RECOVERY.md](./OPS_AND_RECOVERY.md)

## 1. County-year operator checklist

Run this checklist per county-year. Do not skip steps because a later dashboard looks healthy.

| Step | Operator question | Done when | Primary evidence |
|---|---|---|---|
| Raw files present | Are the expected raw files downloaded for this county-year? | All required raw files exist in the canonical year-scoped raw directory. | `MANUAL_COUNTY_FILE_PREP.md` |
| Prep complete | Were adapter-ready files and manifests generated successfully? | Ready files and manifests exist for each required dataset without prep errors. | `MANUAL_COUNTY_FILE_PREP.md` |
| Manual register or backfill queued | Were ready files handed into ingestion through the supported path? | Manual registration or bounded backfill command completed with explicit file/year/county inputs. | `HISTORICAL_BACKFILL_AND_READINESS.md` |
| Validation reviewed | Did staging and publish-control validation complete without unreviewed blockers? | Validation findings are either clear or explicitly reviewed/escalated. | `ADMIN_COUNTY_YEAR_READINESS.md`, `OPS_AND_RECOVERY.md` |
| Canonical publish verified | Did the intended import batch reach canonical publish? | The target batch shows canonical publish success and the intended year is now represented in readiness. | `COUNTY_ONBOARDING_CONTRACT.md`, `OPS_AND_RECOVERY.md` |
| Derived/read-model checks passed | Are parcel summary, search support, and quote-support signals aligned with expectations? | Readiness and onboarding signals confirm the intended downstream state for the county-year. | `ADMIN_COUNTY_YEAR_READINESS.md`, `COUNTY_ONBOARDING_CONTRACT.md` |
| Rollback path understood | If the publish is wrong, do we know which batch to reverse and how? | The import batch id is known and rollback/retry rules have been checked before any rerun. | `OPS_AND_RECOVERY.md`, `HISTORICAL_BACKFILL_AND_READINESS.md` |

Required datasets for Stage 2 county-year onboarding:

- Harris `property_roll`
- Harris `tax_rates`
- Fort Bend `property_roll`
- Fort Bend `tax_rates`

## 2. Raw -> ready -> register -> ingest workflow contract

This is the canonical Stage 2 workflow. Use the exact county-year scoping below so one year does not silently overwrite another.

1. `raw`
   Raw county exports live under a year-scoped raw root such as `~/county-data/2026/raw/harris/...`.
2. `ready`
   `prepare_manual_county_files` writes adapter-ready files plus manifests under the matching year-scoped ready root.
3. `register`
   Use `register_manual_import` when onboarding a single dataset file or use `run_historical_backfill` when the ready set for the county-year is prepared.
4. `ingest`
   The ingestion pipeline stages, validates, normalizes, publishes, and runs post-commit maintenance through the existing import-batch workflow.

Operator rules:

- Keep `raw` and `ready` roots year-scoped.
- Treat the manifest as the source of truth for what raw files were used to generate a ready artifact.
- Do not infer batch identity from "latest" when retrying, publishing, or rolling back an existing batch. Use the explicit `import_batch_id`.
- Do not call a county-year quote-ready until the downstream readiness signals, not just raw-file presence, say it is ready.

Recommended commands:

```bash
python3 -m infra.scripts.prepare_manual_county_files \
  --county-id both \
  --tax-year 2026 \
  --dataset-type both \
  --raw-root ~/county-data/2026/raw \
  --ready-root ~/county-data/2026/ready
```

```bash
python3 -m infra.scripts.run_historical_backfill \
  --counties harris fort_bend \
  --tax-years 2026 \
  --dataset-types property_roll tax_rates \
  --ready-root ~/county-data/2026/ready
```

## 3. Readiness dashboard status meanings

The readiness API and admin page already use real status fields. This glossary is the Stage 2 operator interpretation contract.

### County-year `overall_status`

| Status | Meaning | Operator interpretation |
|---|---|---|
| `tax_year_missing` | The year is not seeded in readiness inputs. | Do not troubleshoot downstream gaps before seeding or confirming year support. |
| `awaiting_source_data` | No meaningful raw-file/import activity exists yet. | Acquisition or registration is still missing. |
| `source_acquired` | Raw files or an import batch exist, but canonical publish is incomplete. | Stay in prep/ingestion review; do not treat the county-year as searchable. |
| `canonical_partial` | Some canonical work exists, but downstream read models are not ready enough. | Publish happened at least partially; keep working through derived blockers. |
| `derived_ready` | Parcel summary and search-support layers are ready. | The county-year supports the public read-model foundation, but not necessarily quote readiness. |
| `quote_ready` | Quote read-model rows exist for the county-year. | This county-year is ready for the refined quote path at the maturity currently implemented. |

### Dataset `stage_status`

| Status | Meaning | Operator interpretation |
|---|---|---|
| `tax_year_missing` | Dataset year is not configured/seeded. | Stop and confirm county-year support first. |
| `awaiting_source_data` | No raw files have been registered. | Stay in source acquisition or manual prep. |
| `source_acquired` | Files exist, but canonical publish has not completed. | Inspect validation, staging, and publish state. |
| `canonical_published` | Canonical publish completed for the dataset. | Move to derived/read-model checks and any maintenance follow-up. |
| `publish_blocked` | Validation or publish controls block promotion. | Review findings and escalate if the blocker is not clearly operator-fixable. |

### Common blocker codes

| Blocker | Meaning | Usual next action |
|---|---|---|
| `manual_backfill_required` | Ready files exist or are expected, but bounded backfill has not been completed. | Run the supported registration/backfill path. |
| `source_not_acquired` | Required source data is missing. | Confirm download completeness and raw-file placement. |
| `staging_validation_failed` | Validation found blocking issues before publish. | Review validation findings before retrying. |
| `canonical_publish_pending` | Batch exists but has not reached canonical publish. | Inspect import-batch state and publish controls. |
| `recent_job_failures` | One or more recent jobs failed for the county-year. | Review the failed job/step before rerunning. |
| `validation_regression` | Validation got materially worse relative to prior expectations. | Compare against the historical validation baseline before promoting. |

## 4. Publish, rollback, and retry-maintenance rules

### Publish

Use publish when:

- the intended batch is the correct county/year/dataset
- staging validation is complete
- publish-control warnings have been reviewed
- you are ready to promote this batch's canonical state

Do not publish when:

- you are still relying on guessed "latest batch" identity
- the batch has unresolved blocking validation errors
- the county-year still needs schema or raw-file investigation

### Rollback

Use rollback only when:

- a published batch needs to be reversed
- the batch identity is known
- the issue is a canonical publish correction, not just a maintenance retry

Do not use rollback for:

- prep-only mistakes where canonical publish never happened
- generic "start over" behavior
- unresolved uncertainty about which published batch is active

### Retry-maintenance

Use retry-maintenance when:

- canonical publish succeeded
- the failure happened in post-commit maintenance or refresh work
- the correct action is to repair derived/read-model state, not reverse canonical state

Default decision rule:

- publish-state wrong -> rollback
- canonical publish correct but downstream maintenance failed -> retry maintenance
- source/prep wrong before publish -> fix prep or re-register, do not roll back a publish that never occurred

## 5. Schema-drift, malformed-file, and historical-validation SOP

### Schema drift

Symptoms:

- prep script reports missing required columns
- county export naming or shape changes
- manual prep succeeds for one year but fails for the new year with structural differences

Operator response:

1. Stop before reusing older manifests or forcing the file through registration.
2. Confirm the raw file is the intended county export and not a renamed partial download.
3. Compare the new file shape to the canonical raw-file contract in `MANUAL_COUNTY_FILE_PREP.md`.
4. Escalate as schema drift if the county changed shape rather than the operator misplacing the file.

### Malformed files

Symptoms:

- parse failures
- invalid row counts
- manifest `parse_issue_count` or `validation_error_count` is non-zero

Operator response:

1. Treat the output as not ready for backfill.
2. Inspect the manifest and the offending raw file.
3. Re-prep only after the raw input is corrected or replaced.
4. Escalate if the file appears to be an authentic county export that no longer matches the supported parser contract.

### Historical validation anomalies

Symptoms:

- current year materially lags the recommended validation baseline
- readiness regresses year-over-year without a clear source limitation
- validation reports show suspicious drops in coverage or quote-support signals

Operator response:

1. Use the onboarding contract to choose the recommended validation baseline instead of defaulting to the newest year.
2. Compare current year against baseline on canonical publish, search support, and quote support.
3. Treat source limitations documented in the county capability matrix differently from unexplained regressions.
4. Escalate suspicious drops before promoting a county-year as quote-ready.

## 6. Escalate versus continue

Continue without escalation when:

- a raw file is simply missing or misnamed
- a known publish-control warning needs operator review
- a maintenance retry is the clear next step after canonical publish

Escalate when:

- county exports changed schema in a way the prep contract no longer supports
- a readiness regression contradicts the county capability matrix
- rollback versus retry is not clear from the batch history
- the validation baseline cannot be selected from the available years

## 7. Stage 2 completion evidence

Stage 2 is complete for planning-contract purposes when:

- this playbook is the operator's single Stage 2 entrypoint
- the existing runbooks remain the deeper procedural references
- readiness vocabulary here matches the actual admin/readiness models
- publish/rollback/retry decisions can be made without developer-only tribal knowledge

This playbook does not claim that every county-year is already healthy.
It claims that the operator contract for Stage 2 is now explicit, consistent, and repo-native.
