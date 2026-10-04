# Dwellio Architecture State

This is the current repository implementation-status ledger. Approved product
and design documents describe intended behavior and future work; runbooks
describe operating procedures. Code and ordered migrations supply implementation
evidence. Historical summaries do not override current repository evidence.

## Verification metadata

- Last verified: `2026-10-03`, by Codex.
- Repository baseline inspected: `a5ca4cc3e2cbeaabaedd173ffc92256660b291b0`
  on `repo-stabilization`; application baseline is PR #62 merge `7cce266`.
- Latest migration: `0079_unequal_roll_final_value_logic.sql`.
- Scope: static repository verification, not deployment or launch readiness.
- Owner decisions: [approved Human Review Queue](audit/HUMAN_REVIEW_QUEUE.md#approved-decisions--october-3-2026).
- Verification performed: router mounting and handler source review; API/service
  call-chain review; public web client and page inventory; migration and service
  inventory; job parser construction reproduced from its AST in isolation.
- Rebaseline verification: no database connection, application startup, database-dependent tests, full
  Python suite, lint suite, or frontend build/test run was performed for this
  documentation change. Earlier test results are historical evidence, not
  results of this rebaseline.

The previous rebaselining warning is replaced by this bounded verification
record. `IMPLEMENTED` describes wired repository capability; it does not prove
successful operation against live data or production approval.

## Status legend

| Status | Meaning |
| --- | --- |
| `IMPLEMENTED` | Meaningful executable code exists and is wired to the current application or operator path. |
| `PARTIAL` | Meaningful implementation exists, but the named workflow has gaps or incomplete integration. |
| `STUB` | A placeholder or schema foundation exists without the completed named workflow. |
| `UNKNOWN` | No executable workflow was established by this review; no roadmap commitment is inferred. |
| `GOVERNED_NOT_PRODUCTION` | Substantial governed tooling exists and is retained, with production integration requiring separate approval. |
| `DEFERRED` | Future work explicitly deferred by the owner. |
| `SUPERSEDED` | A named approach has explicitly been replaced; age alone does not establish this status. |

## Verified inventory

| Area | Count | Counting rule |
| --- | ---: | --- |
| Application handlers | 32 | Route decorators in the six route families mounted by `app/api/router.py`; excludes framework-generated docs/OpenAPI routes. |
| Service modules | 60 | `app/services/*.py`, excluding `__init__.py`. |
| Registered jobs | 17 | Entries in `app/jobs/cli.py:JOB_REGISTRY`; registration does not establish implementation or CLI operability. |
| Operational scripts | 48 | Audit baseline: 46 Python script modules and two shell launchers, excluding package/readme files. |
| SQL migrations | 77 | Files in `app/db/migrations/*.sql`, ending at `0079`; numbering has gaps. |
| Frontend pages | 19 | `page.tsx` files below `apps/web/app/`. |

See [Active System Index](audit/ACTIVE_SYSTEM_INDEX.md) for the baseline counting
rules. Handlers, services, migrations, pages, and registry entries were recounted
for this rebaseline; the operational-script count is retained from the audit.

## Current implementation state

All paths below are repository-relative evidence.

| Subsystem | Status | Current reality | Evidence |
| --- | --- | --- | --- |
| Backend/API framework | `IMPLEMENTED` | FastAPI mounts health, search, parcel, quote, lead, and protected admin router families. | `app/main.py`, `app/api/router.py` |
| Public search/autocomplete | `IMPLEMENTED` | Address search and autocomplete handlers are mounted. | `app/api/routes/search.py`, `app/services/search_index.py` |
| Public parcel summary | `IMPLEMENTED` | Public parcel-year summary has a dedicated response contract and service path. | `app/api/routes/parcel.py`, `app/models/parcel.py`, `app/services/parcel_summary.py` |
| Refined quote/explanation | `IMPLEMENTED` | Mounted handlers use `QuoteReadService`; the public web client calls these endpoints. | `app/api/routes/quote.py`, `app/api/quote.py`, `app/services/quote_read.py`, `apps/web/app/_lib/public-api.ts` |
| Instant quote | `PARTIAL` | Separate mounted backend service with serving cache, refresh/validation, assessment basis, warning taxonomy, county capability, tax profile, shadow savings, and rollout support. Public web integration is deferred. | `app/services/instant_quote.py`, `app/jobs/job_refresh_instant_quote.py`, `app/jobs/job_validate_instant_quote.py`, migrations `0044`–`0051` and `0057`–`0063`, `apps/web/app/_lib/public-api.ts` |
| Lead capture | `IMPLEMENTED` | `POST /lead` connects to lead persistence and attribution/context support. | `app/api/routes/leads.py`, `app/services/lead_capture.py`, migration `0042_stage16_lead_funnel_backend_contracts.sql` |
| Admin lead reporting | `IMPLEMENTED` | Protected list/detail routes and pages support reporting and duplicate/event review. | `app/api/routes/admin.py`, `app/services/admin_lead_reporting.py`, `apps/web/app/admin/leads/` |
| Public web funnel | `PARTIAL` | Search → parcel → refined quote/explanation → lead capture exists; represented-customer onboarding is incomplete. | `apps/web/app/search/page.tsx`, `apps/web/app/parcel/[countyId]/[taxYear]/[accountNumber]/page.tsx`, `apps/web/app/_components/LeadCaptureCard.tsx`, `apps/web/app/_lib/public-api.ts` |
| County ingestion services | `IMPLEMENTED` | Acquisition, staging, normalization, validation, publish/rollback, lineage and maintenance services exist; standard job CLI parser and dispatch are repaired. | `app/ingestion/service.py`, `app/api/routes/admin.py`, `app/jobs/cli.py` |
| Standard job CLI | `IMPLEMENTED` | Parser and dispatch support all 17 registered names; repeated/file account inputs use one ordered, deduplicated path. Job implementations and live execution remain separate verification boundaries. | `app/jobs/cli.py:build_parser`, `tests/unit/test_jobs_cli.py`, `audit/DISCREPANCY_REPORT.md` |
| Harris county adapter | `PARTIAL` | Registered acquisition/parse/normalize/validation support includes fixture and source acquisition paths; current live readiness is unverified. | `app/ingestion/registry.py`, `app/county_adapters/harris/`, `config/counties/` |
| Fort Bend county adapter | `PARTIAL` | Registered adapter and supported characteristic normalization exist; current live readiness is unverified. | `app/ingestion/registry.py`, `app/county_adapters/fort_bend/`, `app/services/fort_bend_bathroom_features.py` |
| County characteristic contracts | `IMPLEMENTED` | Later migrations add canonical Fort Bend living area, Harris total rooms, and Fort Bend valuation bathroom features; coverage is not inferred from schema. | migrations `0064`–`0066`, `app/county_adapters/harris/normalize.py`, `app/county_adapters/fort_bend/normalize.py`, `app/services/fort_bend_bathroom_features.py` |
| County-year readiness/admin operations | `IMPLEMENTED` | Protected readiness, onboarding, scalability, source/validation inspection, manual registration, publish/rollback and retry-maintenance handlers exist. | `app/api/routes/admin.py`, `app/services/admin_ops.py`, `app/services/admin_readiness.py`, `app/services/county_onboarding.py`, `apps/web/app/admin/ops/` |
| Case operations foundation | `PARTIAL` | Internal list/detail/create, notes, status history and hearing-linked review exist; full operator workbench is not established. | `app/services/case_ops.py`, `app/api/routes/admin.py`, `apps/web/app/admin/cases/`, migration `0041_stage14_case_ops_foundation.sql` |
| Evidence packet review | `PARTIAL` | Internal packet records, items and comp-set review exist; record creation is not final document generation. | `app/services/case_ops.py`, `app/api/routes/admin.py`, `apps/web/app/admin/packets/`, migration `0022_case_ops_and_evidence.sql` |
| Final packet/PDF generation and refresh | `STUB` | Generator returns `not_implemented`; registered refresh job logs start/finish around a TODO. Retain and label; completion is separate future work. | `app/services/packet_generator.py`, `app/jobs/job_packet_refresh.py` |
| Unequal-roll workflow | `GOVERNED_NOT_PRODUCTION` | Subject snapshots, discovery, eligibility, scoring, ranking, shortlist/final selection, chosen-comp semantics, governance, adjustments, final value and analyst evidence exist. No unequal-roll API router or public UI integration is mounted. | migrations `0067`–`0079`, `app/services/unequal_roll_*.py`, `app/api/router.py`, `infra/scripts/*unequal_roll*` |
| Unequal-roll experiments/replay | `GOVERNED_NOT_PRODUCTION` | No-persist replay, reranking, smart-harvest and taxpayer-favorable tiebreak tooling remain distinct from an approved production path; none are declared dead code. | `app/services/unequal_roll_no_persist_replay.py`, `app/services/unequal_roll_smart_harvest.py`, `app/services/unequal_roll_taxpayer_favorable_tiebreak.py`, `infra/scripts/*unequal_roll*` |
| Customer accounts/onboarding | `STUB` | `clients` schema exists; no complete customer account/authentication or represented-customer onboarding workflow was established. | migration `0021_business_flow.sql`, `app/api/router.py`, `apps/web/app/` |
| Representation agreements | `STUB` | Agreement schema supports status/signature/document fields, without executable generation/signing workflow. | migration `0021_business_flow.sql`, `app/api/routes/`, `app/services/` |
| E-sign completion | `UNKNOWN` | No executable e-sign provider, callback or authorization completion path was established. | reviewed `app/api/routes/`, `app/services/`, `apps/web/app/` |
| Invoicing | `STUB` | Invoice schema exists without a complete invoice product workflow. | migration `0023_financials_and_ops.sql`, `app/api/routes/`, `app/services/` |
| Payments | `UNKNOWN` | No executable payment/provider workflow was established. | reviewed `app/api/routes/`, `app/services/`, `apps/web/app/` |
| County filing and confirmation proof | `UNKNOWN` | No county submission adapter, automated filing route or confirmation-proof workflow was established. | reviewed `app/api/router.py`, `app/api/routes/`, `app/services/` |
| Customer dashboard/notifications | `UNKNOWN` | Admin case/packet pages exist; no customer case portal or customer notification workflow was established. | `apps/web/app/`, `app/api/router.py` |
| Observability/reporting | `PARTIAL` | Job/ingestion tracking, internal readiness and reporting scripts exist; complete centralized production monitoring is unverified. | `app/jobs/runner.py`, `app/ingestion/service.py`, `app/services/admin_readiness.py`, `infra/scripts/report_quote_quality_monitor.py` |
| Additional registered placeholder jobs | `STUB` | Geocode repair, sales ingestion and comp-candidate jobs contain TODO implementations; registry membership does not complete these workflows. | `app/jobs/job_geocode_repair.py`, `app/jobs/job_sales_ingestion.py`, `app/jobs/job_comp_candidates.py` |

## Public surface

Mounted public application handlers:

- `GET /healthz`
- `GET /search`
- `GET /search/autocomplete`
- `GET /parcel/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}/explanation`
- `GET /quote/instant/{county_id}/{tax_year}/{account_number}`
- `POST /lead`

Evidence: `app/api/router.py`, `app/api/routes/{health,search,parcel,quote,leads}.py`.
The public web client uses refined quote/explanation; it does not call instant
quote. Backend-only describes the current integration status, not an access
restriction: the instant route is mounted in the public quote router.

Public-safe response requirements are defined by the response models and
[public route/funnel contract](architecture/PUBLIC_ROUTE_AND_FUNNEL_CONTRACT.md).
This review does not certify live payloads or data quality.

## Protected internal surface

All 24 admin handlers are mounted with `require_admin_access` as a router
dependency in `app/api/routes/admin.py`.

- Leads: list and detail.
- County readiness, onboarding contract and scalability review.
- Search inspection.
- Import-batch list/detail, validation, source files, completeness and
  tax-assignment inspection.
- Manual import registration, publish, rollback and retry-maintenance.
- Cases: list/create/detail, add note and update status.
- Packets: list/create/detail.

Evidence: `app/api/routes/admin.py`, `app/api/deps/admin_auth.py`,
`apps/web/app/admin/`. Backend handlers do not require a matching frontend
page to count as implemented. Case and packet routes are internal foundations,
not customer accounts, final PDFs or county submission.

## Schema and governance boundaries

Ordered migrations are the authoritative repository schema implementation
record. There are 77 files through `0079`, not 79 sequential files.
`sql/dwellio_full_schema.sql` remains an unverified reference; generation,
synchronization and maintenance ownership have not been established.

Harris and Fort Bend adapters are present. Supported inputs, years and property
segments depend on county configuration and normalization/readiness logic;
this review does not establish uniform county coverage or current live readiness.
County characteristic and unequal-roll migrations add foundations and tooling,
not automatic product availability.

Remediation, validation, rollout and audit evidence must be retained and indexed.
The first Human Review Gate is resolved. No path-specific cleanup approval has
been given, and no historical migration or experiment is superseded here.

## Outstanding work and verification limits

1. CLI parser/dispatch repair completed on October 3, 2026: removed duplicate
   account flags and the obsolete dispatch block. All 26 tests in
   `tests/unit/test_jobs_cli.py` pass, and `python3 -m app.jobs.cli --help`
   exits successfully. Dispatch tests mock job execution; no database job ran.
2. Triage correctness-related lint separately. The ingestion service still has
   `ImportBatchRecord` annotations without a corresponding import; historical
   lint counts are not a fresh lint result.
3. Configure and verify the frontend test runner separately. Three `.test.mts`
   files exist under `apps/web/app/_lib/`, but `apps/web/package.json` has no
   test script. Presence of test files does not establish an executable suite.
4. Verify runtime/API and database behavior in the appropriate isolated
   development environment. This rebaseline contacted no database.
5. Plan public instant-quote integration, final packet generation and filing
   separately. Customer accounts, agreements, e-sign, billing and payment
   foundations or gaps remain as classified above.

CLI, correctness lint and frontend test-runner repairs are owner-approved in
that order. CLI repair is complete; correctness lint and frontend test-runner
work remain outstanding. The architecture
ledger rebaseline does not imply that other authority documents have been
rewritten or that consolidated-schema maintenance has been settled.

## Maintenance rules

Update this ledger when routes, county support, migrations, job operability,
workflow integration or governance status materially change. Each status change
must cite executable repository evidence and describe its integration boundary.
A table, test file, runbook or registered stub alone is insufficient evidence
of a completed workflow. Record which checks actually ran and their limitations.

Keep future intent in approved design/product documents. Do not promote governed
work, infer production readiness, mark evidence historical, or remove paths
solely because a document or implementation is old. Record separately approved
scope and owners when those decisions exist.

## Change log

- `2026-10-03`: Repaired job CLI argument registration and dispatch; verified
  26 CLI regression tests and module help. No job implementation was changed.

- `2026-10-03`: Reverified repository status through migration `0079` against
  `a5ca4cc`; applied the nine owner-approved decisions, separated governed
  unequal-roll tooling and schema/stub foundations from integrated workflows,
  documented the blocked CLI and verification limits, and removed the temporary
  rebaselining warning.
- `2026-04-24`: Added protected admin lead reporting status and evidence.
- `2026-04-22`: Updated public parcel payload and contract documentation.
- `2026-04-20`: Established the implementation-status ledger.
