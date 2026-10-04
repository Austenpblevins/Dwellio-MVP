# Discrepancy Report

## Confirmed discrepancies

### 1. Job CLI parser failure — resolved October 3, 2026

`app/jobs/cli.py` registers `--account-number` at lines 70 and 84 and registers
`--account-numbers-file` at lines 71 and 85. Running
`python3 -m app.jobs.cli --help` fails while building the parser with:

```text
argparse.ArgumentError: argument --account-number: conflicting option string: --account-number
```

The preceding reproduction describes the audit baseline. The separately
approved repair removed duplicate account flag registration and the obsolete
`args.account_number` dispatch block, retaining the `account_numbers` path.
Verification: 26 CLI tests passed and module `--help` succeeded. Tests cover all
17 registered job names, repeated/file inputs, ordered deduplication, absent
inputs, help and optional argument forwarding. Job dispatch was mocked; no
database job ran and job implementations were unchanged.

### 2. Architecture status ledger predates current migrations

`docs/architecture-state.md` says it was verified against migration `0056`.
The repository contains 77 migrations through `0079`. The ledger must be
re-verified before its implementation claims are treated as authoritative.

### 3. Documentation authority is distributed

The source-of-truth, architecture, runbook, and final-summary collections have
overlapping descriptions of what is canonical, final, or authoritative. The
documents need an approved hierarchy and ownership/refresh policy.

## Important non-discrepancies / bounded conclusions

- There are exactly 48 operational script files under the defined baseline
  counting rule: script modules and launchers, excluding package/readme files.
- Packet review and schema foundations exist, but the generator and refresh job
  remain stubs; no final packet/PDF workflow was established.
- Instant-quote backend capability exists, but a complete customer-facing web
  integration was not established.
- Schema concepts for agreements and invoices do not establish onboarding,
  e-sign, billing, payment, or filing workflows.


## Correctness-related lint triage — October 3, 2026

A fresh `python3 -m ruff check app infra tests --output-format json` scan found
152 findings. The correctness repair resolves all three `F821` findings:

- Import `ImportBatchRecord` from the ingestion repository so both publish-control
  method annotations resolve. A regression test evaluates both method type hints.
- Define the evidence builder's missing `_as_int` helper. The subject-context
  path previously raised `NameError` for any returned subject row. Regression
  tests cover integer/string years, missing values and invalid values using a
  mocked database connection. Invalid optional values produce `None`, matching
  this script's forgiving float conversion convention.

Verification: 47 focused tests passed across ingestion service/repository,
unequal-roll review evidence and the evidence-builder regression suite.
The focused Ruff correctness selection passed. No live database was contacted.

The remaining 149 findings are retained for separately scoped maintenance:
61 import-order, 55 Python modernization (`UP017`, `UP034`, `UP037`, `UP035`),
11 mixed indentation, 8 unused imports, 6 module import placement, 3 unused
locals, 2 lambda-assignment, 2 constant-getattr and 1 non-strict-zip findings.
The mixed tabs occur inside SQL strings rather than Python control-flow
indentation. The tab-file loader explicitly pads short rows and its existing
`zip()` ignores surplus fields; no source contract establishing strict row
width was found. Unused locals were reviewed without evidence supporting a
behavior change. None of those findings is silently fixed or suppressed here.

Correctness lint triage is complete within this scope; full lint is not clean.
Frontend test-runner configuration was subsequently completed locally; see below.


## Frontend runner and API verification — October 3, 2026

Added `npm test` (native Node test discovery for `app/**/*.test.mts`) and
`npm run typecheck`; documented Node >=24.12 and updated matching lock metadata.
Installed dependencies from the existing lockfile using `npm ci`.
All 9 frontend tests, typecheck, ESLint and production build passed.
The runner initially exposed one stale test expectation: missing UTM fields
are `undefined` and omitted from JSON, rather than `null`. The test now checks
both the existing object and serialized contract; production code is unchanged.
A regression check confirms blank email is rejected before building a request.
Node emits module-type detection warnings for existing `.ts` modules; these do
not fail tests. Package-wide module semantics were not changed to hide warnings.

Next-step verification ran 50 tests across public parcel flows, Stage 15/16
workflow/release contracts, admin leads/ops/cases, health, lead capture, quote
and search routes. All passed with mocked services and the prescribed isolated
Stage 21 URL. The initial default-config collection failure was the repository's
protected-database guard, not the earlier cloud TestClient compatibility issue.
No live database was used by these selected tests.

Live isolated-database test verification subsequently completed. Docker was
started and the existing `dwellio_stage21_db_55442` container resumed. A read-only
identity check confirmed `stage21_dev` / `stage21_admin`; the public migration
ledger ends at `0066`. No database was dropped, restored or migrated in place.
Migration tests exercise later migrations in temporary schemas and roll back.

The full Python suite initially passed 752 tests with one stale CLI expectation
(list versus the retained ordered tuple). That expectation was updated without
changing dispatch behavior. The full rerun passed all 753 tests in the isolated
Stage 21 environment. This is test-suite verification, not live-data coverage,
production readiness, or verification of deployed unequal-roll workflows.
Current seeded public-schema workflows after `0066` remain unverified.
No new branch or push was performed; frontend and verification documentation
changes were initially kept local pending owner permission to publish.


## Isolated public-schema migration application — October 3, 2026

After explicit owner approval, applied exactly migrations `0067`–`0079` using
the repository migration runner against `stage21_dev` on `localhost:55442`.
Database identity was verified as `stage21_dev` / `stage21_admin` before writes.
The subsequent dry-run reports no pending migrations. All 77 repository
migration versions are recorded; the seeded ledger additionally retains the
historical `0053` and `0054` entries. The four unequal-roll foundation tables
exist in the public schema. No shared-database migration, restore, new branch,
commit or push was performed.

The post-application full suite returned **752 passed, 1 failed**. The failed
`test_unequal_roll_candidate_eligibility_migration_applies_in_isolated_schema`
exposes migration `0069`'s database-wide constraint-name existence check:
`pg_constraint.conname` is checked without restricting the target relation.
Once the public constraint exists, replaying the migration in a temporary
schema skips that schema's constraint. The public candidates table was checked
read-only and has the expected eligibility constraint allowing `pending`,
`eligible`, `review` and `excluded`; the applied public schema is not missing it.

Historical migration files were preserved. Repairing schema-scoped replay
requires a separately scoped forward repair/test strategy; the failing test
was not weakened or skipped. Earlier 753-pass results describe the pre-public-
migration database state. This failure was the verification
blocker before the forward repair below; live seeded unequal-roll workflows have not been certified.


## Replay issue resolved by forward repair — October 3, 2026

Owner authorized repairing replay while preserving historical migrations.
Added local `0080_unequal_roll_schema_scoped_constraint_repair.sql`, covering
all 12 database-wide constraint guards in migrations `0069`–`0079`. Each guard
now checks the resolved target relation as well as the constraint name. The
repair retains original CHECK expressions, leaves existing constraints intact,
and checks column availability to support partial historical fixtures.
Historical migration files through `0079` are unchanged.

The previously failing eligibility replay test now applies its historical
chain plus the forward repair and retains its original constraint assertions.
A new regression test replays the full unequal-roll historical chain in two
transactional temporary schemas, verifies all 12 constraints independently,
compares their definitions, and verifies repeated repair leaves them unchanged.
No test is skipped or weakened to hide the historical issue.

Applied `0080` to the isolated `stage21_dev` database on port `55442`.
The runner reports no pending migrations. The full Python suite now passes
**754 tests**, superseding the post-`0079` failure result above. Focused Ruff
checks and `git diff --check` also pass. Temporary test schema changes roll back;
no shared-database changes, new branch, commit or push occurred. Repository
changes, including the new migration, were initially kept local. The owner
subsequently authorized reviewing, committing and pushing them to the existing
`repo-stabilization` branch.
