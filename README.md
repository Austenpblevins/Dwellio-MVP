# Dwellio Final Master Repo

This package is the corrected Dwellio repository bundle.

It includes:
- preserved source-of-truth docs
- corrected codex guidance docs
- final reconciled architecture docs
- synchronized migrations
- read-model views
- typed models and service stubs
- ETL job stubs
- API stubs
- architecture map and state tracker
- a canonical precedence note
- a tax-data-pull to repo mapping note

## Important notes

### Backend authority
Dwellio is Python-first for backend and ETL implementation.
React / Next.js may be used as a frontend layer only unless the source-of-truth docs are explicitly changed.

### Full schema file
The full schema convenience file is located at:

`sql/dwellio_full_schema.sql`

It is a reference/bootstrap artifact and is intentionally kept outside the migrations folder.

### Conflict resolution
Use:
- `docs/runbooks/CANONICAL_PRECEDENCE.md`
- `docs/runbooks/TAX_DATA_PULL_REPO_MAPPING.md`

before reconciling imported planning docs with the repo implementation.

### Consolidated Codex prompt file
The consolidated Codex operator file is located at:

`docs/codex/DWELLIO_MASTER_CODEX_PROMPT.md`
