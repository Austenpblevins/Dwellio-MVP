# Stage 11 Search Refinement

This runbook documents the refinement layer added after the core Stage 11 search architecture landed on `main`.

It does not replace:

- `search_documents`
- `dwellio_refresh_search_documents(...)`
- `v_search_read_model`
- `GET /search`
- `GET /search/autocomplete`

## What changed

The refinement slice adds:

- basis-aware confidence labeling
- an internal-only search inspect route
- stronger docs and tests around ranking behavior

## Public search behavior

Public search remains backed by `search_documents`.

The ranking order is deterministic:

1. exact account matches
2. exact normalized address matches
3. address trigram matches
4. search-text trigram matches
5. owner fallback matches
6. county/account tie-breakers

The public payload exposes:

- `match_basis`
- `match_score`
- `confidence_label`

It does not expose internal score breakdowns.

## Confidence policy

Confidence labels now reflect both:

- similarity score
- match basis

Examples:

- `account_exact` -> `very_high`
- `address_exact` -> `very_high`
- `address_prefix` -> `high` when score remains strong
- `owner_fallback` -> at most `medium`

This prevents weaker fallback matches from appearing equivalent to exact parcel/address matches.

## Internal inspect route

Use:

- `GET /admin/search/inspect?query=101%20Main&limit=5`

This route is for admin/debug only and returns:

- normalized query forms
- ordered candidates
- `confidence_reasons`
- matched fields
- score components:
  - basis rank
  - address similarity
  - search-text similarity
  - owner similarity

## Refresh workflow

To rebuild search support rows:

```sql
SELECT dwellio_refresh_search_documents(NULL, NULL);
```

Or from Python:

```python
from app.services.search_index import SearchIndexService

SearchIndexService().rebuild_search_documents(county_id="harris", tax_year=2026)
```

Use county/year-scoped refreshes when rebuilding a single slice after canonical updates.

## Manual Harris refresh

Use a manual Harris-only refresh when canonical Harris parcel state is already published but `search_documents` or `v_search_read_model` is stale, especially after an interrupted post-commit refresh.

Run it in a dedicated SQL session with protective settings:

```sql
SET statement_timeout = 0;
SET lock_timeout = 0;
SET max_parallel_workers_per_gather = 0;

SELECT dwellio_refresh_search_documents('harris', 2026);
```

Expected behavior and risk:

- Harris refresh is a heavy maintenance step and can run for several minutes.
- Avoid interrupting the session once it starts.
- This refresh updates search/read-model freshness only; it does not republish canonical parcel state or change instant-quote logic.
- If a bulk property-roll publish fails after canonical publish, check admin import-batch detail for failed `search_refresh` or `tax_assignment_refresh` step runs before retrying the full pipeline.

Verify freshness afterward:

```sql
SELECT min(updated_at), max(updated_at), count(*)
FROM search_documents
WHERE county_id = 'harris' AND tax_year = 2026;

SELECT min(updated_at), max(updated_at), count(*)
FROM parcel_year_snapshots
WHERE county_id = 'harris' AND tax_year = 2026 AND is_current = true;
```

`search_documents.max(updated_at)` should be at or after the current Harris snapshot update window for the same county-year.

## Boundaries

- Public search stays on `v_search_read_model` semantics and public-safe fields.
- Internal inspectability stays on admin routes only.
- No second search architecture was added.
- No request-time heavy comp or quote logic was added.
