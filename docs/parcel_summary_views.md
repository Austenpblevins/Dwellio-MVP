# Parcel Summary Views

Stage 10 adds a derived parcel-view layer for application screens and admin review. These views are built from canonical tables only and are intended to keep app code out of ad hoc multi-table parcel joins.

## Views

`parcel_summary_view`
- Primary parcel-year summary for app and admin usage.
- Combines parcel identity, address, current owner, property characteristics, improvements, land, assessment, exemption flags, effective tax rate, estimated tax, completeness score, and warning codes.

`parcel_effective_tax_rate_view`
- Parcel-year effective tax-rate summary with component breakdown.
- Includes assignment coverage counts, GIS/manual assignment hints, and warning flags for missing or conflicting county and school assignments.

`parcel_tax_estimate_summary`
- Parcel-year tax estimate helper derived from assessments, exemptions, and effective tax-rate facts.
- Preserves normalized exemption context, summary flags, taxable-value estimates, estimated annual tax, and exemption QA warning codes.

`parcel_owner_current_view`
- Parcel-year owner summary that keeps the selected current owner rollup alongside the raw CAD owner snapshot.
- Intended for public-safe owner display plus internal owner reconciliation review.

`parcel_search_view`
- Internal search-support view that exposes normalized address and owner fields plus parcel metadata needed for lookup.
- The public search contract remains `v_search_read_model`, which now selects its public-safe subset from this view.

`parcel_data_completeness_view`
- Admin-facing parcel-year completeness and warning view.
- Scores parcel coverage across address, characteristics, improvements, land, assessments, exemptions, tax assignments, effective tax rate, owner rollups, and geometry.

## Public Contract Note

Stage 10 introduces `parcel_search_view` as the internal derived support view for parcel lookup without replacing the existing public search read model. `v_search_read_model` remains the contract named in the source-of-truth docs and is now a stable wrapper over `parcel_search_view`.

## Validation Queries

Run after applying `0035_stage10_parcel_summary_views.sql`:

```sql
SELECT viewname
FROM pg_views
WHERE viewname IN (
  'parcel_summary_view',
  'parcel_effective_tax_rate_view',
  'parcel_tax_estimate_summary',
  'parcel_owner_current_view',
  'parcel_search_view',
  'parcel_data_completeness_view',
  'v_search_read_model'
)
ORDER BY viewname;
```

```sql
SELECT county_id, account_number, owner_name, effective_tax_rate, completeness_score, warning_codes
FROM parcel_summary_view
ORDER BY county_id, account_number
LIMIT 10;
```

```sql
SELECT county_id, account_number, effective_tax_rate, component_count, warning_codes
FROM parcel_effective_tax_rate_view
ORDER BY county_id, account_number
LIMIT 10;
```

```sql
SELECT county_id, account_number, homestead_flag, over65_flag, freeze_flag, estimated_annual_tax, warning_codes
FROM parcel_tax_estimate_summary
ORDER BY county_id, account_number
LIMIT 10;
```

```sql
SELECT county_id, account_number, current_owner_name, cad_owner_name, source_basis, cad_owner_mismatch_flag
FROM parcel_owner_current_view
ORDER BY county_id, account_number
LIMIT 10;
```

```sql
SELECT county_id, account_number, situs_address, owner_name
FROM v_search_read_model
ORDER BY county_id, account_number
LIMIT 10;
```

```sql
SELECT county_id, account_number, completeness_score, public_summary_ready_flag, admin_review_required, warning_codes
FROM parcel_data_completeness_view
ORDER BY county_id, account_number
LIMIT 10;
```
