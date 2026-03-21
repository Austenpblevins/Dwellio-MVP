# Exemption Model

Stage 8 keeps exemptions parcel-year centric and detailed instead of collapsing everything into a single tax reduction number.

## Canonical Storage

`parcel_exemptions` remains the canonical parcel-year fact table for exemptions. Stage 8 adds:

- `raw_exemption_codes`
- `source_entry_count`
- `amount_missing_flag`

That keeps the normalized exemption type, granted amount, and county-native source codes together in one auditable record.

## Normalization Rules

- Preserve the county/source raw exemption code whenever it is available.
- Normalize aliases like `HS`, `hs_amt`, `OV65`, and `ov65_amt` into the canonical `exemption_type_code`.
- Merge duplicate source entries that normalize to the same canonical exemption type for the same parcel-year.
- Keep multiple exemption types per parcel-year; do not flatten all exemptions into one row or one amount.

## Rollup View

`parcel_exemption_rollup_view` is the derived parcel-year summary for exemption review and future tax-computation support. It exposes:

- normalized exemption type list
- preserved raw exemption code list
- granted exemption amount total
- summary flags for homestead, over65, disabled, disabled veteran, and freeze
- QA issue codes for missing or conflicting exemption data

## QA Checks

The rollup view currently flags:

- missing exemption records when assessment totals imply exemptions should exist
- missing raw exemption codes
- granted exemptions with missing amounts
- mismatch between `parcel_assessments.exemption_value_total` and the rolled-up exemption total
- mismatch between `parcel_assessments.homestead_flag` and normalized homestead detection
- freeze status without a qualifying senior, disabled, or disabled veteran exemption

## County Adapter Guidance

- County adapters should emit canonical `exemption_type_code` when known.
- County adapters should also emit `raw_exemption_code` when the source exposes a county-native code or source column name.
- Repository normalization is the shared enforcement point before canonical writes.
