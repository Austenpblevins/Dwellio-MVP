# Harris Property Characteristics Contract

This document defines the supported upstream contract for Harris County residential property characteristics used by Dwellio canonical data, quote support, and downstream modeling.

## Scope

This contract is Harris-only and currently covers:

- `bedrooms`
- `full_baths`
- `half_baths`
- `total_rooms`
- `pool_flag`
- `stories`

The goal is a rebuild-safe, source-backed contract that can be rerun on future Harris refreshes without manual database patching.

## Authoritative raw files

The supported canonical operator layout is:

```text
~/county-data/<tax_year>/raw/harris/building_res.txt
~/county-data/<tax_year>/raw/harris/fixtures.txt
~/county-data/<tax_year>/raw/harris/extra_features.txt
~/county-data/<tax_year>/raw/harris/extra_features_detail1.txt
~/county-data/<tax_year>/raw/harris/extra_features_detail2.txt
~/county-data/<tax_year>/raw/harris/desc_r_05_building_data_elements.txt
~/county-data/<tax_year>/raw/harris/desc_r_10_extra_features.txt
~/county-data/<tax_year>/raw/harris/desc_r_11_extra_feature_category.txt
```

Backward-compatible nested Harris download folders are still accepted, including:

- `Harris_Real_building_land/`
- `Harris_Code_description_real (1)/`

When both flattened and nested copies exist, the flattened Harris root is the canonical source of truth and is resolved first.

## Field derivation rules

### Primary-building selection

Room and bath counts are derived from one selected residential building per account.

Selection order:

1. highest living-area priority
2. largest primary area within the same priority
3. lowest building number as a stable tie-breaker

This prevents account-level duplicate inflation when Harris has multiple residential building rows.

### Bedrooms

- authoritative file: `fixtures.txt`
- authoritative code: `RMB`
- canonical field: `bedrooms`
- rule: parse the selected building's `RMB` fixture count as a non-negative integer

### Full baths

- authoritative file: `fixtures.txt`
- authoritative code: `RMF`
- canonical field: `full_baths`
- rule: parse the selected building's `RMF` fixture count as a non-negative integer
- no inference from quality, plumbing, or free text is allowed

### Half baths

- authoritative file: `fixtures.txt`
- authoritative code: `RMH`
- canonical field: `half_baths`
- rule: parse the selected building's `RMH` fixture count as a non-negative integer
- special case: if fixture-room detail exists for the selected building but `RMH` is absent, canonical `half_baths` becomes `0`
- no inference from quality, plumbing, or free text is allowed

### Total rooms

- authoritative file: `fixtures.txt`
- authoritative code: `RMT`
- canonical field: `total_rooms`
- rule: parse the selected building's `RMT` fixture count as a non-negative integer
- no synthetic fill is allowed when `RMT` is absent

### Stories

- authoritative file: `fixtures.txt`
- authoritative code: `STY`
- canonical field: `stories`
- rule: parse the selected building's `STY` fixture count as a non-negative integer when present

### Pool

- authoritative files:
  - `extra_features.txt`
  - `extra_features_detail1.txt`
  - `extra_features_detail2.txt`
- canonical field: `pool_flag`
- rule: set `pool_flag = true` only when at least one explicit Harris pool code is present

Supported pool codes:

- `CSC1`
- `RRP1`
- `RRP2`
- `RRP3`
- `RRP4`
- `RRP5`
- `RRP8`
- `RRP9`

If none of those codes are present, canonical `pool_flag` is set to `false`.

Accessory pool equipment does not create pool presence unless one of the explicit pool codes above is present.

## Code-description support

The prep flow reads the Harris code-description files so the contract stays anchored to source-described semantics instead of ad hoc code guesses.

Current validated semantics include:

- `RMB` = bedroom
- `RMF` = full bath
- `RMH` = half bath
- `RMT` = total
- `STY` = story

## Data-quality safeguards

- Missing required Harris characteristic files fail prep loudly
- Negative counts are rejected rather than normalized silently
- Room and bath counts are taken from the selected building only
- Pool signals are limited to explicit supported codes
- Prep output records lineage fields for auditability

Current lineage fields emitted in prepared Harris property-roll rows:

- `primary_building_num`
- `bedrooms_source`
- `full_baths_source`
- `half_baths_source`
- `total_rooms_source`
- `stories_source`
- `pool_feature_codes`
- `pool_source_files`

## Canonical persistence

These values flow into the Harris property-roll adapter and then into canonical persistence.

Current canonical destinations:

- `parcel_improvements.bedrooms`
- `parcel_improvements.full_baths`
- `parcel_improvements.half_baths`
- `parcel_improvements.total_rooms`
- `parcel_improvements.pool_flag`
- `parcel_improvements.stories`

`total_rooms` is a safe additive schema extension introduced for this contract and is also stored on `improvements.total_rooms`.

## Rebuild workflow

Prepare Harris property-roll files:

```bash
python3 -m infra.scripts.prepare_manual_county_files \
  --county-id harris \
  --tax-year 2026 \
  --dataset-type property_roll \
  --raw-root ~/county-data/2026/raw \
  --ready-root ~/county-data/2026/ready
```

Run the bounded Stage 21 backfill:

```bash
DWELLIO_DATABASE_URL=postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev \
DWELLIO_ENV=stage21_dev \
python3 -m infra.scripts.run_historical_backfill \
  --counties harris \
  --tax-years 2026 \
  --dataset-types property_roll \
  --ready-root ~/county-data/2026/ready
```

## Residual caveats

- This contract intentionally does not use structural-element files for these fields because the supported source signals already exist in `fixtures.txt` and the extra-feature files
- `total_rooms` remains null when `RMT` is absent or invalid
- Pool derivation is intentionally conservative and source-code based
