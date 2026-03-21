# Ownership Reconciliation

Stage 9 adds deed-backed ownership reconciliation without replacing the existing parcel-year warehouse.

## Backbone Rules

- Keep raw CAD owner values intact.
- Store deed evidence separately from the current owner rollup.
- Keep reconciliation explainable through lineage, scoring, and QA views.
- Allow manual ownership overrides without mutating raw deed or CAD evidence.

## Canonical Inputs

- `parcel_year_snapshots.cad_owner_name`
- `parcel_year_snapshots.cad_owner_name_normalized`
- `deed_records`
- `deed_parties`
- `manual_overrides` with `override_scope = 'ownership'`

`parcels.owner_name` remains the parcel-level CAD owner field, while `parcel_year_snapshots` now preserves the tax-year CAD owner snapshot used during reconciliation.

## Derived Ownership Structures

- `parcel_owner_periods`
  Deed grantee evidence is converted into parcel owner periods with confidence scoring and supporting deed ids in `metadata_json`.
- `current_owner_rollups`
  One parcel-year owner decision row. Selection order is:
  1. approved/applied ownership manual override
  2. active deed-derived owner period as of December 31 of the tax year
  3. CAD owner snapshot fallback

## Deed Linking

Stage 9 deed linking stays additive and auditable:

- first try `account_number`
- then `cad_property_id`
- then alias matching against `account_number`, `cad_property_id`, `geo_account_number`, and `quick_ref_id`

The chosen link basis is stored in deed metadata.

## QA / Debug Views

- `v_owner_reconciliation_evidence`
  Shows parcel-year CAD owner snapshot, selected owner rollup, deed support, owner-period counts, and active ownership override.
- `v_owner_reconciliation_qa`
  Flags missing rollups, missing owner periods despite deeds, future-dated deeds, conflicting current periods, invalid periods, and non-override CAD/derived mismatches.

These views are for internal admin/debug workflows and should not be exposed as public API contracts.
