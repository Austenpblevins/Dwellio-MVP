# Manual County File Prep

Use this runbook to turn manually downloaded Harris and Fort Bend county exports into the adapter-ready files consumed by the existing manual registration and historical backfill flow.

This prep workflow is year-safe:

- `2025` and `2026` can coexist side by side
- output filenames are tax-year scoped
- preparing `2026` does not overwrite `2025` unless you point both years at the same ready directory on purpose

## Canonical raw-file contract

Place raw files under a year-scoped root:

```text
~/county-data/<tax_year>/raw/harris/real_acct.txt
~/county-data/<tax_year>/raw/harris/owners.txt
~/county-data/<tax_year>/raw/harris/building_res.txt
~/county-data/<tax_year>/raw/harris/fixtures.txt
~/county-data/<tax_year>/raw/harris/extra_features.txt
~/county-data/<tax_year>/raw/harris/extra_features_detail1.txt
~/county-data/<tax_year>/raw/harris/extra_features_detail2.txt
~/county-data/<tax_year>/raw/harris/land.txt
~/county-data/<tax_year>/raw/harris/jur_tax_dist_exempt_value_rate.txt
~/county-data/<tax_year>/raw/harris/jur_exempt.txt
~/county-data/<tax_year>/raw/harris/jur_exempt_cd.txt
~/county-data/<tax_year>/raw/harris/jur_exemption_dscr.txt
~/county-data/<tax_year>/raw/harris/desc_r_14_exemption_category.txt
~/county-data/<tax_year>/raw/harris/desc_r_05_building_data_elements.txt
~/county-data/<tax_year>/raw/harris/desc_r_10_extra_features.txt
~/county-data/<tax_year>/raw/harris/desc_r_11_extra_feature_category.txt

~/county-data/<tax_year>/raw/fort_bend/PropertyExport.txt
~/county-data/<tax_year>/raw/fort_bend/OwnerExport.txt
~/county-data/<tax_year>/raw/fort_bend/ExemptionExport.txt
~/county-data/<tax_year>/raw/fort_bend/WebsiteResidentialSegs.csv
~/county-data/<tax_year>/raw/fort_bend/Fort Bend_Website_ResidentialSegments.txt
~/county-data/<tax_year>/raw/fort_bend/Fort Bend Tax Rate Source.csv
~/county-data/<tax_year>/raw/fort_bend/Fort Bend_Property Data -3-27-2026 - Redacted/PropertyProperty-E/PropertyDataExport*.txt
```

Notes:

- Harris `property_roll` prep still needs the Harris tax-rate raw file because the prep step uses it to recover school district names.
- Harris `property_roll` prep now also requires `fixtures.txt`, `extra_features.txt`, `extra_features_detail1.txt`, `extra_features_detail2.txt`, `desc_r_05_building_data_elements.txt`, `desc_r_10_extra_features.txt`, and `desc_r_11_extra_feature_category.txt` to support canonical bedrooms, baths, total rooms, and pool derivation.
- Harris exemption dictionary mapping uses `jur_exempt_cd.txt` as the account-level source and keeps `jur_exemption_dscr.txt` and `desc_r_14_exemption_category.txt` in the manifest lineage set.
- Fort Bend `property_roll` prep still needs the Fort Bend tax-rate raw file because the prep step uses it to resolve school district entity names.
- Fort Bend canonical living area comes from the official `PropertyDataExport*.txt` `SquareFootage` field.
- Fort Bend `WebsiteResidentialSegs.csv` is the authoritative residential-segment family for canonical bedrooms, half baths, stories, and pool support. `Fort Bend_Website_ResidentialSegments.txt` is an accepted alternate filename for that same contract family.
- Fort Bend `PropertyDataExport4558084.txt` was reviewed during contract expansion, but it is not sufficient alone for canonical pool support because it does not carry `vTSGRSeg_PoolValue`.
- Fort Bend `WebsiteResidentialSegs.csv` is still used for `gross_component_area_sf` and as a fallback when no authoritative property-summary living area is available.
- If the downloaded filename does not match the canonical local name, either rename it into the canonical contract or use `--raw-file-override`.

## Harris property-characteristics contract

Harris residential characteristics are now a first-class upstream prep contract, not a downstream patch.

Authoritative Harris raw files:

- `building_res.txt`: building grain and area metrics used to choose the primary residential building per account
- `fixtures.txt`: authoritative room and bath fixture counts
- `extra_features.txt`, `extra_features_detail1.txt`, `extra_features_detail2.txt`: explicit pool feature signals
- `desc_r_05_building_data_elements.txt`: authoritative code descriptions for fixture codes such as `RMB`, `RMF`, `RMH`, `RMT`, and `STY`
- `desc_r_10_extra_features.txt` and `desc_r_11_extra_feature_category.txt`: authoritative support for extra-feature code interpretation

Canonical operator layout:

- Supported long-term layout is the flattened Harris root under `~/county-data/<tax_year>/raw/harris/`
- Legacy nested download folders such as `Harris_Real_building_land/` and `Harris_Code_description_real (1)/` are still accepted for backward compatibility
- If both exist, the prep flow resolves the canonical flattened path first and only falls back to the nested legacy folders when the canonical file is absent

Normalization rules:

- `bedrooms`: primary-building `fixtures.txt` code `RMB`
- `full_baths`: primary-building `fixtures.txt` code `RMF`
- `half_baths`: primary-building `fixtures.txt` code `RMH`; when fixture-room detail exists for the selected building but `RMH` is absent, canonical `half_baths` is set to `0`
- `total_rooms`: primary-building `fixtures.txt` code `RMT`
- `stories`: primary-building `fixtures.txt` code `STY`
- `pool_flag`: `true` only when an explicit Harris extra-feature pool code is present; current supported pool codes are `CSC1`, `RRP1`, `RRP2`, `RRP3`, `RRP4`, `RRP5`, `RRP8`, and `RRP9`

Aggregation and lineage rules:

- Room and bath counts are taken from exactly one selected residential building per account
- Primary-building selection is deterministic: highest living-area priority, then largest primary area, then lowest building number
- Pool presence is account-level and is derived from explicit extra-feature rows only
- Prep output includes lineage fields such as `bedrooms_source`, `full_baths_source`, `half_baths_source`, `total_rooms_source`, `stories_source`, and pool source metadata for auditability

Residual caveats:

- Structural-element files were reviewed during contract expansion, but they are not required for this room, bath, and pool contract because the authoritative supported signals already exist in `fixtures.txt` and the extra-feature files
- `total_rooms` is canonical only where `RMT` is present and parseable; no heuristic fill is applied when the source is absent
- The prep step fails loudly when the Harris characteristics files required by this contract are missing

## Fort Bend area contract

Fort Bend area semantics are intentionally split into two first-class fields:

- `living_area_sf`: authoritative living area used by quote math and PSF denominators
- `gross_component_area_sf`: all-component structural area summed from `WebsiteResidentialSegs.csv`

The supported canonical workflow is:

1. build `gross_component_area_sf` from all Fort Bend residential segments
2. override `living_area_sf` from official `PropertyDataExport*.txt` `SquareFootage`
3. keep `living_area_source` so downstream tables can show where canonical living area came from

Do not use `gross_component_area_sf` as the quote-facing living area denominator.

## Fort Bend property-characteristics contract

Fort Bend property characteristics are now a first-class upstream prep contract, but only for fields with defensible source semantics.

Supported Fort Bend fields:

- `bedrooms`
- `half_baths`
- `stories`
- `pool_flag`

Intentionally unsupported Fort Bend fields:

- `full_baths`
- `total_rooms`
- `garage_spaces`
- `quarter_baths` as a canonical warehouse field

Authoritative Fort Bend characteristics source:

- `WebsiteResidentialSegs.csv` or `Fort Bend_Website_ResidentialSegments.txt`

Normalization rules:

- `bedrooms`: selected primary residential improvement `fBedrooms`
- `half_baths`: selected primary residential improvement `fNumHalfBath`; blank selected-main-area rows normalize to `0`, invalid negative values are not promoted
- `stories`: count explicit story-level segment codes `MA`, `MA2`, `MA3`, `MA4` on the selected primary residential improvement
- `pool_flag`: `true` when any parcel segment carries non-zero `vTSGRSeg_PoolValue` or segment type `RP`

Primary-improvement selection:

- choose the residential improvement with the largest summed main-area square footage
- break ties with more explicit story-level segments
- then break remaining ties with lower `vTSGRSeg_ImpNum`

Important non-go decision:

- `full_baths` stays null for Fort Bend because `fPlumbing` / `Plumbing` is not yet proven to mean exact full-bath count

See the durable county contract:

- [FORT_BEND_PROPERTY_CHARACTERISTICS_CONTRACT.md](/Users/nblevins/Desktop/dwellio/docs/runbooks/FORT_BEND_PROPERTY_CHARACTERISTICS_CONTRACT.md)

## Harris primary-building area contract

Harris multi-building account selection for quote-facing `living_area_sf` is intentionally living-area-first:

- prefer `heat_ar` first
- fall back to `im_sq_ft` when `heat_ar` is missing
- use `gross_ar` only as a last-resort living-area fallback when no living-area metric exists

Primary-row selection must not let `gross_ar`, `act_ar`, or `eff_ar` override a row with better living-area coverage.  
Those broader area fields can still exist for diagnostics, but they are not the deciding signal for quote-facing `living_area_sf`.

## Adapter-ready outputs

The prep script writes these exact filenames:

- `harris_property_roll_<tax_year>.json`
- `harris_tax_rates_<tax_year>.json`
- `fort_bend_property_roll_<tax_year>.csv`
- `fort_bend_tax_rates_<tax_year>.csv`

It also writes one manifest per dataset:

- `harris_property_roll_<tax_year>.manifest.json`
- `harris_tax_rates_<tax_year>.manifest.json`
- `fort_bend_property_roll_<tax_year>.manifest.json`
- `fort_bend_tax_rates_<tax_year>.manifest.json`

## Recommended commands

Prepare all four dataset files for `2026`:

```bash
python3 -m infra.scripts.prepare_manual_county_files \
  --county-id both \
  --tax-year 2026 \
  --dataset-type both \
  --raw-root ~/county-data/2026/raw \
  --ready-root ~/county-data/2026/ready
```

Prepare only Harris property-roll files for `2026`:

```bash
python3 -m infra.scripts.prepare_manual_county_files \
  --county-id harris \
  --tax-year 2026 \
  --dataset-type property_roll \
  --raw-root ~/county-data/2026/raw \
  --ready-root ~/county-data/2026/ready
```

Prepare only Fort Bend tax rates for `2026` when the downloaded tax-rate file keeps a non-canonical name:

```bash
python3 -m infra.scripts.prepare_manual_county_files \
  --county-id fort_bend \
  --tax-year 2026 \
  --dataset-type tax_rates \
  --raw-root ~/county-data/2026/raw \
  --ready-root ~/county-data/2026/ready \
  --raw-file-override fort_bend.tax_rates="~/Downloads/Fort Bend Tax Rate Source - Revised.csv"
```

Legacy 2025 nested download folders are still accepted for backward compatibility, but the canonical contract above is the supported operator layout going forward.

## Manifest and audit trail

Each manifest records:

- `county_id`
- `tax_year`
- `dataset_type`
- every raw file used
- file sizes
- SHA-256 checksums
- output files written
- row counts
- validation status
- parse issue count
- validation error count

Use the manifest to confirm exactly which raw files were used for a prep run before handing the outputs to ingestion.

## Common failure modes

Missing raw files:

- the script fails with the missing logical file name and the path it expected
- confirm the file is in the canonical location or pass `--raw-file-override`

Unsupported tax year:

- the script only allows years already supported by the county dataset configs
- confirm the county config lists the target year before preparing files

Malformed raw files or schema shifts:

- the script fails loudly if required columns or parseable values are missing
- inspect the county download and compare it to the canonical raw contract

Validation failures after prep:

- inspect the `<county>_<dataset_type>_<tax_year>.manifest.json` file
- a non-zero `parse_issue_count` or `validation_error_count` means the output is not ready for backfill

## Handoff into ingestion

After prep, use the existing historical backfill path.

Register one file manually:

```bash
python3 -m infra.scripts.register_manual_import \
  --county-id harris \
  --tax-year 2026 \
  --dataset-type property_roll \
  --source-file ~/county-data/2026/ready/harris_property_roll_2026.json
```

Or run the bounded backfill flow once the ready files exist:

```bash
python3 -m infra.scripts.run_historical_backfill \
  --counties harris fort_bend \
  --tax-years 2026 \
  --dataset-types property_roll tax_rates \
  --ready-root ~/county-data/2026/ready
```

## Keeping 2025 intact

Use separate year-scoped roots:

- raw files: `~/county-data/2025/raw` and `~/county-data/2026/raw`
- ready files: `~/county-data/2025/ready` and `~/county-data/2026/ready`

That keeps:

- `harris_property_roll_2025.json` separate from `harris_property_roll_2026.json`
- `fort_bend_tax_rates_2025.csv` separate from `fort_bend_tax_rates_2026.csv`

This is the recommended layout for year-over-year comparison, legal-limit review, and future historical backfill reuse.
