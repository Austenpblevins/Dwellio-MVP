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
~/county-data/<tax_year>/raw/harris/land.txt
~/county-data/<tax_year>/raw/harris/jur_tax_dist_exempt_value_rate.txt
~/county-data/<tax_year>/raw/harris/jur_exempt.txt
~/county-data/<tax_year>/raw/harris/jur_exempt_cd.txt
~/county-data/<tax_year>/raw/harris/jur_exemption_dscr.txt
~/county-data/<tax_year>/raw/harris/desc_r_14_exemption_category.txt

~/county-data/<tax_year>/raw/fort_bend/PropertyExport.txt
~/county-data/<tax_year>/raw/fort_bend/OwnerExport.txt
~/county-data/<tax_year>/raw/fort_bend/ExemptionExport.txt
~/county-data/<tax_year>/raw/fort_bend/WebsiteResidentialSegs.csv
~/county-data/<tax_year>/raw/fort_bend/Fort Bend Tax Rate Source.csv
```

Notes:

- Harris `property_roll` prep still needs the Harris tax-rate raw file because the prep step uses it to recover school district names.
- Harris exemption dictionary mapping uses `jur_exempt_cd.txt` as the account-level source and keeps `jur_exemption_dscr.txt` and `desc_r_14_exemption_category.txt` in the manifest lineage set.
- Fort Bend `property_roll` prep still needs the Fort Bend tax-rate raw file because the prep step uses it to resolve school district entity names.
- If the downloaded filename does not match the canonical local name, either rename it into the canonical contract or use `--raw-file-override`.

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
