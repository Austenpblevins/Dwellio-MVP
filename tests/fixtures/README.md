# Test Fixtures

This directory stores lightweight, reproducible fixture assets used by tests and local verification.

## Current fixture inventory

Workflow fixture payloads:
- `stage15_workflow_samples.json`
  - canonical sample payloads for:
    - `POST /lead`
    - `POST /admin/cases`
    - `POST /admin/packets`

GIS fixtures:
- `gis/spatial_assignment.geojson`
  - spatial assignment test geometry used by GIS unit coverage

County adapter fixture datasets live with the adapters:
- `app/county_adapters/harris/fixtures/property_roll_2026.json`
- `app/county_adapters/harris/fixtures/tax_rates_2026.json`
- `app/county_adapters/harris/fixtures/deeds_2026.json`
- `app/county_adapters/fort_bend/fixtures/property_roll_2026.csv`
- `app/county_adapters/fort_bend/fixtures/tax_rates_2026.csv`
- `app/county_adapters/fort_bend/fixtures/deeds_2026.csv`

## Fixture principles

- Fixtures should reflect implemented route and schema behavior, not aspirational future behavior.
- Public-safe payload fixtures must not contain restricted MLS/listing artifacts.
- UUIDs and parcel identifiers may be synthetic, but route shapes and field names should remain canonical.
