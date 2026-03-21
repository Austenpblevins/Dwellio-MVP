# Tax Unit Assignment

Dwellio keeps parcel-to-tax-unit assignment inside the canonical tax backbone. Stage 7 extends that backbone with GIS support rather than creating a second assignment system.

## Canonical flow

GIS data follows the normal layered path:

`raw file -> stg_gis_raw -> parcel_geometries / taxing_unit_boundaries -> dwellio_spatial_assignment_candidates(...) -> parcel_taxing_units`

This keeps parcel-year identity, provenance, and validation aligned with the rest of the platform.

## Supported GIS workflows

- Parcel polygon workflow: use `parcel_geometries.geometry_role = 'parcel_polygon'` when parcel boundary geometry is available.
- Parcel centroid workflow: use `parcel_geometries.geometry_role = 'parcel_centroid'` or parcel `latitude` / `longitude` when only a point is available.
- Taxing-unit boundary workflow: load school district, MUD, and similar boundary polygons into `taxing_unit_boundaries`.

## Database helpers

Migration `0032_stage7_gis_support.sql` adds these PostGIS helpers:

- `dwellio_normalize_geometry`: coerces geometry to SRID 4326 and the expected role.
- `dwellio_geometry_anchor_point`: creates a stable point-on-surface anchor.
- `dwellio_geometry_area_sqft`: computes auditable polygon area in square feet.
- `dwellio_geometry_validation_issues`: returns lightweight GIS QA issue codes.
- `dwellio_spatial_assignment_candidates`: returns parcel-to-boundary match candidates with match basis and confidence.

The trigger-backed derived fields keep `parcel_geometries` and `taxing_unit_boundaries` normalized on write. Boundary centroid and area are stored so QA and manual review can inspect the same derived values that assignment logic uses.

## Assignment behavior

`dwellio_spatial_assignment_candidates(...)` prefers polygon evidence over centroid fallback:

1. `parcel_polygon_contained`
2. `parcel_polygon_overlap`
3. `parcel_centroid_within`

The Python `GISAssignmentService` groups candidates by `unit_type_code`, keeps the strongest candidate per type, and prepares `parcel_taxing_units` rows with:

- `assignment_method = 'gis'`
- `assignment_confidence`
- JSON audit notes in `notes`

This is intended for school district and MUD use cases first, but it stays generic enough for future special districts.

## Ingestion utilities

`app/ingestion/gis.py` provides county-agnostic helpers to:

- parse GeoJSON features or feature collections
- build `stg_gis_raw` staging rows
- produce canonical parcel-geometry payloads
- produce canonical taxing-unit-boundary payloads
- emit lightweight validation signals before a database write

These utilities are deliberately separate from county adapters so Harris and Fort Bend can each wire GIS acquisition on their own schedule without breaking the shared canonical model.

## QA and audit expectations

- Persist file- and row-level lineage with the existing import batch, raw file, and source record hash fields.
- Persist validation findings in `validation_results` for import, publish, and tax-assignment QA.
- Do not expose restricted sales or MLS content through GIS enrichment paths.
- Keep GIS assignment additive. Manual overrides remain the corrective path when source-direct or GIS assignment is ambiguous.

## Current implementation boundary

Stage 7 adds the shared GIS primitives, but county-specific live GIS fetch/parse wiring is still a follow-on step. The current code is ready for manual upload, fixture-backed testing, and later county adapter integration.
