from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

from app.ingestion.gis import (
    build_gis_staging_rows,
    build_parcel_geometry_record,
    build_taxing_unit_boundary_record,
    load_geojson_features,
    summarize_geometry,
)
from app.services.gis_assignment import GISAssignmentService, SpatialAssignmentCandidate

FIXTURE_PATH = (
    Path(__file__).resolve().parents[1] / "fixtures" / "gis" / "spatial_assignment.geojson"
)


def test_build_gis_staging_rows_from_feature_collection() -> None:
    rows = build_gis_staging_rows(content=FIXTURE_PATH.read_bytes())

    assert len(rows) == 3
    assert {row.table_name for row in rows} == {"stg_gis_raw"}
    assert rows[0].raw_payload["properties"]["account_number"] == "1001001001001"
    assert all(row.row_hash for row in rows)


def test_build_parcel_geometry_record_promotes_polygon_to_multipolygon_wkt() -> None:
    features = load_geojson_features(FIXTURE_PATH.read_bytes())

    record = build_parcel_geometry_record(
        feature=features[0],
        parcel_id="parcel-1",
        tax_year=2026,
        geometry_role="parcel_polygon",
        source_system_id="source-1",
        import_batch_id="batch-1",
    )

    assert record["geometry_role"] == "parcel_polygon"
    assert record["geom_wkt"].startswith("MULTIPOLYGON ")
    assert record["centroid_wkt"].startswith("POINT ")
    assert record["validation_issues"] == []


def test_build_taxing_unit_boundary_record_preserves_scope_metadata() -> None:
    features = load_geojson_features(FIXTURE_PATH.read_bytes())

    record = build_taxing_unit_boundary_record(
        feature=features[1],
        taxing_unit_id="taxing-unit-1",
        tax_year=2026,
        boundary_scope="school_attendance",
    )

    assert record["boundary_scope"] == "school_attendance"
    assert record["boundary_name"] == "Fixture ISD"
    assert record["geom_wkt"].startswith("MULTIPOLYGON ")


def test_summarize_geometry_flags_bad_polygon_ring() -> None:
    summary = summarize_geometry(
        {
            "type": "Polygon",
            "coordinates": [[[-95.37, 29.76], [-95.37, 29.77], [-95.36, 29.77], [-95.36, 29.76]]],
        }
    )

    assert "ring_not_closed" in summary.validation_issues


def test_select_preferred_assignments_prefers_polygon_matches() -> None:
    candidates = [
        SpatialAssignmentCandidate(
            parcel_id="parcel-1",
            tax_year=2026,
            taxing_unit_id="school-centroid",
            taxing_unit_boundary_id="boundary-school-centroid",
            unit_type_code="school_district",
            boundary_scope="school_attendance",
            match_basis="parcel_centroid_within",
            assignment_confidence=0.6,
            overlap_ratio=None,
            centroid_within=True,
            polygon_contained=False,
            parcel_geometry_source="parcel_geometries",
        ),
        SpatialAssignmentCandidate(
            parcel_id="parcel-1",
            tax_year=2026,
            taxing_unit_id="school-polygon",
            taxing_unit_boundary_id="boundary-school-polygon",
            unit_type_code="school_district",
            boundary_scope="school_attendance",
            match_basis="parcel_polygon_overlap",
            assignment_confidence=0.92,
            overlap_ratio=0.92,
            centroid_within=True,
            polygon_contained=False,
            parcel_geometry_source="parcel_geometries",
        ),
        SpatialAssignmentCandidate(
            parcel_id="parcel-1",
            tax_year=2026,
            taxing_unit_id="mud-17",
            taxing_unit_boundary_id="boundary-mud-17",
            unit_type_code="mud",
            boundary_scope="service_area",
            match_basis="parcel_centroid_within",
            assignment_confidence=0.6,
            overlap_ratio=None,
            centroid_within=True,
            polygon_contained=False,
            parcel_geometry_source="parcel_lat_lon",
        ),
    ]

    service = GISAssignmentService()
    selected = service.select_preferred_assignments(candidates)

    selected_by_unit = {candidate.unit_type_code: candidate for candidate in selected}
    assert selected_by_unit["school_district"].taxing_unit_id == "school-polygon"
    assert selected_by_unit["mud"].taxing_unit_id == "mud-17"


def test_build_assignment_rows_captures_auditable_notes() -> None:
    service = GISAssignmentService()
    candidates = [
        SpatialAssignmentCandidate(
            parcel_id="parcel-1",
            tax_year=2026,
            taxing_unit_id="school-polygon",
            taxing_unit_boundary_id="boundary-school-polygon",
            unit_type_code="school_district",
            boundary_scope="school_attendance",
            match_basis="parcel_polygon_contained",
            assignment_confidence=0.99,
            overlap_ratio=1.0,
            centroid_within=True,
            polygon_contained=True,
            parcel_geometry_source="parcel_geometries",
        )
    ]

    rows = service.build_assignment_rows(
        parcel_id="parcel-1",
        tax_year=2026,
        candidates=candidates,
        source_system_id="source-1",
        import_batch_id="batch-1",
        job_run_id="job-1",
    )

    assert len(rows) == 1
    assert rows[0]["assignment_method"] == "gis"
    notes = json.loads(rows[0]["notes"])
    assert notes["match_basis"] == "parcel_polygon_contained"
    assert notes["taxing_unit_boundary_id"] == "boundary-school-polygon"


def test_build_assignment_rows_serializes_uuid_notes_values() -> None:
    service = GISAssignmentService()
    boundary_id = uuid4()
    candidates = [
        SpatialAssignmentCandidate(
            parcel_id="parcel-1",
            tax_year=2026,
            taxing_unit_id="school-polygon",
            taxing_unit_boundary_id=boundary_id,  # type: ignore[arg-type]
            unit_type_code="school_district",
            boundary_scope="school_attendance",
            match_basis="parcel_polygon_contained",
            assignment_confidence=0.99,
            overlap_ratio=1.0,
            centroid_within=True,
            polygon_contained=True,
            parcel_geometry_source="parcel_geometries",
        )
    ]

    rows = service.build_assignment_rows(
        parcel_id="parcel-1",
        tax_year=2026,
        candidates=candidates,
        source_system_id="source-1",
    )

    notes = json.loads(rows[0]["notes"])
    assert notes["taxing_unit_boundary_id"] == str(boundary_id)
