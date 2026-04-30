from __future__ import annotations

from contextlib import contextmanager
from uuid import uuid4

import pytest

from app.services.unequal_roll_subject_snapshot import (
    UnequalRollSubjectSnapshotService,
)


class SequenceCursor:
    def __init__(
        self,
        fetchone_results: list[dict[str, object] | None],
    ) -> None:
        self.fetchone_results = fetchone_results
        self.fetchall_results: list[list[dict[str, object]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...] | None]] = []
        self.fetchone_index = 0

    def __enter__(self) -> SequenceCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, query: str, params: tuple[object, ...] | None = None) -> None:
        self.execute_calls.append((query, params))

    def fetchone(self) -> dict[str, object] | None:
        if self.fetchone_index >= len(self.fetchone_results):
            return None
        result = self.fetchone_results[self.fetchone_index]
        self.fetchone_index += 1
        return result

    def fetchall(self) -> list[dict[str, object]]:
        return []


class SequenceConnection:
    def __init__(self, fetchone_results: list[dict[str, object] | None]) -> None:
        self.cursor_instance = SequenceCursor(fetchone_results)
        self.commit_calls = 0

    def cursor(self) -> SequenceCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commit_calls += 1


def connection_factory(connection: SequenceConnection):
    @contextmanager
    def _connection():
        yield connection

    return _connection


def _base_subject_row(*, county_id: str = "harris", tax_year: int = 2026) -> dict[str, object]:
    return {
        "county_id": county_id,
        "tax_year": tax_year,
        "account_number": "1001001001001",
        "parcel_id": uuid4(),
        "address": "101 Main St, Houston, TX 77002",
        "property_type_code": "sfr",
        "property_class_code": "A1",
        "neighborhood_code": "NBH-001",
        "subdivision_name": "Heights",
        "school_district_name": "Houston ISD",
        "living_area_sf": 2000.0,
        "year_built": 1998,
        "effective_age": 10.0,
        "bedrooms": 3,
        "full_baths": 2.0,
        "half_baths": 1.0,
        "total_rooms": 8,
        "stories": 2.0,
        "quality_code": "AVERAGE_PLUS",
        "condition_code": "GOOD",
        "pool_flag": True,
        "land_sf": 7200.0,
        "land_acres": 0.1653,
        "market_value": 425000.0,
        "assessed_value": 390000.0,
        "appraised_value": 405000.0,
        "certified_value": 399000.0,
        "notice_value": 410000.0,
        "exemption_value_total": 100000.0,
        "homestead_flag": True,
        "over65_flag": False,
        "disabled_flag": False,
        "disabled_veteran_flag": False,
        "freeze_flag": False,
        "warning_codes": ["missing_geometry"],
        "completeness_score": 88.0,
        "public_summary_ready_flag": True,
    }


def test_create_run_with_subject_snapshot_persists_harris_snapshot(monkeypatch) -> None:
    subject_row = _base_subject_row()
    connection = SequenceConnection([subject_row])
    monkeypatch.setattr(
        "app.services.unequal_roll_subject_snapshot.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollSubjectSnapshotService().create_run_with_subject_snapshot(
        county_id="harris",
        tax_year=2026,
        account_number="1001001001001",
    )

    assert result.run_status == "completed"
    assert result.readiness_status == "ready"
    assert result.support_status == "supported"
    assert result.support_blocker_code is None
    assert result.subject_snapshot_status == "completed"
    assert result.source_coverage_status == "canonical_snapshot_only"
    assert result.served_tax_year == 2026
    assert connection.commit_calls == 1

    assert len(connection.cursor_instance.execute_calls) == 4
    insert_run_sql, _ = connection.cursor_instance.execute_calls[0]
    subject_select_sql, subject_select_params = connection.cursor_instance.execute_calls[1]
    insert_snapshot_sql, insert_snapshot_params = connection.cursor_instance.execute_calls[2]
    update_run_sql, update_run_params = connection.cursor_instance.execute_calls[3]

    assert "INSERT INTO unequal_roll_runs" in insert_run_sql
    assert "FROM parcel_summary_view AS psv" in subject_select_sql
    assert subject_select_params == ("harris", "1001001001001", 2026)
    assert "INSERT INTO unequal_roll_subject_snapshots" in insert_snapshot_sql
    assert insert_snapshot_params[5] == "1001001001001"
    assert insert_snapshot_params[17] == 1.0
    assert insert_snapshot_params[19] == 2.0
    assert insert_snapshot_params[22] is True
    assert insert_snapshot_params[36] == pytest.approx(202.5)
    assert insert_snapshot_params[37] is None
    assert insert_snapshot_params[38].obj["subject"]["property_type_code"] == "sfr"
    assert insert_snapshot_params[39].obj["subject_source"]["name"] == "parcel_summary_view"
    assert "UPDATE unequal_roll_runs" in update_run_sql
    assert update_run_params[1] == "completed"
    assert update_run_params[2] == "ready"
    assert update_run_params[3] == "supported"
    assert update_run_params[5] == "canonical_snapshot_only"
    assert update_run_params[6] == "completed"
    assert update_run_params[7].obj["source_coverage_status"] == "canonical_snapshot_only"
    assert update_run_params[7].obj["valuation_bathroom_attachment_status"] == "not_applicable"


def test_create_run_with_subject_snapshot_attaches_fort_bend_bathroom_metadata(monkeypatch) -> None:
    subject_row = _base_subject_row(county_id="fort_bend")
    subject_row["full_baths"] = None
    subject_row["total_rooms"] = None
    bathroom_row = {
        "quick_ref_id": "QR-1",
        "account_number": "1001001001001",
        "selected_improvement_number": "1",
        "selected_improvement_rule_version": "fort_bend_primary_residential_improvement_v1",
        "normalization_rule_version": "fort_bend_bathroom_features_v1",
        "source_file_version": "WebsiteResidentialSegs.csv:sha256:test",
        "source_file_name": "WebsiteResidentialSegs.csv",
        "selected_improvement_source_row_count": 2,
        "plumbing_raw": 2.5,
        "half_baths_raw": None,
        "quarter_baths_raw": 0.0,
        "plumbing_raw_values": [2.5],
        "half_baths_raw_values": [],
        "quarter_baths_raw_values": [0.0],
        "full_baths_derived": 2.0,
        "half_baths_derived": 1.0,
        "quarter_baths_derived": 0.0,
        "bathroom_equivalent_derived": 2.5,
        "bathroom_count_status": "reconciled_fractional_plumbing",
        "bathroom_count_confidence": "medium",
        "bathroom_flags": ["fractional_plumbing_source"],
    }
    connection = SequenceConnection([subject_row, bathroom_row])
    monkeypatch.setattr(
        "app.services.unequal_roll_subject_snapshot.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollSubjectSnapshotService().create_run_with_subject_snapshot(
        county_id="fort_bend",
        tax_year=2026,
        account_number="1001001001001",
    )

    assert result.source_coverage_status == (
        "canonical_snapshot_with_additive_bathroom_metadata"
    )
    assert result.support_status == "supported"

    assert len(connection.cursor_instance.execute_calls) == 5
    insert_snapshot_params = connection.cursor_instance.execute_calls[3][1]
    bathroom_json = insert_snapshot_params[37].obj
    assert bathroom_json["attachment_status"] == "attached"
    assert bathroom_json["bathroom_count_status"] == "reconciled_fractional_plumbing"
    assert bathroom_json["bathroom_count_confidence"] == "medium"
    assert bathroom_json["full_baths_derived"] == 2.0

    update_run_params = connection.cursor_instance.execute_calls[4][1]
    assert update_run_params[5] == "canonical_snapshot_with_additive_bathroom_metadata"


def test_create_run_with_subject_snapshot_preserves_missing_fort_bend_bathroom_metadata(
    monkeypatch,
) -> None:
    subject_row = _base_subject_row(county_id="fort_bend")
    connection = SequenceConnection([subject_row, None])
    monkeypatch.setattr(
        "app.services.unequal_roll_subject_snapshot.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollSubjectSnapshotService().create_run_with_subject_snapshot(
        county_id="fort_bend",
        tax_year=2026,
        account_number="1001001001001",
    )

    assert result.run_status == "completed"
    assert result.source_coverage_status == (
        "canonical_snapshot_with_missing_additive_bathroom_metadata"
    )
    assert result.support_status == "supported_with_review"

    assert len(connection.cursor_instance.execute_calls) == 5
    insert_snapshot_params = connection.cursor_instance.execute_calls[3][1]
    bathroom_json = insert_snapshot_params[37].obj
    assert bathroom_json["attachment_status"] == "missing"
    assert bathroom_json["source_table"] == "fort_bend_valuation_bathroom_features"

    update_run_params = connection.cursor_instance.execute_calls[4][1]
    assert update_run_params[3] == "supported_with_review"
    assert update_run_params[5] == "canonical_snapshot_with_missing_additive_bathroom_metadata"
    assert update_run_params[7].obj["valuation_bathroom_attachment_status"] == "missing"


def test_create_run_with_subject_snapshot_marks_missing_subject_source(monkeypatch) -> None:
    connection = SequenceConnection([None])
    monkeypatch.setattr(
        "app.services.unequal_roll_subject_snapshot.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollSubjectSnapshotService().create_run_with_subject_snapshot(
        county_id="harris",
        tax_year=2026,
        account_number="missing",
    )

    assert result.run_status == "blocked"
    assert result.support_status == "unsupported"
    assert result.support_blocker_code == "subject_not_found"
    assert result.subject_snapshot_status == "missing_subject_source"
    assert connection.commit_calls == 1

    assert len(connection.cursor_instance.execute_calls) == 3
    update_run_params = connection.cursor_instance.execute_calls[2][1]
    assert update_run_params[0] is None
    assert update_run_params[1] == "blocked"
    assert update_run_params[4] == "subject_not_found"


def test_create_run_with_subject_snapshot_marks_non_sfr_subject_not_ready(monkeypatch) -> None:
    subject_row = _base_subject_row()
    subject_row["property_type_code"] = "commercial"
    connection = SequenceConnection([subject_row])
    monkeypatch.setattr(
        "app.services.unequal_roll_subject_snapshot.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollSubjectSnapshotService().create_run_with_subject_snapshot(
        county_id="harris",
        tax_year=2026,
        account_number="1001001001001",
    )

    assert result.run_status == "completed"
    assert result.readiness_status == "not_ready"
    assert result.support_status == "unsupported"
    assert result.support_blocker_code == "unsupported_property_type"

    update_run_params = connection.cursor_instance.execute_calls[3][1]
    assert update_run_params[3] == "unsupported"
    assert update_run_params[4] == "unsupported_property_type"


def test_create_run_with_subject_snapshot_marks_prior_year_fallback_supported_with_review(
    monkeypatch,
) -> None:
    subject_row = _base_subject_row(tax_year=2025)
    connection = SequenceConnection([subject_row])
    monkeypatch.setattr(
        "app.services.unequal_roll_subject_snapshot.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollSubjectSnapshotService().create_run_with_subject_snapshot(
        county_id="harris",
        tax_year=2026,
        account_number="1001001001001",
    )

    assert result.served_tax_year == 2025
    assert result.support_status == "supported_with_review"
    assert result.readiness_status == "ready"

    update_run_params = connection.cursor_instance.execute_calls[3][1]
    assert update_run_params[3] == "supported_with_review"
    assert update_run_params[7].obj["tax_year_fallback_applied"] is True


def test_create_run_with_subject_snapshot_marks_low_completeness_manual_review_required(
    monkeypatch,
) -> None:
    subject_row = _base_subject_row()
    subject_row["completeness_score"] = 72.0
    connection = SequenceConnection([subject_row])
    monkeypatch.setattr(
        "app.services.unequal_roll_subject_snapshot.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollSubjectSnapshotService().create_run_with_subject_snapshot(
        county_id="harris",
        tax_year=2026,
        account_number="1001001001001",
    )

    assert result.run_status == "completed"
    assert result.readiness_status == "not_ready"
    assert result.support_status == "manual_review_required"
    assert result.support_blocker_code == "subject_source_requires_review"

    update_run_params = connection.cursor_instance.execute_calls[3][1]
    assert update_run_params[2] == "not_ready"
    assert update_run_params[3] == "manual_review_required"
    assert update_run_params[4] == "subject_source_requires_review"
