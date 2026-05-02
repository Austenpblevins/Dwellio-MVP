from __future__ import annotations

from contextlib import contextmanager
from uuid import uuid4

import pytest

from app.services.unequal_roll_candidate_discovery import (
    UnequalRollCandidateDiscoveryService,
)


class SequenceCursor:
    def __init__(
        self,
        *,
        fetchone_results: list[dict[str, object] | None],
        fetchall_results: list[list[dict[str, object]]],
    ) -> None:
        self.fetchone_results = fetchone_results
        self.fetchall_results = fetchall_results
        self.execute_calls: list[tuple[str, tuple[object, ...] | None]] = []
        self.fetchone_index = 0
        self.fetchall_index = 0

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
        if self.fetchall_index >= len(self.fetchall_results):
            return []
        result = self.fetchall_results[self.fetchall_index]
        self.fetchall_index += 1
        return result


class SequenceConnection:
    def __init__(
        self,
        *,
        fetchone_results: list[dict[str, object] | None],
        fetchall_results: list[list[dict[str, object]]],
    ) -> None:
        self.cursor_instance = SequenceCursor(
            fetchone_results=fetchone_results,
            fetchall_results=fetchall_results,
        )
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


def _subject_snapshot_row(*, county_id: str = "harris", tax_year: int = 2026) -> dict[str, object]:
    return {
        "run_status": "completed",
        "readiness_status": "ready",
        "support_status": "supported",
        "subject_snapshot_status": "completed",
        "summary_json": {"requested_tax_year": tax_year},
        "unequal_roll_run_id": str(uuid4()),
        "parcel_id": uuid4(),
        "county_id": county_id,
        "tax_year": tax_year,
        "account_number": "1001001001001",
        "address": "101 Main St, Houston, TX 77002",
        "property_type_code": "sfr",
        "property_class_code": "A1",
        "neighborhood_code": "NBH-001",
        "subdivision_name": "Heights",
        "living_area_sf": 2000.0,
        "bedrooms": 4,
        "full_baths": 2.0,
        "half_baths": 1.0,
        "stories": 2.0,
        "quality_code": "AVERAGE_PLUS",
        "condition_code": "GOOD",
        "land_sf": 7300.0,
        "land_acres": 0.1676,
    }


def _candidate_row(
    *,
    parcel_id: str | None = None,
    county_id: str = "harris",
    tax_year: int = 2026,
    account_number: str = "2002002002002",
    neighborhood_code: str = "NBH-001",
    subdivision_name: str = "Heights",
    property_type_code: str = "sfr",
    property_class_code: str = "A1",
    living_area_sf: float = 2100.0,
    quality_code: str | None = "AVERAGE_PLUS",
    condition_code: str | None = "GOOD",
    bedrooms: int = 4,
    half_baths: float | None = 1.0,
    stories: float | None = 2.0,
    land_sf: float | None = 7300.0,
    land_acres: float | None = 0.1676,
    full_baths: float | None = 2.0,
) -> dict[str, object]:
    return {
        "parcel_id": parcel_id or uuid4(),
        "county_id": county_id,
        "tax_year": tax_year,
        "account_number": account_number,
        "address": "202 Elm St, Houston, TX 77002",
        "neighborhood_code": neighborhood_code,
        "subdivision_name": subdivision_name,
        "property_type_code": property_type_code,
        "property_class_code": property_class_code,
        "living_area_sf": living_area_sf,
        "year_built": 2001,
        "effective_age": 8.0,
        "bedrooms": bedrooms,
        "full_baths": full_baths,
        "half_baths": half_baths,
        "total_rooms": 8,
        "stories": stories,
        "quality_code": quality_code,
        "condition_code": condition_code,
        "pool_flag": False,
        "land_sf": land_sf,
        "land_acres": land_acres,
        "market_value": 430000.0,
        "assessed_value": 398000.0,
        "appraised_value": 412000.0,
        "certified_value": 408000.0,
        "notice_value": 415000.0,
    }


def test_discover_candidates_persists_same_neighborhood_candidates(monkeypatch) -> None:
    subject_row = _subject_snapshot_row()
    neighborhood_candidates = [
        _candidate_row(parcel_id="parcel-a"),
        _candidate_row(parcel_id="parcel-b", account_number="3003003003003"),
    ]
    connection = SequenceConnection(
        fetchone_results=[subject_row],
        fetchall_results=[neighborhood_candidates, []],
    )
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_discovery.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateDiscoveryService().discover_candidates_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.discovered_count == 2
    assert result.same_neighborhood_count == 2
    assert result.county_sfr_fallback_count == 0
    assert result.eligible_count == 2
    assert result.review_count == 0
    assert result.excluded_count == 0
    assert result.fallback_used is False
    assert result.sparse_universe_warning is True
    assert connection.commit_calls == 1

    assert len(connection.cursor_instance.execute_calls) == 7
    same_neighborhood_sql = connection.cursor_instance.execute_calls[2][0]
    fallback_sql = connection.cursor_instance.execute_calls[3][0]
    assert "FROM parcel_year_snapshots AS pys" in same_neighborhood_sql
    assert "pc.neighborhood_code = %s" in same_neighborhood_sql
    assert "lower(coalesce(pc.property_type_code, '')) = 'sfr'" in same_neighborhood_sql
    assert "lower(coalesce(pc.property_type_code, '')) = 'sfr'" in fallback_sql

    first_insert_params = connection.cursor_instance.execute_calls[4][1]
    assert first_insert_params[0] == "run-1"
    assert first_insert_params[28] == "same_neighborhood"
    assert first_insert_params[29] == "discovered"
    assert first_insert_params[30] == "eligible"
    assert first_insert_params[31] is None
    assert first_insert_params[32].obj["primary_reason_code"] is None
    assert first_insert_params[33].obj["same_neighborhood_flag"] is True
    assert first_insert_params[34].obj["subject_relationship"]["same_subdivision_flag"] is True
    assert first_insert_params[35] > 0
    assert 0 < first_insert_params[36] <= 1
    assert first_insert_params[37] == "unequal_roll_similarity_v1"
    assert first_insert_params[38] == "unequal_roll_similarity_v1"
    assert (
        first_insert_params[39].obj["components"]["locality"]["weighted_points"] > 0
    )

    run_update_params = connection.cursor_instance.execute_calls[6][1]
    candidate_summary = run_update_params[0].obj["candidate_discovery_summary"]
    assert candidate_summary["eligible_count"] == 2
    assert candidate_summary["review_count"] == 0
    assert candidate_summary["excluded_count"] == 0
    assert candidate_summary["fallback_used"] is False
    assert candidate_summary["sparse_universe_warning"] is True


def test_discover_candidates_adds_bounded_fallback_tier_when_neighborhood_is_thin(
    monkeypatch,
) -> None:
    subject_row = _subject_snapshot_row()
    neighborhood_candidates = [_candidate_row(parcel_id="parcel-a")]
    fallback_candidates = [
        _candidate_row(
            parcel_id="parcel-fallback",
            neighborhood_code="NBH-009",
            subdivision_name="Heights",
        )
    ]
    connection = SequenceConnection(
        fetchone_results=[subject_row],
        fetchall_results=[neighborhood_candidates, fallback_candidates],
    )
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_discovery.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateDiscoveryService().discover_candidates_for_run(
        unequal_roll_run_id="run-2"
    )

    assert result.discovered_count == 2
    assert result.same_neighborhood_count == 1
    assert result.county_sfr_fallback_count == 1
    assert result.eligible_count == 1
    assert result.review_count == 1
    assert result.excluded_count == 0
    assert result.fallback_used is True
    assert result.sparse_universe_warning is True

    first_insert_params = connection.cursor_instance.execute_calls[4][1]
    second_insert_params = connection.cursor_instance.execute_calls[5][1]
    assert first_insert_params[28] == "same_neighborhood"
    assert second_insert_params[28] == "county_sfr_fallback"
    assert second_insert_params[30] == "review"
    assert second_insert_params[31] == "fallback_geography_used"
    assert "fallback_geography_used" in second_insert_params[32].obj["primary_reason_code"]
    assert second_insert_params[33].obj["same_neighborhood_flag"] is False
    assert second_insert_params[34].obj["subject_relationship"]["same_subdivision_flag"] is True

    run_update_params = connection.cursor_instance.execute_calls[6][1]
    candidate_summary = run_update_params[0].obj["candidate_discovery_summary"]
    assert candidate_summary["fallback_used"] is True
    assert (
        candidate_summary["same_neighborhood_insufficient_reason"]
        == "same_neighborhood_supply_below_preferred_pool"
    )


def test_discover_candidates_rejects_unsupported_subject_property_type(monkeypatch) -> None:
    subject_row = _subject_snapshot_row()
    subject_row["property_type_code"] = "commercial"
    connection = SequenceConnection(fetchone_results=[subject_row], fetchall_results=[])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_discovery.get_connection",
        connection_factory(connection),
    )

    with pytest.raises(ValueError):
        UnequalRollCandidateDiscoveryService().discover_candidates_for_run(
            unequal_roll_run_id="run-3"
        )


def test_discover_candidates_blocks_run_when_support_is_unsupported(monkeypatch) -> None:
    subject_row = _subject_snapshot_row()
    subject_row["support_status"] = "unsupported"
    connection = SequenceConnection(fetchone_results=[subject_row], fetchall_results=[])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_discovery.get_connection",
        connection_factory(connection),
    )

    with pytest.raises(ValueError, match="support_status"):
        UnequalRollCandidateDiscoveryService().discover_candidates_for_run(
            unequal_roll_run_id="run-unsupported"
        )


def test_discover_candidates_blocks_manual_review_required_runs(monkeypatch) -> None:
    subject_row = _subject_snapshot_row()
    subject_row["readiness_status"] = "ready"
    subject_row["support_status"] = "manual_review_required"
    connection = SequenceConnection(fetchone_results=[subject_row], fetchall_results=[])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_discovery.get_connection",
        connection_factory(connection),
    )

    with pytest.raises(ValueError, match="support_status"):
        UnequalRollCandidateDiscoveryService().discover_candidates_for_run(
            unequal_roll_run_id="run-manual-review"
        )


def test_discover_candidates_blocks_pending_runs(monkeypatch) -> None:
    subject_row = _subject_snapshot_row()
    subject_row["run_status"] = "pending"
    connection = SequenceConnection(fetchone_results=[subject_row], fetchall_results=[])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_discovery.get_connection",
        connection_factory(connection),
    )

    with pytest.raises(ValueError, match="completed unequal-roll run"):
        UnequalRollCandidateDiscoveryService().discover_candidates_for_run(
            unequal_roll_run_id="run-pending"
        )


def test_discover_candidates_preserves_fort_bend_additive_bathroom_boundary(monkeypatch) -> None:
    subject_row = _subject_snapshot_row(county_id="fort_bend")
    candidate_row = _candidate_row(
        parcel_id="parcel-fb",
        county_id="fort_bend",
        full_baths=None,
    )
    bathroom_row = {
        "quick_ref_id": "QR-10",
        "account_number": "2002002002002",
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
    connection = SequenceConnection(
        fetchone_results=[subject_row, bathroom_row],
        fetchall_results=[[candidate_row], []],
    )
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_discovery.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateDiscoveryService().discover_candidates_for_run(
        unequal_roll_run_id="run-4"
    )

    assert result.discovered_count == 1
    assert result.eligible_count == 1

    insert_params = connection.cursor_instance.execute_calls[5][1]
    assert insert_params[14] is None
    assert (
        insert_params[33].obj["valuation_bathroom_attachment_status"] == "attached"
    )
    assert insert_params[32].obj["fort_bend_bathroom_review"]["review_required"] is False
    assert insert_params[34].obj["candidate"]["full_baths"] is None
    assert (
        insert_params[34].obj["valuation_bathroom_features"]["full_baths_derived"] == 2.0
    )


def test_discover_candidates_persists_exclusion_reason_for_extreme_living_area_gap(
    monkeypatch,
) -> None:
    subject_row = _subject_snapshot_row()
    neighborhood_candidates = [_candidate_row(parcel_id="parcel-gap", living_area_sf=3100.0)]
    connection = SequenceConnection(
        fetchone_results=[subject_row],
        fetchall_results=[neighborhood_candidates, []],
    )
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_discovery.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateDiscoveryService().discover_candidates_for_run(
        unequal_roll_run_id="run-gap"
    )

    assert result.discovered_count == 1
    assert result.excluded_count == 1

    insert_params = connection.cursor_instance.execute_calls[4][1]
    assert insert_params[30] == "excluded"
    assert insert_params[31] == "living_area_out_of_bounds"
    assert insert_params[32].obj["primary_reason_code"] == "living_area_out_of_bounds"
    assert insert_params[34].obj["eligibility"]["eligibility_status"] == "excluded"


def test_discover_candidates_persists_review_reason_for_missing_quality_code(
    monkeypatch,
) -> None:
    subject_row = _subject_snapshot_row()
    neighborhood_candidates = [_candidate_row(parcel_id="parcel-review", quality_code=None)]
    connection = SequenceConnection(
        fetchone_results=[subject_row],
        fetchall_results=[neighborhood_candidates, []],
    )
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_discovery.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateDiscoveryService().discover_candidates_for_run(
        unequal_roll_run_id="run-review"
    )

    assert result.discovered_count == 1
    assert result.review_count == 1

    insert_params = connection.cursor_instance.execute_calls[4][1]
    assert insert_params[30] == "review"
    assert insert_params[31] == "missing_quality_code"
    assert insert_params[32].obj["primary_reason_code"] == "missing_quality_code"
    assert insert_params[34].obj["eligibility"]["eligibility_status"] == "review"


def test_discover_candidates_excludes_non_adjacent_property_class_candidates(
    monkeypatch,
) -> None:
    subject_row = _subject_snapshot_row()
    neighborhood_candidates = [
        _candidate_row(parcel_id="parcel-class", property_class_code="B2")
    ]
    connection = SequenceConnection(
        fetchone_results=[subject_row],
        fetchall_results=[neighborhood_candidates, []],
    )
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_discovery.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateDiscoveryService().discover_candidates_for_run(
        unequal_roll_run_id="run-class"
    )

    assert result.excluded_count == 1
    insert_params = connection.cursor_instance.execute_calls[4][1]
    assert insert_params[30] == "excluded"
    assert insert_params[31] == "property_class_non_adjacent"
    assert insert_params[32].obj["subject_relationship"]["property_class_relation"] == "non_adjacent"


def test_discover_candidates_reviews_adjacent_quality_and_condition_candidates(
    monkeypatch,
) -> None:
    subject_row = _subject_snapshot_row()
    neighborhood_candidates = [
        _candidate_row(
            parcel_id="parcel-quality",
            quality_code="GOOD",
            condition_code="VERY_GOOD",
        )
    ]
    connection = SequenceConnection(
        fetchone_results=[subject_row],
        fetchall_results=[neighborhood_candidates, []],
    )
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_discovery.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateDiscoveryService().discover_candidates_for_run(
        unequal_roll_run_id="run-quality"
    )

    assert result.review_count == 1
    insert_params = connection.cursor_instance.execute_calls[4][1]
    assert insert_params[30] == "review"
    assert insert_params[31] == "condition_adjacent"
    assert insert_params[32].obj["threshold_observations"]["quality_gap_steps"] == 0
    assert insert_params[32].obj["threshold_observations"]["condition_gap_steps"] == 1


def test_discover_candidates_applies_harris_quality_letter_normalization(
    monkeypatch,
) -> None:
    subject_row = _subject_snapshot_row()
    subject_row["quality_code"] = "B"
    subject_row["condition_code"] = "GOOD"
    neighborhood_candidates = [
        _candidate_row(
            parcel_id="parcel-harris-quality",
            quality_code="C",
            condition_code="AVERAGE",
        )
    ]
    connection = SequenceConnection(
        fetchone_results=[subject_row],
        fetchall_results=[neighborhood_candidates, []],
    )
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_discovery.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateDiscoveryService().discover_candidates_for_run(
        unequal_roll_run_id="run-harris-quality"
    )

    assert result.review_count == 1
    insert_params = connection.cursor_instance.execute_calls[4][1]
    assert insert_params[30] == "review"
    assert insert_params[31] == "quality_adjacent"
    assert "condition_adjacent" in insert_params[32].obj["secondary_reason_codes"]
    assert insert_params[32].obj["threshold_observations"]["quality_gap_steps"] == 1
    assert insert_params[32].obj["threshold_observations"]["condition_gap_steps"] == 1


def test_discover_candidates_applies_fort_bend_numeric_quality_condition_normalization(
    monkeypatch,
) -> None:
    subject_row = _subject_snapshot_row(county_id="fort_bend")
    subject_row["quality_code"] = "0"
    subject_row["condition_code"] = "0"
    neighborhood_candidates = [
        _candidate_row(
            parcel_id="parcel-fb-quality",
            county_id="fort_bend",
            quality_code="1",
            condition_code="1",
        )
    ]
    connection = SequenceConnection(
        fetchone_results=[subject_row, None],
        fetchall_results=[neighborhood_candidates, []],
    )
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_discovery.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateDiscoveryService().discover_candidates_for_run(
        unequal_roll_run_id="run-fb-quality"
    )

    assert result.review_count == 1
    insert_params = connection.cursor_instance.execute_calls[5][1]
    assert insert_params[30] == "review"
    assert insert_params[31] == "quality_adjacent"
    assert "condition_adjacent" in insert_params[32].obj["secondary_reason_codes"]
    assert insert_params[32].obj["threshold_observations"]["quality_gap_steps"] == 1
    assert insert_params[32].obj["threshold_observations"]["condition_gap_steps"] == 1


def test_discover_candidates_reviews_wide_lot_size_gap_with_detail(monkeypatch) -> None:
    subject_row = _subject_snapshot_row()
    neighborhood_candidates = [
        _candidate_row(parcel_id="parcel-lot", land_sf=9800.0, land_acres=0.225)
    ]
    connection = SequenceConnection(
        fetchone_results=[subject_row],
        fetchall_results=[neighborhood_candidates, []],
    )
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_discovery.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateDiscoveryService().discover_candidates_for_run(
        unequal_roll_run_id="run-lot"
    )

    assert result.review_count == 1
    insert_params = connection.cursor_instance.execute_calls[4][1]
    assert insert_params[30] == "review"
    assert insert_params[31] == "wide_lot_size_gap"
    assert insert_params[32].obj["threshold_observations"]["land_size_diff_pct"] > 0.25


def test_discover_candidates_excludes_severe_acreage_profile_mismatch(monkeypatch) -> None:
    subject_row = _subject_snapshot_row()
    neighborhood_candidates = [
        _candidate_row(parcel_id="parcel-acreage", land_sf=None, land_acres=2.0)
    ]
    connection = SequenceConnection(
        fetchone_results=[subject_row],
        fetchall_results=[neighborhood_candidates, []],
    )
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_discovery.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateDiscoveryService().discover_candidates_for_run(
        unequal_roll_run_id="run-acreage"
    )

    assert result.excluded_count == 1
    insert_params = connection.cursor_instance.execute_calls[4][1]
    assert insert_params[30] == "excluded"
    assert insert_params[31] == "acreage_profile_mismatch"


def test_discover_candidates_reviews_harris_whitelisted_property_class_adjacency(
    monkeypatch,
) -> None:
    subject_row = _subject_snapshot_row()
    neighborhood_candidates = [
        _candidate_row(parcel_id="parcel-harris-class", property_class_code="A4")
    ]
    connection = SequenceConnection(
        fetchone_results=[subject_row],
        fetchall_results=[neighborhood_candidates, []],
    )
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_discovery.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateDiscoveryService().discover_candidates_for_run(
        unequal_roll_run_id="run-harris-class"
    )

    assert result.review_count == 1
    insert_params = connection.cursor_instance.execute_calls[4][1]
    assert insert_params[30] == "review"
    assert insert_params[31] == "property_class_adjacent_family"
    assert insert_params[32].obj["subject_relationship"]["property_class_relation"] == "adjacent_family"


def test_discover_candidates_persists_fort_bend_review_detail_without_coercing_baths(
    monkeypatch,
) -> None:
    subject_row = _subject_snapshot_row(county_id="fort_bend")
    candidate_row = _candidate_row(
        parcel_id="parcel-fb-review",
        county_id="fort_bend",
        full_baths=None,
    )
    bathroom_row = {
        "quick_ref_id": "QR-11",
        "account_number": "2002002002002",
        "selected_improvement_number": "1",
        "selected_improvement_rule_version": "fort_bend_primary_residential_improvement_v1",
        "normalization_rule_version": "fort_bend_bathroom_features_v1",
        "source_file_version": "WebsiteResidentialSegs.csv:sha256:test",
        "source_file_name": "WebsiteResidentialSegs.csv",
        "selected_improvement_source_row_count": 3,
        "plumbing_raw": 2.75,
        "half_baths_raw": None,
        "quarter_baths_raw": 0.25,
        "plumbing_raw_values": [2.75],
        "half_baths_raw_values": [],
        "quarter_baths_raw_values": [0.25],
        "full_baths_derived": 2.0,
        "half_baths_derived": 1.0,
        "quarter_baths_derived": 0.25,
        "bathroom_equivalent_derived": 2.75,
        "bathroom_count_status": "ambiguous_multi_improvement",
        "bathroom_count_confidence": "low",
        "bathroom_flags": ["ambiguous_improvement_match"],
    }
    connection = SequenceConnection(
        fetchone_results=[subject_row, bathroom_row],
        fetchall_results=[[candidate_row], []],
    )
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_discovery.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateDiscoveryService().discover_candidates_for_run(
        unequal_roll_run_id="run-fb-review"
    )

    assert result.review_count == 1
    insert_params = connection.cursor_instance.execute_calls[5][1]
    assert insert_params[14] is None
    assert insert_params[30] == "review"
    assert insert_params[31] == "fort_bend_bathroom_status_review"
    assert insert_params[32].obj["fort_bend_bathroom_review"]["review_required"] is True
    assert insert_params[34].obj["candidate"]["full_baths"] is None
