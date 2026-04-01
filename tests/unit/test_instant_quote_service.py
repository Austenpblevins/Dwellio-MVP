from __future__ import annotations

from uuid import uuid4

from app.models.quote import InstantQuoteResponse
from app.services.instant_quote import (
    MATERIAL_CAP_GAP_RATIO,
    TELEMETRY_MAX_INFLIGHT_TASKS,
    InstantQuoteService,
    InstantQuoteStatsRow,
    assign_age_bucket,
    assign_size_bucket,
    build_public_estimate,
    calculate_distribution_stats,
    choose_fallback,
    confidence_label_for_score,
    determine_tax_limitation_outcome,
    has_uncertain_tax_limitation_signal,
    is_material_homestead_cap_limited,
    score_confidence,
)


class _StubCursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, *_args, **_kwargs) -> None:
        return None


class _StubConnection:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> _StubCursor:
        return _StubCursor()


def _patch_request_connection(monkeypatch) -> None:
    monkeypatch.setattr("app.services.instant_quote.get_connection", lambda: _StubConnection())


def test_assign_size_bucket_uses_canonical_ranges() -> None:
    assert assign_size_bucket(1399) == "lt_1400"
    assert assign_size_bucket(1400) == "1400_1699"
    assert assign_size_bucket(1700) == "1700_1999"
    assert assign_size_bucket(3500) == "3500_plus"


def test_assign_age_bucket_uses_canonical_ranges() -> None:
    assert assign_age_bucket(None) == "unknown"
    assert assign_age_bucket(1969) == "pre_1970"
    assert assign_age_bucket(1970) == "1970_1989"
    assert assign_age_bucket(2005) == "2005_2014"
    assert assign_age_bucket(2018) == "2015_plus"


def test_calculate_distribution_stats_returns_monotonic_percentiles() -> None:
    summary = calculate_distribution_stats([90, 100, 110, 120, 130, 140, 150])

    assert summary is not None
    assert summary.p10 <= summary.p25 <= summary.p50 <= summary.p75 <= summary.p90
    assert summary.parcel_count == 7
    assert summary.trimmed_parcel_count <= summary.parcel_count


def test_choose_fallback_prefers_segment_when_support_is_strong() -> None:
    neighborhood = InstantQuoteStatsRow(
        parcel_count=30,
        p10_assessed_psf=100,
        p25_assessed_psf=110,
        p50_assessed_psf=120,
        p75_assessed_psf=130,
        p90_assessed_psf=140,
        mean_assessed_psf=121,
        median_assessed_psf=120,
        stddev_assessed_psf=9,
        coefficient_of_variation=0.07,
        support_level="strong",
        support_threshold_met=True,
    )
    segment = InstantQuoteStatsRow(
        parcel_count=25,
        p10_assessed_psf=105,
        p25_assessed_psf=115,
        p50_assessed_psf=125,
        p75_assessed_psf=135,
        p90_assessed_psf=145,
        mean_assessed_psf=126,
        median_assessed_psf=125,
        stddev_assessed_psf=8,
        coefficient_of_variation=0.06,
        support_level="strong",
        support_threshold_met=True,
    )

    fallback_tier, segment_weight, neighborhood_weight, basis_code = choose_fallback(
        segment_stats=segment,
        neighborhood_stats=neighborhood,
    )

    assert fallback_tier == "segment_within_neighborhood"
    assert segment_weight == 0.70
    assert neighborhood_weight == 0.30
    assert basis_code == "assessment_basis_segment_blend"


def test_choose_fallback_uses_neighborhood_when_segment_support_is_thin() -> None:
    neighborhood = InstantQuoteStatsRow(
        parcel_count=30,
        p10_assessed_psf=100,
        p25_assessed_psf=110,
        p50_assessed_psf=120,
        p75_assessed_psf=130,
        p90_assessed_psf=140,
        mean_assessed_psf=121,
        median_assessed_psf=120,
        stddev_assessed_psf=9,
        coefficient_of_variation=0.07,
        support_level="strong",
        support_threshold_met=True,
    )
    segment = InstantQuoteStatsRow(
        parcel_count=6,
        p10_assessed_psf=105,
        p25_assessed_psf=115,
        p50_assessed_psf=125,
        p75_assessed_psf=135,
        p90_assessed_psf=145,
        mean_assessed_psf=126,
        median_assessed_psf=125,
        stddev_assessed_psf=8,
        coefficient_of_variation=0.06,
        support_level="thin",
        support_threshold_met=False,
    )

    fallback_tier, segment_weight, neighborhood_weight, basis_code = choose_fallback(
        segment_stats=segment,
        neighborhood_stats=neighborhood,
    )

    assert fallback_tier == "neighborhood_only"
    assert segment_weight == 0.0
    assert neighborhood_weight == 1.0
    assert basis_code == "assessment_basis_neighborhood_only"


def test_build_public_estimate_returns_constrained_range_for_tax_protection() -> None:
    estimate = build_public_estimate(
        savings_estimate=1100,
        confidence_label="medium",
        tax_protection_limited=True,
    )

    assert estimate.tax_protection_limited is True
    assert estimate.savings_range_low == 0
    assert estimate.savings_range_high is not None
    assert estimate.tax_protection_note is not None


def test_homestead_alone_does_not_force_tax_limitation_outcome() -> None:
    outcome = determine_tax_limitation_outcome(
        subject_row={
            "homestead_flag": True,
            "freeze_flag": False,
            "capped_value": None,
            "assessment_basis_value": 350000,
            "warning_codes": [],
        },
        confidence_score=82,
    )

    assert outcome == "normal"


def test_material_cap_gap_threshold_is_explicit_and_reasonable() -> None:
    assert MATERIAL_CAP_GAP_RATIO == 0.03

    assert is_material_homestead_cap_limited(
        {
            "homestead_flag": True,
            "capped_value": 97000,
            "assessment_basis_value": 100000,
        }
    )
    assert not is_material_homestead_cap_limited(
        {
            "homestead_flag": True,
            "capped_value": 98000,
            "assessment_basis_value": 100000,
        }
    )


def test_freeze_case_can_trigger_constrained_tax_limitation_outcome() -> None:
    outcome = determine_tax_limitation_outcome(
        subject_row={
            "homestead_flag": True,
            "freeze_flag": True,
            "capped_value": 290000,
            "assessment_basis_value": 350000,
            "warning_codes": [],
        },
        confidence_score=82,
    )

    assert outcome == "constrained"


def test_uncertain_tax_limitation_case_can_trigger_suppressed_outcome() -> None:
    subject_row = {
        "homestead_flag": True,
        "freeze_flag": True,
        "capped_value": 290000,
        "assessment_basis_value": 350000,
        "warning_codes": ["freeze_without_qualifying_exemption"],
    }

    assert has_uncertain_tax_limitation_signal(subject_row) is True
    assert determine_tax_limitation_outcome(subject_row=subject_row, confidence_score=82) == "suppressed"


def test_confidence_score_penalizes_neighborhood_only_and_freeze_flags() -> None:
    neighborhood = InstantQuoteStatsRow(
        parcel_count=22,
        p10_assessed_psf=90,
        p25_assessed_psf=100,
        p50_assessed_psf=110,
        p75_assessed_psf=120,
        p90_assessed_psf=130,
        mean_assessed_psf=111,
        median_assessed_psf=110,
        stddev_assessed_psf=12,
        coefficient_of_variation=0.11,
        support_level="strong",
        support_threshold_met=True,
    )
    score = score_confidence(
        subject_row={
            "year_built": 1998,
            "public_summary_ready_flag": True,
            "effective_tax_rate_source_method": "component_rollup",
            "homestead_flag": True,
            "freeze_flag": True,
            "capped_value": 290000,
            "assessment_basis_value": 350000,
            "warning_codes": [],
        },
        segment_stats=None,
        neighborhood_stats=neighborhood,
        fallback_tier="neighborhood_only",
        subject_assessed_psf=140,
        target_psf=110,
    )

    assert score < 65
    assert confidence_label_for_score(score) == "low"


def test_instant_quote_service_returns_supported_response_when_stats_exist(monkeypatch) -> None:
    service = InstantQuoteService()
    _patch_request_connection(monkeypatch)
    parcel_id = uuid4()
    subject_row = {
        "parcel_id": parcel_id,
        "county_id": "harris",
        "tax_year": 2025,
        "account_number": "1001001001001",
        "address": "101 Main St, Houston, TX 77002",
        "neighborhood_code": "NBHD-1",
        "school_district_name": "Houston ISD",
        "property_type_code": "sfr",
        "property_class_code": "A1",
        "living_area_sf": 2200.0,
        "year_built": 2003,
        "capped_value": 350000.0,
        "notice_value": 360000.0,
        "assessment_basis_value": 350000.0,
        "effective_tax_rate": 0.021,
        "effective_tax_rate_source_method": "manual",
        "subject_assessed_psf": 159.09,
        "size_bucket": "2000_2399",
        "age_bucket": "1990_2004",
        "support_blocker_code": None,
        "public_summary_ready_flag": True,
        "homestead_flag": False,
        "freeze_flag": False,
        "over65_flag": False,
        "disabled_flag": False,
        "disabled_veteran_flag": False,
        "warning_codes": [],
    }
    neighborhood = InstantQuoteStatsRow(
        parcel_count=35,
        p10_assessed_psf=120,
        p25_assessed_psf=130,
        p50_assessed_psf=145,
        p75_assessed_psf=155,
        p90_assessed_psf=170,
        mean_assessed_psf=146,
        median_assessed_psf=145,
        stddev_assessed_psf=11,
        coefficient_of_variation=0.07,
        support_level="strong",
        support_threshold_met=True,
    )
    segment = InstantQuoteStatsRow(
        parcel_count=18,
        p10_assessed_psf=125,
        p25_assessed_psf=135,
        p50_assessed_psf=140,
        p75_assessed_psf=150,
        p90_assessed_psf=160,
        mean_assessed_psf=141,
        median_assessed_psf=140,
        stddev_assessed_psf=8,
        coefficient_of_variation=0.05,
        support_level="medium",
        support_threshold_met=True,
    )

    monkeypatch.setattr(service, "_fetch_subject_row", lambda **_: subject_row)
    monkeypatch.setattr(service, "_fetch_neighborhood_stats", lambda **_: neighborhood)
    monkeypatch.setattr(service, "_fetch_segment_stats", lambda **_: segment)
    monkeypatch.setattr(service, "_enqueue_request_log_persistence", lambda **_: None)
    monkeypatch.setattr(service, "_emit_logs", lambda **_: None)

    response = service.get_quote(
        county_id="harris",
        tax_year=2026,
        account_number="1001001001001",
    )

    assert isinstance(response, InstantQuoteResponse)
    assert response.supported is True
    assert response.tax_year == 2025
    assert response.tax_year_fallback_applied is True
    assert response.estimate is not None
    assert response.estimate.savings_range_high is not None
    assert response.explanation.methodology == "segment_within_neighborhood"


def test_fetch_subject_row_prefers_latest_year_with_ready_stats(monkeypatch) -> None:
    service = InstantQuoteService()
    ready_row = {"tax_year": 2025, "account_number": "1001001001001"}
    latest_row = {"tax_year": 2026, "account_number": "1001001001001"}

    monkeypatch.setattr(service, "_fetch_subject_row_with_ready_stats", lambda **_: ready_row)
    monkeypatch.setattr(service, "_fetch_latest_subject_row", lambda **_: latest_row)

    row = service._fetch_subject_row(
        connection=_StubConnection(),
        county_id="harris",
        requested_tax_year=2026,
        account_number="1001001001001",
    )

    assert row == ready_row


def test_fetch_subject_row_falls_back_to_latest_subject_row_when_no_ready_year_exists(
    monkeypatch,
) -> None:
    service = InstantQuoteService()
    latest_row = {"tax_year": 2026, "account_number": "1001001001001"}

    monkeypatch.setattr(service, "_fetch_subject_row_with_ready_stats", lambda **_: None)
    monkeypatch.setattr(service, "_fetch_latest_subject_row", lambda **_: latest_row)

    row = service._fetch_subject_row(
        connection=_StubConnection(),
        county_id="harris",
        requested_tax_year=2026,
        account_number="1001001001001",
    )

    assert row == latest_row


def test_supported_homestead_parcel_is_not_forced_into_constrained_range(monkeypatch) -> None:
    service = InstantQuoteService()
    _patch_request_connection(monkeypatch)
    subject_row = {
        "parcel_id": uuid4(),
        "county_id": "harris",
        "tax_year": 2026,
        "account_number": "1001001001001",
        "address": "101 Main St, Houston, TX 77002",
        "neighborhood_code": "NBHD-1",
        "school_district_name": "Houston ISD",
        "property_type_code": "sfr",
        "property_class_code": "A1",
        "living_area_sf": 2200.0,
        "year_built": 2003,
        "capped_value": None,
        "notice_value": 360000.0,
        "assessment_basis_value": 350000.0,
        "effective_tax_rate": 0.021,
        "effective_tax_rate_source_method": "manual",
        "subject_assessed_psf": 159.09,
        "size_bucket": "2000_2399",
        "age_bucket": "1990_2004",
        "support_blocker_code": None,
        "public_summary_ready_flag": True,
        "homestead_flag": True,
        "freeze_flag": False,
        "over65_flag": False,
        "disabled_flag": False,
        "disabled_veteran_flag": False,
        "warning_codes": [],
    }
    neighborhood = InstantQuoteStatsRow(
        parcel_count=35,
        p10_assessed_psf=120,
        p25_assessed_psf=130,
        p50_assessed_psf=145,
        p75_assessed_psf=155,
        p90_assessed_psf=170,
        mean_assessed_psf=146,
        median_assessed_psf=145,
        stddev_assessed_psf=11,
        coefficient_of_variation=0.07,
        support_level="strong",
        support_threshold_met=True,
    )
    segment = InstantQuoteStatsRow(
        parcel_count=25,
        p10_assessed_psf=125,
        p25_assessed_psf=135,
        p50_assessed_psf=140,
        p75_assessed_psf=150,
        p90_assessed_psf=160,
        mean_assessed_psf=141,
        median_assessed_psf=140,
        stddev_assessed_psf=8,
        coefficient_of_variation=0.05,
        support_level="strong",
        support_threshold_met=True,
    )

    monkeypatch.setattr(service, "_fetch_subject_row", lambda **_: subject_row)
    monkeypatch.setattr(service, "_fetch_neighborhood_stats", lambda **_: neighborhood)
    monkeypatch.setattr(service, "_fetch_segment_stats", lambda **_: segment)
    monkeypatch.setattr(service, "_enqueue_request_log_persistence", lambda **_: None)
    monkeypatch.setattr(service, "_emit_logs", lambda **_: None)

    response = service.get_quote(
        county_id="harris",
        tax_year=2026,
        account_number="1001001001001",
    )

    assert response.supported is True
    assert response.estimate is not None
    assert response.estimate.tax_protection_limited is False
    assert response.estimate.tax_protection_note is None


def test_freeze_case_can_constrain_numeric_range_without_suppressing_it(monkeypatch) -> None:
    service = InstantQuoteService()
    _patch_request_connection(monkeypatch)
    subject_row = {
        "parcel_id": uuid4(),
        "county_id": "harris",
        "tax_year": 2026,
        "account_number": "1001001001001",
        "address": "101 Main St, Houston, TX 77002",
        "neighborhood_code": "NBHD-1",
        "school_district_name": "Houston ISD",
        "property_type_code": "sfr",
        "property_class_code": "A1",
        "living_area_sf": 2200.0,
        "year_built": 2003,
        "capped_value": 290000.0,
        "notice_value": 360000.0,
        "assessment_basis_value": 350000.0,
        "effective_tax_rate": 0.021,
        "effective_tax_rate_source_method": "manual",
        "subject_assessed_psf": 159.09,
        "size_bucket": "2000_2399",
        "age_bucket": "1990_2004",
        "support_blocker_code": None,
        "public_summary_ready_flag": True,
        "homestead_flag": True,
        "freeze_flag": True,
        "over65_flag": True,
        "disabled_flag": False,
        "disabled_veteran_flag": False,
        "warning_codes": [],
    }
    neighborhood = InstantQuoteStatsRow(
        parcel_count=35,
        p10_assessed_psf=120,
        p25_assessed_psf=130,
        p50_assessed_psf=145,
        p75_assessed_psf=155,
        p90_assessed_psf=170,
        mean_assessed_psf=146,
        median_assessed_psf=145,
        stddev_assessed_psf=11,
        coefficient_of_variation=0.07,
        support_level="strong",
        support_threshold_met=True,
    )
    segment = InstantQuoteStatsRow(
        parcel_count=25,
        p10_assessed_psf=125,
        p25_assessed_psf=135,
        p50_assessed_psf=140,
        p75_assessed_psf=150,
        p90_assessed_psf=160,
        mean_assessed_psf=141,
        median_assessed_psf=140,
        stddev_assessed_psf=8,
        coefficient_of_variation=0.05,
        support_level="strong",
        support_threshold_met=True,
    )

    monkeypatch.setattr(service, "_fetch_subject_row", lambda **_: subject_row)
    monkeypatch.setattr(service, "_fetch_neighborhood_stats", lambda **_: neighborhood)
    monkeypatch.setattr(service, "_fetch_segment_stats", lambda **_: segment)
    monkeypatch.setattr(service, "_enqueue_request_log_persistence", lambda **_: None)
    monkeypatch.setattr(service, "_emit_logs", lambda **_: None)

    response = service.get_quote(
        county_id="harris",
        tax_year=2026,
        account_number="1001001001001",
    )

    assert response.supported is True
    assert response.estimate is not None
    assert response.estimate.tax_protection_limited is True
    assert response.estimate.tax_protection_note is not None


def test_instant_quote_service_returns_supported_false_when_not_ready(monkeypatch) -> None:
    service = InstantQuoteService()
    _patch_request_connection(monkeypatch)
    parcel_id = uuid4()
    subject_row = {
        "parcel_id": parcel_id,
        "county_id": "harris",
        "tax_year": 2026,
        "account_number": "1001001001001",
        "address": "101 Main St, Houston, TX 77002",
        "neighborhood_code": "NBHD-1",
        "school_district_name": "Houston ISD",
        "property_type_code": "sfr",
        "property_class_code": "A1",
        "living_area_sf": 2200.0,
        "year_built": 2003,
        "capped_value": 290000.0,
        "notice_value": 360000.0,
        "assessment_basis_value": 350000.0,
        "effective_tax_rate": 0.021,
        "effective_tax_rate_source_method": "manual",
        "subject_assessed_psf": 159.09,
        "size_bucket": "2000_2399",
        "age_bucket": "1990_2004",
        "support_blocker_code": None,
        "public_summary_ready_flag": True,
        "homestead_flag": False,
        "freeze_flag": False,
        "over65_flag": False,
        "disabled_flag": False,
        "disabled_veteran_flag": False,
        "warning_codes": [],
    }

    monkeypatch.setattr(service, "_fetch_subject_row", lambda **_: subject_row)
    monkeypatch.setattr(service, "_fetch_neighborhood_stats", lambda **_: None)
    monkeypatch.setattr(service, "_has_any_stats_for_year", lambda **_: False)
    monkeypatch.setattr(service, "_enqueue_request_log_persistence", lambda **_: None)
    monkeypatch.setattr(service, "_emit_logs", lambda **_: None)

    response = service.get_quote(
        county_id="harris",
        tax_year=2026,
        account_number="1001001001001",
    )

    assert response.supported is False
    assert response.unsupported_reason == "instant_quote_not_ready"
    assert response.next_step_cta is not None


def test_uncertain_tax_limitation_case_suppresses_numeric_range(monkeypatch) -> None:
    service = InstantQuoteService()
    _patch_request_connection(monkeypatch)
    subject_row = {
        "parcel_id": uuid4(),
        "county_id": "harris",
        "tax_year": 2026,
        "account_number": "1001001001001",
        "address": "101 Main St, Houston, TX 77002",
        "neighborhood_code": "NBHD-1",
        "school_district_name": "Houston ISD",
        "property_type_code": "sfr",
        "property_class_code": "A1",
        "living_area_sf": 2200.0,
        "year_built": 2003,
        "capped_value": 290000.0,
        "notice_value": 360000.0,
        "assessment_basis_value": 350000.0,
        "effective_tax_rate": 0.021,
        "effective_tax_rate_source_method": "manual",
        "subject_assessed_psf": 159.09,
        "size_bucket": "2000_2399",
        "age_bucket": "1990_2004",
        "support_blocker_code": None,
        "public_summary_ready_flag": True,
        "homestead_flag": True,
        "freeze_flag": True,
        "over65_flag": True,
        "disabled_flag": False,
        "disabled_veteran_flag": False,
        "warning_codes": ["freeze_without_qualifying_exemption"],
    }
    neighborhood = InstantQuoteStatsRow(
        parcel_count=35,
        p10_assessed_psf=120,
        p25_assessed_psf=130,
        p50_assessed_psf=145,
        p75_assessed_psf=155,
        p90_assessed_psf=170,
        mean_assessed_psf=146,
        median_assessed_psf=145,
        stddev_assessed_psf=11,
        coefficient_of_variation=0.07,
        support_level="strong",
        support_threshold_met=True,
    )
    segment = InstantQuoteStatsRow(
        parcel_count=25,
        p10_assessed_psf=125,
        p25_assessed_psf=135,
        p50_assessed_psf=140,
        p75_assessed_psf=150,
        p90_assessed_psf=160,
        mean_assessed_psf=141,
        median_assessed_psf=140,
        stddev_assessed_psf=8,
        coefficient_of_variation=0.05,
        support_level="strong",
        support_threshold_met=True,
    )

    monkeypatch.setattr(service, "_fetch_subject_row", lambda **_: subject_row)
    monkeypatch.setattr(service, "_fetch_neighborhood_stats", lambda **_: neighborhood)
    monkeypatch.setattr(service, "_fetch_segment_stats", lambda **_: segment)
    monkeypatch.setattr(service, "_enqueue_request_log_persistence", lambda **_: None)
    monkeypatch.setattr(service, "_emit_logs", lambda **_: None)

    response = service.get_quote(
        county_id="harris",
        tax_year=2026,
        account_number="1001001001001",
    )

    assert response.supported is False
    assert response.unsupported_reason == "tax_limitation_uncertain"


def test_enqueue_request_log_persistence_swallow_submit_failures(monkeypatch) -> None:
    service = InstantQuoteService()

    class BrokenExecutor:
        def submit(self, *args, **kwargs):
            raise RuntimeError("queue unavailable")

    monkeypatch.setattr("app.services.instant_quote._PERSISTENCE_EXECUTOR", BrokenExecutor())

    service._enqueue_request_log_persistence(
        response=InstantQuoteResponse(
            supported=True,
            county_id="harris",
            tax_year=2026,
            requested_tax_year=2026,
            served_tax_year=2026,
            tax_year_fallback_applied=False,
            tax_year_fallback_reason=None,
            data_freshness_label="current_year",
            account_number="1001001001001",
            basis_code="assessment_basis_segment_blend",
            subject=None,
            estimate=None,
            explanation={"methodology": "neighborhood_only", "summary": "x", "bullets": []},
            disclaimers=[],
        ),
        telemetry={
            "request_id": uuid4(),
            "quote_version": "stage17",
        },
    )


def test_enqueue_request_log_persistence_drops_when_queue_is_full(monkeypatch) -> None:
    service = InstantQuoteService()

    for _ in range(TELEMETRY_MAX_INFLIGHT_TASKS):
        assert getattr(__import__("app.services.instant_quote", fromlist=["_PERSISTENCE_SLOTS"]), "_PERSISTENCE_SLOTS").acquire(blocking=False)

    try:
        service._enqueue_request_log_persistence(
            response=InstantQuoteResponse(
                supported=True,
                county_id="harris",
                tax_year=2026,
                requested_tax_year=2026,
                served_tax_year=2026,
                tax_year_fallback_applied=False,
                tax_year_fallback_reason=None,
                data_freshness_label="current_year",
                account_number="1001001001001",
                basis_code="assessment_basis_segment_blend",
                subject=None,
                estimate=None,
                explanation={"methodology": "neighborhood_only", "summary": "x", "bullets": []},
                disclaimers=[],
            ),
            telemetry={
                "request_id": uuid4(),
                "quote_version": "stage17",
            },
        )
    finally:
        persistence_slots = getattr(
            __import__("app.services.instant_quote", fromlist=["_PERSISTENCE_SLOTS"]),
            "_PERSISTENCE_SLOTS",
        )
        for _ in range(TELEMETRY_MAX_INFLIGHT_TASKS):
            persistence_slots.release()
