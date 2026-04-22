from __future__ import annotations

from uuid import uuid4

from app.models.quote import InstantQuoteResponse
from app.services.instant_quote import (
    MATERIAL_CAP_GAP_RATIO,
    MIN_TRIM_GROUP_SIZE,
    SEGMENT_MIN_COUNT,
    TELEMETRY_MAX_INFLIGHT_TASKS,
    TRIM_METHOD_P05_P95_PRESERVE_ALL_LT3,
    InstantQuoteRefreshService,
    InstantQuoteService,
    InstantQuoteStatsRow,
    assign_age_bucket,
    assign_size_bucket,
    build_public_estimate,
    calculate_distribution_stats,
    choose_fallback,
    confidence_label_for_score,
    determine_tax_limitation_outcome,
    extract_assessment_basis_contract,
    has_uncertain_tax_limitation_signal,
    is_material_homestead_cap_limited,
    score_confidence,
)
from app.services.instant_quote_tax_rate_basis import (
    INSTANT_QUOTE_TAX_RATE_BASIS_MIN_EFFECTIVE_TAX_RATE_COVERAGE_RATIO,
    TAX_RATE_ADOPTION_STATUS_SOURCE_GOVERNING_BODY_ADOPTION_RECORD,
    TAX_RATE_ADOPTION_STATUS_SOURCE_OPERATOR_ASSERTED,
    TAX_RATE_BASIS_STATUS_CURRENT_YEAR_FINAL_ADOPTED_RATES,
    TAX_RATE_BASIS_STATUS_CURRENT_YEAR_UNOFFICIAL_OR_PROPOSED_RATES,
    TAX_RATE_BASIS_STATUS_PRIOR_YEAR_ADOPTED_RATES,
    TAX_RATE_BASIS_WARNING_FINAL_ADOPTION_METADATA_INCOMPLETE,
    TAX_RATE_BASIS_WARNING_FINAL_ADOPTION_SOURCE_UNVERIFIED,
    SameYearTaxRateAdoptionStatus,
    TaxRateBasisCandidate,
    assign_tax_rate_basis_status,
    choose_tax_rate_basis,
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

    def commit(self) -> None:
        return None


def _patch_request_connection(monkeypatch) -> None:
    monkeypatch.setattr("app.services.instant_quote.get_connection", lambda: _StubConnection())


class _RefreshCursor:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self._row: dict[str, object] | None = None
        self._rows: list[dict[str, object]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, *_args, **_kwargs) -> None:
        normalized = " ".join(sql.split())
        self.statements.append(normalized)
        if "SELECT DISTINCT county_id, tax_year FROM tmp_instant_quote_subject_scope" in normalized:
            self._row = None
            self._rows = [{"county_id": "harris", "tax_year": 2026}]
        elif "candidate_basis_years.basis_year AS tax_year" in normalized:
            self._row = None
            self._rows = [
                {
                    "tax_year": 2026,
                    "quoteable_subject_row_count": 100,
                    "supportable_subject_row_count": 20,
                    "assignment_complete_row_count": 60,
                    "continuity_parcel_match_row_count": 100,
                    "continuity_account_number_match_row_count": 0,
                },
                {
                    "tax_year": 2025,
                    "quoteable_subject_row_count": 100,
                    "supportable_subject_row_count": 92,
                    "assignment_complete_row_count": 95,
                    "continuity_parcel_match_row_count": 88,
                    "continuity_account_number_match_row_count": 6,
                },
                {
                    "tax_year": 2024,
                    "quoteable_subject_row_count": 100,
                    "supportable_subject_row_count": 90,
                    "assignment_complete_row_count": 94,
                    "continuity_parcel_match_row_count": 70,
                    "continuity_account_number_match_row_count": 12,
                },
            ]
        elif "SELECT COUNT(*)::integer AS count FROM tmp_instant_quote_subject_refresh" in normalized:
            self._row = {"count": 5}
            self._rows = []
        elif "SELECT COUNT(*)::integer AS subject_cache_row_count" in normalized:
            self._row = {
                "subject_cache_row_count": 5,
                "supportable_subject_row_count": 4,
            }
            self._rows = []
        else:
            self._row = None
            self._rows = []

    def fetchone(self) -> dict[str, object] | None:
        return self._row

    def fetchall(self) -> list[dict[str, object]]:
        return self._rows


class _RefreshConnection:
    def __init__(self, cursor: _RefreshCursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> _RefreshCursor:
        return self._cursor

    def commit(self) -> None:
        return None


class _SegmentRefreshCursor:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self._row: dict[str, object] | None = None
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, *_args, **_kwargs) -> None:
        normalized = " ".join(sql.split())
        self.statements.append(normalized)
        if normalized.startswith("INSERT INTO instant_quote_segment_stats"):
            self.rowcount = 7
            self._row = None
        elif "SELECT COUNT(*)::integer AS count FROM instant_quote_segment_stats" in normalized:
            self._row = {"count": 3}
        else:
            self._row = None

    def fetchone(self) -> dict[str, object] | None:
        return self._row


class _SegmentRefreshConnection:
    def __init__(self, cursor: _SegmentRefreshCursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> _SegmentRefreshCursor:
        return self._cursor

    def commit(self) -> None:
        return None


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


def test_extract_assessment_basis_contract_coerces_typed_basis_metadata() -> None:
    contract = extract_assessment_basis_contract(
        {
            "assessment_basis_value": "350000.5",
            "assessment_basis_source_value_type": "certified",
            "assessment_basis_source_year": "2025",
            "assessment_basis_source_reason": "prior_year_certified_fallback",
            "assessment_basis_quality_code": "prior_year_fallback",
        }
    )

    assert contract == {
        "assessment_basis_value": 350000.5,
        "assessment_basis_source_value_type": "certified",
        "assessment_basis_source_year": 2025,
        "assessment_basis_source_reason": "prior_year_certified_fallback",
        "assessment_basis_quality_code": "prior_year_fallback",
    }


def test_refresh_subject_cache_builds_from_scoped_canonical_tables(monkeypatch) -> None:
    cursor = _RefreshCursor()
    monkeypatch.setattr(
        "app.services.instant_quote.get_connection",
        lambda: _RefreshConnection(cursor),
    )

    metrics = InstantQuoteRefreshService()._refresh_subject_cache(  # type: ignore[attr-defined]
        county_id="harris",
        tax_year=2026,
    )

    assert metrics.source_view_row_count == 5
    assert metrics.subject_cache_row_count == 5
    assert metrics.supportable_subject_row_count == 4
    assert metrics.selected_tax_rate_basis is not None
    assert metrics.selected_tax_rate_basis.basis_tax_year == 2025
    assert metrics.selected_tax_rate_basis.fallback_applied is True
    assert metrics.selected_tax_rate_basis.reason_code == (
        "fallback_requested_year_below_effective_coverage_threshold"
    )
    assert metrics.selected_tax_rate_basis.quoteable_subject_row_count == 100
    assert metrics.selected_tax_rate_basis.requested_year_effective_tax_rate_coverage_ratio == 0.2
    assert metrics.selected_tax_rate_basis.requested_year_assignment_coverage_ratio == 0.6
    assert metrics.selected_tax_rate_basis.selected_basis_effective_tax_rate_coverage_ratio == 0.92
    assert metrics.selected_tax_rate_basis.selected_basis_assignment_coverage_ratio == 0.95
    assert metrics.selected_tax_rate_basis.selected_basis_continuity_parcel_match_ratio == 0.88
    assert metrics.selected_tax_rate_basis.selected_basis_warning_codes == (
        "parcel_continuity_warning",
        "account_number_continuity_diagnostic",
    )
    assert (
        metrics.selected_tax_rate_basis.basis_status
        == TAX_RATE_BASIS_STATUS_PRIOR_YEAR_ADOPTED_RATES
    )
    assert any("FROM parcel_year_snapshots pys" in statement for statement in cursor.statements)
    assert any(
        "CREATE TEMP TABLE tmp_instant_quote_subject_scope ON COMMIT DROP AS" in statement
        for statement in cursor.statements
    )
    assert any(
        "LEFT JOIN property_characteristics pc ON pc.parcel_year_snapshot_id = pys.parcel_year_snapshot_id"
        in statement
        and "WHEN pc.property_characteristic_id IS NOT NULL THEN pc.property_type_code"
        in statement
        and "ELSE p.property_type_code" in statement
        and "END = 'sfr'" in statement
        for statement in cursor.statements
    )
    assert any(
        "CREATE TEMP TABLE tmp_instant_quote_tax_rate_basis_selection" in statement
        for statement in cursor.statements
    )
    assert any(
        "etr.tax_year = tax_basis.effective_tax_rate_basis_year" in statement
        for statement in cursor.statements
    )
    assert any(
        "COALESCE(NULLIF(pi.living_area_sf, 0), pi_prior.living_area_sf) AS living_area_sf"
        in statement
        for statement in cursor.statements
    )
    assert any(
        "THEN COALESCE(pi_prior.year_built, pi.year_built)" in statement
        for statement in cursor.statements
    )
    assert any(
        "LEFT JOIN parcel_improvements pi_prior" in statement
        and "pi_prior.tax_year = scope.tax_year - 1" in statement
        for statement in cursor.statements
    )
    assert any(
        "LEFT JOIN parcel_assessments pa_prior" in statement
        and "pa_prior.tax_year = scope.tax_year - 1" in statement
        for statement in cursor.statements
    )
    assert any(
        "COALESCE(NULLIF(pa.certified_value, 0), NULLIF(pa_prior.certified_value, 0)) AS certified_value"
        in statement
        for statement in cursor.statements
    )
    assert any(
        "COALESCE(NULLIF(pa.assessed_value, 0), NULLIF(pa_prior.assessed_value, 0)) AS assessed_value"
        in statement
        for statement in cursor.statements
    )
    assert any(
        "AS assessment_basis_source_value_type" in statement for statement in cursor.statements
    )
    assert any(
        "AS assessment_basis_source_year" in statement for statement in cursor.statements
    )
    assert any(
        "AS assessment_basis_source_reason" in statement for statement in cursor.statements
    )
    assert any(
        "AS assessment_basis_quality_code" in statement for statement in cursor.statements
    )
    assert any(
        "COALESCE( NULLIF(pa.certified_value, 0), NULLIF(pa.appraised_value, 0), NULLIF(pa.assessed_value, 0), NULLIF(pa.market_value, 0), NULLIF(pa.notice_value, 0), 0 ) <= 0"
        in statement
        for statement in cursor.statements
    )
    assert any(
        "COALESCE( NULLIF(pa_prior.certified_value, 0), NULLIF(pa_prior.appraised_value, 0), NULLIF(pa_prior.assessed_value, 0), NULLIF(pa_prior.market_value, 0), NULLIF(pa_prior.notice_value, 0), 0 ) > 0"
        in statement
        for statement in cursor.statements
    )
    assert any(
        "prior_year_living_area_fallback" in statement for statement in cursor.statements
    )
    assert any(
        "prior_year_assessment_basis_fallback" in statement for statement in cursor.statements
    )
    assert any(
        "assessment_basis_source_value_type" in statement
        and "assessment_basis_quality_code" in statement
        for statement in cursor.statements
    )
    assert any("basis_assignment_requirements AS (" in statement for statement in cursor.statements)
    assert any(
        "NOT COALESCE(requirements.requires_school_assignment, false)" in statement
        for statement in cursor.statements
    )
    assert any(
        "ptu.tax_year = tax_basis.effective_tax_rate_basis_year" in statement
        for statement in cursor.statements
    )
    assert any(
        "WHEN sb.requires_school_assignment" in statement for statement in cursor.statements
    )
    assert not any("FROM instant_quote_subject_view" in statement for statement in cursor.statements)


def test_choose_tax_rate_basis_prefers_requested_year_once_usable() -> None:
    selection = choose_tax_rate_basis(
        quote_tax_year=2026,
        candidates=[
            TaxRateBasisCandidate(
                tax_year=2026,
                quoteable_subject_row_count=30,
                supportable_subject_row_count=27,
                assignment_complete_row_count=28,
                continuity_parcel_match_row_count=30,
            ),
            TaxRateBasisCandidate(
                tax_year=2025,
                quoteable_subject_row_count=30,
                supportable_subject_row_count=30,
                assignment_complete_row_count=30,
                continuity_parcel_match_row_count=29,
            ),
        ],
    )

    assert selection.basis_tax_year == 2026
    assert selection.fallback_applied is False
    assert selection.reason_code == "requested_year_usable"

def test_assign_tax_rate_basis_status_marks_prior_year_fallback_as_adopted() -> None:
    selection = assign_tax_rate_basis_status(
        selection=choose_tax_rate_basis(
            quote_tax_year=2026,
            candidates=[
                TaxRateBasisCandidate(
                    tax_year=2026,
                    quoteable_subject_row_count=30,
                    supportable_subject_row_count=0,
                    assignment_complete_row_count=0,
                    continuity_parcel_match_row_count=30,
                ),
                TaxRateBasisCandidate(
                    tax_year=2025,
                    quoteable_subject_row_count=30,
                    supportable_subject_row_count=26,
                    assignment_complete_row_count=27,
                    continuity_parcel_match_row_count=28,
                ),
            ],
        )
    )

    assert selection.basis_tax_year == 2025
    assert selection.basis_status == TAX_RATE_BASIS_STATUS_PRIOR_YEAR_ADOPTED_RATES
    assert selection.basis_status_reason == "basis_year_precedes_quote_year"


def test_assign_tax_rate_basis_status_defaults_same_year_to_unofficial_without_proof() -> None:
    selection = assign_tax_rate_basis_status(
        selection=choose_tax_rate_basis(
            quote_tax_year=2026,
            candidates=[
                TaxRateBasisCandidate(
                    tax_year=2026,
                    quoteable_subject_row_count=30,
                    supportable_subject_row_count=27,
                    assignment_complete_row_count=28,
                    continuity_parcel_match_row_count=30,
                ),
                TaxRateBasisCandidate(
                    tax_year=2025,
                    quoteable_subject_row_count=30,
                    supportable_subject_row_count=30,
                    assignment_complete_row_count=30,
                    continuity_parcel_match_row_count=29,
                ),
            ],
        )
    )

    assert selection.basis_tax_year == 2026
    assert (
        selection.basis_status
        == TAX_RATE_BASIS_STATUS_CURRENT_YEAR_UNOFFICIAL_OR_PROPOSED_RATES
    )
    assert selection.basis_status_reason == "same_year_rates_without_final_adoption_proof"


def test_assign_tax_rate_basis_status_uses_explicit_final_adoption_truth() -> None:
    selection = assign_tax_rate_basis_status(
        selection=choose_tax_rate_basis(
            quote_tax_year=2026,
            candidates=[
                TaxRateBasisCandidate(
                    tax_year=2026,
                    quoteable_subject_row_count=30,
                    supportable_subject_row_count=27,
                    assignment_complete_row_count=28,
                    continuity_parcel_match_row_count=30,
                ),
                TaxRateBasisCandidate(
                    tax_year=2025,
                    quoteable_subject_row_count=30,
                    supportable_subject_row_count=30,
                    assignment_complete_row_count=30,
                    continuity_parcel_match_row_count=29,
                ),
            ],
        ),
        same_year_adoption_status=SameYearTaxRateAdoptionStatus(
            county_id="harris",
            tax_year=2026,
            adoption_status=TAX_RATE_BASIS_STATUS_CURRENT_YEAR_FINAL_ADOPTED_RATES,
            adoption_status_reason="operator_marked_final_adopted",
            status_source=TAX_RATE_ADOPTION_STATUS_SOURCE_GOVERNING_BODY_ADOPTION_RECORD,
            source_note="Minutes reviewed.",
        ),
    )

    assert selection.basis_tax_year == 2026
    assert selection.basis_status == TAX_RATE_BASIS_STATUS_CURRENT_YEAR_FINAL_ADOPTED_RATES
    assert selection.basis_status_reason == "operator_marked_final_adopted"
    assert selection.selected_basis_warning_codes == ()


def test_assign_tax_rate_basis_status_flags_legacy_final_adoption_without_audit_metadata() -> None:
    selection = assign_tax_rate_basis_status(
        selection=choose_tax_rate_basis(
            quote_tax_year=2026,
            candidates=[
                TaxRateBasisCandidate(
                    tax_year=2026,
                    quoteable_subject_row_count=30,
                    supportable_subject_row_count=27,
                    assignment_complete_row_count=28,
                    continuity_parcel_match_row_count=30,
                ),
            ],
        ),
        same_year_adoption_status=SameYearTaxRateAdoptionStatus(
            county_id="harris",
            tax_year=2026,
            adoption_status=TAX_RATE_BASIS_STATUS_CURRENT_YEAR_FINAL_ADOPTED_RATES,
            adoption_status_reason=None,
            status_source=TAX_RATE_ADOPTION_STATUS_SOURCE_OPERATOR_ASSERTED,
            source_note=None,
        ),
    )

    assert selection.basis_status == TAX_RATE_BASIS_STATUS_CURRENT_YEAR_FINAL_ADOPTED_RATES
    assert selection.selected_basis_warning_codes == (
        TAX_RATE_BASIS_WARNING_FINAL_ADOPTION_METADATA_INCOMPLETE,
        TAX_RATE_BASIS_WARNING_FINAL_ADOPTION_SOURCE_UNVERIFIED,
    )

def test_choose_tax_rate_basis_rejects_requested_year_with_row_floor_but_weak_coverage() -> None:
    selection = choose_tax_rate_basis(
        quote_tax_year=2026,
        candidates=[
            TaxRateBasisCandidate(
                tax_year=2026,
                quoteable_subject_row_count=40,
                supportable_subject_row_count=20,
                assignment_complete_row_count=39,
                continuity_parcel_match_row_count=40,
            ),
            TaxRateBasisCandidate(
                tax_year=2025,
                quoteable_subject_row_count=40,
                supportable_subject_row_count=35,
                assignment_complete_row_count=38,
                continuity_parcel_match_row_count=37,
            ),
        ],
    )

    assert selection.basis_tax_year == 2025
    assert selection.fallback_applied is True
    assert selection.reason_code == "fallback_requested_year_below_effective_coverage_threshold"
    assert (
        selection.requested_year_effective_tax_rate_coverage_ratio
        < INSTANT_QUOTE_TAX_RATE_BASIS_MIN_EFFECTIVE_TAX_RATE_COVERAGE_RATIO
    )

def test_choose_tax_rate_basis_falls_back_to_nearest_prior_usable_year() -> None:
    selection = choose_tax_rate_basis(
        quote_tax_year=2027,
        candidates=[
            TaxRateBasisCandidate(
                tax_year=2027,
                quoteable_subject_row_count=50,
                supportable_subject_row_count=44,
                assignment_complete_row_count=35,
                continuity_parcel_match_row_count=50,
            ),
            TaxRateBasisCandidate(
                tax_year=2026,
                quoteable_subject_row_count=50,
                supportable_subject_row_count=46,
                assignment_complete_row_count=47,
                continuity_parcel_match_row_count=44,
            ),
            TaxRateBasisCandidate(
                tax_year=2025,
                quoteable_subject_row_count=50,
                supportable_subject_row_count=48,
                assignment_complete_row_count=49,
                continuity_parcel_match_row_count=42,
            ),
        ],
    )

    assert selection.basis_tax_year == 2026
    assert selection.fallback_applied is True
    assert selection.reason_code == "fallback_requested_year_below_assignment_coverage_threshold"
    assert selection.selected_basis_warning_codes == (
        "parcel_continuity_warning",
    )


def test_choose_tax_rate_basis_returns_safe_no_basis_when_no_year_is_usable() -> None:
    selection = choose_tax_rate_basis(
        quote_tax_year=2026,
        candidates=[
            TaxRateBasisCandidate(
                tax_year=2026,
                quoteable_subject_row_count=25,
                supportable_subject_row_count=0,
                assignment_complete_row_count=0,
                continuity_parcel_match_row_count=25,
            ),
            TaxRateBasisCandidate(
                tax_year=2025,
                quoteable_subject_row_count=25,
                supportable_subject_row_count=22,
                assignment_complete_row_count=24,
                continuity_parcel_match_row_count=18,
            ),
        ],
    )

    assert selection.basis_tax_year is None
    assert selection.fallback_applied is False
    assert selection.reason_code == "no_usable_tax_rate_basis"


def test_calculate_distribution_stats_returns_monotonic_percentiles() -> None:
    summary = calculate_distribution_stats([90, 100, 110, 120, 130, 140, 150])

    assert summary is not None
    assert summary.p10 <= summary.p25 <= summary.p50 <= summary.p75 <= summary.p90
    assert summary.parcel_count == 7
    assert summary.trimmed_parcel_count <= summary.parcel_count


def test_calculate_distribution_stats_preserves_tiny_group_when_trim_eliminates_all_rows() -> None:
    summary = calculate_distribution_stats([100, 200])

    assert summary is not None
    assert summary.parcel_count == 2
    assert summary.trimmed_parcel_count == 2
    assert summary.excluded_parcel_count == 0
    assert summary.trim_method_code == TRIM_METHOD_P05_P95_PRESERVE_ALL_LT3


def test_refresh_segment_stats_preserves_raw_group_when_trim_would_remove_every_row(
    monkeypatch,
) -> None:
    cursor = _SegmentRefreshCursor()
    monkeypatch.setattr(
        "app.services.instant_quote.get_connection",
        lambda: _SegmentRefreshConnection(cursor),
    )

    metrics = InstantQuoteRefreshService()._refresh_segment_stats(  # type: ignore[attr-defined]
        county_id="harris",
        tax_year=2025,
    )

    assert metrics.total_row_count == 7
    assert metrics.supported_row_count == 3
    segment_insert = next(
        statement
        for statement in cursor.statements
        if statement.startswith("INSERT INTO instant_quote_segment_stats")
    )
    assert "trimmed_preferred" in segment_insert
    assert f"WHERE bounds.parcel_count < {MIN_TRIM_GROUP_SIZE}" in segment_insert
    assert "BOOL_OR(used_trim_fallback)" in segment_insert
    assert "preserve_small_group" in segment_insert
    assert TRIM_METHOD_P05_P95_PRESERVE_ALL_LT3 in segment_insert


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
        parcel_count=SEGMENT_MIN_COUNT - 1,
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


def test_choose_fallback_uses_segment_when_segment_support_meets_minimum_threshold() -> None:
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
        parcel_count=SEGMENT_MIN_COUNT,
        p10_assessed_psf=105,
        p25_assessed_psf=115,
        p50_assessed_psf=125,
        p75_assessed_psf=135,
        p90_assessed_psf=145,
        mean_assessed_psf=126,
        median_assessed_psf=125,
        stddev_assessed_psf=8,
        coefficient_of_variation=0.06,
        support_level="medium",
        support_threshold_met=True,
    )

    fallback_tier, segment_weight, neighborhood_weight, basis_code = choose_fallback(
        segment_stats=segment,
        neighborhood_stats=neighborhood,
    )

    assert fallback_tier == "segment_within_neighborhood"
    assert segment_weight == 0.55
    assert neighborhood_weight == 0.45
    assert basis_code == "assessment_basis_segment_blend"


def test_choose_fallback_uses_neighborhood_when_segment_stats_lack_median() -> None:
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
        p50_assessed_psf=None,
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

    assert fallback_tier == "neighborhood_only"
    assert segment_weight == 0.0
    assert neighborhood_weight == 1.0
    assert basis_code == "assessment_basis_neighborhood_only"


def test_choose_fallback_returns_unsupported_when_neighborhood_lacks_median() -> None:
    neighborhood = InstantQuoteStatsRow(
        parcel_count=30,
        p10_assessed_psf=100,
        p25_assessed_psf=110,
        p50_assessed_psf=None,
        p75_assessed_psf=130,
        p90_assessed_psf=140,
        mean_assessed_psf=121,
        median_assessed_psf=120,
        stddev_assessed_psf=9,
        coefficient_of_variation=0.07,
        support_level="strong",
        support_threshold_met=True,
    )

    fallback_tier, segment_weight, neighborhood_weight, basis_code = choose_fallback(
        segment_stats=None,
        neighborhood_stats=neighborhood,
    )

    assert fallback_tier == "unsupported"
    assert segment_weight == 0.0
    assert neighborhood_weight == 0.0
    assert basis_code == "assessment_basis_unsupported"


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


def test_missing_exemption_amount_alone_does_not_force_suppressed_tax_limitation_outcome() -> None:
    subject_row = {
        "homestead_flag": True,
        "freeze_flag": False,
        "capped_value": 290000,
        "assessment_basis_value": 350000,
        "warning_codes": ["missing_exemption_amount", "homestead_flag_mismatch"],
    }

    assert has_uncertain_tax_limitation_signal(subject_row) is False
    assert determine_tax_limitation_outcome(subject_row=subject_row, confidence_score=82) == "constrained"


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


def test_confidence_score_uses_tightened_neighborhood_only_penalty() -> None:
    neighborhood = InstantQuoteStatsRow(
        parcel_count=40,
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
            "effective_tax_rate_source_method": "manual",
            "homestead_flag": False,
            "freeze_flag": False,
            "warning_codes": [],
        },
        segment_stats=None,
        neighborhood_stats=neighborhood,
        fallback_tier="neighborhood_only",
        subject_assessed_psf=110,
        target_psf=110,
    )

    assert score == 80.0


def test_instant_quote_service_tightened_neighborhood_only_penalty_suppresses_borderline_case(
    monkeypatch,
) -> None:
    service = InstantQuoteService()
    _patch_request_connection(monkeypatch)
    subject_row = {
        "parcel_id": uuid4(),
        "county_id": "harris",
        "tax_year": 2025,
        "account_number": "2002002002002",
        "address": "202 Main St, Houston, TX 77002",
        "neighborhood_code": "NBHD-2",
        "school_district_name": "Houston ISD",
        "property_type_code": "sfr",
        "property_class_code": "A1",
        "living_area_sf": 2200.0,
        "year_built": None,
        "capped_value": 300000.0,
        "notice_value": 365000.0,
        "assessment_basis_value": 360000.0,
        "effective_tax_rate": 0.021,
        "effective_tax_rate_source_method": "component_rollup",
        "subject_assessed_psf": 163.64,
        "size_bucket": "2000_2399",
        "age_bucket": "unknown",
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
        parcel_count=40,
        p10_assessed_psf=90,
        p25_assessed_psf=100,
        p50_assessed_psf=110,
        p75_assessed_psf=120,
        p90_assessed_psf=130,
        mean_assessed_psf=111,
        median_assessed_psf=110,
        stddev_assessed_psf=50,
        coefficient_of_variation=0.41,
        support_level="strong",
        support_threshold_met=True,
    )

    monkeypatch.setattr(service, "_fetch_subject_row", lambda **_: subject_row)
    monkeypatch.setattr(service, "_fetch_neighborhood_stats", lambda **_: neighborhood)
    monkeypatch.setattr(service, "_fetch_segment_stats", lambda **_: None)
    monkeypatch.setattr(service, "_enqueue_request_log_persistence", lambda **_: None)
    monkeypatch.setattr(service, "_emit_logs", lambda **_: None)

    response = service.get_quote(
        county_id="harris",
        tax_year=2025,
        account_number="2002002002002",
    )

    assert response.supported is False
    assert response.basis_code == "assessment_basis_neighborhood_only"
    assert response.unsupported_reason == "low_confidence_refined_review"
    assert response.estimate is None


def test_confidence_score_uses_reduced_segment_cv_penalties() -> None:
    neighborhood = InstantQuoteStatsRow(
        parcel_count=40,
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
    medium_segment = InstantQuoteStatsRow(
        parcel_count=10,
        p10_assessed_psf=95,
        p25_assessed_psf=100,
        p50_assessed_psf=105,
        p75_assessed_psf=110,
        p90_assessed_psf=115,
        mean_assessed_psf=105,
        median_assessed_psf=105,
        stddev_assessed_psf=20,
        coefficient_of_variation=0.30,
        support_level="medium",
        support_threshold_met=True,
    )
    noisy_segment = InstantQuoteStatsRow(
        parcel_count=10,
        p10_assessed_psf=95,
        p25_assessed_psf=100,
        p50_assessed_psf=105,
        p75_assessed_psf=110,
        p90_assessed_psf=115,
        mean_assessed_psf=105,
        median_assessed_psf=105,
        stddev_assessed_psf=30,
        coefficient_of_variation=0.45,
        support_level="medium",
        support_threshold_met=True,
    )
    subject_row = {
        "year_built": 1998,
        "public_summary_ready_flag": True,
        "effective_tax_rate_source_method": "manual",
        "homestead_flag": False,
        "freeze_flag": False,
        "warning_codes": [],
    }

    medium_score = score_confidence(
        subject_row=subject_row,
        segment_stats=medium_segment,
        neighborhood_stats=neighborhood,
        fallback_tier="segment_within_neighborhood",
        subject_assessed_psf=110,
        target_psf=110,
    )
    noisy_score = score_confidence(
        subject_row=subject_row,
        segment_stats=noisy_segment,
        neighborhood_stats=neighborhood,
        fallback_tier="segment_within_neighborhood",
        subject_assessed_psf=110,
        target_psf=110,
    )

    assert medium_score == 85.0
    assert noisy_score == 80.0


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


def test_instant_quote_service_falls_back_to_neighborhood_only_when_segment_stats_missing(
    monkeypatch,
) -> None:
    service = InstantQuoteService()
    _patch_request_connection(monkeypatch)
    subject_row = {
        "parcel_id": uuid4(),
        "county_id": "harris",
        "tax_year": 2025,
        "account_number": "1163480010045",
        "address": "101 Main St, Houston, TX 77002",
        "neighborhood_code": "2610.04",
        "school_district_name": "Houston ISD",
        "property_type_code": "sfr",
        "property_class_code": "A1",
        "living_area_sf": 1232.0,
        "year_built": 1978,
        "capped_value": None,
        "notice_value": 112000.0,
        "assessment_basis_value": 108129.0,
        "effective_tax_rate": 0.0072096,
        "effective_tax_rate_source_method": "component_rollup",
        "subject_assessed_psf": 87.77,
        "size_bucket": "lt_1400",
        "age_bucket": "1970_1989",
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
        parcel_count=94,
        p10_assessed_psf=70,
        p25_assessed_psf=80,
        p50_assessed_psf=85,
        p75_assessed_psf=95,
        p90_assessed_psf=110,
        mean_assessed_psf=86,
        median_assessed_psf=85,
        stddev_assessed_psf=12,
        coefficient_of_variation=0.12,
        support_level="strong",
        support_threshold_met=True,
    )

    monkeypatch.setattr(service, "_fetch_subject_row", lambda **_: subject_row)
    monkeypatch.setattr(service, "_fetch_neighborhood_stats", lambda **_: neighborhood)
    monkeypatch.setattr(service, "_fetch_segment_stats", lambda **_: None)
    monkeypatch.setattr(service, "_enqueue_request_log_persistence", lambda **_: None)
    monkeypatch.setattr(service, "_emit_logs", lambda **_: None)

    response = service.get_quote(
        county_id="harris",
        tax_year=2025,
        account_number="1163480010045",
    )

    assert response.supported is True
    assert response.basis_code == "assessment_basis_neighborhood_only"
    assert response.estimate is not None
    assert response.explanation.methodology == "neighborhood_only"


def test_instant_quote_service_adds_disclaimer_for_prior_year_assessment_basis_fallback(
    monkeypatch,
) -> None:
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
        "homestead_flag": False,
        "freeze_flag": False,
        "over65_flag": False,
        "disabled_flag": False,
        "disabled_veteran_flag": False,
        "warning_codes": ["prior_year_assessment_basis_fallback"],
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

    assert response.supported is True
    assert any(
        "prior year's assessed basis as a fallback" in disclaimer
        for disclaimer in response.disclaimers
    )


def test_instant_quote_service_public_payload_does_not_expose_stage1_basis_contract(
    monkeypatch,
) -> None:
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
        "assessment_basis_source_value_type": "certified",
        "assessment_basis_source_year": 2026,
        "assessment_basis_source_reason": "current_year_certified",
        "assessment_basis_quality_code": "current_year_authoritative",
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
    payload = response.model_dump()

    assert response.supported is True
    assert "assessment_basis_source_value_type" not in payload
    assert "assessment_basis_source_year" not in payload
    assert "assessment_basis_source_reason" not in payload
    assert "assessment_basis_quality_code" not in payload


def test_instant_quote_service_returns_unsupported_when_neighborhood_basis_is_missing(
    monkeypatch,
) -> None:
    service = InstantQuoteService()
    _patch_request_connection(monkeypatch)
    subject_row = {
        "parcel_id": uuid4(),
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
        p50_assessed_psf=None,
        p75_assessed_psf=155,
        p90_assessed_psf=170,
        mean_assessed_psf=146,
        median_assessed_psf=145,
        stddev_assessed_psf=11,
        coefficient_of_variation=0.07,
        support_level="strong",
        support_threshold_met=True,
    )

    monkeypatch.setattr(service, "_fetch_subject_row", lambda **_: subject_row)
    monkeypatch.setattr(service, "_fetch_neighborhood_stats", lambda **_: neighborhood)
    monkeypatch.setattr(service, "_fetch_segment_stats", lambda **_: None)
    monkeypatch.setattr(service, "_enqueue_request_log_persistence", lambda **_: None)
    monkeypatch.setattr(service, "_emit_logs", lambda **_: None)

    response = service.get_quote(
        county_id="harris",
        tax_year=2025,
        account_number="1001001001001",
    )

    assert response.supported is False
    assert response.unsupported_reason == "thin_market_support"


def test_instant_quote_service_blocks_implausible_savings_outlier(monkeypatch) -> None:
    service = InstantQuoteService()
    _patch_request_connection(monkeypatch)
    subject_row = {
        "parcel_id": uuid4(),
        "county_id": "fort_bend",
        "tax_year": 2026,
        "account_number": "1001001001001",
        "address": "101 Main St, Houston, TX 77002",
        "neighborhood_code": "NBHD-1",
        "school_district_name": "Fort Bend ISD",
        "property_type_code": "sfr",
        "property_class_code": "A1",
        "living_area_sf": 2200.0,
        "year_built": 2003,
        "capped_value": None,
        "notice_value": 360000.0,
        "assessment_basis_value": 350000.0,
        "effective_tax_rate": 0.30,
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
        p10_assessed_psf=5,
        p25_assessed_psf=8,
        p50_assessed_psf=10,
        p75_assessed_psf=12,
        p90_assessed_psf=15,
        mean_assessed_psf=10,
        median_assessed_psf=10,
        stddev_assessed_psf=2,
        coefficient_of_variation=0.2,
        support_level="strong",
        support_threshold_met=True,
    )
    segment = InstantQuoteStatsRow(
        parcel_count=25,
        p10_assessed_psf=4,
        p25_assessed_psf=7,
        p50_assessed_psf=9,
        p75_assessed_psf=11,
        p90_assessed_psf=14,
        mean_assessed_psf=9,
        median_assessed_psf=9,
        stddev_assessed_psf=2,
        coefficient_of_variation=0.2,
        support_level="strong",
        support_threshold_met=True,
    )

    monkeypatch.setattr(service, "_fetch_subject_row", lambda **_: subject_row)
    monkeypatch.setattr(service, "_fetch_neighborhood_stats", lambda **_: neighborhood)
    monkeypatch.setattr(service, "_fetch_segment_stats", lambda **_: segment)
    monkeypatch.setattr(service, "_enqueue_request_log_persistence", lambda **_: None)
    monkeypatch.setattr(service, "_emit_logs", lambda **_: None)

    response = service.get_quote(
        county_id="fort_bend",
        tax_year=2026,
        account_number="1001001001001",
    )

    assert response.supported is False
    assert response.unsupported_reason == "implausible_savings_outlier"


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


def test_missing_exemption_amount_warning_constrains_but_does_not_suppress_numeric_range(
    monkeypatch,
) -> None:
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
        "freeze_flag": False,
        "over65_flag": False,
        "disabled_flag": False,
        "disabled_veteran_flag": False,
        "warning_codes": ["missing_exemption_amount", "homestead_flag_mismatch"],
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
    assert response.unsupported_reason is None


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
