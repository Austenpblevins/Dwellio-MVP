from __future__ import annotations

from app.services.instant_quote_tax_completeness import InstantQuoteTaxCompletenessPosture
from app.services.data_readiness import DataReadinessService


class StubCursor:
    def __init__(self) -> None:
        self._row: dict[str, object] | None = None

    def __enter__(self) -> StubCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        if "SET LOCAL max_parallel_workers_per_gather = 0" in sql:
            self._row = None
        elif "FROM tax_years WHERE tax_year = %s" in sql:
            self._row = {"present": 1}
        elif "FROM import_batches ib" in sql:
            if params == ("harris", 2025, "property_roll"):
                self._row = {
                    "import_batch_id": "batch-1",
                    "status": "normalized",
                    "status_reason": "published_to_canonical: property_roll publish succeeded.",
                    "publish_state": "published",
                }
            else:
                self._row = None
        elif "FROM raw_files WHERE county_id = %s AND tax_year = %s AND file_kind = %s" in sql:
            count = 1 if params == ("harris", 2025, "property_roll") else 0
            self._row = {"count": count}
        elif "information_schema.tables" in sql:
            table_name = params[0]
            self._row = {
                "present": table_name
                in {
                    "instant_quote_subject_cache",
                    "instant_quote_neighborhood_stats",
                    "instant_quote_segment_stats",
                    "instant_quote_refresh_runs",
                    "search_documents",
                    "parcel_features",
                    "comp_candidate_pools",
                    "neighborhood_stats",
                    "valuation_runs",
                    "parcel_savings_estimates",
                    "decision_tree_results",
                    "quote_explanations",
                    "protest_recommendations",
                }
            }
        elif "information_schema.views" in sql:
            view_name = params[0]
            self._row = {
                "present": view_name
                in {
                    "instant_quote_subject_view",
                    "parcel_summary_view",
                    "parcel_year_trend_view",
                    "neighborhood_year_trend_view",
                    "v_quote_read_model",
                }
            }
        elif "FROM parcel_year_snapshots" in sql and "is_current = true" in sql:
            self._row = {"count": 2}
        elif "FROM parcel_year_trend_view" in sql:
            self._row = {"count": 2}
        elif "FROM neighborhood_stats" in sql:
            self._row = {"count": 3}
        elif "FROM neighborhood_year_trend_view" in sql:
            self._row = {"count": 2}
        elif "FROM instant_quote_subject_cache" in sql and "support_blocker_code IS NULL" in sql:
            self._row = {"count": 12}
        elif "FROM instant_quote_subject_cache" in sql:
            self._row = {"count": 18}
        elif "FROM instant_quote_neighborhood_stats" in sql and "support_threshold_met IS TRUE" in sql:
            self._row = {"count": 0}
        elif "FROM instant_quote_neighborhood_stats" in sql:
            self._row = {"count": 3}
        elif "FROM instant_quote_segment_stats" in sql and "support_threshold_met IS TRUE" in sql:
            self._row = {"count": 0}
        elif "FROM instant_quote_segment_stats" in sql:
            self._row = {"count": 2}
        elif "FROM instant_quote_refresh_runs" in sql:
            self._row = {
                "refresh_status": "completed",
                "refresh_finished_at": None,
                "validated_at": None,
                "cache_view_row_delta": 0,
                "tax_rate_basis_year": 2024,
                "tax_rate_basis_reason": "fallback_requested_year_missing_supportable_subjects",
                "tax_rate_basis_fallback_applied": True,
                "tax_rate_basis_status": "prior_year_adopted_rates",
                "tax_rate_basis_status_reason": "basis_year_precedes_quote_year",
                "requested_tax_rate_supportable_subject_row_count": 0,
                "tax_rate_basis_supportable_subject_row_count": 24,
                "tax_rate_quoteable_subject_row_count": 100,
                "requested_tax_rate_effective_tax_rate_coverage_ratio": 0.2,
                "requested_tax_rate_assignment_coverage_ratio": 0.6,
                "tax_rate_basis_effective_tax_rate_coverage_ratio": 0.92,
                "tax_rate_basis_assignment_coverage_ratio": 0.95,
                "tax_rate_basis_continuity_parcel_match_row_count": 88,
                "tax_rate_basis_continuity_parcel_gap_row_count": 12,
                "tax_rate_basis_continuity_parcel_match_ratio": 0.88,
                "tax_rate_basis_continuity_account_number_match_row_count": 6,
                "tax_rate_basis_warning_codes": [
                    "parcel_continuity_warning",
                    "account_number_continuity_diagnostic",
                ],
                "validation_report": {
                    "supported_public_quote_exists": True,
                    "supportable_row_rate": 0.6667,
                    "high_value_subject_row_count": 30,
                    "high_value_supportable_subject_row_count": 21,
                    "high_value_support_rate": 0.7,
                    "special_district_heavy_subject_row_count": 40,
                    "special_district_heavy_supportable_subject_row_count": 28,
                    "special_district_heavy_support_rate": 0.7,
                    "monitored_zero_savings_sample_row_count": 20,
                    "monitored_zero_savings_supported_quote_count": 18,
                    "monitored_zero_savings_quote_count": 9,
                    "monitored_zero_savings_quote_share": 0.5,
                    "monitored_extreme_savings_watchlist_count": 10,
                    "monitored_extreme_savings_flagged_count": 1,
                    "subject_rows_without_usable_neighborhood_stats": 1,
                    "subject_rows_without_usable_segment_stats": 9,
                    "subject_rows_missing_segment_row": 7,
                    "subject_rows_thin_segment_support": 2,
                    "subject_rows_unusable_segment_basis": 0,
                    "served_neighborhood_only_quote_count": 5,
                    "served_supported_neighborhood_only_quote_count": 3,
                    "served_unsupported_neighborhood_only_quote_count": 2,
                },
            }
        elif "FROM search_documents" in sql:
            self._row = {"count": 2}
        elif "FROM parcel_features pf" in sql:
            self._row = {"count": 0}
        elif "FROM comp_candidate_pools" in sql:
            self._row = {"count": 0}
        elif "FROM valuation_runs" in sql:
            self._row = {"count": 1}
        elif "FROM parcel_savings_estimates pse" in sql:
            self._row = {"count": 1}
        elif "FROM decision_tree_results dtr" in sql:
            self._row = {"count": 1}
        elif "FROM quote_explanations qe" in sql:
            self._row = {"count": 1}
        elif "FROM protest_recommendations pr" in sql:
            self._row = {"count": 1}
        elif "FROM v_quote_read_model" in sql:
            self._row = {"count": 1}
        else:
            raise AssertionError(f"Unexpected SQL: {sql}")

    def fetchone(self) -> dict[str, object] | None:
        return self._row


class StubConnection:
    def __enter__(self) -> StubConnection:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> StubCursor:
        return StubCursor()


def test_data_readiness_summary(monkeypatch) -> None:
    monkeypatch.setattr("app.services.data_readiness.get_connection", lambda: StubConnection())

    readiness = DataReadinessService().build_tax_year_readiness(county_id="harris", tax_year=2025)

    assert readiness.tax_year_known is True
    property_roll = next(item for item in readiness.datasets if item.dataset_type == "property_roll")
    assert property_roll.access_method == "manual_upload"
    assert property_roll.availability_status == "manual_upload_required"
    assert property_roll.raw_file_count == 1
    assert property_roll.latest_import_status == "normalized"
    assert property_roll.latest_status_reason == "published_to_canonical: property_roll publish succeeded."
    assert property_roll.canonical_published is True
    assert readiness.derived.parcel_summary_ready is True
    assert readiness.derived.parcel_year_trend_ready is True
    assert readiness.derived.neighborhood_stats_ready is True
    assert readiness.derived.neighborhood_year_trend_ready is True
    assert readiness.derived.instant_quote_subject_ready is True
    assert readiness.derived.instant_quote_neighborhood_stats_ready is True
    assert readiness.derived.instant_quote_segment_stats_ready is True
    assert readiness.derived.instant_quote_asset_ready is True
    assert readiness.derived.instant_quote_supportable_row_count == 12
    assert readiness.derived.instant_quote_supported_neighborhood_stats_row_count == 0
    assert readiness.derived.instant_quote_supported_segment_stats_row_count == 0
    assert readiness.derived.instant_quote_refresh_status == "completed"
    assert readiness.derived.instant_quote_cache_view_row_delta == 0
    assert readiness.derived.instant_quote_tax_rate_basis_year == 2024
    assert readiness.derived.instant_quote_tax_rate_basis_reason == (
        "fallback_requested_year_missing_supportable_subjects"
    )
    assert readiness.derived.instant_quote_tax_rate_basis_fallback_applied is True
    assert readiness.derived.instant_quote_tax_rate_basis_status == "prior_year_adopted_rates"
    assert readiness.derived.instant_quote_tax_rate_basis_status_reason == (
        "basis_year_precedes_quote_year"
    )
    assert readiness.derived.instant_quote_tax_rate_basis_internal_note == (
        "2025 instant quote currently uses 2024 adopted tax-rate basis until 2025 rates "
        "are available and refreshed. Current-year rates are typically updated later in "
        "the year, often around September-October."
    )
    assert readiness.derived.instant_quote_tax_rate_requested_year_supportable_subject_row_count == 0
    assert readiness.derived.instant_quote_tax_rate_basis_supportable_subject_row_count == 24
    assert readiness.derived.instant_quote_tax_rate_quoteable_subject_row_count == 100
    assert readiness.derived.instant_quote_tax_rate_requested_year_effective_tax_rate_coverage_ratio == 0.2
    assert readiness.derived.instant_quote_tax_rate_requested_year_assignment_coverage_ratio == 0.6
    assert readiness.derived.instant_quote_tax_rate_basis_effective_tax_rate_coverage_ratio == 0.92
    assert readiness.derived.instant_quote_tax_rate_basis_assignment_coverage_ratio == 0.95
    assert readiness.derived.instant_quote_tax_rate_basis_continuity_parcel_match_row_count == 88
    assert readiness.derived.instant_quote_tax_rate_basis_continuity_parcel_gap_row_count == 12
    assert readiness.derived.instant_quote_tax_rate_basis_continuity_parcel_match_ratio == 0.88
    assert readiness.derived.instant_quote_tax_rate_basis_continuity_account_number_match_row_count == 6
    assert readiness.derived.instant_quote_tax_rate_basis_warning_codes == [
        "parcel_continuity_warning",
        "account_number_continuity_diagnostic",
    ]
    assert readiness.derived.instant_quote_supported_public_quote_exists is True
    assert readiness.derived.instant_quote_supportable_row_rate == 0.6667
    assert readiness.derived.instant_quote_high_value_subject_row_count == 30
    assert readiness.derived.instant_quote_high_value_supportable_subject_row_count == 21
    assert readiness.derived.instant_quote_high_value_support_rate == 0.7
    assert readiness.derived.instant_quote_special_district_heavy_subject_row_count == 40
    assert readiness.derived.instant_quote_special_district_heavy_supportable_subject_row_count == 28
    assert readiness.derived.instant_quote_special_district_heavy_support_rate == 0.7
    assert readiness.derived.instant_quote_monitored_zero_savings_sample_row_count == 20
    assert readiness.derived.instant_quote_monitored_zero_savings_supported_quote_count == 18
    assert readiness.derived.instant_quote_monitored_zero_savings_quote_count == 9
    assert readiness.derived.instant_quote_monitored_zero_savings_quote_share == 0.5
    assert readiness.derived.instant_quote_monitored_extreme_savings_watchlist_count == 10
    assert readiness.derived.instant_quote_monitored_extreme_savings_flagged_count == 1
    assert readiness.derived.instant_quote_subject_rows_without_usable_neighborhood_stats == 1
    assert readiness.derived.instant_quote_subject_rows_without_usable_segment_stats == 9
    assert readiness.derived.instant_quote_subject_rows_missing_segment_row == 7
    assert readiness.derived.instant_quote_subject_rows_thin_segment_support == 2
    assert readiness.derived.instant_quote_subject_rows_unusable_segment_basis == 0
    assert readiness.derived.instant_quote_served_neighborhood_only_quote_count == 5
    assert readiness.derived.instant_quote_served_supported_neighborhood_only_quote_count == 3
    assert readiness.derived.instant_quote_served_unsupported_neighborhood_only_quote_count == 2
    assert readiness.derived.instant_quote_ready is False
    assert readiness.derived.search_support_ready is True
    assert readiness.derived.feature_ready is False
    assert readiness.derived.valuation_ready is True
    assert readiness.derived.savings_ready is True
    assert readiness.derived.decision_tree_ready is True
    assert readiness.derived.explanation_ready is True
    assert readiness.derived.recommendation_ready is True
    assert readiness.derived.quote_ready is True


def test_data_readiness_surfaces_tax_completeness_posture(monkeypatch) -> None:
    monkeypatch.setattr("app.services.data_readiness.get_connection", lambda: StubConnection())
    monkeypatch.setattr(
        "app.services.data_readiness.classify_instant_quote_tax_completeness",
        lambda **kwargs: InstantQuoteTaxCompletenessPosture(
            status="operational_with_caveats",
            reason="fort_bend_revalidation_residual_risk",
            internal_note="Fort Bend 2026 parcel tax completeness is operational with caveats.",
            warning_codes=(
                "acceptable_caution_rows_operational",
                "risky_caution_rows_monitored",
                "continuity_gap_rows_monitored",
            ),
        ),
    )

    readiness = DataReadinessService().build_tax_year_readiness(
        county_id="fort_bend",
        tax_year=2026,
    )

    assert readiness.derived.instant_quote_ready is False
    assert readiness.derived.instant_quote_tax_completeness_status == "operational_with_caveats"
    assert readiness.derived.instant_quote_tax_completeness_reason == (
        "fort_bend_revalidation_residual_risk"
    )
    assert readiness.derived.instant_quote_tax_completeness_internal_note == (
        "Fort Bend 2026 parcel tax completeness is operational with caveats."
    )
    assert readiness.derived.instant_quote_tax_completeness_warning_codes == [
        "acceptable_caution_rows_operational",
        "risky_caution_rows_monitored",
        "continuity_gap_rows_monitored",
    ]
