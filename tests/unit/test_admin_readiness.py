from __future__ import annotations

from datetime import datetime, timezone

from app.api.admin import get_county_onboarding_contract, get_county_year_readiness
from app.models.admin import AdminCountyOnboardingContract, AdminCountyYearReadinessDashboard
from app.models.parcel import ParcelSummaryResponse
from app.services.admin_readiness import (
    AdminOperationalMetricsProvider,
    AdminReadinessService,
    DatasetOperationalMetrics,
)
from app.services.county_onboarding import (
    CountyOnboardingContract,
    OnboardingAction,
    OnboardingBaselineComparison,
    OnboardingDatasetSnapshot,
    OnboardingPhase,
    OnboardingReadinessSnapshot,
    OnboardingSummary,
    OnboardingValidationYear,
)
from app.services.data_readiness import (
    CountyTaxYearReadiness,
    DatasetYearReadiness,
    TaxYearDerivedReadiness,
)


class StubDataReadinessService:
    def build_tax_year_readiness(self, *, county_id: str, tax_year: int) -> CountyTaxYearReadiness:
        assert county_id == "harris"
        if tax_year == 2024:
            return CountyTaxYearReadiness(
                county_id=county_id,
                tax_year=tax_year,
                tax_year_known=True,
                datasets=[
                    DatasetYearReadiness(
                        county_id=county_id,
                        tax_year=tax_year,
                        dataset_type="property_roll",
                        source_system_code="HCAD_BULK",
                        access_method="manual_upload",
                        availability_status="manual_upload_required",
                        tax_year_known=True,
                        raw_file_count=1,
                        latest_import_batch_id="batch-2024",
                        latest_import_status="normalized",
                        latest_publish_state="published",
                        staged=True,
                        canonical_published=True,
                    )
                ],
                derived=TaxYearDerivedReadiness(
                    parcel_summary_ready=True,
                    parcel_year_trend_ready=False,
                    neighborhood_stats_ready=False,
                    neighborhood_year_trend_ready=False,
                    instant_quote_subject_ready=False,
                    instant_quote_neighborhood_stats_ready=False,
                    instant_quote_segment_stats_ready=False,
                    instant_quote_asset_ready=False,
                    instant_quote_ready=False,
                    search_support_ready=False,
                    feature_ready=False,
                    comp_ready=False,
                    valuation_ready=False,
                    savings_ready=False,
                    decision_tree_ready=False,
                    explanation_ready=False,
                    recommendation_ready=False,
                    quote_ready=False,
                    parcel_summary_row_count=12,
                    search_document_row_count=0,
                    parcel_feature_row_count=0,
                    comp_pool_row_count=0,
                    quote_row_count=0,
                ),
            )
        if tax_year == 2025:
            return CountyTaxYearReadiness(
                county_id=county_id,
                tax_year=tax_year,
                tax_year_known=True,
                datasets=[
                    DatasetYearReadiness(
                        county_id=county_id,
                        tax_year=tax_year,
                        dataset_type="property_roll",
                        source_system_code="HCAD_BULK",
                        access_method="manual_upload",
                        availability_status="manual_upload_required",
                        tax_year_known=True,
                        raw_file_count=1,
                        latest_import_batch_id="batch-2025",
                        latest_import_status="normalized",
                        latest_publish_state="published",
                        staged=True,
                        canonical_published=True,
                    ),
                    DatasetYearReadiness(
                        county_id=county_id,
                        tax_year=tax_year,
                        dataset_type="deeds",
                        source_system_code="HCAD_BULK",
                        access_method="manual_upload",
                        availability_status="manual_upload_required",
                        tax_year_known=True,
                        raw_file_count=0,
                        latest_import_batch_id=None,
                        latest_import_status=None,
                        latest_publish_state=None,
                        staged=False,
                        canonical_published=False,
                    ),
                ],
                derived=TaxYearDerivedReadiness(
                    parcel_summary_ready=True,
                    parcel_year_trend_ready=False,
                    neighborhood_stats_ready=False,
                    neighborhood_year_trend_ready=False,
                    instant_quote_subject_ready=True,
                    instant_quote_neighborhood_stats_ready=True,
                    instant_quote_segment_stats_ready=True,
                    instant_quote_asset_ready=True,
                    instant_quote_ready=False,
                    instant_quote_tax_rate_basis_year=2024,
                    instant_quote_tax_rate_basis_reason=(
                        "fallback_requested_year_missing_supportable_subjects"
                    ),
                    instant_quote_tax_rate_basis_fallback_applied=True,
                    instant_quote_tax_rate_basis_status="prior_year_adopted_rates",
                    instant_quote_tax_rate_basis_status_reason="basis_year_precedes_quote_year",
                    instant_quote_tax_rate_basis_internal_note=(
                        "2025 instant quote currently uses 2024 adopted tax-rate basis until 2025 "
                        "rates are available and refreshed. Current-year rates are typically updated "
                        "later in the year, often around September-October."
                    ),
                    instant_quote_tax_rate_requested_year_supportable_subject_row_count=0,
                    instant_quote_tax_rate_basis_supportable_subject_row_count=31,
                    instant_quote_tax_rate_quoteable_subject_row_count=100,
                    instant_quote_tax_rate_requested_year_effective_tax_rate_coverage_ratio=0.31,
                    instant_quote_tax_rate_requested_year_assignment_coverage_ratio=0.62,
                    instant_quote_tax_rate_basis_effective_tax_rate_coverage_ratio=0.93,
                    instant_quote_tax_rate_basis_assignment_coverage_ratio=0.96,
                    instant_quote_tax_rate_basis_continuity_parcel_match_row_count=88,
                    instant_quote_tax_rate_basis_continuity_parcel_gap_row_count=12,
                    instant_quote_tax_rate_basis_continuity_parcel_match_ratio=0.88,
                    instant_quote_tax_rate_basis_continuity_account_number_match_row_count=5,
                    instant_quote_tax_rate_basis_warning_codes=[
                        "parcel_continuity_warning",
                        "account_number_continuity_diagnostic",
                        "current_year_final_adoption_metadata_incomplete",
                    ],
                    search_support_ready=True,
                    feature_ready=False,
                    comp_ready=False,
                    valuation_ready=False,
                    savings_ready=False,
                    decision_tree_ready=False,
                    explanation_ready=False,
                    recommendation_ready=False,
                    quote_ready=False,
                    parcel_summary_row_count=18,
                    instant_quote_subject_row_count=18,
                    instant_quote_neighborhood_stats_row_count=3,
                    instant_quote_segment_stats_row_count=2,
                    instant_quote_supportable_row_count=12,
                    instant_quote_supported_neighborhood_stats_row_count=0,
                    instant_quote_supported_segment_stats_row_count=0,
                    search_document_row_count=18,
                    parcel_feature_row_count=0,
                    comp_pool_row_count=0,
                    quote_row_count=0,
                ),
            )
        return CountyTaxYearReadiness(
            county_id=county_id,
            tax_year=tax_year,
            tax_year_known=False,
            datasets=[],
            derived=TaxYearDerivedReadiness(
                parcel_summary_ready=False,
                parcel_year_trend_ready=False,
                neighborhood_stats_ready=False,
                neighborhood_year_trend_ready=False,
                instant_quote_subject_ready=False,
                instant_quote_neighborhood_stats_ready=False,
                instant_quote_segment_stats_ready=False,
                instant_quote_asset_ready=False,
                instant_quote_ready=False,
                search_support_ready=False,
                feature_ready=False,
                comp_ready=False,
                valuation_ready=False,
                savings_ready=False,
                decision_tree_ready=False,
                explanation_ready=False,
                recommendation_ready=False,
                quote_ready=False,
                parcel_summary_row_count=0,
                search_document_row_count=0,
                parcel_feature_row_count=0,
                comp_pool_row_count=0,
                quote_row_count=0,
            ),
        )


class StubOperationalMetricsProvider:
    def build_dataset_metrics(
        self,
        connection,
        *,
        county_id: str,
        tax_year: int,
        dataset_type: str,
    ) -> DatasetOperationalMetrics:
        assert county_id == "harris"
        if tax_year == 2025 and dataset_type == "property_roll":
            return DatasetOperationalMetrics(
                freshness_status="fresh",
                freshness_sla_days=30,
                freshness_age_days=7,
                maintenance_status="failed",
                maintenance_failed_step_name="search_refresh",
            )
        if tax_year == 2025 and dataset_type == "deeds":
            return DatasetOperationalMetrics(
                freshness_status="stale",
                freshness_sla_days=30,
                freshness_age_days=65,
                recent_failed_job_count=1,
                stale_running_job_count=1,
                validation_error_count=2,
                validation_regression=True,
            )
        if tax_year == 2024 and dataset_type == "property_roll":
            return DatasetOperationalMetrics(
                freshness_status="fresh",
                freshness_sla_days=90,
                freshness_age_days=10,
            )
        return DatasetOperationalMetrics()


class NullConnection:
    def __enter__(self) -> NullConnection:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def test_admin_readiness_uses_prior_year_support_for_trend() -> None:
    dashboard = AdminReadinessService(
        data_readiness_service=StubDataReadinessService(),
        operational_metrics_provider=StubOperationalMetricsProvider(),
        connection_factory=lambda: NullConnection(),
    ).build_dashboard(county_id="harris", tax_years=[2025])

    assert dashboard.access_scope == "internal"
    assert dashboard.tax_years == [2025]
    capability_statuses = {
        capability.capability_code: capability.status for capability in dashboard.capabilities
    }
    assert capability_statuses["parcel_level_homestead"] == "supported"
    assert capability_statuses["parcel_level_over65"] == "limited"
    assert capability_statuses["search_refresh_runtime"] == "heavy"
    row = dashboard.readiness_rows[0]
    assert row.overall_status == "derived_ready"
    assert row.trend_label == "improving"
    assert row.trend_delta == 10
    assert "manual_backfill_required" in row.blockers
    assert "instant_quote_public_support_thin" in row.blockers
    assert row.datasets[0].maintenance_status == "failed"
    assert row.datasets[0].maintenance_failed_step_name == "search_refresh"
    assert "property_roll_maintenance_failed" in row.operational.alerts
    assert row.derived.instant_quote_asset_ready is True
    assert row.derived.instant_quote_tax_rate_basis_year == 2024
    assert row.derived.instant_quote_tax_rate_basis_fallback_applied is True
    assert row.derived.instant_quote_tax_rate_basis_reason == (
        "fallback_requested_year_missing_supportable_subjects"
    )
    assert row.derived.instant_quote_tax_rate_basis_status == "prior_year_adopted_rates"
    assert row.derived.instant_quote_tax_rate_basis_internal_note == (
        "2025 instant quote currently uses 2024 adopted tax-rate basis until 2025 rates are "
        "available and refreshed. Current-year rates are typically updated later in the year, "
        "often around September-October."
    )
    assert row.derived.instant_quote_tax_rate_basis_status_reason == (
        "basis_year_precedes_quote_year"
    )
    assert row.derived.instant_quote_tax_rate_basis_continuity_parcel_match_ratio == 0.88
    assert row.derived.instant_quote_tax_rate_basis_warning_codes == [
        "parcel_continuity_warning",
        "account_number_continuity_diagnostic",
        "current_year_final_adoption_metadata_incomplete",
    ]
    assert "search_read_model_not_ready" not in row.blockers
    assert row.operational.quality_status == "critical"
    assert row.operational.stale_running_job_count == 1
    assert row.operational.validation_regression_count == 1
    assert "deeds_validation_regression" in row.operational.alerts
    assert "deeds_stale_jobs" in row.operational.alerts
    assert "instant_quote_support_too_thin" in row.operational.alerts
    assert "instant_quote_tax_rate_parcel_continuity_warning" in row.operational.alerts
    assert "instant_quote_tax_rate_final_adoption_metadata_incomplete" in row.operational.alerts
    assert dashboard.kpi_summary.critical_year_count == 1
    assert dashboard.kpi_summary.validation_regression_count == 1


def test_get_county_year_readiness_wraps_service(monkeypatch) -> None:
    class StubAdminReadinessService:
        def build_dashboard(
            self,
            *,
            county_id: str,
            tax_years: list[int],
        ) -> AdminCountyYearReadinessDashboard:
            assert county_id == "fort_bend"
            assert tax_years == [2025, 2024]
            return AdminCountyYearReadinessDashboard(
                county_id=county_id,
                tax_years=tax_years,
                readiness_rows=[],
            )

    monkeypatch.setattr("app.api.admin.AdminReadinessService", StubAdminReadinessService)

    dashboard = get_county_year_readiness("fort_bend", [2025, 2024])

    assert dashboard.county_id == "fort_bend"
    assert dashboard.tax_years == [2025, 2024]


def test_get_county_onboarding_contract_wraps_service(monkeypatch) -> None:
    class StubCountyOnboardingService:
        def build_contract(
            self,
            *,
            county_id: str,
            tax_years: list[int],
            current_tax_year: int | None = None,
        ) -> CountyOnboardingContract:
            assert county_id == "fort_bend"
            assert tax_years == [2026, 2025]
            assert current_tax_year == 2026
            return CountyOnboardingContract(
                county_id=county_id,
                current_tax_year=2026,
                validation_tax_year=2025,
                validation_recommended=True,
                onboarding_summary=OnboardingSummary(
                    overall_status="blocked",
                    done_phase_count=1,
                    pending_phase_count=1,
                    blocked_phase_count=0,
                    blocking_phase_codes=["dataset_prep_contract"],
                    next_phase_code="dataset_prep_contract",
                    next_blocking_phase_code="dataset_prep_contract",
                ),
                baseline_comparison=OnboardingBaselineComparison(
                    baseline_tax_year=2025,
                    current_tax_year=2026,
                    comparable=True,
                    current_year_lagging=True,
                    lagging_signals=["property_roll:canonical_publish"],
                ),
                capabilities=[],
                validation_candidates=[
                    OnboardingValidationYear(
                        tax_year=2025,
                        readiness_score=42,
                        recommended_for_qa=True,
                        caveats=["comp_generation_not_ready"],
                        validation_capabilities={"parcel_summary_validation_ready": True},
                    )
                ],
                current_year_snapshot=OnboardingReadinessSnapshot(
                    tax_year=2026,
                    datasets=[
                        OnboardingDatasetSnapshot(
                            dataset_type="property_roll",
                            access_method="manual_upload",
                            availability_status="manual_upload_required",
                            raw_file_count=0,
                            latest_import_batch_id=None,
                            latest_import_status=None,
                            latest_publish_state=None,
                            canonical_published=False,
                        )
                    ],
                    parcel_summary_ready=False,
                    search_support_ready=False,
                    feature_ready=False,
                    comp_ready=False,
                    quote_ready=False,
                ),
                validation_year_snapshot=None,
                phases=[
                    OnboardingPhase(
                        phase_code="validation_year_selection",
                        label="Validation year selection",
                        status="done",
                        blocking=False,
                        summary="Use tax year 2025 as the repeatable onboarding QA baseline.",
                        details=["comp_generation_not_ready"],
                        success_criteria=["A repeatable prior-year QA baseline is selected."],
                    )
                ],
                recommended_actions=[
                    OnboardingAction(
                        action_code="prepare_adapter_ready_files",
                        phase_code="dataset_prep_contract",
                        blocking=True,
                        summary="Prepare adapter-ready files and manifests.",
                        command_hint="python3 -m infra.scripts.prepare_manual_county_files --county-id fort_bend --tax-year 2025 --dataset-type both",
                    )
                ],
            )

    monkeypatch.setattr("app.api.admin.CountyOnboardingService", StubCountyOnboardingService)

    contract = get_county_onboarding_contract(
        "fort_bend",
        tax_years=[2026, 2025],
        current_tax_year=2026,
    )

    assert isinstance(contract, AdminCountyOnboardingContract)
    assert contract.validation_tax_year == 2025
    assert contract.onboarding_summary.overall_status == "blocked"
    assert contract.baseline_comparison.current_year_lagging is True
    assert contract.validation_candidates[0].readiness_score == 42
    assert contract.current_year_snapshot is not None
    assert contract.current_year_snapshot.tax_year == 2026
    assert contract.phases[0].phase_code == "validation_year_selection"
    assert contract.phases[0].success_criteria == ["A repeatable prior-year QA baseline is selected."]
    assert contract.recommended_actions[0].action_code == "prepare_adapter_ready_files"


def test_public_parcel_summary_model_has_no_admin_only_fields() -> None:
    assert "blockers" not in ParcelSummaryResponse.model_fields
    assert "readiness_score" not in ParcelSummaryResponse.model_fields
    assert "trend_delta" not in ParcelSummaryResponse.model_fields
    assert "admin_review_required" not in ParcelSummaryResponse.model_fields


def test_admin_readiness_marks_publish_blocked_dataset() -> None:
    dataset = AdminReadinessService()._build_dataset_readiness(
        DatasetYearReadiness(
            county_id="harris",
            tax_year=2025,
            dataset_type="property_roll",
            source_system_code="HCAD_BULK",
            access_method="live_file",
            availability_status="live_ready",
            tax_year_known=True,
            raw_file_count=1,
            latest_import_batch_id="batch-1",
            latest_import_status="publish_blocked",
            latest_status_reason="Publish blocked because 2 validation error finding(s) exist for import batch batch-1.",
            latest_publish_state="blocked_validation",
            staged=False,
            canonical_published=False,
        ),
        DatasetOperationalMetrics(
            freshness_status="stale",
            freshness_sla_days=30,
            freshness_age_days=45,
            recent_failed_job_count=1,
            validation_error_count=2,
            validation_regression=True,
        ),
    )

    assert dataset.stage_status == "publish_blocked"
    assert "publish_blocked_validation" in dataset.blockers
    assert "stale_source_activity" in dataset.blockers
    assert "recent_job_failures" in dataset.blockers
    assert "validation_regression" in dataset.blockers
    assert dataset.latest_status_reason == (
        "Publish blocked because 2 validation error finding(s) exist for import batch batch-1."
    )
    assert dataset.freshness_status == "stale"
    assert dataset.validation_regression is True
    assert dataset.validation_regression is True


def test_admin_readiness_surfaces_fort_bend_tax_completeness_caveats() -> None:
    class FortBendStubDataReadinessService:
        def build_tax_year_readiness(self, *, county_id: str, tax_year: int) -> CountyTaxYearReadiness:
            assert county_id == "fort_bend"
            return CountyTaxYearReadiness(
                county_id=county_id,
                tax_year=tax_year,
                tax_year_known=True,
                datasets=[
                    DatasetYearReadiness(
                        county_id=county_id,
                        tax_year=tax_year,
                        dataset_type="property_roll",
                        source_system_code="FBCAD_BULK",
                        access_method="manual_upload",
                        availability_status="manual_upload_required",
                        tax_year_known=True,
                        raw_file_count=1,
                        latest_import_batch_id=f"batch-{tax_year}",
                        latest_import_status="normalized",
                        latest_publish_state="published",
                        staged=True,
                        canonical_published=True,
                    )
                ],
                derived=TaxYearDerivedReadiness(
                    parcel_summary_ready=True,
                    parcel_year_trend_ready=True,
                    neighborhood_stats_ready=True,
                    neighborhood_year_trend_ready=True,
                    instant_quote_subject_ready=True,
                    instant_quote_neighborhood_stats_ready=True,
                    instant_quote_segment_stats_ready=True,
                    instant_quote_asset_ready=True,
                    instant_quote_ready=True,
                    instant_quote_refresh_status="completed",
                    instant_quote_tax_rate_basis_year=2025,
                    instant_quote_tax_rate_basis_fallback_applied=True,
                    instant_quote_tax_rate_basis_status="prior_year_adopted_rates",
                    instant_quote_tax_completeness_status="operational_with_caveats",
                    instant_quote_tax_completeness_reason="fort_bend_revalidation_residual_risk",
                    instant_quote_tax_completeness_internal_note=(
                        "Fort Bend 2026 parcel tax completeness is operational with caveats."
                    ),
                    instant_quote_tax_completeness_warning_codes=[
                        "acceptable_caution_rows_operational",
                        "risky_caution_rows_monitored",
                        "continuity_gap_rows_monitored",
                    ],
                    instant_quote_supportable_row_rate=0.72,
                    instant_quote_support_rate_all_sfr_flagged_denominator_count=100,
                    instant_quote_support_rate_all_sfr_flagged_supportable_count=72,
                    instant_quote_support_rate_all_sfr_flagged=0.72,
                    instant_quote_total_count_all_sfr_flagged=100,
                    instant_quote_support_count_all_sfr_flagged=72,
                    instant_quote_support_rate_strict_sfr_eligible_denominator_count=80,
                    instant_quote_support_rate_strict_sfr_eligible_supportable_count=72,
                    instant_quote_support_rate_strict_sfr_eligible=0.9,
                    instant_quote_total_count_strict_sfr_eligible=80,
                    instant_quote_support_count_strict_sfr_eligible=72,
                    instant_quote_denominator_shift_alert={
                        "status": "threshold_exceeded",
                        "triggered": True,
                        "threshold_pct": 0.05,
                        "current_total_count_all_sfr_flagged": 100,
                        "prior_total_count_all_sfr_flagged": 90,
                        "pct_change": 0.1111111111111111,
                        "abs_pct_change": 0.1111111111111111,
                        "warning_codes": ["all_sfr_flagged_denominator_shift_exceeded"],
                    },
                    instant_quote_denominator_shift_warning_codes=[
                        "all_sfr_flagged_denominator_shift_exceeded"
                    ],
                    instant_quote_high_value_subject_row_count=40,
                    instant_quote_high_value_supportable_subject_row_count=28,
                    instant_quote_high_value_support_rate=0.7,
                    instant_quote_special_district_heavy_subject_row_count=30,
                    instant_quote_special_district_heavy_supportable_subject_row_count=24,
                    instant_quote_special_district_heavy_support_rate=0.8,
                    instant_quote_monitored_zero_savings_sample_row_count=20,
                    instant_quote_monitored_zero_savings_supported_quote_count=18,
                    instant_quote_monitored_zero_savings_quote_count=9,
                    instant_quote_monitored_zero_savings_quote_share=0.5,
                    instant_quote_monitored_extreme_savings_watchlist_count=10,
                    instant_quote_monitored_extreme_savings_flagged_count=2,
                    search_support_ready=True,
                    feature_ready=False,
                    comp_ready=False,
                    valuation_ready=False,
                    savings_ready=False,
                    decision_tree_ready=False,
                    explanation_ready=False,
                    recommendation_ready=False,
                    quote_ready=False,
                    parcel_summary_row_count=10,
                    instant_quote_subject_row_count=10,
                    instant_quote_neighborhood_stats_row_count=5,
                    instant_quote_segment_stats_row_count=5,
                    instant_quote_supportable_row_count=10,
                    instant_quote_supported_neighborhood_stats_row_count=5,
                    instant_quote_supported_segment_stats_row_count=5,
                    search_document_row_count=10,
                    parcel_feature_row_count=0,
                    comp_pool_row_count=0,
                    quote_row_count=0,
                ),
            )

    dashboard = AdminReadinessService(
        data_readiness_service=FortBendStubDataReadinessService(),
        operational_metrics_provider=type(
            "FortBendStubOperationalMetricsProvider",
            (),
            {
                "build_dataset_metrics": lambda self, connection, *, county_id, tax_year, dataset_type: DatasetOperationalMetrics(
                    freshness_status="fresh",
                    freshness_sla_days=14,
                    freshness_age_days=1,
                )
            },
        )(),
        connection_factory=lambda: NullConnection(),
    ).build_dashboard(county_id="fort_bend", tax_years=[2026])

    row = dashboard.readiness_rows[0]
    assert row.derived.instant_quote_ready is True
    assert row.derived.instant_quote_tax_completeness_status == "operational_with_caveats"
    assert row.derived.instant_quote_tax_completeness_reason == (
        "fort_bend_revalidation_residual_risk"
    )
    assert row.derived.instant_quote_tax_completeness_warning_codes == [
        "acceptable_caution_rows_operational",
        "risky_caution_rows_monitored",
        "continuity_gap_rows_monitored",
    ]
    assert row.derived.instant_quote_supportable_row_rate == 0.72
    assert row.derived.instant_quote_support_rate_all_sfr_flagged_denominator_count == 100
    assert row.derived.instant_quote_support_rate_all_sfr_flagged_supportable_count == 72
    assert row.derived.instant_quote_support_rate_all_sfr_flagged == 0.72
    assert row.derived.instant_quote_total_count_all_sfr_flagged == 100
    assert row.derived.instant_quote_support_count_all_sfr_flagged == 72
    assert row.derived.instant_quote_support_rate_strict_sfr_eligible_denominator_count == 80
    assert row.derived.instant_quote_support_rate_strict_sfr_eligible_supportable_count == 72
    assert row.derived.instant_quote_support_rate_strict_sfr_eligible == 0.9
    assert row.derived.instant_quote_total_count_strict_sfr_eligible == 80
    assert row.derived.instant_quote_support_count_strict_sfr_eligible == 72
    assert row.derived.instant_quote_denominator_shift_alert["status"] == (
        "threshold_exceeded"
    )
    assert row.derived.instant_quote_denominator_shift_warning_codes == [
        "all_sfr_flagged_denominator_shift_exceeded"
    ]
    assert row.derived.instant_quote_high_value_support_rate == 0.7
    assert row.derived.instant_quote_special_district_heavy_support_rate == 0.8
    assert row.derived.instant_quote_monitored_zero_savings_quote_share == 0.5
    assert row.derived.instant_quote_monitored_extreme_savings_watchlist_count == 10
    assert row.derived.instant_quote_monitored_extreme_savings_flagged_count == 2
    assert "instant_quote_tax_completeness_operational_caveat" in row.operational.alerts
    assert "instant_quote_tax_completeness_risky_caution_monitored" in row.operational.alerts
    assert "instant_quote_tax_completeness_continuity_gap_monitored" in row.operational.alerts
    assert "instant_quote_extreme_savings_review_required" in row.operational.alerts
    assert "instant_quote_denominator_shift_review_required" in row.operational.alerts


def test_admin_readiness_surfaces_harris_caveated_special_family_monitoring() -> None:
    class HarrisStubDataReadinessService:
        def build_tax_year_readiness(self, *, county_id: str, tax_year: int) -> CountyTaxYearReadiness:
            assert county_id == "harris"
            return CountyTaxYearReadiness(
                county_id=county_id,
                tax_year=tax_year,
                tax_year_known=True,
                datasets=[],
                derived=TaxYearDerivedReadiness(
                    parcel_summary_ready=True,
                    parcel_year_trend_ready=True,
                    neighborhood_stats_ready=True,
                    neighborhood_year_trend_ready=True,
                    instant_quote_subject_ready=True,
                    instant_quote_neighborhood_stats_ready=True,
                    instant_quote_segment_stats_ready=True,
                    instant_quote_asset_ready=True,
                    instant_quote_ready=True,
                    instant_quote_refresh_status="completed",
                    instant_quote_tax_rate_basis_year=2025,
                    instant_quote_tax_rate_basis_fallback_applied=True,
                    instant_quote_tax_rate_basis_status="prior_year_adopted_rates",
                    instant_quote_tax_completeness_status="operational_with_caveats",
                    instant_quote_tax_completeness_reason="harris_refreshed_special_family_recovery",
                    instant_quote_tax_completeness_internal_note=(
                        "Harris 2026 parcel tax completeness is operational with caveats."
                    ),
                    instant_quote_tax_completeness_warning_codes=[
                        "recovered_special_family_billable_rows_operational",
                        "caveated_special_family_rows_monitored",
                        "missing_school_assignment_rows_monitored",
                        "continuity_gap_rows_monitored",
                    ],
                    search_support_ready=True,
                    feature_ready=False,
                    comp_ready=False,
                    valuation_ready=False,
                    savings_ready=False,
                    decision_tree_ready=False,
                    explanation_ready=False,
                    recommendation_ready=False,
                    quote_ready=False,
                    parcel_summary_row_count=10,
                    instant_quote_subject_row_count=10,
                    instant_quote_neighborhood_stats_row_count=5,
                    instant_quote_segment_stats_row_count=5,
                    instant_quote_supportable_row_count=10,
                    instant_quote_supported_neighborhood_stats_row_count=5,
                    instant_quote_supported_segment_stats_row_count=5,
                    search_document_row_count=10,
                    parcel_feature_row_count=0,
                    comp_pool_row_count=0,
                    quote_row_count=0,
                ),
            )

    dashboard = AdminReadinessService(
        data_readiness_service=HarrisStubDataReadinessService(),
        operational_metrics_provider=type(
            "HarrisStubOperationalMetricsProvider",
            (),
            {
                "build_dataset_metrics": lambda self, connection, *, county_id, tax_year, dataset_type: DatasetOperationalMetrics(
                    freshness_status="fresh",
                    freshness_sla_days=14,
                    freshness_age_days=1,
                )
            },
        )(),
        connection_factory=lambda: NullConnection(),
    ).build_dashboard(county_id="harris", tax_years=[2026])

    row = dashboard.readiness_rows[0]
    assert row.derived.instant_quote_tax_completeness_reason == (
        "harris_refreshed_special_family_recovery"
    )
    assert "instant_quote_tax_completeness_operational_caveat" in row.operational.alerts
    assert "instant_quote_tax_completeness_continuity_gap_monitored" in row.operational.alerts
    assert "instant_quote_tax_completeness_caveated_special_family_monitored" in row.operational.alerts
    assert "instant_quote_tax_completeness_missing_school_assignment_monitored" in row.operational.alerts


def test_operational_metrics_provider_applies_sla_windows() -> None:
    provider = AdminOperationalMetricsProvider(
        now_fn=lambda: datetime(2026, 3, 27, tzinfo=timezone.utc)
    )

    assert provider._freshness_sla_days(tax_year=2026) == 14
    assert provider._freshness_sla_days(tax_year=2025) == 30
    assert provider._freshness_sla_days(tax_year=2022) == 90
    assert provider._freshness_status(
        latest_activity_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
        freshness_age_days=54,
        freshness_sla_days=30,
    ) == "stale"
