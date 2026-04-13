from __future__ import annotations

from app.models.admin import (
    AdminCountyYearDatasetReadiness,
    AdminCountyYearDerivedReadiness,
    AdminCountyYearOperationalReadiness,
    AdminCountyYearReadiness,
    AdminCountyYearReadinessDashboard,
    AdminCountyYearReadinessKpiSummary,
    AdminImportBatchActions,
    AdminImportBatchDetail,
    AdminImportBatchInspection,
    AdminImportBatchSummary,
    AdminValidationResultsResponse,
)
from infra.scripts.report_readiness_metrics import build_payload as build_metrics_payload
from infra.scripts.verify_ingestion_to_searchable import build_payload as build_smoke_payload


def _dashboard() -> AdminCountyYearReadinessDashboard:
    return AdminCountyYearReadinessDashboard(
        county_id="harris",
        tax_years=[2025],
        readiness_rows=[
            AdminCountyYearReadiness(
                county_id="harris",
                tax_year=2025,
                overall_status="derived_ready",
                readiness_score=90,
                trend_label="improving",
                trend_delta=10,
                tax_year_known=True,
                blockers=[],
                datasets=[
                    AdminCountyYearDatasetReadiness(
                        dataset_type="property_roll",
                        source_system_code="HCAD_BULK",
                        access_method="live_file",
                        availability_status="live_ready",
                        raw_file_count=1,
                        latest_import_batch_id="batch-property",
                        latest_import_status="normalized",
                        latest_publish_state="published",
                        stage_status="canonical_published",
                        blockers=[],
                        freshness_status="fresh",
                        freshness_age_days=2,
                        freshness_sla_days=30,
                    ),
                    AdminCountyYearDatasetReadiness(
                        dataset_type="tax_rates",
                        source_system_code="HCAD_BULK",
                        access_method="live_file",
                        availability_status="live_ready",
                        raw_file_count=1,
                        latest_import_batch_id="batch-tax",
                        latest_import_status="normalized",
                        latest_publish_state="published",
                        stage_status="canonical_published",
                        blockers=[],
                        freshness_status="fresh",
                        freshness_age_days=2,
                        freshness_sla_days=30,
                    ),
                ],
                derived=AdminCountyYearDerivedReadiness(
                    parcel_summary_ready=True,
                    instant_quote_supportable_row_rate=0.75,
                    instant_quote_support_rate_all_sfr_flagged_denominator_count=100,
                    instant_quote_support_rate_all_sfr_flagged_supportable_count=75,
                    instant_quote_support_rate_all_sfr_flagged=0.75,
                    instant_quote_total_count_all_sfr_flagged=100,
                    instant_quote_support_count_all_sfr_flagged=75,
                    instant_quote_support_rate_strict_sfr_eligible_denominator_count=80,
                    instant_quote_support_rate_strict_sfr_eligible_supportable_count=70,
                    instant_quote_support_rate_strict_sfr_eligible=0.875,
                    instant_quote_total_count_strict_sfr_eligible=80,
                    instant_quote_support_count_strict_sfr_eligible=70,
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
                    instant_quote_high_value_support_rate=0.6,
                    instant_quote_special_district_heavy_support_rate=0.8,
                    instant_quote_monitored_zero_savings_quote_share=0.4,
                    instant_quote_monitored_extreme_savings_watchlist_count=10,
                    instant_quote_monitored_extreme_savings_flagged_count=2,
                    search_support_ready=True,
                    feature_ready=False,
                    comp_ready=False,
                    quote_ready=False,
                    parcel_summary_row_count=100,
                    search_document_row_count=100,
                    parcel_feature_row_count=0,
                    comp_pool_row_count=0,
                    quote_row_count=0,
                ),
                operational=AdminCountyYearOperationalReadiness(
                    quality_score=90,
                    quality_status="healthy",
                    freshness_status="fresh",
                    freshness_sla_days=30,
                    freshness_age_days=2,
                    searchable_ready=True,
                    alerts=[],
                ),
            )
        ],
        kpi_summary=AdminCountyYearReadinessKpiSummary(
            total_year_count=1,
            healthy_year_count=1,
            searchable_year_count=1,
        ),
    )


def test_report_readiness_metrics_builds_alertable_payload(monkeypatch) -> None:
    class StubAdminReadinessService:
        def build_dashboard(self, *, county_id: str, tax_years: list[int]) -> AdminCountyYearReadinessDashboard:
            assert county_id == "harris"
            assert tax_years == [2025]
            return _dashboard()

    monkeypatch.setattr("infra.scripts.report_readiness_metrics.AdminReadinessService", StubAdminReadinessService)

    payload = build_metrics_payload(county_id="harris", tax_years=[2025])

    assert payload["county_id"] == "harris"
    assert payload["kpi_summary"]["healthy_year_count"] == 1
    assert payload["readiness_rows"][0]["operational"]["quality_status"] == "healthy"
    assert payload["readiness_rows"][0]["derived_monitoring"]["instant_quote_supportable_row_rate"] == 0.75
    assert (
        payload["readiness_rows"][0]["derived_monitoring"][
            "instant_quote_support_rate_all_sfr_flagged"
        ]
        == 0.75
    )
    assert (
        payload["readiness_rows"][0]["derived_monitoring"][
            "instant_quote_total_count_all_sfr_flagged"
        ]
        == 100
    )
    assert (
        payload["readiness_rows"][0]["derived_monitoring"][
            "instant_quote_support_count_all_sfr_flagged"
        ]
        == 75
    )
    assert (
        payload["readiness_rows"][0]["derived_monitoring"][
            "instant_quote_support_rate_strict_sfr_eligible"
        ]
        == 0.875
    )
    assert (
        payload["readiness_rows"][0]["derived_monitoring"][
            "instant_quote_total_count_strict_sfr_eligible"
        ]
        == 80
    )
    assert (
        payload["readiness_rows"][0]["derived_monitoring"][
            "instant_quote_support_count_strict_sfr_eligible"
        ]
        == 70
    )
    assert (
        payload["readiness_rows"][0]["derived_monitoring"][
            "instant_quote_denominator_shift_alert"
        ]["status"]
        == "threshold_exceeded"
    )
    assert payload["readiness_rows"][0]["derived_monitoring"][
        "instant_quote_denominator_shift_warning_codes"
    ] == ["all_sfr_flagged_denominator_shift_exceeded"]
    assert (
        payload["readiness_rows"][0]["derived_monitoring"][
            "instant_quote_monitored_extreme_savings_flagged_count"
        ]
        == 2
    )
    assert payload["readiness_rows"][0]["datasets"][0]["latest_import_batch_id"] == "batch-property"


def test_verify_ingestion_to_searchable_reports_pass(monkeypatch) -> None:
    class StubAdminReadinessService:
        def build_dashboard(self, *, county_id: str, tax_years: list[int]) -> AdminCountyYearReadinessDashboard:
            assert county_id == "harris"
            assert tax_years == [2025]
            return _dashboard()

    class StubAdminOpsService:
        def get_import_batch_detail(self, *, import_batch_id: str) -> AdminImportBatchDetail:
            return AdminImportBatchDetail(
                batch=AdminImportBatchSummary(
                    import_batch_id=import_batch_id,
                    county_id="harris",
                    tax_year=2025,
                    dataset_type="property_roll" if "property" in import_batch_id else "tax_rates",
                    source_system_code="HCAD_BULK",
                    status="normalized",
                    publish_state="published",
                    raw_file_count=1,
                    validation_result_count=0,
                    validation_error_count=0,
                ),
                inspection=AdminImportBatchInspection(
                    status="normalized",
                    publish_state="published",
                    raw_file_count=1,
                    job_run_count=1,
                    staging_row_count=100,
                    lineage_record_count=100,
                    validation_result_count=0,
                    validation_error_count=0,
                ),
                validation_summary=AdminValidationResultsResponse(
                    import_batch_id=import_batch_id,
                    total_count=0,
                    error_count=0,
                    warning_count=0,
                    info_count=0,
                    findings=[],
                ),
                source_files=[],
                job_runs=[],
                actions=AdminImportBatchActions(
                    can_publish=False,
                    can_rollback=True,
                    manual_fallback_supported=True,
                ),
            )

    monkeypatch.setattr(
        "infra.scripts.verify_ingestion_to_searchable.AdminReadinessService",
        StubAdminReadinessService,
    )
    monkeypatch.setattr(
        "infra.scripts.verify_ingestion_to_searchable.AdminOpsService",
        StubAdminOpsService,
    )

    payload = build_smoke_payload(
        county_id="harris",
        tax_year=2025,
        dataset_types=["property_roll", "tax_rates"],
    )

    assert payload["passed"] is True
    assert payload["quality_status"] == "healthy"
    assert payload["checks"][0]["detail"]["staging_row_count"] == 100
