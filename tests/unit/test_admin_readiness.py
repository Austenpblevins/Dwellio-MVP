from __future__ import annotations

from app.api.admin import get_county_year_readiness
from app.models.admin import AdminCountyYearReadinessDashboard
from app.models.parcel import ParcelSummaryResponse
from app.services.admin_readiness import AdminReadinessService
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


def test_admin_readiness_uses_prior_year_support_for_trend() -> None:
    dashboard = AdminReadinessService(
        data_readiness_service=StubDataReadinessService()
    ).build_dashboard(county_id="harris", tax_years=[2025])

    assert dashboard.access_scope == "internal"
    assert dashboard.tax_years == [2025]
    row = dashboard.readiness_rows[0]
    assert row.overall_status == "derived_ready"
    assert row.trend_label == "improving"
    assert row.trend_delta == 10
    assert "manual_backfill_required" in row.blockers
    assert "search_read_model_not_ready" not in row.blockers


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
        )
    )

    assert dataset.stage_status == "publish_blocked"
    assert "publish_blocked_validation" in dataset.blockers
    assert dataset.latest_status_reason == (
        "Publish blocked because 2 validation error finding(s) exist for import batch batch-1."
    )
