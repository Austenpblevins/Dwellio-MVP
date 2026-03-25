from __future__ import annotations

from app.models.admin import (
    AdminCountyYearDatasetReadiness,
    AdminCountyYearDerivedReadiness,
    AdminCountyYearReadiness,
    AdminCountyYearReadinessDashboard,
)
from app.services.data_readiness import (
    CountyTaxYearReadiness,
    DataReadinessService,
    DatasetYearReadiness,
)


class AdminReadinessService:
    def __init__(self, *, data_readiness_service: DataReadinessService | None = None) -> None:
        self.data_readiness_service = data_readiness_service or DataReadinessService()

    def build_dashboard(
        self,
        *,
        county_id: str,
        tax_years: list[int],
    ) -> AdminCountyYearReadinessDashboard:
        normalized_years = list(dict.fromkeys(tax_years))
        lookup_years = list(dict.fromkeys([*normalized_years, *(tax_year - 1 for tax_year in normalized_years)]))
        readiness_by_year = {
            tax_year: self.data_readiness_service.build_tax_year_readiness(
                county_id=county_id,
                tax_year=tax_year,
            )
            for tax_year in lookup_years
        }

        rows = [
            self._build_row(
                readiness=readiness_by_year[tax_year],
                prior_year_readiness=readiness_by_year.get(tax_year - 1),
            )
            for tax_year in normalized_years
        ]

        return AdminCountyYearReadinessDashboard(
            county_id=county_id,
            tax_years=normalized_years,
            readiness_rows=rows,
        )

    def _build_row(
        self,
        *,
        readiness: CountyTaxYearReadiness,
        prior_year_readiness: CountyTaxYearReadiness | None,
    ) -> AdminCountyYearReadiness:
        dataset_models = [
            self._build_dataset_readiness(dataset)
            for dataset in readiness.datasets
        ]
        blockers = self._collect_blockers(readiness, dataset_models)
        readiness_score = self._score_readiness(readiness)
        prior_score = self._score_readiness(prior_year_readiness) if prior_year_readiness else None
        trend_delta = readiness_score - prior_score if prior_score is not None else None

        return AdminCountyYearReadiness(
            county_id=readiness.county_id,
            tax_year=readiness.tax_year,
            overall_status=self._overall_status(readiness),
            readiness_score=readiness_score,
            trend_label=self._trend_label(trend_delta),
            trend_delta=trend_delta,
            tax_year_known=readiness.tax_year_known,
            blockers=blockers,
            datasets=dataset_models,
            derived=AdminCountyYearDerivedReadiness(
                parcel_summary_ready=readiness.derived.parcel_summary_ready,
                parcel_year_trend_ready=readiness.derived.parcel_year_trend_ready,
                neighborhood_stats_ready=readiness.derived.neighborhood_stats_ready,
                neighborhood_year_trend_ready=readiness.derived.neighborhood_year_trend_ready,
                search_support_ready=readiness.derived.search_support_ready,
                feature_ready=readiness.derived.feature_ready,
                comp_ready=readiness.derived.comp_ready,
                valuation_ready=readiness.derived.valuation_ready,
                savings_ready=readiness.derived.savings_ready,
                decision_tree_ready=readiness.derived.decision_tree_ready,
                explanation_ready=readiness.derived.explanation_ready,
                recommendation_ready=readiness.derived.recommendation_ready,
                quote_ready=readiness.derived.quote_ready,
                parcel_summary_row_count=readiness.derived.parcel_summary_row_count,
                parcel_year_trend_row_count=readiness.derived.parcel_year_trend_row_count,
                neighborhood_stats_row_count=readiness.derived.neighborhood_stats_row_count,
                neighborhood_year_trend_row_count=readiness.derived.neighborhood_year_trend_row_count,
                search_document_row_count=readiness.derived.search_document_row_count,
                parcel_feature_row_count=readiness.derived.parcel_feature_row_count,
                comp_pool_row_count=readiness.derived.comp_pool_row_count,
                valuation_run_row_count=readiness.derived.valuation_run_row_count,
                savings_row_count=readiness.derived.savings_row_count,
                decision_tree_row_count=readiness.derived.decision_tree_row_count,
                explanation_row_count=readiness.derived.explanation_row_count,
                recommendation_row_count=readiness.derived.recommendation_row_count,
                quote_row_count=readiness.derived.quote_row_count,
            ),
        )

    def _build_dataset_readiness(
        self,
        dataset: DatasetYearReadiness,
    ) -> AdminCountyYearDatasetReadiness:
        blockers: list[str] = []
        if not dataset.tax_year_known:
            blockers.append("tax_year_not_seeded")
        if dataset.availability_status == "manual_upload_required" and dataset.raw_file_count == 0:
            blockers.append("manual_backfill_required")
        elif dataset.raw_file_count == 0 and dataset.latest_import_status is None:
            blockers.append("source_not_acquired")
        if dataset.latest_import_status == "validation_failed":
            blockers.append("staging_validation_failed")
        if dataset.raw_file_count > 0 and not dataset.canonical_published:
            blockers.append("canonical_publish_pending")

        return AdminCountyYearDatasetReadiness(
            dataset_type=dataset.dataset_type,
            source_system_code=dataset.source_system_code,
            access_method=dataset.access_method,
            availability_status=dataset.availability_status,
            raw_file_count=dataset.raw_file_count,
            latest_import_status=dataset.latest_import_status,
            latest_publish_state=dataset.latest_publish_state,
            stage_status=self._dataset_stage_status(dataset),
            blockers=blockers,
        )

    def _dataset_stage_status(self, dataset: DatasetYearReadiness) -> str:
        if not dataset.tax_year_known:
            return "tax_year_missing"
        if dataset.raw_file_count == 0:
            return (
                "awaiting_manual_backfill"
                if dataset.availability_status == "manual_upload_required"
                else "awaiting_source_data"
            )
        if dataset.canonical_published:
            return "canonical_published"
        if dataset.staged:
            return "staged"
        return "source_acquired"

    def _overall_status(self, readiness: CountyTaxYearReadiness) -> str:
        published_datasets = sum(1 for dataset in readiness.datasets if dataset.canonical_published)
        if not readiness.tax_year_known:
            return "tax_year_missing"
        if readiness.derived.quote_ready:
            return "quote_ready"
        if readiness.derived.parcel_summary_ready and readiness.derived.search_support_ready:
            return "derived_ready"
        if published_datasets > 0:
            return "canonical_partial"
        if any(dataset.raw_file_count > 0 for dataset in readiness.datasets):
            return "source_acquired"
        return "awaiting_source_data"

    def _score_readiness(self, readiness: CountyTaxYearReadiness | None) -> int:
        if readiness is None:
            return 0
        dataset_score = sum(
            25 if dataset.canonical_published else 10 if dataset.staged else 5 if dataset.raw_file_count > 0 else 0
            for dataset in readiness.datasets
        )
        derived_score = sum(
            10
            for flag in (
                readiness.derived.parcel_summary_ready,
                readiness.derived.search_support_ready,
                readiness.derived.feature_ready,
                readiness.derived.comp_ready,
                readiness.derived.quote_ready,
            )
            if flag
        )
        return min(dataset_score + derived_score, 100)

    def _trend_label(self, trend_delta: int | None) -> str:
        if trend_delta is None:
            return "baseline"
        if trend_delta >= 10:
            return "improving"
        if trend_delta <= -10:
            return "weaker"
        return "stable"

    def _collect_blockers(
        self,
        readiness: CountyTaxYearReadiness,
        dataset_models: list[AdminCountyYearDatasetReadiness],
    ) -> list[str]:
        blockers = [blocker for dataset in dataset_models for blocker in dataset.blockers]
        if not readiness.derived.parcel_summary_ready:
            blockers.append("parcel_summary_not_ready")
        if not readiness.derived.search_support_ready:
            blockers.append("search_read_model_not_ready")
        if not readiness.derived.feature_ready:
            blockers.append("feature_layer_not_ready")
        if not readiness.derived.comp_ready:
            blockers.append("comp_layer_not_ready")
        if not readiness.derived.quote_ready:
            blockers.append("quote_read_model_not_ready")
        return list(dict.fromkeys(blockers))
