from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from app.db.connection import get_connection
from app.models.admin import (
    AdminCountyYearDatasetReadiness,
    AdminCountyYearDerivedReadiness,
    AdminCountyYearOperationalReadiness,
    AdminCountyYearReadiness,
    AdminCountyYearReadinessDashboard,
    AdminCountyYearReadinessKpiSummary,
)
from app.services.data_readiness import (
    CountyTaxYearReadiness,
    DataReadinessService,
    DatasetYearReadiness,
)


@dataclass(frozen=True)
class DatasetOperationalMetrics:
    latest_activity_at: datetime | None = None
    freshness_status: str = "unknown"
    freshness_sla_days: int | None = None
    freshness_age_days: int | None = None
    recent_failed_job_count: int = 0
    stale_running_job_count: int = 0
    validation_error_count: int = 0
    validation_regression: bool = False


class AdminOperationalMetricsProvider:
    def __init__(self, *, now_fn=None) -> None:
        self._now_fn = now_fn or (lambda: datetime.now(timezone.utc))

    def build_dataset_metrics(
        self,
        connection: object,
        *,
        county_id: str,
        tax_year: int,
        dataset_type: str,
    ) -> DatasetOperationalMetrics:
        latest_activity_at = self._latest_activity_at(
            connection,
            county_id=county_id,
            tax_year=tax_year,
            dataset_type=dataset_type,
        )
        freshness_sla_days = self._freshness_sla_days(tax_year=tax_year)
        freshness_age_days = self._freshness_age_days(latest_activity_at)
        validation_error_count, prior_validation_error_count = self._validation_error_counts(
            connection,
            county_id=county_id,
            tax_year=tax_year,
            dataset_type=dataset_type,
        )
        validation_regression = validation_error_count > prior_validation_error_count
        recent_failed_job_count = self._recent_failed_job_count(
            connection,
            county_id=county_id,
            tax_year=tax_year,
            dataset_type=dataset_type,
        )
        stale_running_job_count = self._stale_running_job_count(
            connection,
            county_id=county_id,
            tax_year=tax_year,
            dataset_type=dataset_type,
        )
        freshness_status = self._freshness_status(
            latest_activity_at=latest_activity_at,
            freshness_age_days=freshness_age_days,
            freshness_sla_days=freshness_sla_days,
        )
        return DatasetOperationalMetrics(
            latest_activity_at=latest_activity_at,
            freshness_status=freshness_status,
            freshness_sla_days=freshness_sla_days,
            freshness_age_days=freshness_age_days,
            recent_failed_job_count=recent_failed_job_count,
            stale_running_job_count=stale_running_job_count,
            validation_error_count=validation_error_count,
            validation_regression=validation_regression,
        )

    def _latest_activity_at(
        self,
        connection: object,
        *,
        county_id: str,
        tax_year: int,
        dataset_type: str,
    ) -> datetime | None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT MAX(event_at) AS latest_activity_at
                FROM (
                  SELECT rf.created_at AS event_at
                  FROM raw_files rf
                  WHERE rf.county_id = %s
                    AND rf.tax_year = %s
                    AND rf.file_kind = %s
                  UNION ALL
                  SELECT ib.created_at AS event_at
                  FROM import_batches ib
                  WHERE ib.county_id = %s
                    AND ib.tax_year = %s
                    AND ib.dataset_type = %s
                  UNION ALL
                  SELECT COALESCE(jr.finished_at, jr.started_at) AS event_at
                  FROM job_runs jr
                  JOIN import_batches ib
                    ON ib.import_batch_id = jr.import_batch_id
                  WHERE ib.county_id = %s
                    AND ib.tax_year = %s
                    AND ib.dataset_type = %s
                ) activity
                """,
                (
                    county_id,
                    tax_year,
                    dataset_type,
                    county_id,
                    tax_year,
                    dataset_type,
                    county_id,
                    tax_year,
                    dataset_type,
                ),
            )
            row = cursor.fetchone()
        return row.get("latest_activity_at") if row else None

    def _recent_failed_job_count(
        self,
        connection: object,
        *,
        county_id: str,
        tax_year: int,
        dataset_type: str,
    ) -> int:
        cutoff = self._now_fn() - timedelta(days=30)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) AS count
                FROM job_runs jr
                JOIN import_batches ib
                  ON ib.import_batch_id = jr.import_batch_id
                WHERE ib.county_id = %s
                  AND ib.tax_year = %s
                  AND ib.dataset_type = %s
                  AND jr.status = 'failed'
                  AND COALESCE(jr.finished_at, jr.started_at) >= %s
                """,
                (county_id, tax_year, dataset_type, cutoff),
            )
            row = cursor.fetchone()
        return int(row["count"] if row is not None else 0)

    def _validation_error_counts(
        self,
        connection: object,
        *,
        county_id: str,
        tax_year: int,
        dataset_type: str,
    ) -> tuple[int, int]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT validation_error_count
                FROM (
                  SELECT
                    ib.import_batch_id,
                    ib.created_at,
                    COUNT(vr.validation_result_id) FILTER (WHERE vr.severity = 'error') AS validation_error_count
                  FROM import_batches ib
                  LEFT JOIN validation_results vr
                    ON vr.import_batch_id = ib.import_batch_id
                  WHERE ib.county_id = %s
                    AND ib.tax_year = %s
                    AND ib.dataset_type = %s
                  GROUP BY ib.import_batch_id, ib.created_at
                  ORDER BY ib.created_at DESC, ib.import_batch_id DESC
                  LIMIT 2
                ) ranked
                ORDER BY ranked.created_at DESC, ranked.import_batch_id DESC
                """,
                (county_id, tax_year, dataset_type),
            )
            rows = cursor.fetchall()
        counts = [int(row["validation_error_count"] or 0) for row in rows]
        latest = counts[0] if counts else 0
        prior = counts[1] if len(counts) > 1 else 0
        return latest, prior

    def _stale_running_job_count(
        self,
        connection: object,
        *,
        county_id: str,
        tax_year: int,
        dataset_type: str,
    ) -> int:
        cutoff = self._now_fn() - timedelta(hours=6)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) AS count
                FROM job_runs jr
                JOIN import_batches ib
                  ON ib.import_batch_id = jr.import_batch_id
                WHERE ib.county_id = %s
                  AND ib.tax_year = %s
                  AND ib.dataset_type = %s
                  AND jr.status = 'running'
                  AND jr.finished_at IS NULL
                  AND jr.started_at < %s
                """,
                (county_id, tax_year, dataset_type, cutoff),
            )
            row = cursor.fetchone()
        return int(row["count"] if row is not None else 0)

    def _freshness_sla_days(self, *, tax_year: int) -> int:
        current_year = self._now_fn().year
        if tax_year >= current_year:
            return 14
        if tax_year == current_year - 1:
            return 30
        return 90

    def _freshness_age_days(self, latest_activity_at: datetime | None) -> int | None:
        if latest_activity_at is None:
            return None
        latest_utc = latest_activity_at.astimezone(timezone.utc)
        current_utc = self._now_fn().astimezone(timezone.utc)
        return max((current_utc.date() - latest_utc.date()).days, 0)

    def _freshness_status(
        self,
        *,
        latest_activity_at: datetime | None,
        freshness_age_days: int | None,
        freshness_sla_days: int | None,
    ) -> str:
        if latest_activity_at is None:
            return "missing"
        if freshness_age_days is None or freshness_sla_days is None:
            return "unknown"
        if freshness_age_days > freshness_sla_days:
            return "stale"
        if freshness_age_days >= max(1, int(freshness_sla_days * 0.75)):
            return "warning"
        return "fresh"


class AdminReadinessService:
    def __init__(
        self,
        *,
        data_readiness_service: DataReadinessService | None = None,
        operational_metrics_provider: AdminOperationalMetricsProvider | None = None,
        connection_factory=None,
    ) -> None:
        self.data_readiness_service = data_readiness_service or DataReadinessService()
        self.operational_metrics_provider = operational_metrics_provider or AdminOperationalMetricsProvider()
        self.connection_factory = connection_factory or get_connection

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

        with self.connection_factory() as connection:
            operational_metrics_by_year = {
                tax_year: self._build_operational_metrics_by_dataset(
                    connection,
                    readiness=readiness_by_year[tax_year],
                )
                for tax_year in normalized_years
            }

        rows = [
            self._build_row(
                readiness=readiness_by_year[tax_year],
                prior_year_readiness=readiness_by_year.get(tax_year - 1),
                dataset_operational_metrics=operational_metrics_by_year[tax_year],
            )
            for tax_year in normalized_years
        ]

        return AdminCountyYearReadinessDashboard(
            county_id=county_id,
            tax_years=normalized_years,
            readiness_rows=rows,
            kpi_summary=self._build_kpi_summary(rows),
        )

    def _build_operational_metrics_by_dataset(
        self,
        connection: object,
        *,
        readiness: CountyTaxYearReadiness,
    ) -> dict[str, DatasetOperationalMetrics]:
        return {
            dataset.dataset_type: self.operational_metrics_provider.build_dataset_metrics(
                connection,
                county_id=readiness.county_id,
                tax_year=readiness.tax_year,
                dataset_type=dataset.dataset_type,
            )
            for dataset in readiness.datasets
        }

    def _build_row(
        self,
        *,
        readiness: CountyTaxYearReadiness,
        prior_year_readiness: CountyTaxYearReadiness | None,
        dataset_operational_metrics: dict[str, DatasetOperationalMetrics],
    ) -> AdminCountyYearReadiness:
        dataset_models = [
            self._build_dataset_readiness(
                dataset,
                dataset_operational_metrics.get(dataset.dataset_type, DatasetOperationalMetrics()),
            )
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
                instant_quote_subject_ready=readiness.derived.instant_quote_subject_ready,
                instant_quote_neighborhood_stats_ready=readiness.derived.instant_quote_neighborhood_stats_ready,
                instant_quote_segment_stats_ready=readiness.derived.instant_quote_segment_stats_ready,
                instant_quote_asset_ready=readiness.derived.instant_quote_asset_ready,
                instant_quote_ready=readiness.derived.instant_quote_ready,
                instant_quote_refresh_status=readiness.derived.instant_quote_refresh_status,
                instant_quote_last_refresh_at=readiness.derived.instant_quote_last_refresh_at,
                instant_quote_last_validated_at=readiness.derived.instant_quote_last_validated_at,
                instant_quote_cache_view_row_delta=readiness.derived.instant_quote_cache_view_row_delta,
                instant_quote_tax_rate_basis_year=readiness.derived.instant_quote_tax_rate_basis_year,
                instant_quote_tax_rate_basis_reason=readiness.derived.instant_quote_tax_rate_basis_reason,
                instant_quote_tax_rate_basis_fallback_applied=(
                    readiness.derived.instant_quote_tax_rate_basis_fallback_applied
                ),
                instant_quote_tax_rate_basis_status=(
                    readiness.derived.instant_quote_tax_rate_basis_status
                ),
                instant_quote_tax_rate_basis_status_reason=(
                    readiness.derived.instant_quote_tax_rate_basis_status_reason
                ),
                instant_quote_tax_rate_requested_year_supportable_subject_row_count=(
                    readiness.derived.instant_quote_tax_rate_requested_year_supportable_subject_row_count
                ),
                instant_quote_tax_rate_basis_supportable_subject_row_count=(
                    readiness.derived.instant_quote_tax_rate_basis_supportable_subject_row_count
                ),
                instant_quote_supported_public_quote_exists=(
                    readiness.derived.instant_quote_supported_public_quote_exists
                ),
                instant_quote_subject_rows_without_usable_neighborhood_stats=(
                    readiness.derived.instant_quote_subject_rows_without_usable_neighborhood_stats
                ),
                instant_quote_subject_rows_without_usable_segment_stats=(
                    readiness.derived.instant_quote_subject_rows_without_usable_segment_stats
                ),
                instant_quote_subject_rows_missing_segment_row=(
                    readiness.derived.instant_quote_subject_rows_missing_segment_row
                ),
                instant_quote_subject_rows_thin_segment_support=(
                    readiness.derived.instant_quote_subject_rows_thin_segment_support
                ),
                instant_quote_subject_rows_unusable_segment_basis=(
                    readiness.derived.instant_quote_subject_rows_unusable_segment_basis
                ),
                instant_quote_served_neighborhood_only_quote_count=(
                    readiness.derived.instant_quote_served_neighborhood_only_quote_count
                ),
                instant_quote_served_supported_neighborhood_only_quote_count=(
                    readiness.derived.instant_quote_served_supported_neighborhood_only_quote_count
                ),
                instant_quote_served_unsupported_neighborhood_only_quote_count=(
                    readiness.derived.instant_quote_served_unsupported_neighborhood_only_quote_count
                ),
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
                instant_quote_subject_row_count=readiness.derived.instant_quote_subject_row_count,
                instant_quote_neighborhood_stats_row_count=readiness.derived.instant_quote_neighborhood_stats_row_count,
                instant_quote_segment_stats_row_count=readiness.derived.instant_quote_segment_stats_row_count,
                instant_quote_supportable_row_count=readiness.derived.instant_quote_supportable_row_count,
                instant_quote_supported_neighborhood_stats_row_count=(
                    readiness.derived.instant_quote_supported_neighborhood_stats_row_count
                ),
                instant_quote_supported_segment_stats_row_count=(
                    readiness.derived.instant_quote_supported_segment_stats_row_count
                ),
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
            operational=self._build_operational_readiness(
                readiness=readiness,
                dataset_models=dataset_models,
                readiness_score=readiness_score,
            ),
        )

    def _build_dataset_readiness(
        self,
        dataset: DatasetYearReadiness,
        metrics: DatasetOperationalMetrics,
    ) -> AdminCountyYearDatasetReadiness:
        blockers: list[str] = []
        if dataset.availability_status == "manual_upload_required" and dataset.raw_file_count == 0:
            blockers.append("manual_backfill_required")
        elif dataset.raw_file_count == 0 and dataset.latest_import_status is None:
            blockers.append("source_not_acquired")
        if dataset.latest_import_status == "validation_failed":
            blockers.append("staging_validation_failed")
        if dataset.latest_import_status == "publish_blocked":
            blockers.append("publish_blocked_validation")
        if dataset.raw_file_count > 0 and not dataset.canonical_published:
            blockers.append("canonical_publish_pending")
        if metrics.freshness_status == "stale":
            blockers.append("stale_source_activity")
        if metrics.recent_failed_job_count > 0:
            blockers.append("recent_job_failures")
        if metrics.stale_running_job_count > 0:
            blockers.append("stale_running_jobs")
        if metrics.validation_regression:
            blockers.append("validation_regression")

        return AdminCountyYearDatasetReadiness(
            dataset_type=dataset.dataset_type,
            source_system_code=dataset.source_system_code,
            access_method=dataset.access_method,
            availability_status=dataset.availability_status,
            raw_file_count=dataset.raw_file_count,
            latest_import_batch_id=dataset.latest_import_batch_id,
            latest_import_status=dataset.latest_import_status,
            latest_status_reason=dataset.latest_status_reason,
            latest_publish_state=dataset.latest_publish_state,
            stage_status=self._dataset_stage_status(dataset),
            blockers=blockers,
            latest_activity_at=metrics.latest_activity_at,
            freshness_status=metrics.freshness_status,
            freshness_sla_days=metrics.freshness_sla_days,
            freshness_age_days=metrics.freshness_age_days,
            recent_failed_job_count=metrics.recent_failed_job_count,
            stale_running_job_count=metrics.stale_running_job_count,
            validation_error_count=metrics.validation_error_count,
            validation_regression=metrics.validation_regression,
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
        if dataset.latest_import_status == "publish_blocked":
            return "publish_blocked"
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

    def _build_operational_readiness(
        self,
        *,
        readiness: CountyTaxYearReadiness,
        dataset_models: list[AdminCountyYearDatasetReadiness],
        readiness_score: int,
    ) -> AdminCountyYearOperationalReadiness:
        latest_activity_at = max(
            (dataset.latest_activity_at for dataset in dataset_models if dataset.latest_activity_at is not None),
            default=None,
        )
        worst_dataset = max(dataset_models, key=self._freshness_rank, default=None)
        recent_failed_job_count = sum(dataset.recent_failed_job_count for dataset in dataset_models)
        stale_running_job_count = sum(dataset.stale_running_job_count for dataset in dataset_models)
        validation_error_count = sum(dataset.validation_error_count for dataset in dataset_models)
        validation_regression_count = sum(1 for dataset in dataset_models if dataset.validation_regression)
        property_roll_published = any(
            dataset.dataset_type == "property_roll" and dataset.latest_publish_state == "published"
            for dataset in dataset_models
        )
        searchable_ready = (
            property_roll_published
            and readiness.derived.parcel_summary_ready
            and readiness.derived.search_support_ready
        )
        alerts = self._collect_operational_alerts(readiness, dataset_models, searchable_ready=searchable_ready)

        quality_score = readiness_score
        if worst_dataset is not None and worst_dataset.freshness_status == "warning":
            quality_score -= 5
        if worst_dataset is not None and worst_dataset.freshness_status in {"stale", "missing"}:
            quality_score -= 15
        if recent_failed_job_count > 0:
            quality_score -= min(recent_failed_job_count * 10, 20)
        if stale_running_job_count > 0:
            quality_score -= min(stale_running_job_count * 10, 20)
        if validation_error_count > 0:
            quality_score -= 10
        if validation_regression_count > 0:
            quality_score -= min(validation_regression_count * 10, 20)
        if not searchable_ready:
            quality_score -= 10
        quality_score = max(min(quality_score, 100), 0)

        if quality_score >= 85:
            quality_status = "healthy"
        elif quality_score >= 60:
            quality_status = "warning"
        else:
            quality_status = "critical"

        return AdminCountyYearOperationalReadiness(
            quality_score=quality_score,
            quality_status=quality_status,
            freshness_status=worst_dataset.freshness_status if worst_dataset else "unknown",
            freshness_sla_days=worst_dataset.freshness_sla_days if worst_dataset else None,
            freshness_age_days=worst_dataset.freshness_age_days if worst_dataset else None,
            latest_activity_at=latest_activity_at,
            recent_failed_job_count=recent_failed_job_count,
            stale_running_job_count=stale_running_job_count,
            validation_error_count=validation_error_count,
            validation_regression_count=validation_regression_count,
            searchable_ready=searchable_ready,
            alerts=alerts,
        )

    def _build_kpi_summary(
        self,
        rows: list[AdminCountyYearReadiness],
    ) -> AdminCountyYearReadinessKpiSummary:
        return AdminCountyYearReadinessKpiSummary(
            total_year_count=len(rows),
            healthy_year_count=sum(1 for row in rows if row.operational.quality_status == "healthy"),
            warning_year_count=sum(1 for row in rows if row.operational.quality_status == "warning"),
            critical_year_count=sum(1 for row in rows if row.operational.quality_status == "critical"),
            stale_year_count=sum(1 for row in rows if row.operational.freshness_status == "stale"),
            searchable_year_count=sum(1 for row in rows if row.operational.searchable_ready),
            failed_job_count=sum(row.operational.recent_failed_job_count for row in rows),
            validation_regression_count=sum(
                row.operational.validation_regression_count for row in rows
            ),
        )

    def _freshness_rank(self, dataset: AdminCountyYearDatasetReadiness) -> tuple[int, int]:
        rank = {
            "fresh": 0,
            "warning": 1,
            "stale": 2,
            "missing": 3,
            "unknown": 4,
        }.get(dataset.freshness_status, 5)
        return (rank, -(dataset.freshness_age_days or 0))

    def _collect_operational_alerts(
        self,
        readiness: CountyTaxYearReadiness,
        dataset_models: list[AdminCountyYearDatasetReadiness],
        *,
        searchable_ready: bool,
    ) -> list[str]:
        alerts: list[str] = []
        for dataset in dataset_models:
            if dataset.freshness_status == "stale":
                alerts.append(f"{dataset.dataset_type}_stale_data")
            elif dataset.freshness_status == "warning":
                alerts.append(f"{dataset.dataset_type}_freshness_warning")
            if dataset.recent_failed_job_count > 0:
                alerts.append(f"{dataset.dataset_type}_job_failures")
            if dataset.stale_running_job_count > 0:
                alerts.append(f"{dataset.dataset_type}_stale_jobs")
            if dataset.validation_error_count > 0:
                alerts.append(f"{dataset.dataset_type}_validation_errors")
            if dataset.validation_regression:
                alerts.append(f"{dataset.dataset_type}_validation_regression")
        if not readiness.derived.parcel_summary_ready:
            alerts.append("parcel_summary_not_ready")
        if not readiness.derived.search_support_ready:
            alerts.append("search_read_model_not_ready")
        if readiness.derived.instant_quote_refresh_status not in {None, "completed"}:
            alerts.append("instant_quote_refresh_incomplete")
        if (readiness.derived.instant_quote_cache_view_row_delta or 0) != 0:
            alerts.append("instant_quote_cache_mismatch")
        if readiness.derived.instant_quote_subject_ready and not readiness.derived.instant_quote_ready:
            alerts.append("instant_quote_support_too_thin")
        if not searchable_ready:
            alerts.append("ingestion_to_searchable_incomplete")
        return list(dict.fromkeys(alerts))

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
        if readiness.derived.instant_quote_refresh_status not in {None, "completed"}:
            blockers.append("instant_quote_refresh_incomplete")
        if (readiness.derived.instant_quote_cache_view_row_delta or 0) != 0:
            blockers.append("instant_quote_cache_mismatch")
        if readiness.derived.instant_quote_subject_ready and not readiness.derived.instant_quote_ready:
            blockers.append("instant_quote_public_support_thin")
        return list(dict.fromkeys(blockers))
