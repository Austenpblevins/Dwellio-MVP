from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from app.county_adapters.common.config_loader import (
    load_county_adapter_config,
    resolve_dataset_year_support,
)
from app.db.connection import get_connection
from app.services.instant_quote_tax_rate_basis import (
    INSTANT_QUOTE_TAX_RATE_BASIS_MIN_SUPPORTABLE_SUBJECTS,
)

INSTANT_QUOTE_PUBLIC_SUPPORT_MIN_COUNT = INSTANT_QUOTE_TAX_RATE_BASIS_MIN_SUPPORTABLE_SUBJECTS


@dataclass(frozen=True)
class DatasetYearReadiness:
    county_id: str
    tax_year: int
    dataset_type: str
    source_system_code: str
    access_method: str
    availability_status: str
    availability_notes: list[str] = field(default_factory=list)
    tax_year_known: bool = False
    raw_file_count: int = 0
    latest_import_batch_id: str | None = None
    latest_import_status: str | None = None
    latest_status_reason: str | None = None
    latest_publish_state: str | None = None
    staged: bool = False
    canonical_published: bool = False


@dataclass(frozen=True)
class TaxYearDerivedReadiness:
    parcel_summary_ready: bool
    parcel_year_trend_ready: bool
    neighborhood_stats_ready: bool
    neighborhood_year_trend_ready: bool
    instant_quote_subject_ready: bool = False
    instant_quote_neighborhood_stats_ready: bool = False
    instant_quote_segment_stats_ready: bool = False
    instant_quote_asset_ready: bool = False
    instant_quote_ready: bool = False
    instant_quote_refresh_status: str | None = None
    instant_quote_last_refresh_at: datetime | None = None
    instant_quote_last_validated_at: datetime | None = None
    instant_quote_cache_view_row_delta: int | None = None
    instant_quote_tax_rate_basis_year: int | None = None
    instant_quote_tax_rate_basis_reason: str | None = None
    instant_quote_tax_rate_basis_fallback_applied: bool = False
    instant_quote_tax_rate_requested_year_supportable_subject_row_count: int = 0
    instant_quote_tax_rate_basis_supportable_subject_row_count: int = 0
    instant_quote_tax_rate_quoteable_subject_row_count: int = 0
    instant_quote_tax_rate_requested_year_effective_tax_rate_coverage_ratio: float = 0.0
    instant_quote_tax_rate_requested_year_assignment_coverage_ratio: float = 0.0
    instant_quote_tax_rate_basis_effective_tax_rate_coverage_ratio: float = 0.0
    instant_quote_tax_rate_basis_assignment_coverage_ratio: float = 0.0
    instant_quote_tax_rate_basis_continuity_parcel_match_row_count: int = 0
    instant_quote_tax_rate_basis_continuity_parcel_gap_row_count: int = 0
    instant_quote_tax_rate_basis_continuity_parcel_match_ratio: float = 0.0
    instant_quote_tax_rate_basis_continuity_account_number_match_row_count: int = 0
    instant_quote_tax_rate_basis_warning_codes: list[str] = field(default_factory=list)
    instant_quote_supported_public_quote_exists: bool = False
    instant_quote_subject_rows_without_usable_neighborhood_stats: int = 0
    instant_quote_subject_rows_without_usable_segment_stats: int = 0
    instant_quote_subject_rows_missing_segment_row: int = 0
    instant_quote_subject_rows_thin_segment_support: int = 0
    instant_quote_subject_rows_unusable_segment_basis: int = 0
    instant_quote_served_neighborhood_only_quote_count: int = 0
    instant_quote_served_supported_neighborhood_only_quote_count: int = 0
    instant_quote_served_unsupported_neighborhood_only_quote_count: int = 0
    search_support_ready: bool = False
    feature_ready: bool = False
    comp_ready: bool = False
    valuation_ready: bool = False
    savings_ready: bool = False
    decision_tree_ready: bool = False
    explanation_ready: bool = False
    recommendation_ready: bool = False
    quote_ready: bool = False
    parcel_summary_row_count: int = 0
    parcel_year_trend_row_count: int = 0
    neighborhood_stats_row_count: int = 0
    neighborhood_year_trend_row_count: int = 0
    instant_quote_subject_row_count: int = 0
    instant_quote_neighborhood_stats_row_count: int = 0
    instant_quote_segment_stats_row_count: int = 0
    instant_quote_supportable_row_count: int = 0
    instant_quote_supported_neighborhood_stats_row_count: int = 0
    instant_quote_supported_segment_stats_row_count: int = 0
    search_document_row_count: int = 0
    parcel_feature_row_count: int = 0
    comp_pool_row_count: int = 0
    valuation_run_row_count: int = 0
    savings_row_count: int = 0
    decision_tree_row_count: int = 0
    explanation_row_count: int = 0
    recommendation_row_count: int = 0
    quote_row_count: int = 0


@dataclass(frozen=True)
class CountyTaxYearReadiness:
    county_id: str
    tax_year: int
    tax_year_known: bool
    datasets: list[DatasetYearReadiness]
    derived: TaxYearDerivedReadiness


class DataReadinessService:
    def build_tax_year_readiness(self, *, county_id: str, tax_year: int) -> CountyTaxYearReadiness:
        config = load_county_adapter_config(county_id)
        with get_connection() as connection:
            self._prepare_readiness_session(connection)
            tax_year_known = self._tax_year_exists(connection, tax_year=tax_year)
            datasets = [
                self._build_dataset_readiness(
                    connection,
                    county_id=county_id,
                    tax_year=tax_year,
                    dataset_type=dataset_type,
                    config=config,
                    tax_year_known=tax_year_known,
                )
                for dataset_type in config.datasets
            ]
            derived = self._build_derived_readiness(
                connection,
                county_id=county_id,
                tax_year=tax_year,
            )

        return CountyTaxYearReadiness(
            county_id=county_id,
            tax_year=tax_year,
            tax_year_known=tax_year_known,
            datasets=datasets,
            derived=derived,
        )

    def _prepare_readiness_session(self, connection: object) -> None:
        # Readiness/reporting queries hit wide derived views; disabling parallel gather
        # keeps local and constrained Postgres instances from exhausting shared memory.
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL max_parallel_workers_per_gather = 0")

    def _build_dataset_readiness(
        self,
        connection: object,
        *,
        county_id: str,
        tax_year: int,
        dataset_type: str,
        config,
        tax_year_known: bool,
    ) -> DatasetYearReadiness:
        year_support = resolve_dataset_year_support(
            config=config,
            dataset_type=dataset_type,
            tax_year=tax_year,
        )
        latest_batch = self._latest_import_batch(
            connection,
            county_id=county_id,
            tax_year=tax_year,
            dataset_type=dataset_type,
        )
        raw_file_count = self._raw_file_count(
            connection,
            county_id=county_id,
            tax_year=tax_year,
            dataset_type=dataset_type,
        )
        latest_status = latest_batch["status"] if latest_batch is not None else None
        latest_status_reason = latest_batch["status_reason"] if latest_batch is not None else None
        latest_publish_state = latest_batch["publish_state"] if latest_batch is not None else None
        return DatasetYearReadiness(
            county_id=county_id,
            tax_year=tax_year,
            dataset_type=dataset_type,
            source_system_code=year_support.source_system_code,
            access_method=year_support.access_method,
            availability_status=year_support.availability_status,
            availability_notes=list(year_support.availability_notes),
            tax_year_known=tax_year_known,
            raw_file_count=raw_file_count,
            latest_import_batch_id=(
                str(latest_batch["import_batch_id"]) if latest_batch is not None else None
            ),
            latest_import_status=latest_status,
            latest_status_reason=latest_status_reason,
            latest_publish_state=latest_publish_state,
            staged=latest_status in {"staged", "normalized"},
            canonical_published=latest_publish_state == "published",
        )

    def _build_derived_readiness(
        self,
        connection: object,
        *,
        county_id: str,
        tax_year: int,
    ) -> TaxYearDerivedReadiness:
        parcel_summary_exists = self._view_exists(connection, "parcel_summary_view")
        parcel_year_trend_exists = self._view_exists(connection, "parcel_year_trend_view")
        neighborhood_stats_exists = self._table_exists(connection, "neighborhood_stats")
        neighborhood_year_trend_exists = self._view_exists(connection, "neighborhood_year_trend_view")
        instant_quote_subject_exists = self._table_exists(connection, "instant_quote_subject_cache")
        instant_quote_neighborhood_stats_exists = self._table_exists(
            connection, "instant_quote_neighborhood_stats"
        )
        instant_quote_segment_stats_exists = self._table_exists(
            connection, "instant_quote_segment_stats"
        )
        instant_quote_refresh_runs_exists = self._table_exists(
            connection, "instant_quote_refresh_runs"
        )
        search_documents_exists = self._table_exists(connection, "search_documents")
        parcel_features_exists = self._table_exists(connection, "parcel_features")
        comp_pools_exists = self._table_exists(connection, "comp_candidate_pools")
        valuation_runs_exists = self._table_exists(connection, "valuation_runs")
        savings_exists = self._table_exists(connection, "parcel_savings_estimates")
        decision_tree_exists = self._table_exists(connection, "decision_tree_results")
        explanations_exists = self._table_exists(connection, "quote_explanations")
        recommendations_exists = self._table_exists(connection, "protest_recommendations")
        quote_view_exists = self._view_exists(connection, "v_quote_read_model")

        parcel_summary_row_count = (
            self._count_rows(
                connection,
                """
                SELECT COUNT(*) AS count
                FROM parcel_year_snapshots
                WHERE county_id = %s
                  AND tax_year = %s
                  AND is_current = true
                """,
                (county_id, tax_year),
            )
            if parcel_summary_exists
            else 0
        )
        # parcel_year_trend_view preserves one row per current parcel-year summary row,
        # so we can safely reuse the scoped current-snapshot count instead of paying
        # for another wide derived-view scan on every readiness request.
        parcel_year_trend_row_count = (
            parcel_summary_row_count
            if parcel_year_trend_exists and parcel_summary_row_count > 0
            else 0
        )
        neighborhood_stats_row_count = (
            self._count_rows(
                connection,
                "SELECT COUNT(*) AS count FROM neighborhood_stats WHERE county_id = %s AND tax_year = %s",
                (county_id, tax_year),
            )
            if neighborhood_stats_exists
            else 0
        )
        neighborhood_year_trend_row_count = (
            self._count_rows(
                connection,
                "SELECT COUNT(*) AS count FROM neighborhood_year_trend_view WHERE county_id = %s AND tax_year = %s",
                (county_id, tax_year),
            )
            if neighborhood_year_trend_exists
            else 0
        )
        instant_quote_subject_row_count = (
            self._count_rows(
                connection,
                "SELECT COUNT(*) AS count FROM instant_quote_subject_cache WHERE county_id = %s AND tax_year = %s",
                (county_id, tax_year),
            )
            if instant_quote_subject_exists
            else 0
        )
        instant_quote_neighborhood_stats_row_count = (
            self._count_rows(
                connection,
                """
                SELECT COUNT(*) AS count
                FROM instant_quote_neighborhood_stats
                WHERE county_id = %s
                  AND tax_year = %s
                """,
                (county_id, tax_year),
            )
            if instant_quote_neighborhood_stats_exists
            else 0
        )
        instant_quote_segment_stats_row_count = (
            self._count_rows(
                connection,
                """
                SELECT COUNT(*) AS count
                FROM instant_quote_segment_stats
                WHERE county_id = %s
                  AND tax_year = %s
                """,
                (county_id, tax_year),
            )
            if instant_quote_segment_stats_exists
            else 0
        )
        instant_quote_supportable_row_count = (
            self._count_rows(
                connection,
                """
                SELECT COUNT(*) AS count
                FROM instant_quote_subject_cache
                WHERE county_id = %s
                  AND tax_year = %s
                  AND support_blocker_code IS NULL
                """,
                (county_id, tax_year),
            )
            if instant_quote_subject_exists
            else 0
        )
        instant_quote_supported_neighborhood_stats_row_count = (
            self._count_rows(
                connection,
                """
                SELECT COUNT(*) AS count
                FROM instant_quote_neighborhood_stats
                WHERE county_id = %s
                  AND tax_year = %s
                  AND support_threshold_met IS TRUE
                """,
                (county_id, tax_year),
            )
            if instant_quote_neighborhood_stats_exists
            else 0
        )
        instant_quote_supported_segment_stats_row_count = (
            self._count_rows(
                connection,
                """
                SELECT COUNT(*) AS count
                FROM instant_quote_segment_stats
                WHERE county_id = %s
                  AND tax_year = %s
                  AND support_threshold_met IS TRUE
                """,
                (county_id, tax_year),
            )
            if instant_quote_segment_stats_exists
            else 0
        )
        latest_instant_quote_refresh = (
            self._latest_instant_quote_refresh_run(
                connection,
                county_id=county_id,
                tax_year=tax_year,
            )
            if instant_quote_refresh_runs_exists
            else None
        )
        search_document_row_count = (
            self._count_rows(
                connection,
                "SELECT COUNT(*) AS count FROM search_documents WHERE county_id = %s AND tax_year = %s",
                (county_id, tax_year),
            )
            if search_documents_exists
            else 0
        )
        parcel_feature_row_count = (
            self._count_rows(
                connection,
                "SELECT COUNT(*) AS count FROM parcel_features pf JOIN parcels p ON p.parcel_id = pf.parcel_id WHERE p.county_id = %s AND pf.tax_year = %s",
                (county_id, tax_year),
            )
            if parcel_features_exists
            else 0
        )
        comp_pool_row_count = (
            self._count_rows(
                connection,
                "SELECT COUNT(*) AS count FROM comp_candidate_pools WHERE county_id = %s AND tax_year = %s",
                (county_id, tax_year),
            )
            if comp_pools_exists
            else 0
        )
        valuation_run_row_count = (
            self._count_rows(
                connection,
                "SELECT COUNT(*) AS count FROM valuation_runs WHERE county_id = %s AND tax_year = %s",
                (county_id, tax_year),
            )
            if valuation_runs_exists
            else 0
        )
        savings_row_count = (
            self._count_rows(
                connection,
                """
                SELECT COUNT(*) AS count
                FROM parcel_savings_estimates pse
                JOIN valuation_runs vr
                  ON vr.valuation_run_id = pse.valuation_run_id
                WHERE vr.county_id = %s
                  AND vr.tax_year = %s
                """,
                (county_id, tax_year),
            )
            if savings_exists and valuation_runs_exists
            else 0
        )
        decision_tree_row_count = (
            self._count_rows(
                connection,
                """
                SELECT COUNT(*) AS count
                FROM decision_tree_results dtr
                JOIN valuation_runs vr
                  ON vr.valuation_run_id = dtr.valuation_run_id
                WHERE vr.county_id = %s
                  AND vr.tax_year = %s
                """,
                (county_id, tax_year),
            )
            if decision_tree_exists and valuation_runs_exists
            else 0
        )
        explanation_row_count = (
            self._count_rows(
                connection,
                """
                SELECT COUNT(*) AS count
                FROM quote_explanations qe
                JOIN valuation_runs vr
                  ON vr.valuation_run_id = qe.valuation_run_id
                WHERE vr.county_id = %s
                  AND vr.tax_year = %s
                """,
                (county_id, tax_year),
            )
            if explanations_exists and valuation_runs_exists
            else 0
        )
        recommendation_row_count = (
            self._count_rows(
                connection,
                """
                SELECT COUNT(*) AS count
                FROM protest_recommendations pr
                JOIN valuation_runs vr
                  ON vr.valuation_run_id = pr.valuation_run_id
                WHERE vr.county_id = %s
                  AND vr.tax_year = %s
                """,
                (county_id, tax_year),
            )
            if recommendations_exists and valuation_runs_exists
            else 0
        )
        quote_row_count = (
            self._count_rows(
                connection,
                "SELECT COUNT(*) AS count FROM v_quote_read_model WHERE county_id = %s AND tax_year = %s",
                (county_id, tax_year),
            )
            if quote_view_exists
            else 0
        )

        instant_quote_asset_ready = (
            latest_instant_quote_refresh is not None
            and str(latest_instant_quote_refresh.get("refresh_status")) == "completed"
            and
            instant_quote_subject_row_count > 0
            and instant_quote_neighborhood_stats_row_count > 0
            and instant_quote_segment_stats_row_count > 0
            and int(latest_instant_quote_refresh.get("cache_view_row_delta") or 0) == 0
        )

        return TaxYearDerivedReadiness(
            parcel_summary_ready=parcel_summary_row_count > 0,
            parcel_year_trend_ready=parcel_year_trend_row_count > 0,
            neighborhood_stats_ready=neighborhood_stats_row_count > 0,
            neighborhood_year_trend_ready=neighborhood_year_trend_row_count > 0,
            instant_quote_subject_ready=instant_quote_subject_row_count > 0,
            instant_quote_neighborhood_stats_ready=instant_quote_neighborhood_stats_row_count > 0,
            instant_quote_segment_stats_ready=instant_quote_segment_stats_row_count > 0,
            instant_quote_asset_ready=instant_quote_asset_ready,
            instant_quote_ready=(
                instant_quote_asset_ready
                and
                instant_quote_supportable_row_count >= INSTANT_QUOTE_PUBLIC_SUPPORT_MIN_COUNT
                and instant_quote_supported_neighborhood_stats_row_count > 0
            ),
            instant_quote_refresh_status=(
                None
                if latest_instant_quote_refresh is None
                else str(latest_instant_quote_refresh.get("refresh_status"))
            ),
            instant_quote_last_refresh_at=(
                None
                if latest_instant_quote_refresh is None
                else latest_instant_quote_refresh.get("refresh_finished_at")
            ),
            instant_quote_last_validated_at=(
                None
                if latest_instant_quote_refresh is None
                else latest_instant_quote_refresh.get("validated_at")
            ),
            instant_quote_cache_view_row_delta=(
                None
                if latest_instant_quote_refresh is None
                else int(latest_instant_quote_refresh.get("cache_view_row_delta") or 0)
            ),
            instant_quote_tax_rate_basis_year=(
                None
                if latest_instant_quote_refresh is None
                else (
                    None
                    if latest_instant_quote_refresh.get("tax_rate_basis_year") is None
                    else int(latest_instant_quote_refresh.get("tax_rate_basis_year"))
                )
            ),
            instant_quote_tax_rate_basis_reason=(
                None
                if latest_instant_quote_refresh is None
                else (
                    None
                    if latest_instant_quote_refresh.get("tax_rate_basis_reason") is None
                    else str(latest_instant_quote_refresh.get("tax_rate_basis_reason"))
                )
            ),
            instant_quote_tax_rate_basis_fallback_applied=bool(
                latest_instant_quote_refresh
                and latest_instant_quote_refresh.get("tax_rate_basis_fallback_applied")
            ),
            instant_quote_tax_rate_requested_year_supportable_subject_row_count=int(
                (latest_instant_quote_refresh or {}).get(
                    "requested_tax_rate_supportable_subject_row_count"
                )
                or 0
            ),
            instant_quote_tax_rate_basis_supportable_subject_row_count=int(
                (latest_instant_quote_refresh or {}).get(
                    "tax_rate_basis_supportable_subject_row_count"
                )
                or 0
            ),
            instant_quote_tax_rate_quoteable_subject_row_count=int(
                (latest_instant_quote_refresh or {}).get("tax_rate_quoteable_subject_row_count")
                or 0
            ),
            instant_quote_tax_rate_requested_year_effective_tax_rate_coverage_ratio=float(
                (latest_instant_quote_refresh or {}).get(
                    "requested_tax_rate_effective_tax_rate_coverage_ratio"
                )
                or 0.0
            ),
            instant_quote_tax_rate_requested_year_assignment_coverage_ratio=float(
                (latest_instant_quote_refresh or {}).get(
                    "requested_tax_rate_assignment_coverage_ratio"
                )
                or 0.0
            ),
            instant_quote_tax_rate_basis_effective_tax_rate_coverage_ratio=float(
                (latest_instant_quote_refresh or {}).get(
                    "tax_rate_basis_effective_tax_rate_coverage_ratio"
                )
                or 0.0
            ),
            instant_quote_tax_rate_basis_assignment_coverage_ratio=float(
                (latest_instant_quote_refresh or {}).get(
                    "tax_rate_basis_assignment_coverage_ratio"
                )
                or 0.0
            ),
            instant_quote_tax_rate_basis_continuity_parcel_match_row_count=int(
                (latest_instant_quote_refresh or {}).get(
                    "tax_rate_basis_continuity_parcel_match_row_count"
                )
                or 0
            ),
            instant_quote_tax_rate_basis_continuity_parcel_gap_row_count=int(
                (latest_instant_quote_refresh or {}).get(
                    "tax_rate_basis_continuity_parcel_gap_row_count"
                )
                or 0
            ),
            instant_quote_tax_rate_basis_continuity_parcel_match_ratio=float(
                (latest_instant_quote_refresh or {}).get(
                    "tax_rate_basis_continuity_parcel_match_ratio"
                )
                or 0.0
            ),
            instant_quote_tax_rate_basis_continuity_account_number_match_row_count=int(
                (latest_instant_quote_refresh or {}).get(
                    "tax_rate_basis_continuity_account_number_match_row_count"
                )
                or 0
            ),
            instant_quote_tax_rate_basis_warning_codes=list(
                (latest_instant_quote_refresh or {}).get("tax_rate_basis_warning_codes") or []
            ),
            instant_quote_supported_public_quote_exists=bool(
                latest_instant_quote_refresh
                and (latest_instant_quote_refresh.get("validation_report") or {}).get(
                    "supported_public_quote_exists"
                )
            ),
            instant_quote_subject_rows_without_usable_neighborhood_stats=int(
                (
                    (latest_instant_quote_refresh or {}).get("validation_report") or {}
                ).get("subject_rows_without_usable_neighborhood_stats")
                or 0
            ),
            instant_quote_subject_rows_without_usable_segment_stats=int(
                (
                    (latest_instant_quote_refresh or {}).get("validation_report") or {}
                ).get("subject_rows_without_usable_segment_stats")
                or 0
            ),
            instant_quote_subject_rows_missing_segment_row=int(
                (
                    (latest_instant_quote_refresh or {}).get("validation_report") or {}
                ).get("subject_rows_missing_segment_row")
                or 0
            ),
            instant_quote_subject_rows_thin_segment_support=int(
                (
                    (latest_instant_quote_refresh or {}).get("validation_report") or {}
                ).get("subject_rows_thin_segment_support")
                or 0
            ),
            instant_quote_subject_rows_unusable_segment_basis=int(
                (
                    (latest_instant_quote_refresh or {}).get("validation_report") or {}
                ).get("subject_rows_unusable_segment_basis")
                or 0
            ),
            instant_quote_served_neighborhood_only_quote_count=int(
                (
                    (latest_instant_quote_refresh or {}).get("validation_report") or {}
                ).get("served_neighborhood_only_quote_count")
                or 0
            ),
            instant_quote_served_supported_neighborhood_only_quote_count=int(
                (
                    (latest_instant_quote_refresh or {}).get("validation_report") or {}
                ).get("served_supported_neighborhood_only_quote_count")
                or 0
            ),
            instant_quote_served_unsupported_neighborhood_only_quote_count=int(
                (
                    (latest_instant_quote_refresh or {}).get("validation_report") or {}
                ).get("served_unsupported_neighborhood_only_quote_count")
                or 0
            ),
            search_support_ready=search_document_row_count > 0,
            feature_ready=parcel_feature_row_count > 0,
            comp_ready=comp_pool_row_count > 0,
            valuation_ready=valuation_run_row_count > 0,
            savings_ready=savings_row_count > 0,
            decision_tree_ready=decision_tree_row_count > 0,
            explanation_ready=explanation_row_count > 0,
            recommendation_ready=recommendation_row_count > 0,
            quote_ready=quote_row_count > 0,
            parcel_summary_row_count=parcel_summary_row_count,
            parcel_year_trend_row_count=parcel_year_trend_row_count,
            neighborhood_stats_row_count=neighborhood_stats_row_count,
            neighborhood_year_trend_row_count=neighborhood_year_trend_row_count,
            instant_quote_subject_row_count=instant_quote_subject_row_count,
            instant_quote_neighborhood_stats_row_count=instant_quote_neighborhood_stats_row_count,
            instant_quote_segment_stats_row_count=instant_quote_segment_stats_row_count,
            instant_quote_supportable_row_count=instant_quote_supportable_row_count,
            instant_quote_supported_neighborhood_stats_row_count=(
                instant_quote_supported_neighborhood_stats_row_count
            ),
            instant_quote_supported_segment_stats_row_count=(
                instant_quote_supported_segment_stats_row_count
            ),
            search_document_row_count=search_document_row_count,
            parcel_feature_row_count=parcel_feature_row_count,
            comp_pool_row_count=comp_pool_row_count,
            valuation_run_row_count=valuation_run_row_count,
            savings_row_count=savings_row_count,
            decision_tree_row_count=decision_tree_row_count,
            explanation_row_count=explanation_row_count,
            recommendation_row_count=recommendation_row_count,
            quote_row_count=quote_row_count,
        )

    def _tax_year_exists(self, connection: object, *, tax_year: int) -> bool:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 AS present FROM tax_years WHERE tax_year = %s", (tax_year,))
            row = cursor.fetchone()
        return bool(row)

    def _raw_file_count(
        self,
        connection: object,
        *,
        county_id: str,
        tax_year: int,
        dataset_type: str,
    ) -> int:
        return self._count_rows(
            connection,
            "SELECT COUNT(*) AS count FROM raw_files WHERE county_id = %s AND tax_year = %s AND file_kind = %s",
            (county_id, tax_year, dataset_type),
        )

    def _latest_import_batch(
        self,
        connection: object,
        *,
        county_id: str,
        tax_year: int,
        dataset_type: str,
    ) -> dict[str, object] | None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  ib.import_batch_id,
                  ib.status,
                  ib.status_reason,
                  ib.publish_state
                FROM import_batches ib
                WHERE ib.county_id = %s
                  AND ib.tax_year = %s
                  AND ib.dataset_type = %s
                ORDER BY ib.created_at DESC, ib.import_batch_id DESC
                LIMIT 1
                """,
                (county_id, tax_year, dataset_type),
            )
            return cursor.fetchone()

    def _latest_instant_quote_refresh_run(
        self,
        connection: object,
        *,
        county_id: str,
        tax_year: int,
    ) -> dict[str, object] | None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  refresh_status,
                  refresh_finished_at,
                  validated_at,
                  cache_view_row_delta,
                  tax_rate_basis_year,
                  tax_rate_basis_reason,
                  tax_rate_basis_fallback_applied,
                  requested_tax_rate_supportable_subject_row_count,
                  tax_rate_basis_supportable_subject_row_count,
                  tax_rate_quoteable_subject_row_count,
                  requested_tax_rate_effective_tax_rate_coverage_ratio,
                  requested_tax_rate_assignment_coverage_ratio,
                  tax_rate_basis_effective_tax_rate_coverage_ratio,
                  tax_rate_basis_assignment_coverage_ratio,
                  tax_rate_basis_continuity_parcel_match_row_count,
                  tax_rate_basis_continuity_parcel_gap_row_count,
                  tax_rate_basis_continuity_parcel_match_ratio,
                  tax_rate_basis_continuity_account_number_match_row_count,
                  tax_rate_basis_warning_codes,
                  validation_report
                FROM instant_quote_refresh_runs
                WHERE county_id = %s
                  AND tax_year = %s
                ORDER BY refresh_started_at DESC
                LIMIT 1
                """,
                (county_id, tax_year),
            )
            return cursor.fetchone()

    def _table_exists(self, connection: object, table_name: str) -> bool:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXISTS (
                  SELECT 1
                  FROM information_schema.tables
                  WHERE table_schema = 'public'
                    AND table_name = %s
                ) AS present
                """,
                (table_name,),
            )
            row = cursor.fetchone()
        return bool(row and row["present"])

    def _view_exists(self, connection: object, view_name: str) -> bool:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXISTS (
                  SELECT 1
                  FROM information_schema.views
                  WHERE table_schema = 'public'
                    AND table_name = %s
                ) AS present
                """,
                (view_name,),
            )
            row = cursor.fetchone()
        return bool(row and row["present"])

    def _count_rows(
        self,
        connection: object,
        sql: str,
        params: tuple[object, ...],
    ) -> int:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            row = cursor.fetchone()
        return int(row["count"] if row is not None else 0)
