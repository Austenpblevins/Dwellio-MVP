from __future__ import annotations

from dataclasses import dataclass

from app.db.connection import get_connection


class SchemaReadinessError(RuntimeError):
    """Raised when required schema dependencies are missing for a job."""


@dataclass(frozen=True)
class SchemaCatalog:
    tables: frozenset[str]
    views: frozenset[str]
    columns: frozenset[tuple[str, str]]


@dataclass(frozen=True)
class SchemaReadinessSpec:
    job_name: str
    required_tables: tuple[str, ...] = ()
    required_views: tuple[str, ...] = ()
    required_columns: tuple[tuple[str, str], ...] = ()
    require_tax_year_row: bool = False
    require_tax_year_valuation_date: bool = False


MIGRATION_HINTS: dict[str, str] = {
    "table:tax_years": "Apply the migration chain through 0004_tax_years before running this job.",
    "column:tax_years.valuation_date": (
        "Apply migration 0037_tax_years_valuation_date_alignment to add tax_years.valuation_date."
    ),
    "view:parcel_summary_view": (
        "Apply migration 0035_stage10_parcel_summary_views before running this job."
    ),
    "view:parcel_year_trend_view": (
        "Apply migration 0039_historical_validation_yoy_trends before running this job."
    ),
    "view:neighborhood_year_trend_view": (
        "Apply migration 0039_historical_validation_yoy_trends before running this job."
    ),
    "view:v_quote_read_model": "Apply migration 0025_views_quote_read_model before running this job.",
    "table:instant_quote_subject_cache": (
        "Apply migration 0045_stage17_instant_quote_serving_cache before running this job."
    ),
    "table:instant_quote_refresh_runs": (
        "Apply migration 0046_stage17_instant_quote_refresh_runs before running this job."
    ),
    "table:instant_quote_tax_rate_adoption_statuses": (
        "Apply migration 0050_stage17_tax_rate_adoption_status_admin_truth before running this job."
    ),
    "column:instant_quote_subject_cache.effective_tax_rate_basis_year": (
        "Apply migration 0049_stage17_dynamic_tax_rate_basis before running this job."
    ),
    "column:instant_quote_subject_cache.effective_tax_rate_basis_reason": (
        "Apply migration 0049_stage17_dynamic_tax_rate_basis before running this job."
    ),
    "column:instant_quote_subject_cache.effective_tax_rate_basis_fallback_applied": (
        "Apply migration 0049_stage17_dynamic_tax_rate_basis before running this job."
    ),
    "column:instant_quote_refresh_runs.tax_rate_basis_year": (
        "Apply migration 0049_stage17_dynamic_tax_rate_basis before running this job."
    ),
    "column:instant_quote_refresh_runs.tax_rate_basis_reason": (
        "Apply migration 0049_stage17_dynamic_tax_rate_basis before running this job."
    ),
    "column:instant_quote_refresh_runs.tax_rate_basis_fallback_applied": (
        "Apply migration 0049_stage17_dynamic_tax_rate_basis before running this job."
    ),
    "column:instant_quote_refresh_runs.requested_tax_rate_supportable_subject_row_count": (
        "Apply migration 0049_stage17_dynamic_tax_rate_basis before running this job."
    ),
    "column:instant_quote_refresh_runs.tax_rate_basis_supportable_subject_row_count": (
        "Apply migration 0049_stage17_dynamic_tax_rate_basis before running this job."
    ),
    "column:instant_quote_subject_cache.effective_tax_rate_basis_status": (
        "Apply migration 0050_stage17_tax_rate_adoption_status_admin_truth before running this job."
    ),
    "column:instant_quote_subject_cache.effective_tax_rate_basis_status_reason": (
        "Apply migration 0050_stage17_tax_rate_adoption_status_admin_truth before running this job."
    ),
    "column:instant_quote_refresh_runs.tax_rate_basis_status": (
        "Apply migration 0050_stage17_tax_rate_adoption_status_admin_truth before running this job."
    ),
    "column:instant_quote_refresh_runs.tax_rate_basis_status_reason": (
        "Apply migration 0050_stage17_tax_rate_adoption_status_admin_truth before running this job."
    ),
    "table:parcel_features": "Apply migration 0019_features_comps before running this job.",
    "table:comp_candidate_pools": "Apply migration 0019_features_comps before running this job.",
    "table:comp_candidates": "Apply migration 0019_features_comps before running this job.",
    "table:valuation_runs": "Apply migration 0020_valuation_quote before running this job.",
    "table:parcel_savings_estimates": (
        "Apply migration 0020_valuation_quote before running this job."
    ),
    "table:quote_explanations": "Apply migration 0020_valuation_quote before running this job.",
    "table:protest_recommendations": "Apply migration 0020_valuation_quote before running this job.",
    "table:decision_tree_results": "Apply migration 0020_valuation_quote before running this job.",
    "table:instant_quote_neighborhood_stats": (
        "Apply migration 0044_stage17_instant_quote_service before running this job."
    ),
    "table:instant_quote_segment_stats": (
        "Apply migration 0044_stage17_instant_quote_service before running this job."
    ),
}

JOB_READINESS_SPECS: dict[str, SchemaReadinessSpec] = {
    "job_features": SchemaReadinessSpec(
        job_name="job_features",
        required_tables=("tax_years", "parcel_features"),
        required_views=("parcel_summary_view", "parcel_year_trend_view", "neighborhood_year_trend_view"),
        required_columns=(("tax_years", "valuation_date"),),
        require_tax_year_valuation_date=True,
    ),
    "job_comp_candidates": SchemaReadinessSpec(
        job_name="job_comp_candidates",
        required_tables=(
            "tax_years",
            "parcel_features",
            "comp_candidate_pools",
            "comp_candidates",
        ),
        required_views=("parcel_summary_view",),
        required_columns=(("tax_years", "valuation_date"),),
        require_tax_year_valuation_date=True,
    ),
    "job_score_models": SchemaReadinessSpec(
        job_name="job_score_models",
        required_tables=(
            "tax_years",
            "valuation_runs",
            "decision_tree_results",
            "quote_explanations",
            "protest_recommendations",
        ),
        required_views=("parcel_summary_view",),
        required_columns=(("tax_years", "valuation_date"),),
        require_tax_year_valuation_date=True,
    ),
    "job_score_savings": SchemaReadinessSpec(
        job_name="job_score_savings",
        required_tables=("tax_years", "valuation_runs", "parcel_savings_estimates"),
        required_views=("parcel_summary_view",),
        required_columns=(("tax_years", "valuation_date"),),
        require_tax_year_valuation_date=True,
    ),
    "job_refresh_quote_cache": SchemaReadinessSpec(
        job_name="job_refresh_quote_cache",
        required_tables=(
            "tax_years",
            "valuation_runs",
            "parcel_savings_estimates",
            "quote_explanations",
            "protest_recommendations",
        ),
        required_views=("parcel_summary_view", "v_quote_read_model"),
        required_columns=(("tax_years", "valuation_date"),),
        require_tax_year_valuation_date=True,
    ),
    "job_refresh_instant_quote": SchemaReadinessSpec(
        job_name="job_refresh_instant_quote",
        required_tables=(
            "tax_years",
            "instant_quote_subject_cache",
            "instant_quote_neighborhood_stats",
            "instant_quote_segment_stats",
            "instant_quote_refresh_runs",
            "instant_quote_tax_rate_adoption_statuses",
        ),
        required_columns=(
            ("tax_years", "valuation_date"),
            ("instant_quote_subject_cache", "effective_tax_rate_basis_year"),
            ("instant_quote_subject_cache", "effective_tax_rate_basis_reason"),
            ("instant_quote_subject_cache", "effective_tax_rate_basis_fallback_applied"),
            ("instant_quote_subject_cache", "effective_tax_rate_basis_status"),
            ("instant_quote_subject_cache", "effective_tax_rate_basis_status_reason"),
            ("instant_quote_refresh_runs", "tax_rate_basis_year"),
            ("instant_quote_refresh_runs", "tax_rate_basis_reason"),
            ("instant_quote_refresh_runs", "tax_rate_basis_fallback_applied"),
            ("instant_quote_refresh_runs", "tax_rate_basis_status"),
            ("instant_quote_refresh_runs", "tax_rate_basis_status_reason"),
            ("instant_quote_refresh_runs", "requested_tax_rate_supportable_subject_row_count"),
            ("instant_quote_refresh_runs", "tax_rate_basis_supportable_subject_row_count"),
        ),
        require_tax_year_valuation_date=True,
    ),
    "job_validate_instant_quote": SchemaReadinessSpec(
        job_name="job_validate_instant_quote",
        required_tables=(
            "tax_years",
            "instant_quote_subject_cache",
            "instant_quote_neighborhood_stats",
            "instant_quote_segment_stats",
            "instant_quote_refresh_runs",
            "instant_quote_tax_rate_adoption_statuses",
        ),
        required_columns=(
            ("tax_years", "valuation_date"),
            ("instant_quote_subject_cache", "effective_tax_rate_basis_year"),
            ("instant_quote_subject_cache", "effective_tax_rate_basis_reason"),
            ("instant_quote_subject_cache", "effective_tax_rate_basis_fallback_applied"),
            ("instant_quote_subject_cache", "effective_tax_rate_basis_status"),
            ("instant_quote_subject_cache", "effective_tax_rate_basis_status_reason"),
            ("instant_quote_refresh_runs", "tax_rate_basis_year"),
            ("instant_quote_refresh_runs", "tax_rate_basis_reason"),
            ("instant_quote_refresh_runs", "tax_rate_basis_fallback_applied"),
            ("instant_quote_refresh_runs", "tax_rate_basis_status"),
            ("instant_quote_refresh_runs", "tax_rate_basis_status_reason"),
            ("instant_quote_refresh_runs", "requested_tax_rate_supportable_subject_row_count"),
            ("instant_quote_refresh_runs", "tax_rate_basis_supportable_subject_row_count"),
        ),
        require_tax_year_valuation_date=True,
    ),
}


def load_schema_catalog(connection: object) -> SchemaCatalog:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
            """
        )
        tables = frozenset(row["table_name"] for row in cursor.fetchall())

        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema = 'public'
            """
        )
        views = frozenset(row["table_name"] for row in cursor.fetchall())

        cursor.execute(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
            """
        )
        columns = frozenset((row["table_name"], row["column_name"]) for row in cursor.fetchall())

    return SchemaCatalog(tables=tables, views=views, columns=columns)


def fetch_tax_year_state(
    connection: object,
    *,
    tax_year: int,
    has_valuation_date_column: bool,
) -> dict[str, object] | None:
    select_columns = ["tax_year"]
    if has_valuation_date_column:
        select_columns.append("valuation_date")

    sql = f"SELECT {', '.join(select_columns)} FROM tax_years WHERE tax_year = %s"
    with connection.cursor() as cursor:
        cursor.execute(sql, (tax_year,))
        return cursor.fetchone()


def validate_schema_readiness(
    *,
    catalog: SchemaCatalog,
    spec: SchemaReadinessSpec,
    tax_year: int | None = None,
    tax_year_state: dict[str, object] | None = None,
) -> list[str]:
    issues: list[str] = []

    for table_name in spec.required_tables:
        if table_name not in catalog.tables:
            issues.append(_format_issue(f"Missing table public.{table_name}.", f"table:{table_name}"))

    for view_name in spec.required_views:
        if view_name not in catalog.views:
            issues.append(_format_issue(f"Missing view public.{view_name}.", f"view:{view_name}"))

    for table_name, column_name in spec.required_columns:
        if (table_name, column_name) not in catalog.columns:
            issues.append(
                _format_issue(
                    f"Missing column public.{table_name}.{column_name}.",
                    f"column:{table_name}.{column_name}",
                )
            )

    if spec.require_tax_year_row and tax_year is None:
        issues.append("A tax_year value is required for this job readiness check.")

    if tax_year is not None and "tax_years" in catalog.tables:
        if tax_year_state is None:
            issues.append(
                f"tax_years row for tax_year={tax_year} could not be inspected. Verify database connectivity and rerun."
            )
        else:
            if tax_year_state.get("tax_year") is None:
                issues.append(
                    f"Missing tax_years row for tax_year={tax_year}. Seed or insert that tax year before running this job."
                )
            elif spec.require_tax_year_valuation_date:
                if ("tax_years", "valuation_date") not in catalog.columns:
                    issues.append(
                        _format_issue(
                            "Missing column public.tax_years.valuation_date.",
                            "column:tax_years.valuation_date",
                        )
                    )
                elif tax_year_state.get("valuation_date") is None:
                    issues.append(
                        f"tax_years.valuation_date is NULL for tax_year={tax_year}. Backfill tax_years.valuation_date before running this job."
                    )

    return issues


def assert_job_schema_ready(job_name: str, *, tax_year: int | None = None) -> None:
    spec = JOB_READINESS_SPECS.get(job_name)
    if spec is None:
        raise ValueError(f"Unsupported job readiness spec: {job_name}")

    with get_connection() as connection:
        catalog = load_schema_catalog(connection)
        tax_year_state = None
        if tax_year is not None and "tax_years" in catalog.tables:
            tax_year_state = fetch_tax_year_state(
                connection,
                tax_year=tax_year,
                has_valuation_date_column=("tax_years", "valuation_date") in catalog.columns,
            )

    issues = validate_schema_readiness(
        catalog=catalog,
        spec=spec,
        tax_year=tax_year,
        tax_year_state=tax_year_state,
    )
    if issues:
        raise SchemaReadinessError(
            f"{job_name} readiness check failed:\n- " + "\n- ".join(issues)
        )


def _format_issue(issue: str, hint_key: str) -> str:
    hint = MIGRATION_HINTS.get(hint_key)
    if hint is None:
        return issue
    return f"{issue} {hint}"
