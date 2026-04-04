from __future__ import annotations

import pytest

from app.services.schema_readiness import (
    JOB_READINESS_SPECS,
    SchemaCatalog,
    SchemaReadinessError,
    SchemaReadinessSpec,
    assert_job_schema_ready,
    validate_schema_readiness,
)


def test_validate_schema_readiness_reports_missing_stage10_view_and_valuation_date_column() -> None:
    catalog = SchemaCatalog(
        tables=frozenset({"tax_years", "parcel_features"}),
        views=frozenset(),
        columns=frozenset({("tax_years", "tax_year")}),
    )
    spec = JOB_READINESS_SPECS["job_features"]

    issues = validate_schema_readiness(
        catalog=catalog,
        spec=spec,
        tax_year=2026,
        tax_year_state={"tax_year": 2026},
    )

    assert any("Missing view public.parcel_summary_view." in issue for issue in issues)
    assert any("0035_stage10_parcel_summary_views" in issue for issue in issues)
    assert any("Missing view public.parcel_year_trend_view." in issue for issue in issues)
    assert any("0039_historical_validation_yoy_trends" in issue for issue in issues)
    assert any("Missing column public.tax_years.valuation_date." in issue for issue in issues)
    assert any("0037_tax_years_valuation_date_alignment" in issue for issue in issues)


def test_validate_schema_readiness_requires_non_null_valuation_date_for_selected_tax_year() -> None:
    catalog = SchemaCatalog(
        tables=frozenset({"tax_years", "parcel_features"}),
        views=frozenset(
            {"parcel_summary_view", "parcel_year_trend_view", "neighborhood_year_trend_view"}
        ),
        columns=frozenset(
            {
                ("tax_years", "tax_year"),
                ("tax_years", "valuation_date"),
            }
        ),
    )
    spec = JOB_READINESS_SPECS["job_features"]

    issues = validate_schema_readiness(
        catalog=catalog,
        spec=spec,
        tax_year=2026,
        tax_year_state={"tax_year": 2026, "valuation_date": None},
    )

    assert issues == [
        "tax_years.valuation_date is NULL for tax_year=2026. Backfill tax_years.valuation_date before running this job."
    ]


def test_validate_schema_readiness_passes_when_required_objects_and_tax_year_state_exist() -> None:
    catalog = SchemaCatalog(
        tables=frozenset({"tax_years", "parcel_features"}),
        views=frozenset(
            {"parcel_summary_view", "parcel_year_trend_view", "neighborhood_year_trend_view"}
        ),
        columns=frozenset(
            {
                ("tax_years", "tax_year"),
                ("tax_years", "valuation_date"),
            }
        ),
    )
    spec = SchemaReadinessSpec(
        job_name="test_job",
        required_tables=("tax_years", "parcel_features"),
        required_views=("parcel_summary_view", "parcel_year_trend_view"),
        required_columns=(("tax_years", "valuation_date"),),
        require_tax_year_valuation_date=True,
    )

    issues = validate_schema_readiness(
        catalog=catalog,
        spec=spec,
        tax_year=2026,
        tax_year_state={"tax_year": 2026, "valuation_date": "2026-01-01"},
    )

    assert issues == []


def test_instant_quote_schema_readiness_requires_hardening_columns() -> None:
    spec = JOB_READINESS_SPECS["job_refresh_instant_quote"]

    assert ("instant_quote_refresh_runs", "tax_rate_quoteable_subject_row_count") in spec.required_columns
    assert (
        "instant_quote_refresh_runs",
        "requested_tax_rate_effective_tax_rate_coverage_ratio",
    ) in spec.required_columns
    assert (
        "instant_quote_refresh_runs",
        "tax_rate_basis_continuity_parcel_match_ratio",
    ) in spec.required_columns
    assert ("instant_quote_refresh_runs", "tax_rate_basis_warning_codes") in spec.required_columns


def test_assert_job_schema_ready_raises_actionable_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Cursor:
        def __init__(self) -> None:
            self._rows: list[dict[str, object]] = []

        def __enter__(self) -> _Cursor:
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
            if "information_schema.tables" in sql:
                self._rows = [{"table_name": "tax_years"}, {"table_name": "parcel_features"}]
            elif "information_schema.views" in sql:
                self._rows = []
            elif "information_schema.columns" in sql:
                self._rows = [{"table_name": "tax_years", "column_name": "tax_year"}]
            elif "FROM tax_years WHERE tax_year = %s" in sql:
                self._rows = [{"tax_year": 2026}]
            else:
                raise AssertionError(f"Unexpected SQL: {sql}")

        def fetchall(self) -> list[dict[str, object]]:
            return self._rows

        def fetchone(self) -> dict[str, object] | None:
            return self._rows[0] if self._rows else None

    class _Connection:
        def __enter__(self) -> _Connection:
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def cursor(self) -> _Cursor:
            return _Cursor()

    monkeypatch.setattr(
        "app.services.schema_readiness.get_connection",
        lambda: _Connection(),
    )

    with pytest.raises(SchemaReadinessError) as exc_info:
        assert_job_schema_ready("job_features", tax_year=2026)

    message = str(exc_info.value)
    assert "job_features readiness check failed" in message
    assert "Missing view public.parcel_summary_view." in message
    assert "0035_stage10_parcel_summary_views" in message
    assert "Missing column public.tax_years.valuation_date." in message
    assert "0037_tax_years_valuation_date_alignment" in message
