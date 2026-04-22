from __future__ import annotations

from app.services.instant_quote_tax_profile import InstantQuoteTaxProfileService


class _ProfileCursor:
    def __init__(self) -> None:
        self._rows: list[dict[str, object]] = []

    def __enter__(self) -> _ProfileCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        if "FROM instant_quote_tax_profile" in sql and "GROUP BY profile_version" in sql:
            self._rows = [
                {
                    "profile_version": "v5_summary_profile_v1",
                    "row_count": 25,
                    "source_data_cutoff_at": None,
                    "rows_with_assessment_basis_value": 24,
                    "rows_with_raw_exemption_codes": 20,
                    "rows_with_normalized_exemption_codes": 24,
                    "rows_with_complete_tax_unit_assignment": 21,
                    "rows_with_complete_tax_rate": 19,
                    "fallback_tax_profile_count": 6,
                    "missing_assessment_basis_warning_count": 1,
                    "over65_reliability_limited_count": 2,
                    "school_ceiling_amount_unavailable_count": 3,
                }
            ]
        elif "FROM instant_quote_tax_profile" in sql and "GROUP BY tax_profile_status" in sql:
            self._rows = [
                {"tax_profile_status": "supported_with_disclosure", "count": 19},
                {"tax_profile_status": "unsupported", "count": 4},
                {"tax_profile_status": "constrained", "count": 2},
            ]
        else:
            raise AssertionError(f"Unexpected SQL: {sql}")

    def fetchone(self) -> dict[str, object] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[dict[str, object]]:
        return self._rows


class _ProfileConnection:
    def __enter__(self) -> _ProfileConnection:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> _ProfileCursor:
        return _ProfileCursor()


def test_fetch_materialized_summary_returns_status_distribution(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.services.instant_quote_tax_profile.get_connection",
        lambda: _ProfileConnection(),
    )

    summary = InstantQuoteTaxProfileService().fetch_materialized_summary(
        county_id="harris",
        tax_year=2026,
    )

    assert summary["profile_version"] == "v5_summary_profile_v1"
    assert summary["row_count"] == 25
    assert summary["rows_with_complete_tax_rate"] == 19
    assert summary["fallback_tax_profile_count"] == 6
    assert summary["status_distribution"] == {
        "supported_with_disclosure": 19,
        "unsupported": 4,
        "constrained": 2,
    }
