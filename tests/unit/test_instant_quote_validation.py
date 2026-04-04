from __future__ import annotations

from app.services.instant_quote_validation import InstantQuoteValidationService


class StubCursor:
    def __init__(self) -> None:
        self._rows: list[dict[str, object]] = []

    def __enter__(self) -> StubCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        if "SET LOCAL max_parallel_workers_per_gather = 0" in sql:
            self._rows = []
        elif "FROM instant_quote_subject_cache" in sql and "living_area_sf" in sql:
            self._rows = [{"count": 15}]
        elif "FROM instant_quote_subject_cache" in sql and "effective_tax_rate" in sql:
            self._rows = [{"count": 12}]
        elif "GROUP BY COALESCE(support_blocker_code, 'supportable')" in sql:
            self._rows = [
                {"blocker_code": "missing_effective_tax_rate", "count": 8},
                {"blocker_code": "supportable", "count": 4},
            ]
        elif (
            "FROM instant_quote_subject_cache" in sql
            and "support_blocker_code IS NULL" in sql
            and "COUNT(*) AS count" in sql
        ):
            self._rows = [{"count": 4}]
        elif "FROM instant_quote_subject_cache" in sql and "COUNT(*) AS count" in sql:
            self._rows = [{"count": 9}]
        elif "FROM instant_quote_neighborhood_stats" in sql:
            self._rows = [{"count": 2}]
        elif "FROM instant_quote_segment_stats" in sql:
            self._rows = [{"count": 1}]
        elif "served_neighborhood_only_quote_count" in sql:
            self._rows = [
                {
                    "served_neighborhood_only_quote_count": 5,
                    "served_supported_neighborhood_only_quote_count": 3,
                    "served_unsupported_neighborhood_only_quote_count": 2,
                }
            ]
        elif "subject_rows_without_usable_segment_stats" in sql:
            self._rows = [
                {
                    "subject_rows_without_usable_neighborhood_stats": 0,
                    "subject_rows_without_usable_segment_stats": 6,
                    "subject_rows_missing_segment_row": 4,
                    "subject_rows_thin_segment_support": 2,
                    "subject_rows_unusable_segment_basis": 0,
                }
            ]
        elif "FROM instant_quote_refresh_runs" in sql and "ORDER BY refresh_started_at DESC" in sql:
            self._rows = [
                {
                    "refresh_status": "completed",
                    "refresh_finished_at": None,
                    "validated_at": None,
                    "cache_view_row_delta": 0,
                    "tax_rate_basis_year": 2025,
                    "tax_rate_basis_reason": "fallback_requested_year_missing_supportable_subjects",
                    "tax_rate_basis_fallback_applied": True,
                    "tax_rate_basis_status": "prior_year_adopted_rates",
                    "tax_rate_basis_status_reason": "basis_year_precedes_quote_year",
                    "requested_tax_rate_supportable_subject_row_count": 0,
                    "tax_rate_basis_supportable_subject_row_count": 24,
                }
            ]
        elif "FROM instant_quote_subject_cache" in sql and "LIMIT 2" in sql:
            blocker_code = None if params is None else params[2]
            if blocker_code is None:
                self._rows = [
                    {"account_number": "1001001001001"},
                    {"account_number": "1001001001002"},
                ]
            elif blocker_code == "missing_effective_tax_rate":
                self._rows = [{"account_number": "1001001001003"}]
            else:
                self._rows = []
        elif "FROM instant_quote_subject_cache" in sql and "LIMIT 12" in sql:
            self._rows = [
                {"account_number": "1001001001001"},
                {"account_number": "1001001001002"},
                {"account_number": "1001001001003"},
            ]
        elif "UPDATE instant_quote_refresh_runs" in sql:
            self._rows = []
        else:
            raise AssertionError(f"Unexpected SQL: {sql}")

    def fetchone(self) -> dict[str, object] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[dict[str, object]]:
        return self._rows


class StubConnection:
    def __enter__(self) -> StubConnection:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> StubCursor:
        return StubCursor()

    def commit(self) -> None:
        return None


class StubEstimate:
    estimate_bucket = "moderate"
    estimate_strength_label = "medium"


class StubResponse:
    def __init__(self, *, supported: bool, unsupported_reason: str | None = None) -> None:
        self.supported = supported
        self.unsupported_reason = unsupported_reason
        self.served_tax_year = 2025
        self.estimate = None if not supported else StubEstimate()


class StubQuoteService:
    def get_quote(self, *, county_id: str, tax_year: int, account_number: str) -> StubResponse:
        if account_number == "1001001001001":
            return StubResponse(supported=True)
        return StubResponse(supported=False, unsupported_reason="thin_market_support")


def test_instant_quote_validation_report_summarizes_counts_and_examples(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.services.instant_quote_validation.get_connection",
        lambda: StubConnection(),
    )

    report = InstantQuoteValidationService(
        quote_service=StubQuoteService(),  # type: ignore[arg-type]
    ).build_report(county_id="harris", tax_year=2025)

    assert report.parcel_rows_with_living_area == 15
    assert report.parcel_rows_with_effective_tax_rate == 12
    assert report.subject_cache_row_count == 9
    assert report.instant_quote_supportable_rows == 4
    assert report.supported_neighborhood_stats_rows == 2
    assert report.supported_segment_stats_rows == 1
    assert report.tax_rate_basis_year == 2025
    assert report.tax_rate_basis_reason == "fallback_requested_year_missing_supportable_subjects"
    assert report.tax_rate_basis_fallback_applied is True
    assert report.tax_rate_basis_status == "prior_year_adopted_rates"
    assert report.tax_rate_basis_status_reason == "basis_year_precedes_quote_year"
    assert report.requested_tax_rate_supportable_subject_row_count == 0
    assert report.tax_rate_basis_supportable_subject_row_count == 24
    assert report.subject_rows_without_usable_neighborhood_stats == 0
    assert report.subject_rows_without_usable_segment_stats == 6
    assert report.subject_rows_missing_segment_row == 4
    assert report.subject_rows_thin_segment_support == 2
    assert report.subject_rows_unusable_segment_basis == 0
    assert report.served_neighborhood_only_quote_count == 5
    assert report.served_supported_neighborhood_only_quote_count == 3
    assert report.served_unsupported_neighborhood_only_quote_count == 2
    assert report.latest_refresh_status == "completed"
    assert report.cache_view_row_delta == 0
    assert report.blocker_distribution == {
        "missing_effective_tax_rate": 8,
        "supportable": 4,
    }
    assert report.supported_public_quote_exists is True
    assert report.examples[0].account_number == "1001001001001"
    assert report.examples[0].supported is True
