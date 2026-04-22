from __future__ import annotations

from app.services.instant_quote_county_tax_capability import (
    InstantQuoteCountyTaxCapabilityService,
)


class _CapabilityCursor:
    def __init__(
        self,
        *,
        observed_metrics: dict[str, object],
        latest_refresh_run: dict[str, object] | None,
    ) -> None:
        self.observed_metrics = observed_metrics
        self.latest_refresh_run = latest_refresh_run
        self._rows: list[dict[str, object]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        if "FROM instant_quote_subject_cache" in sql and "missing_exemption_amount_rows" in sql:
            self._rows = [self.observed_metrics]
        elif "FROM instant_quote_refresh_runs" in sql and "tax_rate_basis_status" in sql:
            self._rows = [] if self.latest_refresh_run is None else [self.latest_refresh_run]
        else:
            raise AssertionError(f"Unexpected SQL: {sql}")

    def fetchone(self) -> dict[str, object] | None:
        return self._rows[0] if self._rows else None


class _CapabilityConnection:
    def __init__(
        self,
        *,
        observed_metrics: dict[str, object],
        latest_refresh_run: dict[str, object] | None,
    ) -> None:
        self._cursor = _CapabilityCursor(
            observed_metrics=observed_metrics,
            latest_refresh_run=latest_refresh_run,
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> _CapabilityCursor:
        return self._cursor


def test_build_capability_downgrades_fort_bend_over65_when_no_rows_are_observed(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.services.instant_quote_county_tax_capability.get_connection",
        lambda: _CapabilityConnection(
            observed_metrics={
                "subject_cache_row_count": 278427,
                "over65_rows": 0,
                "disabled_rows": 2988,
                "disabled_veteran_rows": 8123,
                "freeze_rows": 2885,
                "missing_exemption_amount_rows": 210887,
                "assessment_exemption_total_mismatch_rows": 0,
                "homestead_flag_mismatch_rows": 0,
            },
            latest_refresh_run={
                "tax_rate_basis_status": "prior_year_adopted_rates",
                "tax_rate_basis_reason": "fallback_requested_year_missing_supportable_subjects",
                "tax_rate_basis_fallback_applied": True,
                "tax_rate_basis_effective_tax_rate_coverage_ratio": 0.9983,
                "tax_rate_basis_assignment_coverage_ratio": 0.9982,
                "tax_rate_basis_warning_codes": [],
            },
        ),
    )

    capability = InstantQuoteCountyTaxCapabilityService().build_capability(
        county_id="fort_bend",
        tax_year=2026,
    )

    assert capability.exemption_normalization_confidence == "limited"
    assert capability.over65_reliability == "limited"
    assert capability.disabled_reliability == "supported"
    assert capability.disabled_veteran_reliability == "supported"
    assert capability.freeze_reliability == "limited"
    assert capability.tax_unit_assignment_reliability == "supported"
    assert capability.tax_rate_reliability == "limited"
    assert capability.school_ceiling_amount_available is False
    assert capability.local_option_policy_available is False
    assert capability.profile_support_level == "summary_only"


def test_build_capability_keeps_harris_over65_limit_visible(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.services.instant_quote_county_tax_capability.get_connection",
        lambda: _CapabilityConnection(
            observed_metrics={
                "subject_cache_row_count": 1171610,
                "over65_rows": 0,
                "disabled_rows": 4,
                "disabled_veteran_rows": 27430,
                "freeze_rows": 0,
                "missing_exemption_amount_rows": 827052,
                "assessment_exemption_total_mismatch_rows": 0,
                "homestead_flag_mismatch_rows": 0,
            },
            latest_refresh_run={
                "tax_rate_basis_status": "prior_year_adopted_rates",
                "tax_rate_basis_reason": "fallback_requested_year_missing_supportable_subjects",
                "tax_rate_basis_fallback_applied": True,
                "tax_rate_basis_effective_tax_rate_coverage_ratio": 0.9999,
                "tax_rate_basis_assignment_coverage_ratio": 0.9999,
                "tax_rate_basis_warning_codes": [],
            },
        ),
    )

    capability = InstantQuoteCountyTaxCapabilityService().build_capability(
        county_id="harris",
        tax_year=2026,
    )

    assert capability.exemption_normalization_confidence == "limited"
    assert capability.over65_reliability == "limited"
    assert capability.disabled_reliability == "limited"
    assert capability.disabled_veteran_reliability == "supported"
    assert capability.freeze_reliability == "limited"
    assert capability.tax_unit_assignment_reliability == "supported"
    assert capability.tax_rate_reliability == "limited"
    assert "missing_exemption_amount_rows=827052" in (capability.notes or "")
