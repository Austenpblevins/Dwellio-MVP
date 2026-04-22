from __future__ import annotations

from dataclasses import dataclass

from infra.scripts.report_instant_quote_v5_stage0_baseline import build_payload


@dataclass
class _StubValidationReport:
    county_id: str
    tax_year: int
    quote_version: str = "instant_quote_v5_stage0_baseline"
    current_public_savings_model: str = "reduction_estimate_times_effective_tax_rate"
    support_rate_all_sfr_flagged: float = 0.9
    support_rate_all_sfr_flagged_denominator_count: int = 100
    support_rate_all_sfr_flagged_supportable_count: int = 90
    support_rate_strict_sfr_eligible: float = 0.95
    monitored_zero_savings_sample_row_count: int = 20
    monitored_zero_savings_quote_count: int = 8
    monitored_zero_savings_supported_quote_count: int = 12
    monitored_zero_savings_quote_share: float = 0.4
    blocker_distribution: dict[str, int] = None  # type: ignore[assignment]
    tax_rate_basis_year: int = 2025
    tax_rate_basis_reason: str = "fallback_requested_year_missing_supportable_subjects"
    tax_rate_basis_status: str = "prior_year_adopted_rates"
    tax_rate_basis_warning_codes: list[str] = None  # type: ignore[assignment]
    denominator_shift_warning_codes: list[str] = None  # type: ignore[assignment]
    monitored_extreme_savings_flagged_count: int = 1

    def __post_init__(self) -> None:
        if self.blocker_distribution is None:
            self.blocker_distribution = {"supportable": 90, "missing_effective_tax_rate": 10}
        if self.tax_rate_basis_warning_codes is None:
            self.tax_rate_basis_warning_codes = ["parcel_continuity_warning"]
        if self.denominator_shift_warning_codes is None:
            self.denominator_shift_warning_codes = []


def test_report_instant_quote_v5_stage0_baseline_builds_payload(monkeypatch) -> None:
    class StubValidationService:
        def build_report(self, *, county_id: str, tax_year: int) -> _StubValidationReport:
            return _StubValidationReport(county_id=county_id, tax_year=tax_year)

    class StubSettings:
        instant_quote_v5_enabled = False

    monkeypatch.setattr(
        "infra.scripts.report_instant_quote_v5_stage0_baseline.InstantQuoteValidationService",
        StubValidationService,
    )
    monkeypatch.setattr(
        "infra.scripts.report_instant_quote_v5_stage0_baseline.get_settings",
        lambda: StubSettings(),
    )

    payload = build_payload(county_ids=["harris", "fort_bend"], tax_year=2026)

    assert payload["quote_version"] == "instant_quote_v5_stage0_baseline"
    assert payload["current_public_savings_model"] == "reduction_estimate_times_effective_tax_rate"
    assert payload["instant_quote_v5_enabled"] is False
    assert "tax_limitation_uncertain" in payload["documented_unsupported_reasons"]
    assert payload["documented_warning_codes"][0]["code"] == "prior_year_assessment_basis_fallback"
    assert payload["counties"][0]["county_id"] == "harris"
    assert payload["counties"][1]["county_id"] == "fort_bend"
    assert payload["counties"][0]["monitored_zero_savings_quote_share"] == 0.4
