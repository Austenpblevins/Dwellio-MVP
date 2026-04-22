from __future__ import annotations

from app.services.instant_quote_shadow_savings import (
    SHADOW_LOW_CASH_CAP,
    build_shadow_savings_comparison,
)


def test_build_shadow_savings_comparison_marks_missing_profile_as_unavailable() -> None:
    comparison = build_shadow_savings_comparison(
        tax_profile=None,
        reduction_estimate=25000.0,
        current_savings_estimate=525.0,
    )

    assert comparison.profile_version is None
    assert comparison.shadow_savings_estimate_raw is None
    assert comparison.shadow_savings_delta_raw is None
    assert comparison.limiting_reason_codes == ("missing_tax_profile",)


def test_build_shadow_savings_comparison_uses_non_school_rate_when_school_truth_is_unknown() -> None:
    comparison = build_shadow_savings_comparison(
        tax_profile={
            "profile_version": "v5_summary_profile_v1",
            "tax_profile_status": "constrained",
            "tax_profile_quality_score": 58,
            "marginal_model_type": "school_non_school_split",
            "marginal_tax_rate_total": 0.021,
            "marginal_tax_rate_non_school": 0.011,
            "opportunity_vs_savings_state": "school_limited_non_school_possible",
            "savings_limited_by_codes": ["school_ceiling_amount_unavailable"],
            "fallback_tax_profile_used_flag": True,
            "total_exemption_flag": False,
            "near_total_exemption_flag": False,
            "freeze_flag": True,
        },
        reduction_estimate=50000.0,
        current_savings_estimate=1050.0,
    )

    assert comparison.shadow_savings_estimate_raw == 550.0
    assert comparison.shadow_savings_delta_raw == -500.0
    assert comparison.tax_profile_status == "constrained"
    assert comparison.marginal_model_type == "school_non_school_split"
    assert comparison.opportunity_vs_savings_state == "school_limited_non_school_possible"
    assert comparison.limiting_reason_codes == ("school_ceiling_amount_unavailable",)


def test_build_shadow_savings_comparison_preserves_opportunity_only_profiles_without_fake_cash() -> None:
    comparison = build_shadow_savings_comparison(
        tax_profile={
            "profile_version": "v5_summary_profile_v1",
            "tax_profile_status": "opportunity_only",
            "tax_profile_quality_score": 35,
            "marginal_model_type": "opportunity_only_no_reliable_tax_profile",
            "marginal_tax_rate_total": 0.019,
            "marginal_tax_rate_non_school": 0.011,
            "opportunity_vs_savings_state": "opportunity_only_tax_profile_incomplete",
            "savings_limited_by_codes": [
                "tax_rate_basis_fallback_applied",
                "profile_support_level_summary_only",
            ],
            "fallback_tax_profile_used_flag": True,
            "total_exemption_flag": False,
            "near_total_exemption_flag": False,
            "freeze_flag": False,
        },
        reduction_estimate=42000.0,
        current_savings_estimate=798.0,
    )

    assert comparison.shadow_savings_estimate_raw is None
    assert comparison.shadow_savings_delta_raw is None
    assert comparison.tax_profile_status == "opportunity_only"
    assert comparison.opportunity_vs_savings_state == "opportunity_only_tax_profile_incomplete"


def test_build_shadow_savings_comparison_caps_near_total_exemption_cases_at_low_cash() -> None:
    comparison = build_shadow_savings_comparison(
        tax_profile={
            "profile_version": "v5_summary_profile_v1",
            "tax_profile_status": "constrained",
            "tax_profile_quality_score": 48,
            "marginal_model_type": "limited_by_near_total_exemption",
            "marginal_tax_rate_total": 0.021,
            "marginal_tax_rate_non_school": 0.011,
            "opportunity_vs_savings_state": "near_total_exemption_low_cash",
            "savings_limited_by_codes": ["near_total_exemption_likely"],
            "fallback_tax_profile_used_flag": True,
            "total_exemption_flag": False,
            "near_total_exemption_flag": True,
            "freeze_flag": False,
        },
        reduction_estimate=50000.0,
        current_savings_estimate=1050.0,
    )

    assert comparison.shadow_savings_estimate_raw == SHADOW_LOW_CASH_CAP
    assert comparison.shadow_savings_delta_raw == SHADOW_LOW_CASH_CAP - 1050.0
    assert comparison.opportunity_vs_savings_state == "near_total_exemption_low_cash"
