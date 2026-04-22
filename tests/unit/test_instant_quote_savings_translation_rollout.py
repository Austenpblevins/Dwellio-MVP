from app.services.instant_quote_savings_translation_rollout import (
    apply_savings_translation_rollout,
    decide_savings_translation_rollout,
)
from app.models.quote import (
    InstantQuoteEstimate,
    InstantQuoteExplanation,
    InstantQuoteResponse,
)


def _response_with_estimate() -> InstantQuoteResponse:
    return InstantQuoteResponse(
        supported=True,
        county_id="fort_bend",
        tax_year=2026,
        requested_tax_year=2026,
        served_tax_year=2026,
        tax_year_fallback_applied=False,
        tax_year_fallback_reason=None,
        data_freshness_label="current_year",
        account_number="123",
        basis_code="assessment_basis_segment_blend",
        subject=None,
        estimate=InstantQuoteEstimate(
            savings_range_low=700.0,
            savings_range_high=1300.0,
            savings_midpoint_display=1000.0,
            estimate_bucket="500_to_1499",
            estimate_strength_label="medium",
            tax_protection_limited=False,
            tax_protection_note=None,
        ),
        explanation=InstantQuoteExplanation(
            methodology="segment_within_neighborhood",
            estimate_strength_label="medium",
            summary="Base summary",
            bullets=["Base bullet"],
            limitation_note=None,
        ),
        disclaimers=["Base disclaimer"],
        unsupported_reason=None,
        next_step_cta=None,
    )


def test_stage7_rollout_keeps_default_path_when_flag_disabled(monkeypatch) -> None:
    monkeypatch.setenv("DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_ENABLED", "false")
    monkeypatch.setenv("DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_COUNTY_IDS", "fort_bend")
    monkeypatch.setenv(
        "DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_ROLLOUT_STATES",
        "total_exemption_low_cash",
    )
    monkeypatch.setenv("DWELLIO_DATABASE_URL", "postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev")
    from app.core.config import get_settings

    get_settings.cache_clear()
    try:
        decision = decide_savings_translation_rollout(
            county_id="fort_bend",
            response_supported=True,
            unsupported_reason=None,
            public_rollout_state="total_exemption_low_cash",
            current_savings_estimate_raw=1072.0,
            shadow_savings_estimate_raw=0.0,
            shadow_tax_profile_status="supported_with_disclosure",
            shadow_limiting_reason_codes=["total_exemption_likely"],
            shadow_fallback_tax_profile_used_flag=True,
        )
    finally:
        get_settings.cache_clear()
    assert decision.savings_translation_mode == "current_public_formula"
    assert decision.savings_translation_applied_flag is False
    assert decision.selected_public_savings_estimate_raw == 1072.0


def test_stage7_rollout_applies_only_to_enabled_cohort(monkeypatch) -> None:
    monkeypatch.setenv("DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_ENABLED", "true")
    monkeypatch.setenv("DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_COUNTY_IDS", "fort_bend")
    monkeypatch.setenv(
        "DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_ROLLOUT_STATES",
        "total_exemption_low_cash",
    )
    monkeypatch.setenv("DWELLIO_DATABASE_URL", "postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev")
    from app.core.config import get_settings

    get_settings.cache_clear()
    try:
        enabled = decide_savings_translation_rollout(
            county_id="fort_bend",
            response_supported=True,
            unsupported_reason=None,
            public_rollout_state="total_exemption_low_cash",
            current_savings_estimate_raw=1072.0,
            shadow_savings_estimate_raw=0.0,
            shadow_tax_profile_status="supported_with_disclosure",
            shadow_limiting_reason_codes=[
                "profile_support_level_summary_only",
                "tax_rate_basis_fallback_applied",
                "total_exemption_likely",
            ],
            shadow_fallback_tax_profile_used_flag=True,
        )
        blocked = decide_savings_translation_rollout(
            county_id="fort_bend",
            response_supported=True,
            unsupported_reason=None,
            public_rollout_state="school_limited_non_school_possible",
            current_savings_estimate_raw=500.0,
            shadow_savings_estimate_raw=200.0,
            shadow_tax_profile_status="constrained",
            shadow_limiting_reason_codes=["school_ceiling_amount_unavailable"],
            shadow_fallback_tax_profile_used_flag=True,
        )
    finally:
        get_settings.cache_clear()
    assert enabled.savings_translation_applied_flag is True
    assert enabled.selected_public_savings_estimate_raw == 0.0
    assert enabled.savings_translation_mode == "v5_shadow_tax_profile_rollout"
    assert blocked.savings_translation_applied_flag is False
    assert blocked.savings_translation_reason_code == "rollout_state_not_enabled"


def test_apply_savings_translation_rollout_replaces_public_estimate_when_enabled(
    monkeypatch,
) -> None:
    from app.core.config import get_settings

    monkeypatch.setenv("DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_ENABLED", "true")
    monkeypatch.setenv("DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_COUNTY_IDS", "fort_bend")
    monkeypatch.setenv(
        "DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_ROLLOUT_STATES",
        "total_exemption_low_cash",
    )
    monkeypatch.setenv("DWELLIO_DATABASE_URL", "postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev")
    get_settings.cache_clear()
    response = _response_with_estimate()
    translated_estimate = response.estimate.model_copy(
        update={
            "savings_range_low": 0.0,
            "savings_range_high": 0.0,
            "savings_midpoint_display": 0.0,
            "estimate_bucket": "under_500",
        }
    )
    try:
        decision = decide_savings_translation_rollout(
            county_id="fort_bend",
            response_supported=True,
            unsupported_reason=None,
            public_rollout_state="total_exemption_low_cash",
            current_savings_estimate_raw=1072.0,
            shadow_savings_estimate_raw=0.0,
            shadow_tax_profile_status="supported_with_disclosure",
            shadow_limiting_reason_codes=["tax_rate_basis_fallback_applied", "total_exemption_likely"],
            shadow_fallback_tax_profile_used_flag=True,
        )
    finally:
        get_settings.cache_clear()

    updated = apply_savings_translation_rollout(
        response=response,
        translated_estimate=translated_estimate,
        decision=decision,
    )

    assert updated.estimate is not None
    assert updated.estimate.savings_midpoint_display == 0.0
    assert any("summary tax profile" in text for text in updated.disclaimers)


def test_stage8_rollout_blocks_non_calibrated_state_even_if_env_enabled(monkeypatch) -> None:
    monkeypatch.setenv("DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_ENABLED", "true")
    monkeypatch.setenv("DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_COUNTY_IDS", "fort_bend")
    monkeypatch.setenv(
        "DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_ROLLOUT_STATES",
        "total_exemption_low_cash,high_opportunity_low_cash",
    )
    monkeypatch.setenv(
        "DWELLIO_DATABASE_URL", "postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev"
    )
    from app.core.config import get_settings

    get_settings.cache_clear()
    try:
        blocked = decide_savings_translation_rollout(
            county_id="fort_bend",
            response_supported=True,
            unsupported_reason=None,
            public_rollout_state="high_opportunity_low_cash",
            current_savings_estimate_raw=225.0,
            shadow_savings_estimate_raw=225.0,
            shadow_tax_profile_status="supported_with_disclosure",
            shadow_limiting_reason_codes=[
                "profile_support_level_summary_only",
                "tax_rate_basis_fallback_applied",
            ],
            shadow_fallback_tax_profile_used_flag=True,
        )
    finally:
        get_settings.cache_clear()

    assert blocked.savings_translation_applied_flag is False
    assert blocked.savings_translation_reason_code == "rollout_state_not_stage8_calibrated"


def test_stage8_rollout_requires_limiting_evidence_for_enabled_state(monkeypatch) -> None:
    monkeypatch.setenv("DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_ENABLED", "true")
    monkeypatch.setenv("DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_COUNTY_IDS", "fort_bend")
    monkeypatch.setenv(
        "DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_ROLLOUT_STATES",
        "total_exemption_low_cash,near_total_exemption_low_cash",
    )
    monkeypatch.setenv(
        "DWELLIO_DATABASE_URL", "postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev"
    )
    from app.core.config import get_settings

    get_settings.cache_clear()
    try:
        blocked = decide_savings_translation_rollout(
            county_id="fort_bend",
            response_supported=True,
            unsupported_reason=None,
            public_rollout_state="near_total_exemption_low_cash",
            current_savings_estimate_raw=250.0,
            shadow_savings_estimate_raw=100.0,
            shadow_tax_profile_status="supported_with_disclosure",
            shadow_limiting_reason_codes=["tax_rate_basis_fallback_applied"],
            shadow_fallback_tax_profile_used_flag=True,
        )
    finally:
        get_settings.cache_clear()

    assert blocked.savings_translation_applied_flag is False
    assert blocked.savings_translation_reason_code == "rollout_state_limiting_evidence_missing"


def test_stage8_rollout_never_raises_public_estimate(monkeypatch) -> None:
    monkeypatch.setenv("DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_ENABLED", "true")
    monkeypatch.setenv("DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_COUNTY_IDS", "fort_bend")
    monkeypatch.setenv(
        "DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_ROLLOUT_STATES",
        "near_total_exemption_low_cash",
    )
    monkeypatch.setenv(
        "DWELLIO_DATABASE_URL", "postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev"
    )
    from app.core.config import get_settings

    get_settings.cache_clear()
    try:
        blocked = decide_savings_translation_rollout(
            county_id="fort_bend",
            response_supported=True,
            unsupported_reason=None,
            public_rollout_state="near_total_exemption_low_cash",
            current_savings_estimate_raw=150.0,
            shadow_savings_estimate_raw=175.0,
            shadow_tax_profile_status="supported_with_disclosure",
            shadow_limiting_reason_codes=["near_total_exemption_likely"],
            shadow_fallback_tax_profile_used_flag=True,
        )
    finally:
        get_settings.cache_clear()

    assert blocked.savings_translation_applied_flag is False
    assert blocked.savings_translation_reason_code == "shadow_exceeds_current_public_estimate"
