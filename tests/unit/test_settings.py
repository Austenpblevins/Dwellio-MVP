from __future__ import annotations

from app.core.config import get_settings


def test_settings_have_database_url() -> None:
    settings = get_settings()
    assert settings.database_url.startswith("postgresql://")


def test_settings_default_tax_year_is_int() -> None:
    settings = get_settings()
    assert isinstance(settings.default_tax_year, int)


def test_settings_have_admin_token_default() -> None:
    settings = get_settings()
    assert isinstance(settings.admin_api_token, str)
    assert settings.admin_api_token != ""


def test_settings_default_instant_quote_v5_flag_is_disabled() -> None:
    settings = get_settings()
    assert settings.instant_quote_v5_enabled is False


def test_settings_default_stage7_savings_translation_flag_is_disabled() -> None:
    settings = get_settings()
    assert settings.instant_quote_v5_savings_translation_enabled is False


def test_settings_default_stage8_rollout_states_are_total_exemption_only() -> None:
    settings = get_settings()
    assert settings.instant_quote_v5_savings_translation_rollout_states == "total_exemption_low_cash"
