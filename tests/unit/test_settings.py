from __future__ import annotations

from app.core.config import get_settings


def test_settings_have_database_url() -> None:
    settings = get_settings()
    assert settings.database_url.startswith("postgresql://")


def test_settings_default_tax_year_is_int() -> None:
    settings = get_settings()
    assert isinstance(settings.default_tax_year, int)

