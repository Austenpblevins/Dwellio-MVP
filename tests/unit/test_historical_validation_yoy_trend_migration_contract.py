from __future__ import annotations

from infra.scripts.run_migrations import MIGRATIONS_DIR, discover_migrations


def test_historical_validation_yoy_trend_migration_present() -> None:
    versions = [migration.version for migration in discover_migrations()]
    assert "0039" in versions


def test_historical_validation_yoy_trend_migration_contains_required_views() -> None:
    sql = (
        MIGRATIONS_DIR / "0039_historical_validation_yoy_trends.sql"
    ).read_text(encoding="utf-8")

    assert "CREATE OR REPLACE VIEW parcel_year_trend_view AS" in sql
    assert "CREATE OR REPLACE VIEW neighborhood_year_trend_view AS" in sql
    assert "appraised_value_change_pct" in sql
    assert "effective_tax_rate_change_pct" in sql
    assert "weak_sample_support_flag" in sql

