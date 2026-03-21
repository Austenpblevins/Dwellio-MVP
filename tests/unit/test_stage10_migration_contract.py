from __future__ import annotations

from pathlib import Path

from infra.scripts.run_migrations import MIGRATIONS_DIR, discover_migrations


def test_stage10_migration_version_present() -> None:
    versions = [migration.version for migration in discover_migrations()]
    assert "0035" in versions


def test_stage10_migration_contains_required_views_and_search_wrapper() -> None:
    migration_path = MIGRATIONS_DIR / "0035_stage10_parcel_summary_views.sql"
    sql = migration_path.read_text(encoding="utf-8")

    for view_name in (
        "parcel_summary_view",
        "parcel_effective_tax_rate_view",
        "parcel_tax_estimate_summary",
        "parcel_owner_current_view",
        "parcel_search_view",
        "parcel_data_completeness_view",
    ):
        assert f"CREATE OR REPLACE VIEW {view_name}" in sql

    assert "CREATE OR REPLACE VIEW v_search_read_model AS" in sql
    assert "FROM parcel_search_view" in sql
    assert "completeness_score" in sql
    assert "warning_codes" in sql
    assert "public_summary_ready_flag" in sql


def test_quote_read_model_file_uses_parcel_search_view_wrapper() -> None:
    read_model_path = Path("app/db/views/quote_read_model.sql")
    sql = read_model_path.read_text(encoding="utf-8")

    assert "CREATE OR REPLACE VIEW v_search_read_model AS" in sql
    assert "FROM parcel_search_view" in sql
