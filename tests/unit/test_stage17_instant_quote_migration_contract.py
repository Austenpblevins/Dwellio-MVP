from __future__ import annotations

from infra.scripts.run_migrations import MIGRATIONS_DIR, discover_migrations


def test_stage17_instant_quote_migration_version_present() -> None:
    versions = [migration.version for migration in discover_migrations()]
    assert "0044" in versions
    assert "0045" in versions


def test_stage17_instant_quote_migration_contains_required_tables_and_view() -> None:
    migration_path = MIGRATIONS_DIR / "0044_stage17_instant_quote_service.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS instant_quote_neighborhood_stats" in sql
    assert "CREATE TABLE IF NOT EXISTS instant_quote_segment_stats" in sql
    assert "CREATE TABLE IF NOT EXISTS instant_quote_request_logs" in sql
    assert "CREATE VIEW instant_quote_subject_view AS" in sql
    assert "assessment_basis_value" in sql
    assert "support_blocker_code" in sql
    assert "now() AS last_refresh_at" not in sql


def test_stage17_instant_quote_serving_cache_migration_contains_required_table_and_indexes() -> None:
    migration_path = MIGRATIONS_DIR / "0045_stage17_instant_quote_serving_cache.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS instant_quote_subject_cache" in sql
    assert "PRIMARY KEY (parcel_id, tax_year)" in sql
    assert "idx_instant_quote_subject_cache_lookup" in sql
    assert "idx_instant_quote_subject_cache_neighborhood" in sql
    assert "idx_instant_quote_subject_cache_segment" in sql
