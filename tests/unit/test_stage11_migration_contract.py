from __future__ import annotations

from pathlib import Path

from infra.scripts.run_migrations import MIGRATIONS_DIR, discover_migrations


def test_stage11_migration_version_present() -> None:
    versions = [migration.version for migration in discover_migrations()]
    assert "0036" in versions


def test_stage11_migration_contains_search_refresh_function_and_public_wrapper() -> None:
    migration_path = MIGRATIONS_DIR / "0036_stage11_search_architecture.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "idx_search_documents_owner_trgm" in sql
    assert "dwellio_refresh_search_documents" in sql
    assert "SELECT dwellio_refresh_search_documents(NULL, NULL);" in sql
    assert "CREATE OR REPLACE VIEW v_search_read_model AS" in sql
    assert "FROM search_documents" in sql


def test_quote_read_model_file_matches_stage11_search_wrapper() -> None:
    read_model_path = Path("app/db/views/quote_read_model.sql")
    sql = read_model_path.read_text(encoding="utf-8")

    assert "CREATE OR REPLACE VIEW v_search_read_model AS" in sql
    assert "FROM search_documents" in sql
