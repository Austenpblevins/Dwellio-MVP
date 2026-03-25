from __future__ import annotations

from pathlib import Path

from infra.scripts.run_migrations import MIGRATIONS_DIR, discover_migrations


def test_stage13_migration_version_present() -> None:
    versions = [migration.version for migration in discover_migrations()]
    assert "0040" in versions


def test_stage13_migration_aligns_public_search_read_model() -> None:
    migration_path = MIGRATIONS_DIR / "0040_stage13_public_read_model_alignment.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "DROP VIEW IF EXISTS v_search_read_model;" in sql
    assert "CREATE VIEW v_search_read_model AS" in sql
    assert "tax_year" in sql
    assert "address" in sql
    assert "FROM search_documents" in sql


def test_quote_read_model_file_matches_stage13_public_search_alignment() -> None:
    sql = Path("app/db/views/quote_read_model.sql").read_text(encoding="utf-8")

    assert "CREATE VIEW v_search_read_model AS" in sql
    assert "tax_year" in sql
    assert "address" in sql
