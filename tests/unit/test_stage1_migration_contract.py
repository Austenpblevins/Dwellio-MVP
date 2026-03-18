from __future__ import annotations

from infra.scripts.run_migrations import discover_migrations


def test_stage1_migration_versions_present() -> None:
    versions = [migration.version for migration in discover_migrations()]
    assert "0026" in versions
    assert "0027" in versions
    assert "0028" in versions
    assert "0029" in versions
