from __future__ import annotations

from infra.scripts.run_migrations import discover_migrations


def test_stage3_migration_versions_present() -> None:
    versions = [migration.version for migration in discover_migrations()]
    assert "0030" in versions
