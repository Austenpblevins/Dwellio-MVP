from __future__ import annotations

from infra.scripts.run_migrations import discover_migrations


def test_stage6_migration_version_present() -> None:
    versions = [migration.version for migration in discover_migrations()]
    assert "0031" in versions
