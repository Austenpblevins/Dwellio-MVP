from __future__ import annotations

from infra.scripts.run_migrations import discover_migrations


def test_discover_migrations_ordered() -> None:
    migrations = discover_migrations()
    assert migrations, "Expected at least one migration file."
    versions = [migration.version for migration in migrations]
    assert versions == sorted(versions)
