from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

MIGRATION_FILE_RE = re.compile(r"^(?P<version>\d{4})_(?P<name>.+)\.sql$")
MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "app" / "db" / "migrations"


@dataclass(frozen=True)
class Migration:
    version: str
    name: str
    path: Path


def discover_migrations() -> list[Migration]:
    migrations: list[Migration] = []
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        match = MIGRATION_FILE_RE.match(path.name)
        if match is None:
            continue
        migrations.append(
            Migration(
                version=match.group("version"),
                name=match.group("name"),
                path=path,
            )
        )
    return migrations


def ensure_schema_migrations_table(connection: Any) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )


def fetch_applied_versions(connection: Any) -> set[str]:
    with connection.cursor() as cursor:
        cursor.execute("SELECT version FROM schema_migrations")
        rows = cursor.fetchall()
    return {row[0] for row in rows}


def apply_migration(connection: Any, migration: Migration, dry_run: bool) -> None:
    sql = migration.path.read_text(encoding="utf-8")
    if dry_run:
        print(f"[dry-run] {migration.version}_{migration.name}")
        return

    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute(sql)
            cursor.execute(
                "INSERT INTO schema_migrations(version, name) VALUES (%s, %s)",
                (migration.version, migration.name),
            )
    print(f"[applied] {migration.version}_{migration.name}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Apply ordered SQL migrations.")
    parser.add_argument("--database-url", default=None, help="Override DWELLIO_DATABASE_URL.")
    parser.add_argument("--dry-run", action="store_true", help="List pending migrations only.")
    parser.add_argument("--list", action="store_true", help="List discovered migrations.")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    migrations = discover_migrations()

    if args.list:
        for migration in migrations:
            print(f"{migration.version}_{migration.name}")
        return

    database_url = args.database_url or os.getenv(
        "DWELLIO_DATABASE_URL",
        "postgresql://postgres:postgres@localhost:54322/postgres",
    )

    import psycopg

    with psycopg.connect(database_url, autocommit=False) as connection:
        ensure_schema_migrations_table(connection)
        applied_versions = fetch_applied_versions(connection)

        pending_migrations = [
            migration for migration in migrations if migration.version not in applied_versions
        ]
        if not pending_migrations:
            print("No pending migrations.")
            return

        for migration in pending_migrations:
            apply_migration(connection, migration, dry_run=args.dry_run)

        if args.dry_run:
            connection.rollback()
        else:
            connection.commit()


if __name__ == "__main__":
    main()
