from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from infra.scripts.run_migrations import MIGRATIONS_DIR, ensure_schema_migrations_table

EXPECTED_HARDENING_MIGRATION_VERSION = "0050"
EXPECTED_HARDENING_MIGRATION_NAME = "stage17_tax_rate_basis_hardening"
EXPECTED_ADOPTION_MIGRATION_VERSION = "0051"
EXPECTED_ADOPTION_MIGRATION_NAME = "stage17_tax_rate_adoption_status_admin_truth"
LEGACY_COLLIDING_0050_NAME = EXPECTED_ADOPTION_MIGRATION_NAME

_HARDENING_REQUIRED_COLUMNS = (
    ("instant_quote_refresh_runs", "tax_rate_quoteable_subject_row_count"),
    ("instant_quote_refresh_runs", "requested_tax_rate_effective_tax_rate_coverage_ratio"),
    ("instant_quote_refresh_runs", "requested_tax_rate_assignment_coverage_ratio"),
    ("instant_quote_refresh_runs", "tax_rate_basis_effective_tax_rate_coverage_ratio"),
    ("instant_quote_refresh_runs", "tax_rate_basis_assignment_coverage_ratio"),
    ("instant_quote_refresh_runs", "tax_rate_basis_continuity_parcel_match_row_count"),
    ("instant_quote_refresh_runs", "tax_rate_basis_continuity_parcel_gap_row_count"),
    ("instant_quote_refresh_runs", "tax_rate_basis_continuity_parcel_match_ratio"),
    ("instant_quote_refresh_runs", "tax_rate_basis_continuity_account_number_match_row_count"),
    ("instant_quote_refresh_runs", "tax_rate_basis_warning_codes"),
)
_ADOPTION_REQUIRED_TABLES = ("instant_quote_tax_rate_adoption_statuses",)
_ADOPTION_REQUIRED_COLUMNS = (
    ("instant_quote_tax_rate_adoption_statuses", "adoption_status"),
    ("instant_quote_tax_rate_adoption_statuses", "status_source"),
    ("instant_quote_tax_rate_adoption_statuses", "source_note"),
    ("instant_quote_subject_cache", "effective_tax_rate_basis_status"),
    ("instant_quote_subject_cache", "effective_tax_rate_basis_status_reason"),
    ("instant_quote_refresh_runs", "tax_rate_basis_status"),
    ("instant_quote_refresh_runs", "tax_rate_basis_status_reason"),
)


@dataclass(frozen=True)
class Stage17TaxRateMigrationInspection:
    schema_migrations_present: bool
    applied_versions: dict[str, str]
    missing_hardening_columns: tuple[str, ...]
    missing_adoption_tables: tuple[str, ...]
    missing_adoption_columns: tuple[str, ...]

    @property
    def hardening_ready(self) -> bool:
        return not self.missing_hardening_columns

    @property
    def adoption_ready(self) -> bool:
        return not self.missing_adoption_tables and not self.missing_adoption_columns


@dataclass(frozen=True)
class Stage17TaxRateMigrationPlan:
    environment_shape: str
    safe_to_apply_repair: bool
    repair_actions: tuple[str, ...]
    issues: tuple[str, ...]
    recommended_operator_steps: tuple[str, ...]


def inspect_stage17_tax_rate_migration_state(
    connection: Any,
) -> Stage17TaxRateMigrationInspection:
    schema_migrations_present = _table_exists(connection, "schema_migrations")
    applied_versions: dict[str, str] = {}
    if schema_migrations_present:
        with connection.cursor() as cursor:
            cursor.execute("SELECT version, name FROM schema_migrations ORDER BY version ASC")
            applied_versions = {
                str(row["version"]): str(row["name"]) for row in cursor.fetchall()
            }

    missing_hardening_columns = tuple(
        f"{table}.{column}"
        for table, column in _HARDENING_REQUIRED_COLUMNS
        if not _column_exists(connection, table, column)
    )
    missing_adoption_tables = tuple(
        table for table in _ADOPTION_REQUIRED_TABLES if not _table_exists(connection, table)
    )
    missing_adoption_columns = tuple(
        f"{table}.{column}"
        for table, column in _ADOPTION_REQUIRED_COLUMNS
        if not _column_exists(connection, table, column)
    )
    return Stage17TaxRateMigrationInspection(
        schema_migrations_present=schema_migrations_present,
        applied_versions=applied_versions,
        missing_hardening_columns=missing_hardening_columns,
        missing_adoption_tables=missing_adoption_tables,
        missing_adoption_columns=missing_adoption_columns,
    )


def build_stage17_tax_rate_migration_plan(
    inspection: Stage17TaxRateMigrationInspection,
) -> Stage17TaxRateMigrationPlan:
    applied_0050 = inspection.applied_versions.get(EXPECTED_HARDENING_MIGRATION_VERSION)
    applied_0051 = inspection.applied_versions.get(EXPECTED_ADOPTION_MIGRATION_VERSION)

    if (
        applied_0050 == EXPECTED_HARDENING_MIGRATION_NAME
        and applied_0051 == EXPECTED_ADOPTION_MIGRATION_NAME
        and inspection.hardening_ready
        and inspection.adoption_ready
    ):
        return Stage17TaxRateMigrationPlan(
            environment_shape="integrated_expected",
            safe_to_apply_repair=False,
            repair_actions=(),
            issues=(),
            recommended_operator_steps=(
                "Environment already matches the integrated Stage 17 tax-rate migration chain.",
            ),
        )

    if (
        applied_0050 is None
        and applied_0051 is None
        and not inspection.hardening_ready
        and not inspection.adoption_ready
    ):
        return Stage17TaxRateMigrationPlan(
            environment_shape="pending_normal_migrations",
            safe_to_apply_repair=False,
            repair_actions=(),
            issues=(
                "The integrated Stage 17 tax-rate migrations do not appear to have been applied yet.",
            ),
            recommended_operator_steps=(
                "Run python3 -m infra.scripts.run_migrations to apply the normal migration chain.",
            ),
        )

    repair_actions: list[str] = []
    issues: list[str] = []
    recommended_steps: list[str] = []
    environment_shape = "manual_review_required"
    safe_to_apply_repair = False

    if applied_0050 == LEGACY_COLLIDING_0050_NAME and applied_0051 is None:
        environment_shape = "legacy_old_0050_adoption_collision"
        safe_to_apply_repair = True
        issues.append(
            "schema_migrations records 0050 as the old standalone adoption-status migration."
        )
        recommended_steps.append(
            "Apply the targeted reconciliation repair to restamp 0050 as hardening and register 0051 as adoption status."
        )
        repair_actions.extend(
            [
                "record_0050_hardening_metadata",
                "record_0051_adoption_metadata",
            ]
        )
        if not inspection.hardening_ready:
            repair_actions.append("apply_hardening_sql")
        if not inspection.adoption_ready:
            repair_actions.append("apply_adoption_sql")
    elif (
        applied_0050 == EXPECTED_HARDENING_MIGRATION_NAME
        and inspection.hardening_ready
        and applied_0051 is None
        and inspection.adoption_ready
    ):
        environment_shape = "missing_0051_metadata_record"
        safe_to_apply_repair = True
        issues.append(
            "Adoption-status artifacts are present, but schema_migrations does not record 0051."
        )
        recommended_steps.append(
            "Apply the targeted reconciliation repair to register 0051 without replaying the whole migration chain."
        )
        repair_actions.append("record_0051_adoption_metadata")
    elif (
        applied_0050 == EXPECTED_HARDENING_MIGRATION_NAME
        and applied_0051 == EXPECTED_ADOPTION_MIGRATION_NAME
        and (not inspection.hardening_ready or not inspection.adoption_ready)
    ):
        environment_shape = "artifact_drift_repairable"
        safe_to_apply_repair = True
        issues.append(
            "schema_migrations records the integrated chain, but one or more Stage 17 artifacts are missing."
        )
        recommended_steps.append(
            "Apply the targeted reconciliation repair to reapply only the missing idempotent Stage 17 SQL."
        )
        if not inspection.hardening_ready:
            repair_actions.append("apply_hardening_sql")
        if not inspection.adoption_ready:
            repair_actions.append("apply_adoption_sql")
    else:
        if applied_0050 not in {None, EXPECTED_HARDENING_MIGRATION_NAME, LEGACY_COLLIDING_0050_NAME}:
            issues.append(f"Unexpected schema_migrations entry for 0050: {applied_0050}.")
        if applied_0051 not in {None, EXPECTED_ADOPTION_MIGRATION_NAME}:
            issues.append(f"Unexpected schema_migrations entry for 0051: {applied_0051}.")
        if inspection.missing_hardening_columns:
            issues.append(
                "Missing hardening columns: " + ", ".join(inspection.missing_hardening_columns) + "."
            )
        if inspection.missing_adoption_tables or inspection.missing_adoption_columns:
            issues.append(
                "Missing adoption-status artifacts: "
                + ", ".join(
                    [
                        *inspection.missing_adoption_tables,
                        *inspection.missing_adoption_columns,
                    ]
                )
                + "."
            )
        recommended_steps.append(
            "Inspect the reported schema_migrations rows and Stage 17 artifacts before making any manual repair."
        )

    return Stage17TaxRateMigrationPlan(
        environment_shape=environment_shape,
        safe_to_apply_repair=safe_to_apply_repair,
        repair_actions=tuple(dict.fromkeys(repair_actions)),
        issues=tuple(issues),
        recommended_operator_steps=tuple(recommended_steps),
    )


def apply_stage17_tax_rate_migration_repair(
    connection: Any,
    *,
    plan: Stage17TaxRateMigrationPlan,
) -> tuple[str, ...]:
    if not plan.safe_to_apply_repair:
        raise ValueError(
            f"Environment shape {plan.environment_shape} is not marked safe for automatic repair."
        )

    executed_actions: list[str] = []
    ensure_schema_migrations_table(connection)
    with connection.transaction():
        with connection.cursor() as cursor:
            if "apply_hardening_sql" in plan.repair_actions:
                cursor.execute(_migration_sql("0050_stage17_tax_rate_basis_hardening.sql"))
                executed_actions.append("apply_hardening_sql")
            if "apply_adoption_sql" in plan.repair_actions:
                cursor.execute(_migration_sql("0051_stage17_tax_rate_adoption_status_admin_truth.sql"))
                executed_actions.append("apply_adoption_sql")
            if "record_0050_hardening_metadata" in plan.repair_actions:
                _upsert_schema_migration(
                    cursor,
                    version=EXPECTED_HARDENING_MIGRATION_VERSION,
                    name=EXPECTED_HARDENING_MIGRATION_NAME,
                )
                executed_actions.append("record_0050_hardening_metadata")
            if "record_0051_adoption_metadata" in plan.repair_actions:
                _upsert_schema_migration(
                    cursor,
                    version=EXPECTED_ADOPTION_MIGRATION_VERSION,
                    name=EXPECTED_ADOPTION_MIGRATION_NAME,
                )
                executed_actions.append("record_0051_adoption_metadata")
    return tuple(executed_actions)


def _table_exists(connection: Any, table_name: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT EXISTS (
              SELECT 1
              FROM information_schema.tables
              WHERE table_schema = 'public'
                AND table_name = %s
            ) AS present
            """,
            (table_name,),
        )
        row = cursor.fetchone()
    return bool(row and row["present"])


def _column_exists(connection: Any, table_name: str, column_name: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT EXISTS (
              SELECT 1
              FROM information_schema.columns
              WHERE table_schema = 'public'
                AND table_name = %s
                AND column_name = %s
            ) AS present
            """,
            (table_name, column_name),
        )
        row = cursor.fetchone()
    return bool(row and row["present"])


def _migration_sql(filename: str) -> str:
    return (MIGRATIONS_DIR / filename).read_text(encoding="utf-8")


def _upsert_schema_migration(cursor: Any, *, version: str, name: str) -> None:
    cursor.execute(
        """
        INSERT INTO schema_migrations(version, name)
        VALUES (%s, %s)
        ON CONFLICT (version) DO UPDATE
        SET name = EXCLUDED.name
        """,
        (version, name),
    )
