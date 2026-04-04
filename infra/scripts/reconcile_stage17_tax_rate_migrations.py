from __future__ import annotations

import argparse

from app.db.connection import get_connection
from app.services.stage17_tax_rate_migration_reconciliation import (
    apply_stage17_tax_rate_migration_repair,
    build_stage17_tax_rate_migration_plan,
    inspect_stage17_tax_rate_migration_state,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect or repair Stage 17 tax-rate migration drift."
    )
    parser.add_argument(
        "--apply-repair",
        action="store_true",
        help="Apply the safe targeted repair when the detected environment shape allows it.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()

    with get_connection() as connection:
        inspection = inspect_stage17_tax_rate_migration_state(connection)
        plan = build_stage17_tax_rate_migration_plan(inspection)

        _print_report(inspection=inspection, plan=plan)

        if not args.apply_repair:
            return

        executed_actions = apply_stage17_tax_rate_migration_repair(connection, plan=plan)
        connection.commit()
        print("")
        print("Applied repair actions:")
        for action in executed_actions:
            print(f"- {action}")


def _print_report(*, inspection, plan) -> None:
    print(f"environment_shape: {plan.environment_shape}")
    print("applied_versions:")
    if not inspection.applied_versions:
        print("- <none>")
    else:
        for version, name in sorted(inspection.applied_versions.items()):
            print(f"- {version}: {name}")

    print("artifact_check:")
    print(
        "- missing_hardening_columns: "
        + (
            ", ".join(inspection.missing_hardening_columns)
            if inspection.missing_hardening_columns
            else "<none>"
        )
    )
    print(
        "- missing_adoption_tables: "
        + (
            ", ".join(inspection.missing_adoption_tables)
            if inspection.missing_adoption_tables
            else "<none>"
        )
    )
    print(
        "- missing_adoption_columns: "
        + (
            ", ".join(inspection.missing_adoption_columns)
            if inspection.missing_adoption_columns
            else "<none>"
        )
    )

    print("issues:")
    if not plan.issues:
        print("- <none>")
    else:
        for issue in plan.issues:
            print(f"- {issue}")

    print("recommended_operator_steps:")
    for step in plan.recommended_operator_steps:
        print(f"- {step}")

    if plan.safe_to_apply_repair:
        print("safe_repair_actions:")
        for action in plan.repair_actions:
            print(f"- {action}")


if __name__ == "__main__":
    main()
