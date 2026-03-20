from __future__ import annotations

import argparse
import os

from app.county_adapters.fort_bend.adapter import FortBendCountyAdapter
from app.core.config import get_settings
from app.ingestion.service import IngestionLifecycleService
from app.ingestion.source_registry import get_source_registry_entry
from infra.scripts.verify_stage2_ingestion import apply_pending_migrations


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Stage 5 Fort Bend adapter verifier.")
    parser.add_argument("--database-url", default=None, help="Override DWELLIO_DATABASE_URL.")
    parser.add_argument("--county-id", default="fort_bend")
    parser.add_argument("--tax-year", default=2026, type=int)
    parser.add_argument("--dataset-type", default="property_roll")
    parser.add_argument("--skip-migrate", action="store_true")
    return parser


def _assert_inspection(inspection, *, expected_import_batch_id: str, county_id: str) -> None:
    if inspection.import_batch_id != expected_import_batch_id:
        raise AssertionError("Inspection returned the wrong import batch.")
    if inspection.status != "normalized" or inspection.publish_state != "published":
        raise AssertionError(f"Unexpected {county_id} batch state: {inspection}")
    if inspection.staging_row_count < 2:
        raise AssertionError(f"Expected at least two {county_id} staging rows.")
    if inspection.parcel_year_snapshot_count < 2:
        raise AssertionError(f"Expected at least two {county_id} parcel_year_snapshots.")
    if inspection.parcel_assessment_count < 2:
        raise AssertionError(f"Expected at least two {county_id} parcel_assessments.")
    if inspection.parcel_exemption_count < 2:
        raise AssertionError(f"Expected {county_id} exemptions to publish.")
    if inspection.validation_error_count != 0:
        raise AssertionError(
            f"Expected zero validation errors for {county_id} fixture rows: {inspection.failed_records}"
        )


def _assert_validation_failure_surface() -> None:
    findings = FortBendCountyAdapter().validate_dataset(
        "job-stage5-verify",
        2026,
        "property_roll",
        [
            {
                "site_address": "505 Broken Creek",
                "site_city": "Richmond",
                "site_zip": "77406",
                "market_value": None,
                "exemptions": [{"exemption_type_code": "homestead", "exemption_amount": -1}],
            }
        ],
    )
    error_codes = {finding.validation_code for finding in findings if finding.severity == "error"}
    if "MISSING_ACCOUNT_ID" not in error_codes or "NEGATIVE_EXEMPTION_AMOUNT" not in error_codes:
        raise AssertionError(f"Validation failure surface missing expected error codes: {error_codes}")


def _assert_registry_entries() -> None:
    fort_bend_entry = get_source_registry_entry(county_id="fort_bend", dataset_type="property_roll")
    if not fort_bend_entry.active_flag or fort_bend_entry.access_method != "fixture_csv":
        raise AssertionError(f"Unexpected Fort Bend source registry entry: {fort_bend_entry}")

    harris_entry = get_source_registry_entry(county_id="harris", dataset_type="property_roll")
    if not harris_entry.active_flag or harris_entry.access_method != "fixture_json":
        raise AssertionError(f"Unexpected Harris source registry entry: {harris_entry}")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    database_url = args.database_url or get_settings().database_url
    if database_url is None:
        raise RuntimeError("Set DWELLIO_DATABASE_URL or pass --database-url.")
    os.environ["DWELLIO_DATABASE_URL"] = database_url
    get_settings.cache_clear()

    if not args.skip_migrate:
        apply_pending_migrations(database_url)

    _assert_registry_entries()
    service = IngestionLifecycleService()

    first_run = service.run_dataset_lifecycle(
        county_id=args.county_id,
        tax_year=args.tax_year,
        dataset_type=args.dataset_type,
    )
    if first_run.rerun_of_import_batch_id is not None:
        raise AssertionError("Expected the first Fort Bend Stage 5 run on a clean verifier DB to have no prior batch.")
    first_inspection = service.inspect_import_batch(
        county_id=args.county_id,
        tax_year=args.tax_year,
        dataset_type=args.dataset_type,
        import_batch_id=first_run.import_batch_id,
    )
    _assert_inspection(
        first_inspection,
        expected_import_batch_id=first_run.import_batch_id,
        county_id=args.county_id,
    )

    second_run = service.run_dataset_lifecycle(
        county_id=args.county_id,
        tax_year=args.tax_year,
        dataset_type=args.dataset_type,
    )
    if second_run.import_batch_id == first_run.import_batch_id:
        raise AssertionError("Expected rerun to create a fresh Fort Bend import batch.")
    if second_run.rerun_of_import_batch_id != first_run.import_batch_id:
        raise AssertionError(
            f"Expected rerun_of_import_batch_id={first_run.import_batch_id}, "
            f"got {second_run.rerun_of_import_batch_id}."
        )
    second_inspection = service.inspect_import_batch(
        county_id=args.county_id,
        tax_year=args.tax_year,
        dataset_type=args.dataset_type,
        import_batch_id=second_run.import_batch_id,
    )
    _assert_inspection(
        second_inspection,
        expected_import_batch_id=second_run.import_batch_id,
        county_id=args.county_id,
    )

    harris_run = service.run_dataset_lifecycle(
        county_id="harris",
        tax_year=args.tax_year,
        dataset_type=args.dataset_type,
    )
    harris_inspection = service.inspect_import_batch(
        county_id="harris",
        tax_year=args.tax_year,
        dataset_type=args.dataset_type,
        import_batch_id=harris_run.import_batch_id,
    )
    _assert_inspection(
        harris_inspection,
        expected_import_batch_id=harris_run.import_batch_id,
        county_id="harris",
    )

    _assert_validation_failure_surface()

    print(
        "Stage 5 Fort Bend verification succeeded: Fort Bend fixture acquisition, staging, normalization, "
        "inspection, rerun workflow, and multi-county shared-framework behavior all passed."
    )


if __name__ == "__main__":
    main()
