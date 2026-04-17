from __future__ import annotations

import argparse
import json

from app.services.admin_ops import AdminOpsService
from app.services.admin_readiness import AdminReadinessService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify that a county-year can be traced from ingestion through admin visibility into searchable read models."
    )
    parser.add_argument("--county-id", required=True)
    parser.add_argument("--tax-year", required=True, type=int)
    parser.add_argument(
        "--dataset-types",
        nargs="+",
        default=["property_roll", "tax_rates"],
        help="Datasets that must be present and published for the smoke verification.",
    )
    return parser


def build_payload(
    *,
    county_id: str,
    tax_year: int,
    dataset_types: list[str],
) -> dict[str, object]:
    dashboard = AdminReadinessService().build_dashboard(county_id=county_id, tax_years=[tax_year])
    row = dashboard.readiness_rows[0]
    ops_service = AdminOpsService()

    checks: list[dict[str, object]] = []
    failures: list[str] = []

    for dataset_type in dataset_types:
        dataset = next((item for item in row.datasets if item.dataset_type == dataset_type), None)
        if dataset is None:
            failures.append(f"{dataset_type}_missing_from_readiness")
            checks.append(
                {
                    "dataset_type": dataset_type,
                    "passed": False,
                    "reason": "missing_from_readiness",
                }
            )
            continue

        passed = True
        reasons: list[str] = []
        detail_payload: dict[str, object] | None = None

        if dataset.raw_file_count == 0:
            passed = False
            reasons.append("raw_file_missing")
        if dataset.latest_import_batch_id is None:
            passed = False
            reasons.append("import_batch_missing")
        if dataset.latest_publish_state != "published":
            passed = False
            reasons.append("publish_not_complete")

        if dataset.latest_import_batch_id is not None:
            detail = ops_service.get_import_batch_detail(import_batch_id=dataset.latest_import_batch_id)
            detail_payload = {
                "import_batch_id": detail.batch.import_batch_id,
                "status": detail.batch.status,
                "publish_state": detail.batch.publish_state,
                "raw_file_count": detail.batch.raw_file_count,
                "validation_error_count": detail.validation_summary.error_count,
                "staging_row_count": detail.inspection.staging_row_count,
                "lineage_record_count": detail.inspection.lineage_record_count,
            }
            if detail.inspection.staging_row_count == 0:
                passed = False
                reasons.append("staging_not_visible")

        if dataset_type == "property_roll":
            if not row.derived.parcel_summary_ready:
                passed = False
                reasons.append("parcel_summary_not_ready")
            if not row.derived.search_support_ready:
                passed = False
                reasons.append("search_support_not_ready")
            if not row.operational.searchable_ready:
                passed = False
                reasons.append("searchable_not_ready")

        checks.append(
            {
                "dataset_type": dataset_type,
                "passed": passed,
                "stage_status": dataset.stage_status,
                "freshness_status": dataset.freshness_status,
                "detail": detail_payload,
                "reasons": reasons,
            }
        )
        if not passed:
            failures.extend(f"{dataset_type}:{reason}" for reason in reasons)

    return {
        "county_id": county_id,
        "tax_year": tax_year,
        "passed": not failures,
        "capabilities": [
            {
                "capability_code": capability.capability_code,
                "status": capability.status,
                "source_datasets": capability.source_datasets,
            }
            for capability in dashboard.capabilities
        ],
        "overall_status": row.overall_status,
        "quality_status": row.operational.quality_status,
        "quality_score": row.operational.quality_score,
        "searchable_ready": row.operational.searchable_ready,
        "alerts": row.operational.alerts,
        "checks": checks,
        "failures": failures,
    }


def main() -> None:
    args = build_parser().parse_args()
    payload = build_payload(
        county_id=args.county_id,
        tax_year=args.tax_year,
        dataset_types=args.dataset_types,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    if not payload["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
