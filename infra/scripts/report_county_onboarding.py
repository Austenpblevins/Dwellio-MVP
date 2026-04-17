from __future__ import annotations

import argparse
import json

from app.services.county_onboarding import CountyOnboardingService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a repeatable county onboarding contract that combines capability truth, "
            "validation-year ranking, and onboarding gate status."
        )
    )
    parser.add_argument("--county-id", required=True)
    parser.add_argument("--tax-years", nargs="+", required=True, type=int)
    parser.add_argument("--current-tax-year", default=None, type=int)
    return parser


def build_payload(
    *,
    county_id: str,
    tax_years: list[int],
    current_tax_year: int | None = None,
) -> dict[str, object]:
    contract = CountyOnboardingService().build_contract(
        county_id=county_id,
        tax_years=tax_years,
        current_tax_year=current_tax_year,
    )

    def _snapshot_payload(snapshot) -> dict[str, object] | None:
        if snapshot is None:
            return None
        return {
            "tax_year": snapshot.tax_year,
            "datasets": [
                {
                    "dataset_type": dataset.dataset_type,
                    "access_method": dataset.access_method,
                    "availability_status": dataset.availability_status,
                    "raw_file_count": dataset.raw_file_count,
                    "latest_import_batch_id": dataset.latest_import_batch_id,
                    "latest_import_status": dataset.latest_import_status,
                    "latest_publish_state": dataset.latest_publish_state,
                    "canonical_published": dataset.canonical_published,
                }
                for dataset in snapshot.datasets
            ],
            "derived": {
                "parcel_summary_ready": snapshot.parcel_summary_ready,
                "search_support_ready": snapshot.search_support_ready,
                "feature_ready": snapshot.feature_ready,
                "comp_ready": snapshot.comp_ready,
                "quote_ready": snapshot.quote_ready,
            },
        }

    return {
        "county_id": contract.county_id,
        "current_tax_year": contract.current_tax_year,
        "validation_tax_year": contract.validation_tax_year,
        "validation_recommended": contract.validation_recommended,
        "capabilities": [
            {
                "capability_code": capability.capability_code,
                "label": capability.label,
                "status": capability.status,
                "source_datasets": capability.source_datasets,
                "notes": capability.notes,
                "metadata": capability.metadata,
            }
            for capability in contract.capabilities
        ],
        "validation_candidates": [
            {
                "tax_year": candidate.tax_year,
                "readiness_score": candidate.readiness_score,
                "recommended_for_qa": candidate.recommended_for_qa,
                "caveats": candidate.caveats,
                "validation_capabilities": candidate.validation_capabilities,
            }
            for candidate in contract.validation_candidates
        ],
        "current_year_snapshot": _snapshot_payload(contract.current_year_snapshot),
        "validation_year_snapshot": _snapshot_payload(contract.validation_year_snapshot),
        "phases": [
            {
                "phase_code": phase.phase_code,
                "label": phase.label,
                "status": phase.status,
                "blocking": phase.blocking,
                "summary": phase.summary,
                "details": phase.details,
            }
            for phase in contract.phases
        ],
        "recommended_actions": [
            {
                "action_code": action.action_code,
                "phase_code": action.phase_code,
                "blocking": action.blocking,
                "summary": action.summary,
                "command_hint": action.command_hint,
            }
            for action in contract.recommended_actions
        ],
    }


def main() -> None:
    args = build_parser().parse_args()
    payload = build_payload(
        county_id=args.county_id,
        tax_years=args.tax_years,
        current_tax_year=args.current_tax_year,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
