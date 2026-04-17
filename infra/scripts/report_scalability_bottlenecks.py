from __future__ import annotations

import argparse
import json

from app.services.scalability_bottlenecks import ScalabilityBottleneckReviewService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Report county-year ingestion and instant-quote bottleneck candidates from "
            "persisted runtime telemetry."
        )
    )
    parser.add_argument("--county-id", required=True)
    parser.add_argument("--tax-years", nargs="+", required=True, type=int)
    parser.add_argument("--limit", default=5, type=int)
    return parser


def build_payload(
    *,
    county_id: str,
    tax_years: list[int],
    limit: int = 5,
) -> dict[str, object]:
    review = ScalabilityBottleneckReviewService().build_review(
        county_id=county_id,
        tax_years=tax_years,
        limit=limit,
    )

    def _candidate_payload(candidate) -> dict[str, object]:
        return {
            "candidate_code": candidate.candidate_code,
            "label": candidate.label,
            "component_type": candidate.component_type,
            "component_key": candidate.component_key,
            "status": candidate.status,
            "tax_year": candidate.tax_year,
            "run_count": candidate.run_count,
            "failed_count": candidate.failed_count,
            "retry_count": candidate.retry_count,
            "warning_count": candidate.warning_count,
            "fallback_count": candidate.fallback_count,
            "avg_duration_ms": candidate.avg_duration_ms,
            "p95_duration_ms": candidate.p95_duration_ms,
            "max_duration_ms": candidate.max_duration_ms,
            "latest_duration_ms": candidate.latest_duration_ms,
            "latest_status": candidate.latest_status,
            "latest_finished_at": (
                None
                if candidate.latest_finished_at is None
                else candidate.latest_finished_at.isoformat()
            ),
            "latest_error_message": candidate.latest_error_message,
            "recommendation": candidate.recommendation,
            "evidence_notes": list(candidate.evidence_notes),
        }

    return {
        "county_id": review.county_id,
        "tax_years": list(review.tax_years),
        "summary": {
            "overall_status": review.summary.overall_status,
            "investigate_count": review.summary.investigate_count,
            "monitor_count": review.summary.monitor_count,
            "healthy_count": review.summary.healthy_count,
            "top_candidate_code": review.summary.top_candidate_code,
            "next_actions": list(review.summary.next_actions),
        },
        "top_candidates": [_candidate_payload(candidate) for candidate in review.top_candidates],
        "ingestion_job_candidates": [
            _candidate_payload(candidate) for candidate in review.ingestion_job_candidates
        ],
        "ingestion_step_candidates": [
            _candidate_payload(candidate) for candidate in review.ingestion_step_candidates
        ],
        "instant_quote_refresh_candidates": [
            _candidate_payload(candidate)
            for candidate in review.instant_quote_refresh_candidates
        ],
    }


def main() -> None:
    args = build_parser().parse_args()
    payload = build_payload(
        county_id=args.county_id,
        tax_years=args.tax_years,
        limit=args.limit,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
