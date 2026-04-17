from __future__ import annotations

from app.services.scalability_bottlenecks import (
    ScalabilityBottleneckCandidate,
    ScalabilityBottleneckReview,
    ScalabilityBottleneckSummary,
)
from infra.scripts.report_scalability_bottlenecks import build_payload


def test_report_scalability_bottlenecks_builds_machine_readable_payload(monkeypatch) -> None:
    class StubScalabilityBottleneckReviewService:
        def build_review(
            self,
            *,
            county_id: str,
            tax_years: list[int],
            limit: int = 5,
        ) -> ScalabilityBottleneckReview:
            assert county_id == "harris"
            assert tax_years == [2025, 2026]
            assert limit == 4
            candidate = ScalabilityBottleneckCandidate(
                candidate_code="step:property_roll:search_refresh:2026",
                label="Property Roll search refresh (2026)",
                component_type="ingestion_step",
                component_key="property_roll:search_refresh",
                status="investigate",
                tax_year=2026,
                run_count=3,
                failed_count=1,
                retry_count=1,
                p95_duration_ms=410000,
                max_duration_ms=600000,
                latest_duration_ms=600000,
                latest_status="failed",
                latest_error_message="refresh failed",
                recommendation="Profile before refactoring.",
                evidence_notes=["1 failed run in the review window."],
            )
            return ScalabilityBottleneckReview(
                county_id=county_id,
                tax_years=tax_years,
                summary=ScalabilityBottleneckSummary(
                    overall_status="investigate",
                    investigate_count=1,
                    monitor_count=0,
                    healthy_count=0,
                    top_candidate_code=candidate.candidate_code,
                    next_actions=["Profile before refactoring."],
                ),
                top_candidates=[candidate],
                ingestion_job_candidates=[],
                ingestion_step_candidates=[candidate],
                instant_quote_refresh_candidates=[],
            )

    monkeypatch.setattr(
        "infra.scripts.report_scalability_bottlenecks.ScalabilityBottleneckReviewService",
        StubScalabilityBottleneckReviewService,
    )

    payload = build_payload(county_id="harris", tax_years=[2025, 2026], limit=4)

    assert payload["county_id"] == "harris"
    assert payload["summary"]["overall_status"] == "investigate"
    assert payload["top_candidates"][0]["candidate_code"] == "step:property_roll:search_refresh:2026"
    assert payload["ingestion_step_candidates"][0]["latest_status"] == "failed"
