from __future__ import annotations

from app.services.scalability_bottlenecks import ScalabilityBottleneckReviewService


def test_build_job_candidate_marks_investigate_for_failures_and_runtime() -> None:
    service = ScalabilityBottleneckReviewService()

    candidate = service._build_job_candidate(
        {
            "tax_year": 2026,
            "job_stage": "normalize",
            "representative_job_name": "job_normalize",
            "run_count": 4,
            "failed_count": 1,
            "avg_duration_ms": 120000,
            "p95_duration_ms": 410000,
            "max_duration_ms": 620000,
            "latest_status": "failed",
            "latest_finished_at": None,
            "latest_duration_ms": 620000,
            "latest_error_message": "search refresh failed",
        }
    )

    assert candidate.status == "investigate"
    assert candidate.component_type == "ingestion_job"
    assert candidate.candidate_code == "job:normalize:2026"
    assert "failed run" in candidate.evidence_notes[0]


def test_build_step_candidate_marks_monitor_for_retries() -> None:
    service = ScalabilityBottleneckReviewService()

    candidate = service._build_step_candidate(
        {
            "tax_year": 2025,
            "dataset_type": "property_roll",
            "step_name": "search_refresh",
            "run_count": 3,
            "failed_count": 0,
            "retry_count": 1,
            "avg_duration_ms": 95000,
            "p95_duration_ms": 130000,
            "max_duration_ms": 180000,
            "latest_status": "succeeded",
            "latest_finished_at": None,
            "latest_duration_ms": 110000,
            "latest_error_message": None,
        }
    )

    assert candidate.status == "monitor"
    assert candidate.retry_count == 1
    assert candidate.component_key == "property_roll:search_refresh"


def test_build_refresh_candidate_marks_monitor_for_fallback_and_warnings() -> None:
    service = ScalabilityBottleneckReviewService()

    candidate = service._build_refresh_candidate(
        {
            "tax_year": 2025,
            "run_count": 2,
            "failed_count": 0,
            "fallback_count": 1,
            "warning_count": 2,
            "avg_duration_ms": 220000,
            "p95_duration_ms": 260000,
            "max_duration_ms": 290000,
            "latest_status": "completed",
            "latest_finished_at": None,
            "latest_duration_ms": 240000,
            "latest_error_message": None,
        }
    )

    assert candidate.status == "monitor"
    assert candidate.fallback_count == 1
    assert candidate.warning_count == 2


def test_build_summary_counts_candidates_and_uses_top_recommendation() -> None:
    service = ScalabilityBottleneckReviewService()
    candidates = [
        service._build_step_candidate(
            {
                "tax_year": 2026,
                "dataset_type": "property_roll",
                "step_name": "search_refresh",
                "run_count": 2,
                "failed_count": 1,
                "retry_count": 1,
                "avg_duration_ms": 180000,
                "p95_duration_ms": 500000,
                "max_duration_ms": 600000,
                "latest_status": "failed",
                "latest_finished_at": None,
                "latest_duration_ms": 600000,
                "latest_error_message": "refresh failed",
            }
        ),
        service._build_refresh_candidate(
            {
                "tax_year": 2025,
                "run_count": 3,
                "failed_count": 0,
                "fallback_count": 1,
                "warning_count": 1,
                "avg_duration_ms": 220000,
                "p95_duration_ms": 280000,
                "max_duration_ms": 290000,
                "latest_status": "completed",
                "latest_finished_at": None,
                "latest_duration_ms": 240000,
                "latest_error_message": None,
            }
        ),
    ]

    ordered = sorted(candidates, key=service._candidate_sort_key)
    summary = service._build_summary(ordered)

    assert summary.overall_status == "investigate"
    assert summary.investigate_count == 1
    assert summary.monitor_count == 1
    assert summary.top_candidate_code == ordered[0].candidate_code
    assert summary.next_actions
