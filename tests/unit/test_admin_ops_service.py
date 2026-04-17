from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.services.admin_ops import AdminOpsService


def test_build_ingestion_step_run_adds_duration_and_retry_metadata() -> None:
    service = AdminOpsService()
    started_at = datetime(2026, 4, 17, 12, 0, tzinfo=timezone.utc)
    finished_at = started_at + timedelta(seconds=9)

    run = service._build_ingestion_step_run(
        {
            "step_run_id": "step-1",
            "step_name": "search_refresh",
            "status": "failed",
            "attempt_number": 2,
            "retry_of_step_run_id": "step-0",
            "started_at": started_at,
            "finished_at": finished_at,
            "row_count": 5,
            "error_message": "refresh failed",
            "details_json": {"maintenance_retry": True},
        }
    )

    assert run.duration_ms == 9000
    assert run.is_retry is True
    assert run.attempt_number == 2


def test_build_ingestion_step_summary_rows_groups_attempts_and_failures() -> None:
    service = AdminOpsService()
    latest_started_at = datetime(2026, 4, 17, 12, 10, tzinfo=timezone.utc)
    prior_started_at = datetime(2026, 4, 17, 12, 0, tzinfo=timezone.utc)

    step_runs = [
        service._build_ingestion_step_run(
            {
                "step_run_id": "step-2",
                "step_name": "search_refresh",
                "status": "succeeded",
                "attempt_number": 2,
                "retry_of_step_run_id": "step-1",
                "started_at": latest_started_at,
                "finished_at": latest_started_at + timedelta(seconds=4),
                "row_count": 5,
                "error_message": None,
                "details_json": {},
            }
        ),
        service._build_ingestion_step_run(
            {
                "step_run_id": "step-1",
                "step_name": "search_refresh",
                "status": "failed",
                "attempt_number": 1,
                "retry_of_step_run_id": None,
                "started_at": prior_started_at,
                "finished_at": prior_started_at + timedelta(seconds=7),
                "row_count": 5,
                "error_message": "refresh failed",
                "details_json": {},
            }
        ),
        service._build_ingestion_step_run(
            {
                "step_run_id": "step-3",
                "step_name": "tax_assignment_refresh",
                "status": "succeeded",
                "attempt_number": 1,
                "retry_of_step_run_id": None,
                "started_at": latest_started_at,
                "finished_at": latest_started_at + timedelta(seconds=2),
                "row_count": 5,
                "error_message": None,
                "details_json": {},
            }
        ),
    ]

    summaries = service._build_ingestion_step_summary_rows(step_runs)

    assert [summary.step_name for summary in summaries] == [
        "search_refresh",
        "tax_assignment_refresh",
    ]
    search_summary = summaries[0]
    assert search_summary.latest_status == "succeeded"
    assert search_summary.latest_attempt_number == 2
    assert search_summary.attempt_count == 2
    assert search_summary.retry_count == 1
    assert search_summary.failed_attempt_count == 1
    assert search_summary.latest_duration_ms == 4000
    assert search_summary.last_error_message is None


def test_build_job_run_adds_duration_ms() -> None:
    service = AdminOpsService()
    started_at = datetime(2026, 4, 17, 12, 0, tzinfo=timezone.utc)
    finished_at = started_at + timedelta(seconds=3)

    job_run = service._build_job_run(
        {
            "job_run_id": "job-1",
            "job_name": "job_normalize",
            "job_stage": "normalize",
            "status": "succeeded",
            "started_at": started_at,
            "finished_at": finished_at,
            "row_count": 10,
            "error_message": None,
            "metadata_json": {},
        }
    )

    assert job_run.duration_ms == 3000


def test_summarize_post_commit_maintenance_includes_attempts_retries_and_duration() -> None:
    service = AdminOpsService()
    latest_started_at = datetime(2026, 4, 17, 13, 0, tzinfo=timezone.utc)
    prior_started_at = datetime(2026, 4, 17, 12, 50, tzinfo=timezone.utc)

    summary = service._summarize_post_commit_maintenance(
        connection=None,
        import_batch_id="batch-1",
        dataset_type="property_roll",
        publish_state="published",
        prefetched_step_runs=[
            {
                "step_name": "search_refresh",
                "status": "failed",
                "retry_of_step_run_id": "step-1",
                "started_at": latest_started_at,
                "finished_at": latest_started_at + timedelta(seconds=6),
            },
            {
                "step_name": "search_refresh",
                "status": "failed",
                "retry_of_step_run_id": None,
                "started_at": prior_started_at,
                "finished_at": prior_started_at + timedelta(seconds=5),
            },
            {
                "step_name": "tax_assignment_refresh",
                "status": "succeeded",
                "retry_of_step_run_id": None,
                "started_at": prior_started_at,
                "finished_at": prior_started_at + timedelta(seconds=2),
            },
        ],
    )

    assert summary is not None
    assert summary["status"] == "failed"
    assert summary["failed_step_name"] == "search_refresh"
    assert summary["latest_step_name"] == "search_refresh"
    assert summary["latest_duration_ms"] == 6000
    assert summary["attempt_count"] == 3
    assert summary["retry_count"] == 1
