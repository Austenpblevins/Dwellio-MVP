from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.db.connection import get_connection


@dataclass(frozen=True)
class ScalabilityBottleneckCandidate:
    candidate_code: str
    label: str
    component_type: str
    component_key: str
    status: str
    tax_year: int
    run_count: int
    failed_count: int = 0
    retry_count: int = 0
    warning_count: int = 0
    fallback_count: int = 0
    avg_duration_ms: int | None = None
    p95_duration_ms: int | None = None
    max_duration_ms: int | None = None
    latest_duration_ms: int | None = None
    latest_status: str | None = None
    latest_finished_at: datetime | None = None
    latest_error_message: str | None = None
    recommendation: str = ""
    evidence_notes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ScalabilityBottleneckSummary:
    overall_status: str
    investigate_count: int
    monitor_count: int
    healthy_count: int
    top_candidate_code: str | None = None
    next_actions: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ScalabilityBottleneckReview:
    county_id: str
    tax_years: list[int]
    summary: ScalabilityBottleneckSummary
    top_candidates: list[ScalabilityBottleneckCandidate] = field(default_factory=list)
    ingestion_job_candidates: list[ScalabilityBottleneckCandidate] = field(default_factory=list)
    ingestion_step_candidates: list[ScalabilityBottleneckCandidate] = field(default_factory=list)
    instant_quote_refresh_candidates: list[ScalabilityBottleneckCandidate] = field(
        default_factory=list
    )


class ScalabilityBottleneckReviewService:
    def build_review(
        self,
        *,
        county_id: str,
        tax_years: list[int],
        limit: int = 5,
    ) -> ScalabilityBottleneckReview:
        if not tax_years:
            raise ValueError("tax_years must not be empty.")

        ordered_tax_years = sorted(set(tax_years))
        with get_connection() as connection:
            job_candidates = self._fetch_job_candidates(
                connection,
                county_id=county_id,
                tax_years=ordered_tax_years,
                limit=limit,
            )
            step_candidates = self._fetch_step_candidates(
                connection,
                county_id=county_id,
                tax_years=ordered_tax_years,
                limit=limit,
            )
            refresh_candidates = self._fetch_instant_quote_refresh_candidates(
                connection,
                county_id=county_id,
                tax_years=ordered_tax_years,
                limit=limit,
            )

        all_candidates = sorted(
            [*job_candidates, *step_candidates, *refresh_candidates],
            key=self._candidate_sort_key,
        )
        summary = self._build_summary(all_candidates)
        return ScalabilityBottleneckReview(
            county_id=county_id,
            tax_years=ordered_tax_years,
            summary=summary,
            top_candidates=all_candidates[:limit],
            ingestion_job_candidates=job_candidates,
            ingestion_step_candidates=step_candidates,
            instant_quote_refresh_candidates=refresh_candidates,
        )

    def _fetch_job_candidates(
        self,
        connection: Any,
        *,
        county_id: str,
        tax_years: list[int],
        limit: int,
    ) -> list[ScalabilityBottleneckCandidate]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH scoped AS (
                  SELECT
                    tax_year,
                    COALESCE(job_stage, 'unknown') AS job_stage,
                    job_name,
                    status,
                    started_at,
                    finished_at,
                    error_message,
                    CASE
                      WHEN finished_at IS NULL THEN NULL
                      ELSE ROUND(EXTRACT(EPOCH FROM (finished_at - started_at)) * 1000)::bigint
                    END AS duration_ms
                  FROM job_runs
                  WHERE county_id = %s
                    AND tax_year = ANY(%s::int[])
                ),
                latest AS (
                  SELECT DISTINCT ON (tax_year, job_stage)
                    tax_year,
                    job_stage,
                    status AS latest_status,
                    finished_at AS latest_finished_at,
                    duration_ms AS latest_duration_ms,
                    error_message AS latest_error_message
                  FROM scoped
                  ORDER BY tax_year, job_stage, started_at DESC, finished_at DESC NULLS LAST
                )
                SELECT
                  scoped.tax_year,
                  scoped.job_stage,
                  MIN(scoped.job_name) AS representative_job_name,
                  COUNT(*)::int AS run_count,
                  COUNT(*) FILTER (WHERE scoped.status = 'failed')::int AS failed_count,
                  ROUND(AVG(scoped.duration_ms) FILTER (WHERE scoped.duration_ms IS NOT NULL))::bigint AS avg_duration_ms,
                  ROUND(
                    percentile_cont(0.95) WITHIN GROUP (ORDER BY scoped.duration_ms)
                    FILTER (WHERE scoped.duration_ms IS NOT NULL)
                  )::bigint AS p95_duration_ms,
                  MAX(scoped.duration_ms)::bigint AS max_duration_ms,
                  latest.latest_status,
                  latest.latest_finished_at,
                  latest.latest_duration_ms,
                  latest.latest_error_message
                FROM scoped
                JOIN latest
                  ON latest.tax_year = scoped.tax_year
                 AND latest.job_stage = scoped.job_stage
                GROUP BY
                  scoped.tax_year,
                  scoped.job_stage,
                  latest.latest_status,
                  latest.latest_finished_at,
                  latest.latest_duration_ms,
                  latest.latest_error_message
                ORDER BY
                  scoped.tax_year DESC,
                  failed_count DESC,
                  p95_duration_ms DESC NULLS LAST,
                  max_duration_ms DESC NULLS LAST
                """,
                (county_id, tax_years),
            )
            rows = cursor.fetchall()

        candidates = [self._build_job_candidate(dict(row)) for row in rows]
        return sorted(candidates, key=self._candidate_sort_key)[:limit]

    def _fetch_step_candidates(
        self,
        connection: Any,
        *,
        county_id: str,
        tax_years: list[int],
        limit: int,
    ) -> list[ScalabilityBottleneckCandidate]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH scoped AS (
                  SELECT
                    ib.tax_year,
                    ib.dataset_type,
                    sr.step_name,
                    sr.status,
                    sr.retry_of_step_run_id,
                    sr.started_at,
                    sr.finished_at,
                    sr.error_message,
                    CASE
                      WHEN sr.finished_at IS NULL THEN NULL
                      ELSE ROUND(EXTRACT(EPOCH FROM (sr.finished_at - sr.started_at)) * 1000)::bigint
                    END AS duration_ms
                  FROM ingestion_step_runs sr
                  JOIN import_batches ib
                    ON ib.import_batch_id = sr.import_batch_id
                  WHERE ib.county_id = %s
                    AND ib.tax_year = ANY(%s::int[])
                ),
                latest AS (
                  SELECT DISTINCT ON (tax_year, dataset_type, step_name)
                    tax_year,
                    dataset_type,
                    step_name,
                    status AS latest_status,
                    finished_at AS latest_finished_at,
                    duration_ms AS latest_duration_ms,
                    error_message AS latest_error_message
                  FROM scoped
                  ORDER BY tax_year, dataset_type, step_name, started_at DESC, finished_at DESC NULLS LAST
                )
                SELECT
                  scoped.tax_year,
                  scoped.dataset_type,
                  scoped.step_name,
                  COUNT(*)::int AS run_count,
                  COUNT(*) FILTER (WHERE scoped.status = 'failed')::int AS failed_count,
                  COUNT(*) FILTER (WHERE scoped.retry_of_step_run_id IS NOT NULL)::int AS retry_count,
                  ROUND(AVG(scoped.duration_ms) FILTER (WHERE scoped.duration_ms IS NOT NULL))::bigint AS avg_duration_ms,
                  ROUND(
                    percentile_cont(0.95) WITHIN GROUP (ORDER BY scoped.duration_ms)
                    FILTER (WHERE scoped.duration_ms IS NOT NULL)
                  )::bigint AS p95_duration_ms,
                  MAX(scoped.duration_ms)::bigint AS max_duration_ms,
                  latest.latest_status,
                  latest.latest_finished_at,
                  latest.latest_duration_ms,
                  latest.latest_error_message
                FROM scoped
                JOIN latest
                  ON latest.tax_year = scoped.tax_year
                 AND latest.dataset_type = scoped.dataset_type
                 AND latest.step_name = scoped.step_name
                GROUP BY
                  scoped.tax_year,
                  scoped.dataset_type,
                  scoped.step_name,
                  latest.latest_status,
                  latest.latest_finished_at,
                  latest.latest_duration_ms,
                  latest.latest_error_message
                ORDER BY
                  scoped.tax_year DESC,
                  failed_count DESC,
                  retry_count DESC,
                  p95_duration_ms DESC NULLS LAST,
                  max_duration_ms DESC NULLS LAST
                """,
                (county_id, tax_years),
            )
            rows = cursor.fetchall()

        candidates = [self._build_step_candidate(dict(row)) for row in rows]
        return sorted(candidates, key=self._candidate_sort_key)[:limit]

    def _fetch_instant_quote_refresh_candidates(
        self,
        connection: Any,
        *,
        county_id: str,
        tax_years: list[int],
        limit: int,
    ) -> list[ScalabilityBottleneckCandidate]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH scoped AS (
                  SELECT
                    tax_year,
                    refresh_status,
                    refresh_started_at,
                    refresh_finished_at,
                    error_message,
                    total_refresh_duration_ms,
                    tax_rate_basis_fallback_applied,
                    COALESCE(array_length(warning_codes, 1), 0)
                      + COALESCE(array_length(tax_rate_basis_warning_codes, 1), 0)
                      AS warning_code_count
                  FROM instant_quote_refresh_runs
                  WHERE county_id = %s
                    AND tax_year = ANY(%s::int[])
                ),
                latest AS (
                  SELECT DISTINCT ON (tax_year)
                    tax_year,
                    refresh_status AS latest_status,
                    refresh_finished_at AS latest_finished_at,
                    total_refresh_duration_ms AS latest_duration_ms,
                    error_message AS latest_error_message
                  FROM scoped
                  ORDER BY tax_year, refresh_started_at DESC, refresh_finished_at DESC NULLS LAST
                )
                SELECT
                  scoped.tax_year,
                  COUNT(*)::int AS run_count,
                  COUNT(*) FILTER (WHERE scoped.refresh_status = 'failed')::int AS failed_count,
                  COUNT(*) FILTER (WHERE scoped.tax_rate_basis_fallback_applied)::int AS fallback_count,
                  COUNT(*) FILTER (WHERE scoped.warning_code_count > 0)::int AS warning_count,
                  ROUND(AVG(scoped.total_refresh_duration_ms) FILTER (WHERE scoped.total_refresh_duration_ms IS NOT NULL))::bigint AS avg_duration_ms,
                  ROUND(
                    percentile_cont(0.95) WITHIN GROUP (ORDER BY scoped.total_refresh_duration_ms)
                    FILTER (WHERE scoped.total_refresh_duration_ms IS NOT NULL)
                  )::bigint AS p95_duration_ms,
                  MAX(scoped.total_refresh_duration_ms)::bigint AS max_duration_ms,
                  latest.latest_status,
                  latest.latest_finished_at,
                  latest.latest_duration_ms,
                  latest.latest_error_message
                FROM scoped
                JOIN latest USING (tax_year)
                GROUP BY
                  scoped.tax_year,
                  latest.latest_status,
                  latest.latest_finished_at,
                  latest.latest_duration_ms,
                  latest.latest_error_message
                ORDER BY
                  scoped.tax_year DESC,
                  failed_count DESC,
                  warning_count DESC,
                  p95_duration_ms DESC NULLS LAST,
                  max_duration_ms DESC NULLS LAST
                """,
                (county_id, tax_years),
            )
            rows = cursor.fetchall()

        candidates = [self._build_refresh_candidate(dict(row)) for row in rows]
        return sorted(candidates, key=self._candidate_sort_key)[:limit]

    def _build_job_candidate(self, row: dict[str, Any]) -> ScalabilityBottleneckCandidate:
        status = self._classify_candidate(
            component_type="ingestion_job",
            failed_count=int(row.get("failed_count") or 0),
            p95_duration_ms=self._optional_int(row.get("p95_duration_ms")),
            max_duration_ms=self._optional_int(row.get("max_duration_ms")),
            latest_status=row.get("latest_status"),
        )
        job_stage = row["job_stage"]
        tax_year = int(row["tax_year"])
        recommendation = self._recommendation_for(
            component_type="ingestion_job",
            status=status,
            component_key=job_stage,
        )
        notes = self._build_notes(
            failed_count=int(row.get("failed_count") or 0),
            retry_count=0,
            warning_count=0,
            fallback_count=0,
            p95_duration_ms=self._optional_int(row.get("p95_duration_ms")),
            max_duration_ms=self._optional_int(row.get("max_duration_ms")),
            latest_status=row.get("latest_status"),
            latest_error_message=row.get("latest_error_message"),
        )
        return ScalabilityBottleneckCandidate(
            candidate_code=f"job:{job_stage}:{tax_year}",
            label=f"{job_stage.replace('_', ' ').title()} jobs ({tax_year})",
            component_type="ingestion_job",
            component_key=job_stage,
            status=status,
            tax_year=tax_year,
            run_count=int(row.get("run_count") or 0),
            failed_count=int(row.get("failed_count") or 0),
            avg_duration_ms=self._optional_int(row.get("avg_duration_ms")),
            p95_duration_ms=self._optional_int(row.get("p95_duration_ms")),
            max_duration_ms=self._optional_int(row.get("max_duration_ms")),
            latest_duration_ms=self._optional_int(row.get("latest_duration_ms")),
            latest_status=row.get("latest_status"),
            latest_finished_at=row.get("latest_finished_at"),
            latest_error_message=row.get("latest_error_message"),
            recommendation=recommendation,
            evidence_notes=notes,
        )

    def _build_step_candidate(self, row: dict[str, Any]) -> ScalabilityBottleneckCandidate:
        status = self._classify_candidate(
            component_type="ingestion_step",
            failed_count=int(row.get("failed_count") or 0),
            retry_count=int(row.get("retry_count") or 0),
            p95_duration_ms=self._optional_int(row.get("p95_duration_ms")),
            max_duration_ms=self._optional_int(row.get("max_duration_ms")),
            latest_status=row.get("latest_status"),
        )
        dataset_type = row["dataset_type"]
        step_name = row["step_name"]
        tax_year = int(row["tax_year"])
        recommendation = self._recommendation_for(
            component_type="ingestion_step",
            status=status,
            component_key=step_name,
        )
        notes = self._build_notes(
            failed_count=int(row.get("failed_count") or 0),
            retry_count=int(row.get("retry_count") or 0),
            warning_count=0,
            fallback_count=0,
            p95_duration_ms=self._optional_int(row.get("p95_duration_ms")),
            max_duration_ms=self._optional_int(row.get("max_duration_ms")),
            latest_status=row.get("latest_status"),
            latest_error_message=row.get("latest_error_message"),
        )
        return ScalabilityBottleneckCandidate(
            candidate_code=f"step:{dataset_type}:{step_name}:{tax_year}",
            label=f"{dataset_type.replace('_', ' ').title()} {step_name.replace('_', ' ')} ({tax_year})",
            component_type="ingestion_step",
            component_key=f"{dataset_type}:{step_name}",
            status=status,
            tax_year=tax_year,
            run_count=int(row.get("run_count") or 0),
            failed_count=int(row.get("failed_count") or 0),
            retry_count=int(row.get("retry_count") or 0),
            avg_duration_ms=self._optional_int(row.get("avg_duration_ms")),
            p95_duration_ms=self._optional_int(row.get("p95_duration_ms")),
            max_duration_ms=self._optional_int(row.get("max_duration_ms")),
            latest_duration_ms=self._optional_int(row.get("latest_duration_ms")),
            latest_status=row.get("latest_status"),
            latest_finished_at=row.get("latest_finished_at"),
            latest_error_message=row.get("latest_error_message"),
            recommendation=recommendation,
            evidence_notes=notes,
        )

    def _build_refresh_candidate(self, row: dict[str, Any]) -> ScalabilityBottleneckCandidate:
        status = self._classify_candidate(
            component_type="instant_quote_refresh",
            failed_count=int(row.get("failed_count") or 0),
            warning_count=int(row.get("warning_count") or 0),
            fallback_count=int(row.get("fallback_count") or 0),
            p95_duration_ms=self._optional_int(row.get("p95_duration_ms")),
            max_duration_ms=self._optional_int(row.get("max_duration_ms")),
            latest_status=row.get("latest_status"),
        )
        tax_year = int(row["tax_year"])
        recommendation = self._recommendation_for(
            component_type="instant_quote_refresh",
            status=status,
            component_key="instant_quote_refresh",
        )
        notes = self._build_notes(
            failed_count=int(row.get("failed_count") or 0),
            retry_count=0,
            warning_count=int(row.get("warning_count") or 0),
            fallback_count=int(row.get("fallback_count") or 0),
            p95_duration_ms=self._optional_int(row.get("p95_duration_ms")),
            max_duration_ms=self._optional_int(row.get("max_duration_ms")),
            latest_status=row.get("latest_status"),
            latest_error_message=row.get("latest_error_message"),
        )
        return ScalabilityBottleneckCandidate(
            candidate_code=f"instant_quote_refresh:{tax_year}",
            label=f"Instant quote refresh ({tax_year})",
            component_type="instant_quote_refresh",
            component_key="instant_quote_refresh",
            status=status,
            tax_year=tax_year,
            run_count=int(row.get("run_count") or 0),
            failed_count=int(row.get("failed_count") or 0),
            warning_count=int(row.get("warning_count") or 0),
            fallback_count=int(row.get("fallback_count") or 0),
            avg_duration_ms=self._optional_int(row.get("avg_duration_ms")),
            p95_duration_ms=self._optional_int(row.get("p95_duration_ms")),
            max_duration_ms=self._optional_int(row.get("max_duration_ms")),
            latest_duration_ms=self._optional_int(row.get("latest_duration_ms")),
            latest_status=row.get("latest_status"),
            latest_finished_at=row.get("latest_finished_at"),
            latest_error_message=row.get("latest_error_message"),
            recommendation=recommendation,
            evidence_notes=notes,
        )

    def _classify_candidate(
        self,
        *,
        component_type: str,
        failed_count: int,
        retry_count: int = 0,
        warning_count: int = 0,
        fallback_count: int = 0,
        p95_duration_ms: int | None = None,
        max_duration_ms: int | None = None,
        latest_status: str | None = None,
    ) -> str:
        if component_type == "instant_quote_refresh":
            monitor_duration_ms = 300_000
            investigate_duration_ms = 900_000
        else:
            monitor_duration_ms = 120_000
            investigate_duration_ms = 300_000

        if (
            failed_count > 0
            or latest_status == "failed"
            or (p95_duration_ms is not None and p95_duration_ms >= investigate_duration_ms)
            or (max_duration_ms is not None and max_duration_ms >= investigate_duration_ms * 2)
        ):
            return "investigate"
        if (
            retry_count > 0
            or warning_count > 0
            or fallback_count > 0
            or latest_status in {"running", "pending"}
            or (p95_duration_ms is not None and p95_duration_ms >= monitor_duration_ms)
            or (max_duration_ms is not None and max_duration_ms >= investigate_duration_ms)
        ):
            return "monitor"
        return "healthy"

    def _recommendation_for(
        self,
        *,
        component_type: str,
        status: str,
        component_key: str,
    ) -> str:
        if status == "healthy":
            return "No immediate optimization work is justified; keep collecting telemetry."
        if component_type == "ingestion_job":
            return (
                f"Review batch row counts, query plans, and downstream dependencies for `{component_key}` "
                "before proposing any refactor."
            )
        if component_type == "ingestion_step":
            return (
                f"Profile `{component_key}` against recent batch sizes and retry patterns before changing "
                "orchestration or SQL."
            )
        return (
            "Inspect refresh duration components, basis fallback usage, and warning trends before changing "
            "quote-refresh architecture."
        )

    def _build_notes(
        self,
        *,
        failed_count: int,
        retry_count: int,
        warning_count: int,
        fallback_count: int,
        p95_duration_ms: int | None,
        max_duration_ms: int | None,
        latest_status: str | None,
        latest_error_message: str | None,
    ) -> list[str]:
        notes: list[str] = []
        if failed_count:
            notes.append(f"{failed_count} failed run(s) in the review window.")
        if retry_count:
            notes.append(f"{retry_count} retry attempt(s) recorded in the review window.")
        if warning_count:
            notes.append(f"{warning_count} run(s) emitted warning codes.")
        if fallback_count:
            notes.append(f"{fallback_count} run(s) relied on a fallback basis.")
        if p95_duration_ms is not None:
            notes.append(f"p95 runtime: {p95_duration_ms} ms.")
        if max_duration_ms is not None:
            notes.append(f"Max runtime: {max_duration_ms} ms.")
        if latest_status is not None:
            notes.append(f"Latest status: {latest_status}.")
        if latest_error_message:
            notes.append(f"Latest error: {latest_error_message}")
        return notes

    def _build_summary(
        self,
        candidates: list[ScalabilityBottleneckCandidate],
    ) -> ScalabilityBottleneckSummary:
        investigate_count = sum(1 for candidate in candidates if candidate.status == "investigate")
        monitor_count = sum(1 for candidate in candidates if candidate.status == "monitor")
        healthy_count = sum(1 for candidate in candidates if candidate.status == "healthy")
        if investigate_count:
            overall_status = "investigate"
        elif monitor_count:
            overall_status = "monitor"
        else:
            overall_status = "healthy"

        next_actions: list[str] = []
        top_candidate = candidates[0] if candidates else None
        if top_candidate is not None:
            next_actions.append(top_candidate.recommendation)
        if investigate_count > 1:
            next_actions.append(
                "Rank the investigate candidates by county-year impact before approving any deeper refactor."
            )
        if not next_actions:
            next_actions.append("Keep collecting telemetry until a clear bottleneck emerges.")

        return ScalabilityBottleneckSummary(
            overall_status=overall_status,
            investigate_count=investigate_count,
            monitor_count=monitor_count,
            healthy_count=healthy_count,
            top_candidate_code=None if top_candidate is None else top_candidate.candidate_code,
            next_actions=next_actions,
        )

    def _candidate_sort_key(
        self,
        candidate: ScalabilityBottleneckCandidate,
    ) -> tuple[int, int, int, int, int]:
        status_rank = {"investigate": 0, "monitor": 1, "healthy": 2}.get(candidate.status, 3)
        return (
            status_rank,
            -(candidate.p95_duration_ms or 0),
            -(candidate.max_duration_ms or 0),
            -candidate.failed_count,
            -candidate.tax_year,
        )

    def _optional_int(self, value: Any) -> int | None:
        if value is None:
            return None
        return int(value)
