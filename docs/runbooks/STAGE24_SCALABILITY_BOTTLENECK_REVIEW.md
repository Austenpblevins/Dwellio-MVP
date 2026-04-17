# Stage 24 Scalability Bottleneck Review

Stage 24.3 is an internal review layer that turns persisted runtime telemetry into a ranked list
of bottleneck candidates. The goal is to decide whether a deeper refactor is justified before we
start changing ingestion or instant-quote architecture.

## Sources Used

- `job_runs`
  - grouped by `tax_year` and `job_stage`
- `ingestion_step_runs`
  - grouped by `tax_year`, `dataset_type`, and `step_name`
- `instant_quote_refresh_runs`
  - grouped by `tax_year`

The review is intentionally evidence-first. It does not mutate state and it does not trigger
refreshes, retries, or publishes.

## Internal Surfaces

- Script:
  - `python -m infra.scripts.report_scalability_bottlenecks --county-id harris --tax-years 2025 2026`
- Internal admin API:
  - `GET /admin/scalability/{county_id}?tax_years=2025&tax_years=2026&limit=5`

## Output Shape

The review returns:

- `summary`
  - overall posture: `healthy`, `monitor`, or `investigate`
  - counts by status
  - top candidate code
  - next actions
- `top_candidates`
  - best ranked candidates across all telemetry sources
- `ingestion_job_candidates`
  - county-year candidates grouped by `job_stage`
- `ingestion_step_candidates`
  - county-year candidates grouped by `dataset_type` + `step_name`
- `instant_quote_refresh_candidates`
  - county-year instant-quote refresh candidates

Each candidate includes:

- run counts
- failure counts
- retry counts where applicable
- warning/fallback counts where applicable
- average / p95 / max runtime
- latest status and error
- recommendation
- evidence notes

## How To Use It

1. Start with `summary.overall_status`.
2. Read `top_candidates` before looking at the full section lists.
3. Treat `investigate` as “collect row-count, query-plan, and dependency evidence before changing architecture.”
4. Treat `monitor` as “keep watching; do not refactor yet unless product impact grows.”
5. Treat `healthy` as “no refactor justified from current telemetry.”

## Interpretation Guidance

- `investigate`
  - failures exist, or p95/max runtime is already well beyond the review thresholds
  - deeper analysis is justified before code changes
- `monitor`
  - retries, warnings, fallback behavior, or moderately heavy runtime are present
  - continue gathering evidence and watch trend direction
- `healthy`
  - current telemetry does not justify optimization work

This review is not a replacement for Stage 22/23 operator safeguards. A candidate can be
operationally safe but still expensive, or operationally noisy without being a proven scalability
problem yet.

## Recommended Follow-Through

For any `investigate` candidate:

1. capture representative batch sizes or quote-refresh cohort sizes
2. capture the latest failure/warning context
3. inspect query plans or downstream dependency timing
4. confirm whether the issue is repeatable across multiple runs
5. only then propose a Stage 24.4 architecture change
