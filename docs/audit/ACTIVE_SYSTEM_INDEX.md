# Active System Index

This is an inventory of repository-visible implementation units at the audit
baseline. Counts are reproducible file/registration counts, not a claim that
each unit is production-ready.

| Area | Count | Audit definition / evidence |
| --- | ---: | --- |
| Mounted application handlers | 32 | FastAPI route decorators under `app/api/` on routers included by `app/api/router.py` |
| Service modules | 60 | Python modules in `app/services/`, excluding `__init__.py` |
| Registered jobs | 17 | entries in `app/jobs/cli.py:JOB_REGISTRY` |
| Operational scripts | 48 | 46 Python modules plus `run_api.sh` and `run_job.sh` in `infra/scripts/`; excludes `README.md` and `__init__.py` |
| SQL migrations | 77 | `app/db/migrations/*.sql` |
| Frontend pages | 19 | Next.js `page.tsx` files under `apps/web/app/` |

## Mounted system shape

- `app/main.py` creates the FastAPI application and mounts the router built by
  `app/api/router.py`.
- The router includes health, search, parcel, quote, lead, and admin route
  families.
- The web application includes public search, parcel, and quote-to-lead
  surfaces plus protected admin operations, leads, cases, and packet review.

## Important inventory caution

These counts are a baseline for future audits. They do not establish test
coverage, deployment status, data readiness, or launch approval. The 17-job
registry was blocked at the audit baseline by duplicate option registration.
The separately approved October 3, 2026 repair restores parser construction
and dispatch; 26 CLI tests and module help passed. Job implementation and live
execution readiness remain separate; see `DISCREPANCY_REPORT.md`.
