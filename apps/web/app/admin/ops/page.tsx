import Link from "next/link";

import { AdminOpsNav } from "../_lib/AdminOpsNav";
import {
  getCompletenessIssues,
  getImportBatches,
  getReadinessDashboard,
  getTaxAssignmentIssues,
} from "../_lib/api";
import { requireAdminToken } from "../_lib/auth";

const DEFAULT_COUNTY = "harris";
const DEFAULT_YEARS = [2026, 2025, 2024];

function parseYears(input: string | undefined): number[] {
  if (!input) {
    return DEFAULT_YEARS;
  }
  const parsed = input
    .split(",")
    .map((value) => Number.parseInt(value.trim(), 10))
    .filter((value) => Number.isInteger(value));
  return parsed.length > 0 ? parsed : DEFAULT_YEARS;
}

function formatLabel(value: string): string {
  return value.replaceAll("_", " ");
}

export default async function AdminOpsOverviewPage({
  searchParams,
}: {
  searchParams?: Promise<{ county?: string; years?: string; tax_year?: string }>;
}) {
  const params = (await searchParams) ?? {};
  const countyId = params.county ?? DEFAULT_COUNTY;
  const taxYears = parseYears(params.years);
  const focusYear = Number.parseInt(params.tax_year ?? String(taxYears[0]), 10) || taxYears[0];

  await requireAdminToken();

  let data:
    | Awaited<ReturnType<typeof getReadinessDashboard>> & {
        batches: Awaited<ReturnType<typeof getImportBatches>>;
        completeness: Awaited<ReturnType<typeof getCompletenessIssues>>;
        taxAssignment: Awaited<ReturnType<typeof getTaxAssignmentIssues>>;
      }
    | null = null;
  let errorMessage: string | null = null;

  try {
    const [readiness, batches, completeness, taxAssignment] = await Promise.all([
      getReadinessDashboard(countyId, taxYears),
      getImportBatches(countyId, focusYear),
      getCompletenessIssues(countyId, focusYear),
      getTaxAssignmentIssues(countyId, focusYear),
    ]);
    data = { ...readiness, batches, completeness, taxAssignment };
  } catch (error) {
    errorMessage = error instanceof Error ? error.message : "unknown";
  }

  if (!data) {
    return (
      <main className="min-h-screen bg-slate-100 px-6 py-12 text-slate-950">
        <section className="mx-auto max-w-4xl rounded-[2rem] border border-rose-200 bg-rose-50 p-8 text-rose-950">
          <AdminOpsNav />
          <h1 className="mt-6 text-3xl font-semibold">Admin API connection needed</h1>
          <p className="mt-4 text-sm leading-7">
            The admin ops overview could not reach the backend API. Error: {errorMessage ?? "unknown"}
          </p>
        </section>
      </main>
    );
  }

  const { readiness_rows, batches, completeness, taxAssignment } = data;

  return (
    <main className="min-h-screen bg-[linear-gradient(180deg,#f3ede2_0%,#eef3f8_55%,#ffffff_100%)] px-6 py-10 text-slate-950">
      <section className="mx-auto flex max-w-7xl flex-col gap-8">
        <header className="rounded-[2rem] border border-slate-200/80 bg-white/90 p-8 shadow-[0_20px_60px_rgba(15,23,42,0.08)]">
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">
            Admin ingestion ops
          </p>
          <div className="mt-4 flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
            <div className="max-w-3xl">
              <h1 className="text-4xl font-semibold tracking-tight text-slate-950">
                Ingestion and QA operations
              </h1>
              <p className="mt-4 text-base leading-7 text-slate-600">
                This internal surface is the operator cockpit for raw-file intake, validation,
                publish state, rollback support, and parcel/tax QA. It extends the existing
                ingestion backbone rather than introducing a second ops system.
              </p>
            </div>
            <div className="rounded-3xl bg-slate-950 px-5 py-4 text-sm text-slate-50">
              <div className="font-semibold uppercase tracking-[0.22em] text-slate-300">
                Current scope
              </div>
              <div className="mt-2">{countyId}</div>
              <div className="text-slate-300">Years {taxYears.join(", ")}</div>
            </div>
          </div>
          <div className="mt-6">
            <AdminOpsNav />
          </div>
        </header>

        <section className="grid gap-6 lg:grid-cols-[1.4fr,0.9fr]">
          <div className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <div className="flex items-center justify-between">
              <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">
                County-year readiness
              </h2>
              <Link href={`/admin/readiness?county=${countyId}&years=${taxYears.join(",")}`} className="text-sm font-semibold text-slate-700 underline-offset-4 hover:underline">
                Open readiness detail
              </Link>
            </div>
            <div className="mt-5 grid gap-4">
              {readiness_rows.map((row) => (
                <article
                  key={`${row.county_id}-${row.tax_year}`}
                  className="rounded-3xl border border-slate-200 bg-slate-50 p-5"
                >
                  <div className="flex flex-wrap items-center gap-3">
                    <h3 className="text-2xl font-semibold text-slate-950">
                      {row.county_id} {row.tax_year}
                    </h3>
                    <span className="rounded-full bg-slate-950 px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] text-white">
                      {formatLabel(row.overall_status)}
                    </span>
                    <span className="rounded-full bg-slate-200 px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] text-slate-800">
                      {formatLabel(row.trend_label)}
                    </span>
                  </div>
                  <p className="mt-3 text-sm leading-7 text-slate-600">
                    Readiness score {row.readiness_score}/100.
                    {row.trend_delta !== null ? ` ${row.trend_delta >= 0 ? "+" : ""}${row.trend_delta} vs prior year.` : ""}
                  </p>
                  <div className="mt-4 flex flex-wrap gap-2">
                    {row.blockers.slice(0, 6).map((blocker) => (
                      <span key={blocker} className="rounded-full bg-rose-100 px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] text-rose-900">
                        {formatLabel(blocker)}
                      </span>
                    ))}
                  </div>
                </article>
              ))}
            </div>
          </div>

          <div className="grid gap-6">
            <section className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
              <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">
                QA counts for {focusYear}
              </h2>
              <div className="mt-5 grid gap-4 sm:grid-cols-2">
                <div className="rounded-3xl bg-slate-100 p-5">
                  <div className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">
                    Import batches
                  </div>
                  <div className="mt-3 text-4xl font-semibold">{batches.batches.length}</div>
                </div>
                <div className="rounded-3xl bg-slate-100 p-5">
                  <div className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">
                    Completeness issues
                  </div>
                  <div className="mt-3 text-4xl font-semibold">{completeness.issues.length}</div>
                </div>
                <div className="rounded-3xl bg-slate-100 p-5">
                  <div className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">
                    Tax assignment issues
                  </div>
                  <div className="mt-3 text-4xl font-semibold">{taxAssignment.issues.length}</div>
                </div>
                <div className="rounded-3xl bg-slate-100 p-5">
                  <div className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">
                    Validation errors
                  </div>
                  <div className="mt-3 text-4xl font-semibold">
                    {batches.batches.reduce((sum, batch) => sum + batch.validation_error_count, 0)}
                  </div>
                </div>
              </div>
            </section>

            <section className="rounded-[2rem] border border-slate-200/80 bg-slate-950 p-7 text-slate-50 shadow-[0_18px_45px_rgba(15,23,42,0.14)]">
              <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-300">
                Operator workflow
              </h2>
              <ol className="mt-5 space-y-3 text-sm leading-7 text-slate-100">
                <li>1. Inspect batches and validation results for the target county-year.</li>
                <li>2. Register manual files when automation or current-year availability is weak.</li>
                <li>3. Publish only after staged validation is acceptable.</li>
                <li>4. Roll back a bad publish using the import-batch detail surface.</li>
                <li>5. Review completeness and tax assignment issues before downstream valuation QA.</li>
              </ol>
            </section>
          </div>
        </section>

        <section className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">
              Recent import batches
            </h2>
            <Link href={`/admin/ops/jobs?county=${countyId}&tax_year=${focusYear}`} className="text-sm font-semibold text-slate-700 underline-offset-4 hover:underline">
              Open jobs dashboard
            </Link>
          </div>
          <div className="mt-5 overflow-x-auto">
            <table className="min-w-full text-left text-sm text-slate-700">
              <thead className="text-xs uppercase tracking-[0.18em] text-slate-500">
                <tr>
                  <th className="pb-3 pr-6">Dataset</th>
                  <th className="pb-3 pr-6">Status</th>
                  <th className="pb-3 pr-6">Publish</th>
                  <th className="pb-3 pr-6">Validation Errors</th>
                  <th className="pb-3 pr-6">Latest Job</th>
                  <th className="pb-3 pr-6">Action</th>
                </tr>
              </thead>
              <tbody>
                {batches.batches.map((batch) => (
                  <tr key={batch.import_batch_id} className="border-t border-slate-200">
                    <td className="py-4 pr-6">
                      <div className="font-semibold text-slate-950">{formatLabel(batch.dataset_type)}</div>
                      <div className="text-xs text-slate-500">{batch.source_system_code}</div>
                    </td>
                    <td className="py-4 pr-6">{formatLabel(batch.status)}</td>
                    <td className="py-4 pr-6">{formatLabel(batch.publish_state ?? "draft")}</td>
                    <td className="py-4 pr-6">{batch.validation_error_count}</td>
                    <td className="py-4 pr-6">
                      {batch.latest_job_name ? `${batch.latest_job_name} / ${batch.latest_job_status}` : "none"}
                    </td>
                    <td className="py-4 pr-6">
                      <Link
                        href={`/admin/ops/jobs/${batch.import_batch_id}`}
                        className="font-semibold text-slate-700 underline-offset-4 hover:underline"
                      >
                        Inspect batch
                      </Link>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      </section>
    </main>
  );
}
