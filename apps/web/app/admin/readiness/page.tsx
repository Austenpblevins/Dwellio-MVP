import { AdminOpsNav } from "../_lib/AdminOpsNav";
import { getReadinessDashboard } from "../_lib/api";
import { requireAdminToken } from "../_lib/auth";
import type { AdminCountyYearReadinessDashboard } from "../_lib/types";

const DEFAULT_COUNTY = "harris";
const DEFAULT_YEARS = [2026, 2025, 2024];

function parseTaxYears(input: string | undefined): number[] {
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

function statusTone(status: string): string {
  if (status === "quote_ready" || status === "derived_ready") {
    return "bg-emerald-100 text-emerald-900";
  }
  if (status === "canonical_partial" || status === "source_acquired") {
    return "bg-amber-100 text-amber-950";
  }
  if (status === "awaiting_source_data" || status === "tax_year_missing") {
    return "bg-rose-100 text-rose-950";
  }
  return "bg-slate-200 text-slate-900";
}

function StatusPill({ label, status }: { label: string; status: string }) {
  return (
    <span
      className={`inline-flex rounded-full px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] ${statusTone(status)}`}
    >
      {formatLabel(label)}
    </span>
  );
}

export default async function AdminReadinessPage({
  searchParams,
}: {
  searchParams?: Promise<{ county?: string; years?: string }>;
}) {
  const params = (await searchParams) ?? {};
  const countyId = params.county ?? DEFAULT_COUNTY;
  const taxYears = parseTaxYears(params.years);

  await requireAdminToken();

  let dashboard: AdminCountyYearReadinessDashboard | null = null;
  let errorMessage: string | null = null;

  try {
    dashboard = await getReadinessDashboard(countyId, taxYears);
  } catch (error) {
    errorMessage =
      error instanceof Error ? error.message : "Readiness data is currently unavailable.";
  }

  return (
    <main className="min-h-screen bg-[linear-gradient(180deg,#f7f4ed_0%,#eef2f7_55%,#ffffff_100%)] px-6 py-10 text-slate-950">
      <section className="mx-auto flex w-full max-w-6xl flex-col gap-8">
        <header className="rounded-[2rem] border border-slate-200/80 bg-white/90 p-8 shadow-[0_20px_60px_rgba(15,23,42,0.08)]">
          <div className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
            <div className="max-w-3xl">
              <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">
                Internal readiness
              </p>
              <h1 className="mt-3 text-4xl font-semibold tracking-tight text-slate-950">
                County-year readiness view
              </h1>
              <p className="mt-4 max-w-2xl text-base leading-7 text-slate-600">
                Use this page to verify whether a county-year is actually ready for ingestion,
                canonical publish, derived refresh, and later quote-related work. Prefer fuller
                prior years such as 2025 when 2026 is still sparse.
              </p>
            </div>
            <div className="rounded-3xl bg-slate-950 px-5 py-4 text-sm text-slate-50">
              <div className="font-semibold uppercase tracking-[0.22em] text-slate-300">
                Current query
              </div>
              <div className="mt-2">{countyId}</div>
              <div className="text-slate-300">{taxYears.join(", ")}</div>
            </div>
          </div>
          <div className="mt-6">
            <AdminOpsNav />
          </div>
        </header>

        {errorMessage ? (
          <section className="rounded-[2rem] border border-rose-200 bg-rose-50 p-8 text-rose-950 shadow-sm">
            <h2 className="text-xl font-semibold">Internal API connection needed</h2>
            <p className="mt-3 max-w-2xl text-sm leading-7">
              The readiness page could not reach the protected admin API. Confirm the backend is
              running, the admin token cookie is set, and the web app can reach the configured API
              base URL.
            </p>
            <p className="mt-3 text-sm font-medium">Error: {errorMessage}</p>
          </section>
        ) : null}

        {dashboard ? (
          <section className="grid gap-6">
            {dashboard.readiness_rows.map((row) => (
              <article
                key={`${row.county_id}-${row.tax_year}`}
                className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]"
              >
                <div className="flex flex-col gap-5 lg:flex-row lg:items-start lg:justify-between">
                  <div>
                    <div className="flex flex-wrap items-center gap-3">
                      <h2 className="text-3xl font-semibold tracking-tight text-slate-950">
                        {row.county_id} {row.tax_year}
                      </h2>
                      <StatusPill label={row.overall_status} status={row.overall_status} />
                      <span className="inline-flex rounded-full bg-slate-200 px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] text-slate-900">
                        {formatLabel(row.trend_label)}
                      </span>
                    </div>
                    <p className="mt-3 max-w-2xl text-sm leading-7 text-slate-600">
                      Readiness score {row.readiness_score}/100
                      {row.trend_delta !== null
                        ? `, ${row.trend_delta >= 0 ? "+" : ""}${row.trend_delta} vs prior year`
                        : ""}
                      . Blockers are internal-only and do not flow into public parcel or quote
                      APIs.
                    </p>
                  </div>
                  <dl className="grid grid-cols-2 gap-4 rounded-3xl bg-slate-100 p-5 text-sm text-slate-700 sm:grid-cols-3">
                    <div>
                      <dt className="text-xs uppercase tracking-[0.2em] text-slate-500">
                        Parcel summary
                      </dt>
                      <dd className="mt-2 font-semibold">{row.derived.parcel_summary_row_count}</dd>
                    </div>
                    <div>
                      <dt className="text-xs uppercase tracking-[0.2em] text-slate-500">
                        Search docs
                      </dt>
                      <dd className="mt-2 font-semibold">{row.derived.search_document_row_count}</dd>
                    </div>
                    <div>
                      <dt className="text-xs uppercase tracking-[0.2em] text-slate-500">
                        Trend rows
                      </dt>
                      <dd className="mt-2 font-semibold">{row.derived.parcel_year_trend_row_count}</dd>
                    </div>
                    <div>
                      <dt className="text-xs uppercase tracking-[0.2em] text-slate-500">
                        Feature rows
                      </dt>
                      <dd className="mt-2 font-semibold">{row.derived.parcel_feature_row_count}</dd>
                    </div>
                    <div>
                      <dt className="text-xs uppercase tracking-[0.2em] text-slate-500">
                        Comp pools
                      </dt>
                      <dd className="mt-2 font-semibold">{row.derived.comp_pool_row_count}</dd>
                    </div>
                    <div>
                      <dt className="text-xs uppercase tracking-[0.2em] text-slate-500">
                        Quote rows
                      </dt>
                      <dd className="mt-2 font-semibold">{row.derived.quote_row_count}</dd>
                    </div>
                  </dl>
                </div>

                <div className="mt-6 grid gap-6 lg:grid-cols-[1.4fr,1fr]">
                  <section className="rounded-3xl bg-slate-50 p-5">
                    <h3 className="text-sm font-semibold uppercase tracking-[0.22em] text-slate-500">
                      Dataset flow
                    </h3>
                    <div className="mt-4 grid gap-4">
                      {row.datasets.map((dataset) => (
                        <div
                          key={`${row.tax_year}-${dataset.dataset_type}`}
                          className="rounded-2xl border border-slate-200 bg-white p-4"
                        >
                          <div className="flex flex-wrap items-center gap-3">
                            <div className="text-lg font-semibold text-slate-950">
                              {formatLabel(dataset.dataset_type)}
                            </div>
                            <span className="rounded-full bg-slate-200 px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] text-slate-900">
                              {formatLabel(dataset.stage_status)}
                            </span>
                          </div>
                          <p className="mt-2 text-sm leading-7 text-slate-600">
                            {dataset.source_system_code} via {formatLabel(dataset.access_method)}.
                            {" "}Raw files: {dataset.raw_file_count}. Latest import:{" "}
                            {formatLabel(dataset.latest_import_status ?? "none")}. Publish:{" "}
                            {formatLabel(dataset.latest_publish_state ?? "draft")}.
                          </p>
                          <div className="mt-3 flex flex-wrap gap-2">
                            {dataset.blockers.map((blocker) => (
                              <span
                                key={blocker}
                                className="rounded-full bg-rose-100 px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] text-rose-900"
                              >
                                {formatLabel(blocker)}
                              </span>
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                  </section>

                  <section className="rounded-3xl bg-slate-950 p-5 text-slate-50">
                    <h3 className="text-sm font-semibold uppercase tracking-[0.22em] text-slate-300">
                      Derived status
                    </h3>
                    <ul className="mt-4 space-y-2 text-sm leading-7 text-slate-100">
                      <li>- Parcel summary: {row.derived.parcel_summary_ready ? "ready" : "not ready"}</li>
                      <li>- Search support: {row.derived.search_support_ready ? "ready" : "not ready"}</li>
                      <li>- Parcel trends: {row.derived.parcel_year_trend_ready ? "ready" : "not ready"}</li>
                      <li>- Neighborhood stats: {row.derived.neighborhood_stats_ready ? "ready" : "not ready"}</li>
                      <li>- Feature layer: {row.derived.feature_ready ? "ready" : "not ready"}</li>
                      <li>- Comp layer: {row.derived.comp_ready ? "ready" : "not ready"}</li>
                      <li>- Quote read model: {row.derived.quote_ready ? "ready" : "not ready"}</li>
                    </ul>
                    <div className="mt-4 flex flex-wrap gap-2">
                      {row.blockers.map((blocker) => (
                        <span
                          key={blocker}
                          className="rounded-full bg-white/10 px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] text-slate-100"
                        >
                          {formatLabel(blocker)}
                        </span>
                      ))}
                    </div>
                  </section>
                </div>
              </article>
            ))}
          </section>
        ) : null}
      </section>
    </main>
  );
}
