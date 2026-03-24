type AdminCountyYearDatasetReadiness = {
  dataset_type: string;
  source_system_code: string;
  access_method: string;
  availability_status: string;
  raw_file_count: number;
  latest_import_status: string | null;
  latest_publish_state: string | null;
  stage_status: string;
  blockers: string[];
};

type AdminCountyYearDerivedReadiness = {
  parcel_summary_ready: boolean;
  search_support_ready: boolean;
  feature_ready: boolean;
  comp_ready: boolean;
  quote_ready: boolean;
  parcel_summary_row_count: number;
  search_document_row_count: number;
  parcel_feature_row_count: number;
  comp_pool_row_count: number;
  quote_row_count: number;
};

type AdminCountyYearReadiness = {
  county_id: string;
  tax_year: number;
  overall_status: string;
  readiness_score: number;
  trend_label: string;
  trend_delta: number | null;
  tax_year_known: boolean;
  blockers: string[];
  datasets: AdminCountyYearDatasetReadiness[];
  derived: AdminCountyYearDerivedReadiness;
};

type AdminCountyYearReadinessDashboard = {
  access_scope: string;
  county_id: string;
  tax_years: number[];
  readiness_rows: AdminCountyYearReadiness[];
};

const DEFAULT_COUNTY = "harris";
const DEFAULT_YEARS = [2026, 2025, 2024];

function getApiBaseUrl(): string {
  return (
    process.env.NEXT_PUBLIC_DWELLIO_API_BASE_URL ??
    process.env.DWELLIO_API_BASE_URL ??
    "http://127.0.0.1:8000"
  );
}

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

async function fetchDashboard(
  countyId: string,
  taxYears: number[],
): Promise<AdminCountyYearReadinessDashboard> {
  const params = new URLSearchParams();
  for (const taxYear of taxYears) {
    params.append("tax_years", String(taxYear));
  }

  const response = await fetch(
    `${getApiBaseUrl()}/admin/readiness/${countyId}?${params.toString()}`,
    { cache: "no-store" },
  );

  if (!response.ok) {
    throw new Error(`Readiness API returned ${response.status}`);
  }

  return (await response.json()) as AdminCountyYearReadinessDashboard;
}

function StatusPill({
  label,
  tone = "neutral",
}: {
  label: string;
  tone?: "neutral" | "good" | "warn" | "caution";
}) {
  const toneClass =
    tone === "good"
      ? "bg-emerald-100 text-emerald-900"
      : tone === "warn"
        ? "bg-amber-100 text-amber-950"
        : tone === "caution"
          ? "bg-rose-100 text-rose-950"
          : "bg-slate-200 text-slate-900";

  return (
    <span
      className={`inline-flex rounded-full px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] ${toneClass}`}
    >
      {formatLabel(label)}
    </span>
  );
}

function readinessTone(status: string): "neutral" | "good" | "warn" | "caution" {
  if (status === "quote_ready" || status === "derived_ready") {
    return "good";
  }
  if (status === "canonical_partial" || status === "source_acquired") {
    return "warn";
  }
  if (status === "awaiting_source_data" || status === "tax_year_missing") {
    return "caution";
  }
  return "neutral";
}

export default async function AdminReadinessPage({
  searchParams,
}: {
  searchParams?: Promise<{ county?: string; years?: string }>;
}) {
  const resolvedParams = (await searchParams) ?? {};
  const countyId = resolvedParams.county ?? DEFAULT_COUNTY;
  const taxYears = parseTaxYears(resolvedParams.years);

  let dashboard: AdminCountyYearReadinessDashboard | null = null;
  let errorMessage: string | null = null;

  try {
    dashboard = await fetchDashboard(countyId, taxYears);
  } catch (error) {
    errorMessage =
      error instanceof Error ? error.message : "Readiness data is currently unavailable.";
  }

  return (
    <main className="min-h-screen bg-[linear-gradient(180deg,#f7f4ed_0%,#eef2f7_55%,#ffffff_100%)] text-slate-950">
      <section className="mx-auto flex w-full max-w-6xl flex-col gap-8 px-6 py-10 md:px-10">
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
                Use this page to verify whether a county-year is truly ready for ingestion,
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
        </header>

        {errorMessage ? (
          <section className="rounded-[2rem] border border-rose-200 bg-rose-50 p-8 text-rose-950 shadow-sm">
            <h2 className="text-xl font-semibold">API connection needed</h2>
            <p className="mt-3 max-w-2xl text-sm leading-7">
              The admin readiness page could not reach the internal API at{" "}
              <span className="font-mono">{getApiBaseUrl()}</span>. Set{" "}
              <span className="font-mono">NEXT_PUBLIC_DWELLIO_API_BASE_URL</span> or{" "}
              <span className="font-mono">DWELLIO_API_BASE_URL</span> when running the web app.
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
                      <StatusPill
                        label={row.overall_status}
                        tone={readinessTone(row.overall_status)}
                      />
                      <StatusPill label={row.trend_label} tone="neutral" />
                    </div>
                    <p className="mt-3 max-w-2xl text-sm leading-7 text-slate-600">
                      Readiness score {row.readiness_score}/100
                      {row.trend_delta !== null
                        ? `, ${row.trend_delta >= 0 ? "+" : ""}${row.trend_delta} vs prior year`
                        : ""}
                      . Blockers appear only for internal/admin review and are not exposed to public parcel or quote routes.
                    </p>
                  </div>
                  <dl className="grid grid-cols-2 gap-4 rounded-3xl bg-slate-100 p-5 text-sm text-slate-700 sm:grid-cols-3">
                    <div>
                      <dt className="text-xs uppercase tracking-[0.2em] text-slate-500">Parcel summary</dt>
                      <dd className="mt-2 font-semibold">{row.derived.parcel_summary_row_count}</dd>
                    </div>
                    <div>
                      <dt className="text-xs uppercase tracking-[0.2em] text-slate-500">Search docs</dt>
                      <dd className="mt-2 font-semibold">{row.derived.search_document_row_count}</dd>
                    </div>
                    <div>
                      <dt className="text-xs uppercase tracking-[0.2em] text-slate-500">Feature rows</dt>
                      <dd className="mt-2 font-semibold">{row.derived.parcel_feature_row_count}</dd>
                    </div>
                    <div>
                      <dt className="text-xs uppercase tracking-[0.2em] text-slate-500">Comp pools</dt>
                      <dd className="mt-2 font-semibold">{row.derived.comp_pool_row_count}</dd>
                    </div>
                    <div>
                      <dt className="text-xs uppercase tracking-[0.2em] text-slate-500">Quote rows</dt>
                      <dd className="mt-2 font-semibold">{row.derived.quote_row_count}</dd>
                    </div>
                    <div>
                      <dt className="text-xs uppercase tracking-[0.2em] text-slate-500">Tax year seeded</dt>
                      <dd className="mt-2 font-semibold">{row.tax_year_known ? "Yes" : "No"}</dd>
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
                            <StatusPill
                              label={dataset.stage_status}
                              tone={readinessTone(dataset.stage_status)}
                            />
                          </div>
                          <p className="mt-2 text-sm leading-6 text-slate-600">
                            {dataset.source_system_code} via {formatLabel(dataset.access_method)}.
                            {" "}Availability: {formatLabel(dataset.availability_status)}.
                          </p>
                          <dl className="mt-3 grid grid-cols-2 gap-3 text-sm text-slate-700">
                            <div>
                              <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Raw files</dt>
                              <dd className="mt-1 font-semibold">{dataset.raw_file_count}</dd>
                            </div>
                            <div>
                              <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Import</dt>
                              <dd className="mt-1 font-semibold">{dataset.latest_import_status ?? "none"}</dd>
                            </div>
                            <div>
                              <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Publish</dt>
                              <dd className="mt-1 font-semibold">{dataset.latest_publish_state ?? "none"}</dd>
                            </div>
                          </dl>
                          {dataset.blockers.length > 0 ? (
                            <div className="mt-3 flex flex-wrap gap-2">
                              {dataset.blockers.map((blocker) => (
                                <StatusPill key={blocker} label={blocker} tone="caution" />
                              ))}
                            </div>
                          ) : null}
                        </div>
                      ))}
                    </div>
                  </section>

                  <section className="rounded-3xl bg-slate-950 p-5 text-slate-50">
                    <h3 className="text-sm font-semibold uppercase tracking-[0.22em] text-slate-300">
                      Derived and downstream blockers
                    </h3>
                    <div className="mt-4 flex flex-wrap gap-2">
                      <StatusPill
                        label={row.derived.parcel_summary_ready ? "parcel_summary_ready" : "parcel_summary_pending"}
                        tone={row.derived.parcel_summary_ready ? "good" : "warn"}
                      />
                      <StatusPill
                        label={row.derived.search_support_ready ? "search_ready" : "search_pending"}
                        tone={row.derived.search_support_ready ? "good" : "warn"}
                      />
                      <StatusPill
                        label={row.derived.feature_ready ? "feature_ready" : "feature_pending"}
                        tone={row.derived.feature_ready ? "good" : "warn"}
                      />
                      <StatusPill
                        label={row.derived.comp_ready ? "comp_ready" : "comp_pending"}
                        tone={row.derived.comp_ready ? "good" : "warn"}
                      />
                      <StatusPill
                        label={row.derived.quote_ready ? "quote_ready" : "quote_pending"}
                        tone={row.derived.quote_ready ? "good" : "warn"}
                      />
                    </div>
                    <div className="mt-5 rounded-2xl bg-white/8 p-4">
                      <h4 className="text-sm font-semibold uppercase tracking-[0.2em] text-slate-300">
                        Active blockers
                      </h4>
                      {row.blockers.length > 0 ? (
                        <ul className="mt-3 space-y-2 text-sm leading-6 text-slate-100">
                          {row.blockers.map((blocker) => (
                            <li key={blocker}>- {formatLabel(blocker)}</li>
                          ))}
                        </ul>
                      ) : (
                        <p className="mt-3 text-sm leading-6 text-slate-200">
                          No active blockers for this county-year. This is the right time to run deeper internal QA.
                        </p>
                      )}
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
