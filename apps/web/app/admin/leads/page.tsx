import Link from "next/link";

import { AdminOpsNav } from "../_lib/AdminOpsNav";
import { getAdminLeads } from "../_lib/api";
import { requireAdminToken } from "../_lib/auth";

const DEFAULT_COUNTY = "harris";
const DEFAULT_LIMIT = 50;

function formatLabel(value: string | null): string {
  if (!value) {
    return "N/A";
  }
  return value.replaceAll("_", " ");
}

function formatDateTime(value: string | null): string {
  if (!value) {
    return "Pending";
  }
  return new Intl.DateTimeFormat("en-US", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

function parseBooleanFlag(value: string | undefined): boolean {
  return value === "true";
}

function parseOptionalInteger(value: string | undefined): number | undefined {
  if (!value) {
    return undefined;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isInteger(parsed) ? parsed : undefined;
}

export default async function AdminLeadsPage({
  searchParams,
}: {
  searchParams?: Promise<{
    county?: string;
    requested_tax_year?: string;
    served_tax_year?: string;
    demand_bucket?: string;
    fallback_applied?: string;
    source_channel?: string;
    duplicate_only?: string;
    quote_ready_only?: string;
    submitted_from?: string;
    submitted_to?: string;
    limit?: string;
  }>;
}) {
  const params = (await searchParams) ?? {};
  const countyId = params.county ?? DEFAULT_COUNTY;
  const requestedTaxYear = parseOptionalInteger(params.requested_tax_year);
  const servedTaxYear = parseOptionalInteger(params.served_tax_year);
  const demandBucket = params.demand_bucket || undefined;
  const sourceChannel = params.source_channel || undefined;
  const submittedFrom = params.submitted_from || undefined;
  const submittedTo = params.submitted_to || undefined;
  const duplicateOnly = parseBooleanFlag(params.duplicate_only);
  const quoteReadyOnly = parseBooleanFlag(params.quote_ready_only);
  const fallbackApplied =
    params.fallback_applied === "true"
      ? true
      : params.fallback_applied === "false"
        ? false
        : undefined;
  const limit = parseOptionalInteger(params.limit) ?? DEFAULT_LIMIT;

  await requireAdminToken();

  let data: Awaited<ReturnType<typeof getAdminLeads>> | null = null;
  let errorMessage: string | null = null;

  try {
    data = await getAdminLeads({
      countyId,
      requestedTaxYear,
      servedTaxYear,
      demandBucket,
      fallbackApplied,
      sourceChannel,
      duplicateOnly,
      quoteReadyOnly,
      submittedFrom,
      submittedTo,
      limit,
    });
  } catch (error) {
    errorMessage = error instanceof Error ? error.message : "unknown";
  }

  if (!data) {
    return (
      <main className="min-h-screen bg-slate-100 px-6 py-12 text-slate-950">
        <section className="mx-auto max-w-4xl rounded-[2rem] border border-rose-200 bg-rose-50 p-8 text-rose-950">
          <AdminOpsNav />
          <h1 className="mt-6 text-3xl font-semibold">Lead reporting API connection needed</h1>
          <p className="mt-4 text-sm leading-7">
            The internal lead reporting surface could not reach the backend API. Error: {errorMessage ?? "unknown"}
          </p>
        </section>
      </main>
    );
  }

  return (
    <main className="min-h-screen bg-[linear-gradient(180deg,#eff4f7_0%,#f8f2e9_58%,#ffffff_100%)] px-6 py-10 text-slate-950">
      <section className="mx-auto flex max-w-7xl flex-col gap-8">
        <header className="rounded-[2rem] border border-slate-200/80 bg-white/90 p-8 shadow-[0_20px_60px_rgba(15,23,42,0.08)]">
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">
            Off-board lead reporting
          </p>
          <div className="mt-4 flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
            <div className="max-w-3xl">
              <h1 className="text-4xl font-semibold tracking-tight text-slate-950">
                Lead reporting and ops visibility
              </h1>
              <p className="mt-4 text-base leading-7 text-slate-600">
                Review current lead demand, duplicate groups, fallback behavior, and raw
                submission evidence without leaving the protected admin workflow.
              </p>
            </div>
            <div className="rounded-3xl bg-slate-950 px-5 py-4 text-sm text-slate-50">
              <div className="font-semibold uppercase tracking-[0.22em] text-slate-300">Current filter</div>
              <div className="mt-2">{countyId}</div>
              <div className="text-slate-300">Rows {data.leads.length}</div>
            </div>
          </div>
          <div className="mt-6">
            <AdminOpsNav />
          </div>
        </header>

        <section className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
          <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">Filters</h2>
          <form className="mt-5 grid gap-4 md:grid-cols-2 xl:grid-cols-5">
            <label className="text-sm text-slate-700">
              <span className="mb-2 block text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">County</span>
              <input name="county" defaultValue={countyId} className="w-full rounded-2xl border border-slate-300 px-4 py-3" />
            </label>
            <label className="text-sm text-slate-700">
              <span className="mb-2 block text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Requested year</span>
              <input name="requested_tax_year" defaultValue={requestedTaxYear ?? ""} className="w-full rounded-2xl border border-slate-300 px-4 py-3" />
            </label>
            <label className="text-sm text-slate-700">
              <span className="mb-2 block text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Served year</span>
              <input name="served_tax_year" defaultValue={servedTaxYear ?? ""} className="w-full rounded-2xl border border-slate-300 px-4 py-3" />
            </label>
            <label className="text-sm text-slate-700">
              <span className="mb-2 block text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Demand bucket</span>
              <select name="demand_bucket" defaultValue={demandBucket ?? ""} className="w-full rounded-2xl border border-slate-300 px-4 py-3">
                <option value="">All buckets</option>
                <option value="quote_ready_demand">Quote ready</option>
                <option value="reachable_unquoted_demand">Reachable but unquoted</option>
                <option value="unsupported_county_demand">Unsupported county</option>
                <option value="unsupported_property_demand">Unsupported property</option>
              </select>
            </label>
            <label className="text-sm text-slate-700">
              <span className="mb-2 block text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Source channel</span>
              <input name="source_channel" defaultValue={sourceChannel ?? ""} className="w-full rounded-2xl border border-slate-300 px-4 py-3" />
            </label>
            <label className="text-sm text-slate-700">
              <span className="mb-2 block text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Submitted from</span>
              <input type="date" name="submitted_from" defaultValue={submittedFrom ?? ""} className="w-full rounded-2xl border border-slate-300 px-4 py-3" />
            </label>
            <label className="text-sm text-slate-700">
              <span className="mb-2 block text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Submitted to</span>
              <input type="date" name="submitted_to" defaultValue={submittedTo ?? ""} className="w-full rounded-2xl border border-slate-300 px-4 py-3" />
            </label>
            <label className="text-sm text-slate-700">
              <span className="mb-2 block text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Fallback applied</span>
              <select name="fallback_applied" defaultValue={fallbackApplied === undefined ? "" : String(fallbackApplied)} className="w-full rounded-2xl border border-slate-300 px-4 py-3">
                <option value="">Either</option>
                <option value="true">Yes</option>
                <option value="false">No</option>
              </select>
            </label>
            <label className="text-sm text-slate-700">
              <span className="mb-2 block text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Limit</span>
              <input name="limit" defaultValue={limit} className="w-full rounded-2xl border border-slate-300 px-4 py-3" />
            </label>
            <div className="flex flex-col justify-end gap-3 rounded-3xl bg-slate-50 p-4 text-sm text-slate-700">
              <label className="flex items-center gap-3">
                <input type="checkbox" name="duplicate_only" value="true" defaultChecked={duplicateOnly} />
                Duplicate groups only
              </label>
              <label className="flex items-center gap-3">
                <input type="checkbox" name="quote_ready_only" value="true" defaultChecked={quoteReadyOnly} />
                Quote-ready only
              </label>
              <button type="submit" className="rounded-full bg-slate-950 px-4 py-2 text-sm font-semibold text-white">
                Apply filters
              </button>
            </div>
          </form>
        </section>

        <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-6 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <div className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">Accepted leads</div>
            <div className="mt-3 text-4xl font-semibold">{data.kpi_summary.total_count}</div>
          </article>
          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-6 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <div className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">Quote ready</div>
            <div className="mt-3 text-4xl font-semibold">{data.kpi_summary.quote_ready_count}</div>
          </article>
          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-6 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <div className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">Reachable unquoted</div>
            <div className="mt-3 text-4xl font-semibold">{data.kpi_summary.reachable_unquoted_count}</div>
          </article>
          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-6 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <div className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">Duplicate groups</div>
            <div className="mt-3 text-4xl font-semibold">{data.kpi_summary.duplicate_group_count}</div>
          </article>
        </section>

        <section className="grid gap-6 lg:grid-cols-[0.9fr,1.1fr]">
          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">Demand mix</h2>
            <div className="mt-5 space-y-4">
              {data.demand_buckets.length === 0 ? (
                <p className="text-sm text-slate-500">No lead rows matched the current filter.</p>
              ) : (
                data.demand_buckets.map((bucket) => (
                  <div key={bucket.demand_bucket} className="rounded-3xl bg-slate-50 p-4">
                    <div className="flex items-center justify-between gap-3">
                      <div className="font-semibold text-slate-950">{formatLabel(bucket.demand_bucket)}</div>
                      <div className="text-2xl font-semibold text-slate-950">{bucket.lead_count}</div>
                    </div>
                  </div>
                ))
              )}
            </div>
          </article>

          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <div className="flex items-center justify-between">
              <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">Duplicate groups</h2>
              <div className="text-xs uppercase tracking-[0.18em] text-slate-500">
                {data.duplicate_groups.length} group{data.duplicate_groups.length === 1 ? "" : "s"}
              </div>
            </div>
            <div className="mt-5 space-y-4">
              {data.duplicate_groups.length === 0 ? (
                <p className="text-sm text-slate-500">No duplicate groups matched the current filter.</p>
              ) : (
                data.duplicate_groups.map((group) => (
                  <article key={group.duplicate_group_key} className="rounded-3xl bg-slate-50 p-4">
                    <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                      <div>
                        <div className="font-semibold text-slate-950">
                          {group.county_id} • {group.account_number} • {group.requested_tax_year}
                        </div>
                        <div className="text-xs text-slate-500">
                          {group.lead_count} submission(s) • latest {formatDateTime(group.latest_submitted_at)}
                        </div>
                      </div>
                      <Link
                        href={`/admin/leads/${group.latest_lead_id}`}
                        className="text-sm font-semibold text-slate-700 underline-offset-4 hover:underline"
                      >
                        Review latest lead
                      </Link>
                    </div>
                    <div className="mt-3 flex flex-wrap gap-2 text-xs uppercase tracking-[0.18em] text-slate-600">
                      <span className="rounded-full bg-slate-200 px-3 py-1">
                        {formatLabel(group.latest_demand_bucket)}
                      </span>
                      {group.fallback_present ? (
                        <span className="rounded-full bg-amber-100 px-3 py-1 text-amber-900">Fallback present</span>
                      ) : null}
                      <span className="rounded-full bg-slate-200 px-3 py-1">
                        {group.demand_bucket_count} demand bucket state(s)
                      </span>
                    </div>
                  </article>
                ))
              )}
            </div>
          </article>
        </section>

        <section className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">Lead results</h2>
            <div className="text-sm text-slate-500">
              {data.leads.length} row{data.leads.length === 1 ? "" : "s"} returned
            </div>
          </div>
          <div className="mt-5 overflow-x-auto">
            <table className="min-w-full text-left text-sm text-slate-700">
              <thead className="text-xs uppercase tracking-[0.18em] text-slate-500">
                <tr>
                  <th className="pb-3 pr-6">Submitted</th>
                  <th className="pb-3 pr-6">Parcel-year</th>
                  <th className="pb-3 pr-6">Demand</th>
                  <th className="pb-3 pr-6">Contactability</th>
                  <th className="pb-3 pr-6">Duplicate review</th>
                  <th className="pb-3 pr-6">Action</th>
                </tr>
              </thead>
              <tbody>
                {data.leads.map((lead) => (
                  <tr key={lead.lead_id} className="border-t border-slate-200 align-top">
                    <td className="py-4 pr-6">
                      <div className="font-semibold text-slate-950">{formatDateTime(lead.submitted_at)}</div>
                      <div className="text-xs text-slate-500">{lead.source_channel ?? "source unavailable"}</div>
                    </td>
                    <td className="py-4 pr-6">
                      <div className="font-semibold text-slate-950">{lead.county_id}</div>
                      <div className="text-xs text-slate-500">{lead.account_number}</div>
                      <div className="mt-2 text-xs text-slate-500">
                        Requested {lead.requested_tax_year} • Served {lead.served_tax_year ?? "N/A"}
                      </div>
                    </td>
                    <td className="py-4 pr-6">
                      <div className="font-semibold text-slate-950">{formatLabel(lead.demand_bucket)}</div>
                      <div className="text-xs text-slate-500">{formatLabel(lead.context_status)}</div>
                      {lead.fallback_applied ? (
                        <div className="mt-2 text-xs uppercase tracking-[0.18em] text-amber-700">Fallback applied</div>
                      ) : null}
                    </td>
                    <td className="py-4 pr-6 text-xs text-slate-600">
                      <div>Email {lead.email_present ? "present" : "missing"}</div>
                      <div>Phone {lead.phone_present ? "present" : "missing"}</div>
                      <div>Consent {lead.consent_to_contact ? "yes" : "no"}</div>
                    </td>
                    <td className="py-4 pr-6 text-xs text-slate-600">
                      <div>{lead.duplicate_group_size} submission(s)</div>
                      <div>{lead.duplicate_group_size > 1 ? "Review duplicate history" : "No duplicate group"}</div>
                    </td>
                    <td className="py-4 pr-6">
                      <Link href={`/admin/leads/${lead.lead_id}`} className="font-semibold text-slate-700 underline-offset-4 hover:underline">
                        Inspect lead
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
