import Link from "next/link";

import { AdminOpsNav } from "../../_lib/AdminOpsNav";
import { getAdminLeadDetail } from "../../_lib/api";
import { requireAdminToken } from "../../_lib/auth";

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

function formatCurrency(value: number | null): string {
  if (value === null) {
    return "N/A";
  }
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
}

export default async function AdminLeadDetailPage({
  params,
}: {
  params: Promise<{ leadId: string }>;
}) {
  const { leadId } = await params;

  await requireAdminToken();
  const data = await getAdminLeadDetail(leadId);

  return (
    <main className="min-h-screen bg-[linear-gradient(180deg,#f1f6f8_0%,#f8f1e7_58%,#ffffff_100%)] px-6 py-10 text-slate-950">
      <section className="mx-auto flex max-w-7xl flex-col gap-8">
        <header className="rounded-[2rem] border border-slate-200/80 bg-white/90 p-8 shadow-[0_20px_60px_rgba(15,23,42,0.08)]">
          <div className="flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
            <div className="max-w-4xl">
              <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">
                Internal lead review
              </p>
              <h1 className="mt-4 text-4xl font-semibold tracking-tight text-slate-950">
                {data.lead.account_number} • {data.lead.requested_tax_year}
              </h1>
              <p className="mt-4 text-base leading-7 text-slate-600">
                {data.lead.county_id} lead captured on {formatDateTime(data.lead.submitted_at)} with
                {" "}{formatLabel(data.lead.demand_bucket)} classification.
              </p>
            </div>
            <div className="rounded-3xl bg-slate-950 px-5 py-4 text-sm text-slate-50">
              <div className="font-semibold uppercase tracking-[0.22em] text-slate-300">Duplicate group</div>
              <div className="mt-2">{data.lead.duplicate_group_size} submission(s)</div>
              <div className="text-slate-300">{data.lead.source_channel ?? "source unavailable"}</div>
            </div>
          </div>
          <div className="mt-6 flex flex-wrap items-center gap-4">
            <AdminOpsNav />
            <Link href="/admin/leads" className="text-sm font-semibold text-slate-700 underline-offset-4 hover:underline">
              Back to lead reporting
            </Link>
          </div>
        </header>

        <section className="grid gap-6 lg:grid-cols-[1.2fr,0.8fr]">
          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">Classification snapshot</h2>
            <dl className="mt-5 grid gap-4 sm:grid-cols-2 text-sm text-slate-700">
              <div className="rounded-3xl bg-slate-50 p-4">
                <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Demand bucket</dt>
                <dd className="mt-2 font-semibold text-slate-950">{formatLabel(data.quote_context.demand_bucket)}</dd>
              </div>
              <div className="rounded-3xl bg-slate-50 p-4">
                <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Context status</dt>
                <dd className="mt-2 font-semibold text-slate-950">{formatLabel(data.quote_context.context_status)}</dd>
              </div>
              <div className="rounded-3xl bg-slate-50 p-4">
                <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Requested / served year</dt>
                <dd className="mt-2 font-semibold text-slate-950">
                  {data.quote_context.requested_tax_year} / {data.quote_context.served_tax_year ?? "N/A"}
                </dd>
              </div>
              <div className="rounded-3xl bg-slate-50 p-4">
                <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Fallback</dt>
                <dd className="mt-2 font-semibold text-slate-950">
                  {data.quote_context.tax_year_fallback_applied
                    ? data.quote_context.tax_year_fallback_reason ?? "Applied"
                    : "No"}
                </dd>
              </div>
            </dl>
          </article>

          <article className="rounded-[2rem] border border-slate-200/80 bg-slate-950 p-7 text-slate-50 shadow-[0_18px_45px_rgba(15,23,42,0.14)]">
            <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-300">Operator checklist</h2>
            <ol className="mt-5 space-y-3 text-sm leading-7 text-slate-100">
              <li>1. Confirm the demand bucket matches the persisted `quote_context.status`.</li>
              <li>2. Compare requested vs served year before interpreting fallback demand.</li>
              <li>3. Use the duplicate peer list to inspect demand changes over time.</li>
              <li>4. Keep raw event evidence internal only.</li>
            </ol>
          </article>
        </section>

        <section className="grid gap-6 lg:grid-cols-2">
          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">Contact snapshot</h2>
            <dl className="mt-5 grid gap-4 text-sm text-slate-700">
              <div className="rounded-3xl bg-slate-50 p-4">
                <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Owner name</dt>
                <dd className="mt-2 font-semibold text-slate-950">{data.contact.owner_name ?? "Unavailable"}</dd>
              </div>
              <div className="rounded-3xl bg-slate-50 p-4">
                <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Email</dt>
                <dd className="mt-2 font-semibold text-slate-950">{data.contact.email ?? "Unavailable"}</dd>
                <p className="mt-2 text-xs text-slate-500">Present: {data.contact.email_present ? "yes" : "no"}</p>
              </div>
              <div className="rounded-3xl bg-slate-50 p-4">
                <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Phone</dt>
                <dd className="mt-2 font-semibold text-slate-950">{data.contact.phone ?? "Unavailable"}</dd>
                <p className="mt-2 text-xs text-slate-500">Present: {data.contact.phone_present ? "yes" : "no"}</p>
              </div>
              <div className="rounded-3xl bg-slate-50 p-4">
                <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Consent to contact</dt>
                <dd className="mt-2 font-semibold text-slate-950">{data.contact.consent_to_contact ? "Yes" : "No"}</dd>
              </div>
            </dl>
          </article>

          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">Attribution snapshot</h2>
            <dl className="mt-5 grid gap-4 text-sm text-slate-700">
              <div className="rounded-3xl bg-slate-50 p-4">
                <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Anonymous session</dt>
                <dd className="mt-2 font-semibold text-slate-950">{data.attribution.anonymous_session_id ?? "Unavailable"}</dd>
              </div>
              <div className="rounded-3xl bg-slate-50 p-4">
                <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Funnel stage</dt>
                <dd className="mt-2 font-semibold text-slate-950">{data.attribution.funnel_stage ?? "Unavailable"}</dd>
              </div>
              <div className="rounded-3xl bg-slate-50 p-4">
                <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">UTM source / medium</dt>
                <dd className="mt-2 font-semibold text-slate-950">
                  {data.attribution.utm_source ?? "N/A"} / {data.attribution.utm_medium ?? "N/A"}
                </dd>
              </div>
              <div className="rounded-3xl bg-slate-50 p-4">
                <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">UTM campaign</dt>
                <dd className="mt-2 font-semibold text-slate-950">{data.attribution.utm_campaign ?? "Unavailable"}</dd>
              </div>
            </dl>
          </article>
        </section>

        <section className="grid gap-6 lg:grid-cols-2">
          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">Quote context</h2>
            <dl className="mt-5 grid gap-4 text-sm text-slate-700">
              <div className="rounded-3xl bg-slate-50 p-4">
                <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">County / property supported</dt>
                <dd className="mt-2 font-semibold text-slate-950">
                  {data.quote_context.county_supported ? "County supported" : "County unsupported"}
                  {" • "}
                  {data.quote_context.property_supported === null
                    ? "Property unknown"
                    : data.quote_context.property_supported
                      ? "Property supported"
                      : "Property unsupported"}
                </dd>
              </div>
              <div className="rounded-3xl bg-slate-50 p-4">
                <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Quote ready</dt>
                <dd className="mt-2 font-semibold text-slate-950">{data.quote_context.quote_ready ? "Yes" : "No"}</dd>
              </div>
              <div className="rounded-3xl bg-slate-50 p-4">
                <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Property type / parcel</dt>
                <dd className="mt-2 font-semibold text-slate-950">
                  {data.quote_context.property_type_code ?? "N/A"} • {data.quote_context.parcel_id ?? "No parcel"}
                </dd>
              </div>
              <div className="rounded-3xl bg-slate-50 p-4">
                <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Recommendation / expected savings</dt>
                <dd className="mt-2 font-semibold text-slate-950">
                  {formatLabel(data.quote_context.protest_recommendation)} • {formatCurrency(data.quote_context.expected_tax_savings_point)}
                </dd>
                <p className="mt-2 text-xs text-slate-500">
                  Defensible value {formatCurrency(data.quote_context.defensible_value_point)}
                </p>
              </div>
            </dl>
          </article>

          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <div className="flex items-center justify-between">
              <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">Duplicate peers</h2>
              <div className="text-sm text-slate-500">{data.duplicate_peers.length} peer{data.duplicate_peers.length === 1 ? "" : "s"}</div>
            </div>
            <div className="mt-5 space-y-4">
              {data.duplicate_peers.length === 0 ? (
                <p className="text-sm text-slate-500">No peer submissions in this duplicate group.</p>
              ) : (
                data.duplicate_peers.map((peer) => (
                  <article key={peer.lead_id} className="rounded-3xl bg-slate-50 p-4">
                    <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                      <div>
                        <div className="font-semibold text-slate-950">{formatDateTime(peer.submitted_at)}</div>
                        <div className="text-xs text-slate-500">
                          {formatLabel(peer.demand_bucket)} • {formatLabel(peer.context_status)}
                        </div>
                      </div>
                      <Link href={`/admin/leads/${peer.lead_id}`} className="text-sm font-semibold text-slate-700 underline-offset-4 hover:underline">
                        Inspect peer
                      </Link>
                    </div>
                  </article>
                ))
              )}
            </div>
          </article>
        </section>

        <section className="rounded-[2rem] border border-slate-200/80 bg-slate-950 p-7 text-slate-50 shadow-[0_18px_45px_rgba(15,23,42,0.14)]">
          <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-300">Raw lead_submitted evidence</h2>
          <pre className="mt-5 overflow-x-auto rounded-3xl bg-slate-900 p-5 text-xs leading-6 text-slate-100">
            {JSON.stringify(data.raw_event_payload, null, 2)}
          </pre>
        </section>
      </section>
    </main>
  );
}
