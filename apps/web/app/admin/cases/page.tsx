import Link from "next/link";

import { AdminOpsNav } from "../_lib/AdminOpsNav";
import { getAdminCases } from "../_lib/api";
import { requireAdminToken } from "../_lib/auth";

const DEFAULT_COUNTY = "harris";
const DEFAULT_TAX_YEAR = 2026;

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

function formatLabel(value: string | null): string {
  if (!value) {
    return "N/A";
  }
  return value.replaceAll("_", " ");
}

export default async function AdminCasesPage({
  searchParams,
}: {
  searchParams?: Promise<{ county?: string; tax_year?: string; case_status?: string }>;
}) {
  const params = (await searchParams) ?? {};
  const countyId = params.county ?? DEFAULT_COUNTY;
  const taxYear = Number.parseInt(params.tax_year ?? String(DEFAULT_TAX_YEAR), 10) || DEFAULT_TAX_YEAR;
  const caseStatus = params.case_status;

  await requireAdminToken();

  let data: Awaited<ReturnType<typeof getAdminCases>> | null = null;
  let errorMessage: string | null = null;

  try {
    data = await getAdminCases(countyId, taxYear, caseStatus);
  } catch (error) {
    errorMessage = error instanceof Error ? error.message : "unknown";
  }

  if (!data) {
    return (
      <main className="min-h-screen bg-slate-100 px-6 py-12 text-slate-950">
        <section className="mx-auto max-w-4xl rounded-[2rem] border border-rose-200 bg-rose-50 p-8 text-rose-950">
          <AdminOpsNav />
          <h1 className="mt-6 text-3xl font-semibold">Case review API connection needed</h1>
          <p className="mt-4 text-sm leading-7">
            The internal case review surface could not reach the backend API. Error: {errorMessage ?? "unknown"}
          </p>
        </section>
      </main>
    );
  }

  return (
    <main className="min-h-screen bg-[linear-gradient(180deg,#eef5f8_0%,#f9f6ef_55%,#ffffff_100%)] px-6 py-10 text-slate-950">
      <section className="mx-auto flex max-w-7xl flex-col gap-8">
        <header className="rounded-[2rem] border border-slate-200/80 bg-white/90 p-8 shadow-[0_20px_60px_rgba(15,23,42,0.08)]">
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">Stage 14 admin</p>
          <div className="mt-4 flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
            <div className="max-w-3xl">
              <h1 className="text-4xl font-semibold tracking-tight text-slate-950">
                Protest case review
              </h1>
              <p className="mt-4 text-base leading-7 text-slate-600">
                Review parcel-year protest cases, linked valuation context, packet readiness, notes,
                and current workflow state without introducing a parallel case system.
              </p>
            </div>
            <div className="rounded-3xl bg-slate-950 px-5 py-4 text-sm text-slate-50">
              <div className="font-semibold uppercase tracking-[0.22em] text-slate-300">Filter</div>
              <div className="mt-2">{countyId}</div>
              <div className="text-slate-300">Tax year {taxYear}</div>
            </div>
          </div>
          <div className="mt-6">
            <AdminOpsNav />
          </div>
        </header>

        <section className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
          <div className="flex items-center justify-between">
            <div>
              <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">
                Active review set
              </h2>
              <p className="mt-2 text-sm text-slate-600">
                {data.cases.length} case{data.cases.length === 1 ? "" : "s"} returned for the current
                filter.
              </p>
            </div>
            <Link
              href={`/admin/packets?county=${countyId}&tax_year=${taxYear}`}
              className="rounded-full border border-slate-300 px-4 py-2 text-sm font-semibold text-slate-700 transition hover:border-slate-950 hover:text-slate-950"
            >
              Open packet review
            </Link>
          </div>

          <div className="mt-6 overflow-x-auto">
            <table className="min-w-full text-left text-sm text-slate-700">
              <thead className="text-xs uppercase tracking-[0.18em] text-slate-500">
                <tr>
                  <th className="pb-3 pr-6">Parcel</th>
                  <th className="pb-3 pr-6">Case status</th>
                  <th className="pb-3 pr-6">Client</th>
                  <th className="pb-3 pr-6">Quote linkage</th>
                  <th className="pb-3 pr-6">Ops counts</th>
                  <th className="pb-3 pr-6">Action</th>
                </tr>
              </thead>
              <tbody>
                {data.cases.map((item) => (
                  <tr key={item.protest_case_id} className="border-t border-slate-200 align-top">
                    <td className="py-4 pr-6">
                      <div className="font-semibold text-slate-950">{item.account_number}</div>
                      <div className="max-w-sm text-xs text-slate-500">{item.address ?? "Address pending"}</div>
                      <div className="mt-2 text-xs uppercase tracking-[0.18em] text-slate-500">
                        Owner {item.owner_name ?? "unavailable"}
                      </div>
                    </td>
                    <td className="py-4 pr-6">
                      <div className="font-semibold text-slate-950">{formatLabel(item.case_status)}</div>
                      <div className="text-xs text-slate-500">
                        Workflow {formatLabel(item.workflow_status_code)}
                      </div>
                      <div className="mt-2 text-xs text-slate-500">
                        Outcome {formatLabel(item.latest_outcome_code)}
                      </div>
                    </td>
                    <td className="py-4 pr-6">
                      <div>{item.client_name ?? "Client not linked"}</div>
                      <div className="text-xs text-slate-500">
                        Agreement {item.representation_agreement_id ? "linked" : "pending"}
                      </div>
                    </td>
                    <td className="py-4 pr-6">
                      <div>{formatLabel(item.recommendation_code)}</div>
                      <div className="text-xs text-slate-500">
                        Expected savings {formatCurrency(item.expected_tax_savings_point)}
                      </div>
                      <div className="text-xs text-slate-500">
                        Valuation run {item.valuation_run_id ? "linked" : "pending"}
                      </div>
                    </td>
                    <td className="py-4 pr-6 text-xs text-slate-600">
                      <div>{item.packet_count} packet(s)</div>
                      <div>{item.note_count} note(s)</div>
                      <div>{item.hearing_count} hearing(s)</div>
                    </td>
                    <td className="py-4 pr-6">
                      <Link
                        href={`/admin/cases/${item.protest_case_id}`}
                        className="font-semibold text-slate-700 underline-offset-4 hover:underline"
                      >
                        Review case
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
