import Link from "next/link";

import { AdminOpsNav } from "../_lib/AdminOpsNav";
import { getAdminPackets } from "../_lib/api";
import { requireAdminToken } from "../_lib/auth";

const DEFAULT_COUNTY = "harris";
const DEFAULT_TAX_YEAR = 2026;

function formatLabel(value: string | null): string {
  if (!value) {
    return "N/A";
  }
  return value.replaceAll("_", " ");
}

export default async function AdminPacketsPage({
  searchParams,
}: {
  searchParams?: Promise<{ county?: string; tax_year?: string; packet_status?: string }>;
}) {
  const params = (await searchParams) ?? {};
  const countyId = params.county ?? DEFAULT_COUNTY;
  const taxYear = Number.parseInt(params.tax_year ?? String(DEFAULT_TAX_YEAR), 10) || DEFAULT_TAX_YEAR;
  const packetStatus = params.packet_status;

  await requireAdminToken();
  const data = await getAdminPackets(countyId, taxYear, packetStatus);

  return (
    <main className="min-h-screen bg-[linear-gradient(180deg,#eef4f7_0%,#f8f2e8_58%,#ffffff_100%)] px-6 py-10 text-slate-950">
      <section className="mx-auto flex max-w-7xl flex-col gap-8">
        <header className="rounded-[2rem] border border-slate-200/80 bg-white/90 p-8 shadow-[0_20px_60px_rgba(15,23,42,0.08)]">
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">Stage 14 admin</p>
          <div className="mt-4 flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
            <div className="max-w-3xl">
              <h1 className="text-4xl font-semibold tracking-tight text-slate-950">Evidence packet review</h1>
              <p className="mt-4 text-base leading-7 text-slate-600">
                Inspect packet headers, canonical section rows, and comp-set structures for manual
                review before later evidence-generation automation.
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
            <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">
              Packet queue
            </h2>
            <Link
              href={`/admin/cases?county=${countyId}&tax_year=${taxYear}`}
              className="text-sm font-semibold text-slate-700 underline-offset-4 hover:underline"
            >
              Open case queue
            </Link>
          </div>
          <div className="mt-5 overflow-x-auto">
            <table className="min-w-full text-left text-sm text-slate-700">
              <thead className="text-xs uppercase tracking-[0.18em] text-slate-500">
                <tr>
                  <th className="pb-3 pr-6">Parcel</th>
                  <th className="pb-3 pr-6">Packet</th>
                  <th className="pb-3 pr-6">Case linkage</th>
                  <th className="pb-3 pr-6">Contents</th>
                  <th className="pb-3 pr-6">Action</th>
                </tr>
              </thead>
              <tbody>
                {data.packets.map((packet) => (
                  <tr key={packet.evidence_packet_id} className="border-t border-slate-200 align-top">
                    <td className="py-4 pr-6">
                      <div className="font-semibold text-slate-950">{packet.account_number}</div>
                      <div className="max-w-sm text-xs text-slate-500">{packet.address ?? "Address pending"}</div>
                    </td>
                    <td className="py-4 pr-6">
                      <div className="font-semibold text-slate-950">{formatLabel(packet.packet_type)}</div>
                      <div className="text-xs text-slate-500">{formatLabel(packet.packet_status)}</div>
                    </td>
                    <td className="py-4 pr-6 text-xs text-slate-600">
                      <div>{packet.protest_case_id ? "Case linked" : "Case pending"}</div>
                      <div>{packet.valuation_run_id ? "Valuation linked" : "Valuation pending"}</div>
                      <div>{formatLabel(packet.case_status)}</div>
                    </td>
                    <td className="py-4 pr-6 text-xs text-slate-600">
                      <div>{packet.item_count} section row(s)</div>
                      <div>{packet.comp_set_count} comp set(s)</div>
                    </td>
                    <td className="py-4 pr-6">
                      <Link
                        href={`/admin/packets/${packet.evidence_packet_id}`}
                        className="font-semibold text-slate-700 underline-offset-4 hover:underline"
                      >
                        Review packet
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
