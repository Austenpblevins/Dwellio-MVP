import Link from "next/link";

import { AdminOpsNav } from "../../_lib/AdminOpsNav";
import { getAdminPacketDetail } from "../../_lib/api";
import { requireAdminToken } from "../../_lib/auth";

function formatLabel(value: string | null): string {
  if (!value) {
    return "N/A";
  }
  return value.replaceAll("_", " ");
}

export default async function AdminPacketDetailPage({
  params,
}: {
  params: Promise<{ packetId: string }>;
}) {
  const { packetId } = await params;

  await requireAdminToken();
  const data = await getAdminPacketDetail(packetId);

  return (
    <main className="min-h-screen bg-[linear-gradient(180deg,#f2eee4_0%,#eef5f8_58%,#ffffff_100%)] px-6 py-10 text-slate-950">
      <section className="mx-auto flex max-w-7xl flex-col gap-8">
        <header className="rounded-[2rem] border border-slate-200/80 bg-white/90 p-8 shadow-[0_20px_60px_rgba(15,23,42,0.08)]">
          <div className="flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
            <div className="max-w-4xl">
              <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">
                Internal packet review
              </p>
              <h1 className="mt-4 text-4xl font-semibold tracking-tight text-slate-950">
                {data.packet.account_number} • {formatLabel(data.packet.packet_type)}
              </h1>
              <p className="mt-4 text-base leading-7 text-slate-600">
                {data.packet.address ?? "Address pending"}.
                {" "}Status {formatLabel(data.packet.packet_status)} with
                {" "}{data.packet.item_count} section row(s) and {data.packet.comp_set_count} comp set(s).
              </p>
            </div>
            <div className="rounded-3xl bg-slate-950 px-5 py-4 text-sm text-slate-50">
              <div className="font-semibold uppercase tracking-[0.22em] text-slate-300">Linked case</div>
              <div className="mt-2">{data.packet.protest_case_id ?? "Unlinked"}</div>
              <div className="text-slate-300">{formatLabel(data.packet.case_status)}</div>
            </div>
          </div>
          <div className="mt-6 flex flex-wrap items-center gap-4">
            <AdminOpsNav />
            <Link
              href="/admin/packets"
              className="text-sm font-semibold text-slate-700 underline-offset-4 hover:underline"
            >
              Back to packet queue
            </Link>
          </div>
        </header>

        <section className="grid gap-6 lg:grid-cols-[1.1fr,0.9fr]">
          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">
              Packet sections
            </h2>
            <div className="mt-5 space-y-4">
              {data.items.length === 0 ? (
                <p className="text-sm text-slate-500">No packet section rows yet.</p>
              ) : (
                data.items.map((item) => (
                  <article key={item.evidence_packet_item_id} className="rounded-3xl bg-slate-50 p-4">
                    <div className="flex items-center justify-between gap-3">
                      <div className="font-semibold text-slate-950">{item.title}</div>
                      <div className="text-xs uppercase tracking-[0.18em] text-slate-500">
                        {formatLabel(item.section_code)}
                      </div>
                    </div>
                    <p className="mt-2 text-sm text-slate-600">
                      Type {formatLabel(item.item_type)}
                      {item.source_basis ? ` • Source ${formatLabel(item.source_basis)}` : ""}
                    </p>
                    {item.body_text ? (
                      <p className="mt-3 text-sm leading-7 text-slate-700">{item.body_text}</p>
                    ) : null}
                  </article>
                ))
              )}
            </div>
          </article>

          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">
              Comparable support sets
            </h2>
            <div className="mt-5 space-y-5">
              {data.comp_sets.length === 0 ? (
                <p className="text-sm text-slate-500">No comp sets attached yet.</p>
              ) : (
                data.comp_sets.map((set) => (
                  <article key={set.evidence_comp_set_id} className="rounded-3xl bg-slate-50 p-4">
                    <div className="font-semibold text-slate-950">{set.set_label}</div>
                    <p className="mt-2 text-sm text-slate-600">
                      Basis {formatLabel(set.basis_type)}
                      {set.notes ? ` • ${set.notes}` : ""}
                    </p>
                    <ul className="mt-3 space-y-2 text-sm text-slate-700">
                      {set.items.map((item) => (
                        <li key={item.evidence_comp_set_item_id} className="rounded-2xl bg-white px-3 py-2">
                          Rank {item.comp_rank ?? "N/A"} • {formatLabel(item.comp_role)}
                          {item.parcel_sale_id ? ` • sale ${item.parcel_sale_id}` : ""}
                          {item.rationale_text ? ` • ${item.rationale_text}` : ""}
                        </li>
                      ))}
                    </ul>
                  </article>
                ))
              )}
            </div>
          </article>
        </section>
      </section>
    </main>
  );
}
