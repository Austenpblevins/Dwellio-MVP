import Link from "next/link";

import { AdminOpsNav } from "../../_lib/AdminOpsNav";
import { getAdminCaseDetail } from "../../_lib/api";
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

export default async function AdminCaseDetailPage({
  params,
}: {
  params: Promise<{ caseId: string }>;
}) {
  const { caseId } = await params;

  await requireAdminToken();
  const data = await getAdminCaseDetail(caseId);

  return (
    <main className="min-h-screen bg-[linear-gradient(180deg,#f4efe6_0%,#edf5f7_58%,#ffffff_100%)] px-6 py-10 text-slate-950">
      <section className="mx-auto flex max-w-7xl flex-col gap-8">
        <header className="rounded-[2rem] border border-slate-200/80 bg-white/90 p-8 shadow-[0_20px_60px_rgba(15,23,42,0.08)]">
          <div className="flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
            <div className="max-w-4xl">
              <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">
                Internal case review
              </p>
              <h1 className="mt-4 text-4xl font-semibold tracking-tight text-slate-950">
                {data.case.account_number} • {data.case.tax_year}
              </h1>
              <p className="mt-4 text-base leading-7 text-slate-600">
                {data.case.address ?? "Address pending"}.
                {" "}Status {formatLabel(data.case.case_status)} with workflow
                {" "}{formatLabel(data.case.workflow_status_code)}.
              </p>
            </div>
            <div className="rounded-3xl bg-slate-950 px-5 py-4 text-sm text-slate-50">
              <div className="font-semibold uppercase tracking-[0.22em] text-slate-300">Linked data</div>
              <div className="mt-2">Client {data.case.client_name ?? "pending"}</div>
              <div className="text-slate-300">
                Valuation {data.case.valuation_run_id ? "linked" : "pending"}
              </div>
            </div>
          </div>
          <div className="mt-6 flex flex-wrap items-center gap-4">
            <AdminOpsNav />
            <Link
              href="/admin/cases"
              className="text-sm font-semibold text-slate-700 underline-offset-4 hover:underline"
            >
              Back to case list
            </Link>
          </div>
        </header>

        <section className="grid gap-6 lg:grid-cols-[1.2fr,0.8fr]">
          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">Case context</h2>
            <dl className="mt-5 grid gap-4 sm:grid-cols-2 text-sm text-slate-700">
              <div className="rounded-3xl bg-slate-50 p-4">
                <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Owner</dt>
                <dd className="mt-2 font-semibold text-slate-950">{data.case.owner_name ?? "Unavailable"}</dd>
              </div>
              <div className="rounded-3xl bg-slate-50 p-4">
                <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Expected savings</dt>
                <dd className="mt-2 font-semibold text-slate-950">
                  {data.case.expected_tax_savings_point ?? "N/A"}
                </dd>
              </div>
              <div className="rounded-3xl bg-slate-50 p-4">
                <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Recommendation</dt>
                <dd className="mt-2 font-semibold text-slate-950">
                  {formatLabel(data.case.recommendation_code)}
                </dd>
              </div>
              <div className="rounded-3xl bg-slate-50 p-4">
                <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Latest outcome</dt>
                <dd className="mt-2 font-semibold text-slate-950">
                  {formatLabel(data.case.latest_outcome_code)}
                </dd>
              </div>
            </dl>
          </article>

          <article className="rounded-[2rem] border border-slate-200/80 bg-slate-950 p-7 text-slate-50 shadow-[0_18px_45px_rgba(15,23,42,0.14)]">
            <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-300">
              Review checklist
            </h2>
            <ol className="mt-5 space-y-3 text-sm leading-7 text-slate-100">
              <li>1. Confirm valuation run and recommendation linkage for this parcel-year.</li>
              <li>2. Review case notes and status history before drafting packet sections.</li>
              <li>3. Inspect packet rows to confirm evidence structure is ready for later generation.</li>
              <li>4. Keep restricted comp detail in internal workflows only.</li>
            </ol>
          </article>
        </section>

        <section className="grid gap-6 lg:grid-cols-2">
          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">Notes</h2>
            <div className="mt-5 space-y-4">
              {data.notes.length === 0 ? (
                <p className="text-sm text-slate-500">No internal notes yet.</p>
              ) : (
                data.notes.map((note) => (
                  <article key={note.case_note_id} className="rounded-3xl bg-slate-50 p-4">
                    <div className="flex items-center justify-between gap-3">
                      <div className="text-xs uppercase tracking-[0.18em] text-slate-500">
                        {formatLabel(note.note_code)}
                      </div>
                      <div className="text-xs text-slate-500">{formatDateTime(note.created_at)}</div>
                    </div>
                    <p className="mt-3 text-sm leading-7 text-slate-700">{note.note_text}</p>
                  </article>
                ))
              )}
            </div>
          </article>

          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">
              Status history
            </h2>
            <div className="mt-5 space-y-4">
              {data.status_history.length === 0 ? (
                <p className="text-sm text-slate-500">No workflow history yet.</p>
              ) : (
                data.status_history.map((entry) => (
                  <article key={entry.case_status_history_id} className="rounded-3xl bg-slate-50 p-4">
                    <div className="flex items-center justify-between gap-3">
                      <div className="font-semibold text-slate-950">{formatLabel(entry.case_status)}</div>
                      <div className="text-xs text-slate-500">{formatDateTime(entry.created_at)}</div>
                    </div>
                    <p className="mt-2 text-sm text-slate-600">
                      Workflow {formatLabel(entry.workflow_status_code)}
                      {entry.reason_text ? ` • ${entry.reason_text}` : ""}
                    </p>
                  </article>
                ))
              )}
            </div>
          </article>
        </section>

        <section className="grid gap-6 lg:grid-cols-2">
          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">Assignments</h2>
            <div className="mt-5 space-y-4">
              {data.assignments.length === 0 ? (
                <p className="text-sm text-slate-500">No assignee records yet.</p>
              ) : (
                data.assignments.map((assignment) => (
                  <article key={assignment.case_assignment_id} className="rounded-3xl bg-slate-50 p-4">
                    <div className="font-semibold text-slate-950">{assignment.assignee_name}</div>
                    <p className="mt-2 text-sm text-slate-600">
                      {formatLabel(assignment.assignee_role)} • {formatLabel(assignment.assignment_status)}
                    </p>
                  </article>
                ))
              )}
            </div>
          </article>

          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">Hearings</h2>
            <div className="mt-5 space-y-4">
              {data.hearings.length === 0 ? (
                <p className="text-sm text-slate-500">No hearing rows yet.</p>
              ) : (
                data.hearings.map((hearing) => (
                  <article key={hearing.hearing_id} className="rounded-3xl bg-slate-50 p-4">
                    <div className="font-semibold text-slate-950">
                      {formatLabel(hearing.hearing_type_code)}
                    </div>
                    <p className="mt-2 text-sm text-slate-600">
                      {formatLabel(hearing.hearing_status)} • {formatDateTime(hearing.scheduled_at)}
                    </p>
                  </article>
                ))
              )}
            </div>
          </article>
        </section>

        <section className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">Evidence packets</h2>
            <Link
              href={`/admin/packets?county=${data.case.county_id}&tax_year=${data.case.tax_year}`}
              className="text-sm font-semibold text-slate-700 underline-offset-4 hover:underline"
            >
              Open packet queue
            </Link>
          </div>
          <div className="mt-5 overflow-x-auto">
            <table className="min-w-full text-left text-sm text-slate-700">
              <thead className="text-xs uppercase tracking-[0.18em] text-slate-500">
                <tr>
                  <th className="pb-3 pr-6">Packet</th>
                  <th className="pb-3 pr-6">Status</th>
                  <th className="pb-3 pr-6">Contents</th>
                  <th className="pb-3 pr-6">Action</th>
                </tr>
              </thead>
              <tbody>
                {data.packets.map((packet) => (
                  <tr key={packet.evidence_packet_id} className="border-t border-slate-200">
                    <td className="py-4 pr-6">
                      <div className="font-semibold text-slate-950">{formatLabel(packet.packet_type)}</div>
                      <div className="text-xs text-slate-500">{formatDateTime(packet.updated_at)}</div>
                    </td>
                    <td className="py-4 pr-6">{formatLabel(packet.packet_status)}</td>
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
