import { AdminOpsNav } from "../../_lib/AdminOpsNav";
import { getValidationResults } from "../../_lib/api";

export default async function AdminValidationPage({
  searchParams,
}: {
  searchParams?: Promise<{ import_batch_id?: string; severity?: string }>;
}) {
  const params = (await searchParams) ?? {};
  const importBatchId = params.import_batch_id;

  if (!importBatchId) {
    return (
      <main className="min-h-screen bg-slate-100 px-6 py-10 text-slate-950">
        <section className="mx-auto max-w-5xl rounded-[2rem] border border-slate-200/80 bg-white/95 p-8 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
          <AdminOpsNav />
          <h1 className="mt-6 text-3xl font-semibold">Validation review</h1>
          <p className="mt-4 text-sm leading-7 text-slate-600">
            Provide an <span className="font-mono">import_batch_id</span> query parameter to inspect validation findings.
          </p>
        </section>
      </main>
    );
  }

  const validation = await getValidationResults(importBatchId, params.severity);

  return (
    <main className="min-h-screen bg-slate-100 px-6 py-10 text-slate-950">
      <section className="mx-auto flex max-w-6xl flex-col gap-6">
        <header className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-8 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
          <p className="text-xs font-semibold uppercase tracking-[0.28em] text-slate-500">Validation review</p>
          <h1 className="mt-3 text-4xl font-semibold tracking-tight text-slate-950">Import batch findings</h1>
          <p className="mt-4 text-base leading-7 text-slate-600">
            Import batch {validation.import_batch_id} has {validation.total_count} validation rows,
            including {validation.error_count} errors and {validation.warning_count} warnings.
          </p>
          <div className="mt-6">
            <AdminOpsNav />
          </div>
        </header>

        <section className="grid gap-4">
          {validation.findings.map((finding) => (
            <article key={finding.validation_result_id} className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-6 shadow-[0_12px_35px_rgba(15,23,42,0.08)]">
              <div className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">
                {finding.severity} / {finding.validation_scope}
              </div>
              <div className="mt-2 text-xl font-semibold text-slate-950">{finding.validation_code}</div>
              <p className="mt-3 text-sm leading-7 text-slate-600">{finding.message}</p>
              {finding.entity_table ? (
                <p className="mt-3 text-xs uppercase tracking-[0.2em] text-slate-500">
                  Entity: {finding.entity_table}
                </p>
              ) : null}
            </article>
          ))}
        </section>
      </section>
    </main>
  );
}
