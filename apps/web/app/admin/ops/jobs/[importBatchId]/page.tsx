import Link from "next/link";
import { redirect } from "next/navigation";

import { AdminOpsNav } from "../../../_lib/AdminOpsNav";
import { getImportBatchDetail, postAdminMutation } from "../../../_lib/api";

async function publishAction(formData: FormData): Promise<void> {
  "use server";

  const importBatchId = String(formData.get("import_batch_id") ?? "");
  const countyId = String(formData.get("county_id") ?? "");
  const datasetType = String(formData.get("dataset_type") ?? "");
  const taxYear = Number.parseInt(String(formData.get("tax_year") ?? "0"), 10);

  const result = await postAdminMutation(`/admin/ops/import-batches/${importBatchId}/publish`, {
    county_id: countyId,
    tax_year: taxYear,
    dataset_type: datasetType,
  });
  const params = new URLSearchParams({ message: result.message });
  redirect(`/admin/ops/jobs/${importBatchId}?${params.toString()}`);
}

async function rollbackAction(formData: FormData): Promise<void> {
  "use server";

  const importBatchId = String(formData.get("import_batch_id") ?? "");
  const countyId = String(formData.get("county_id") ?? "");
  const datasetType = String(formData.get("dataset_type") ?? "");
  const taxYear = Number.parseInt(String(formData.get("tax_year") ?? "0"), 10);

  const result = await postAdminMutation(`/admin/ops/import-batches/${importBatchId}/rollback`, {
    county_id: countyId,
    tax_year: taxYear,
    dataset_type: datasetType,
  });
  const params = new URLSearchParams({ message: result.message });
  redirect(`/admin/ops/jobs/${importBatchId}?${params.toString()}`);
}

function formatLabel(value: string): string {
  return value.replaceAll("_", " ");
}

export default async function AdminJobDetailPage({
  params,
  searchParams,
}: {
  params: Promise<{ importBatchId: string }>;
  searchParams?: Promise<{ message?: string }>;
}) {
  const resolvedParams = await params;
  const resolvedSearch = (await searchParams) ?? {};
  const detail = await getImportBatchDetail(resolvedParams.importBatchId);

  return (
    <main className="min-h-screen bg-slate-100 px-6 py-10 text-slate-950">
      <section className="mx-auto flex max-w-7xl flex-col gap-6">
        <header className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-8 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.28em] text-slate-500">
                Job detail
              </p>
              <h1 className="mt-3 text-4xl font-semibold tracking-tight text-slate-950">
                {formatLabel(detail.batch.dataset_type)} batch
              </h1>
              <p className="mt-4 max-w-3xl text-base leading-7 text-slate-600">
                Import batch {detail.batch.import_batch_id} for {detail.batch.county_id} {detail.batch.tax_year}.
              </p>
            </div>
            <Link href="/admin/ops/jobs" className="text-sm font-semibold text-slate-700 underline-offset-4 hover:underline">
              Back to jobs
            </Link>
          </div>
          <div className="mt-6">
            <AdminOpsNav />
          </div>
        </header>

        {resolvedSearch.message ? (
          <section className="rounded-[2rem] border border-emerald-200 bg-emerald-50 p-6 text-sm text-emerald-950">
            {resolvedSearch.message}
          </section>
        ) : null}

        <section className="grid gap-6 lg:grid-cols-[1.2fr,0.8fr]">
          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">
              Batch summary
            </h2>
            <dl className="mt-5 grid gap-4 sm:grid-cols-2">
              <div className="rounded-3xl bg-slate-100 p-4">
                <dt className="text-xs uppercase tracking-[0.2em] text-slate-500">Status</dt>
                <dd className="mt-2 text-xl font-semibold">{formatLabel(detail.batch.status)}</dd>
              </div>
              <div className="rounded-3xl bg-slate-100 p-4">
                <dt className="text-xs uppercase tracking-[0.2em] text-slate-500">Publish</dt>
                <dd className="mt-2 text-xl font-semibold">{formatLabel(detail.batch.publish_state ?? "draft")}</dd>
              </div>
              <div className="rounded-3xl bg-slate-100 p-4">
                <dt className="text-xs uppercase tracking-[0.2em] text-slate-500">Rows</dt>
                <dd className="mt-2 text-xl font-semibold">{detail.batch.row_count ?? "-"}</dd>
              </div>
              <div className="rounded-3xl bg-slate-100 p-4">
                <dt className="text-xs uppercase tracking-[0.2em] text-slate-500">Validation errors</dt>
                <dd className="mt-2 text-xl font-semibold">{detail.validation_summary.error_count}</dd>
              </div>
            </dl>

            <div className="mt-6 grid gap-4 sm:grid-cols-2">
              <form action={publishAction} className="rounded-3xl border border-slate-200 bg-slate-50 p-5">
                <input type="hidden" name="import_batch_id" value={detail.batch.import_batch_id} />
                <input type="hidden" name="county_id" value={detail.batch.county_id} />
                <input type="hidden" name="tax_year" value={detail.batch.tax_year} />
                <input type="hidden" name="dataset_type" value={detail.batch.dataset_type} />
                <h3 className="text-sm font-semibold uppercase tracking-[0.2em] text-slate-500">Publish control</h3>
                <p className="mt-3 text-sm leading-7 text-slate-600">
                  Re-run normalize and publish for this batch using the existing canonical pipeline.
                </p>
                <button
                  type="submit"
                  disabled={!detail.actions.can_publish}
                  className="mt-4 rounded-2xl bg-slate-950 px-4 py-3 text-sm font-semibold text-white disabled:cursor-not-allowed disabled:bg-slate-300"
                >
                  Publish batch
                </button>
              </form>

              <form action={rollbackAction} className="rounded-3xl border border-slate-200 bg-slate-50 p-5">
                <input type="hidden" name="import_batch_id" value={detail.batch.import_batch_id} />
                <input type="hidden" name="county_id" value={detail.batch.county_id} />
                <input type="hidden" name="tax_year" value={detail.batch.tax_year} />
                <input type="hidden" name="dataset_type" value={detail.batch.dataset_type} />
                <h3 className="text-sm font-semibold uppercase tracking-[0.2em] text-slate-500">Rollback control</h3>
                <p className="mt-3 text-sm leading-7 text-slate-600">
                  Roll back the published canonical state using the saved rollback manifest when available.
                </p>
                <button
                  type="submit"
                  disabled={!detail.actions.can_rollback}
                  className="mt-4 rounded-2xl bg-rose-700 px-4 py-3 text-sm font-semibold text-white disabled:cursor-not-allowed disabled:bg-slate-300"
                >
                  Roll back publish
                </button>
              </form>
            </div>
          </article>

          <aside className="grid gap-6">
            <section className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
              <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">
                Inspection counts
              </h2>
              <ul className="mt-5 space-y-3 text-sm leading-7 text-slate-700">
                <li>- Staging rows: {detail.inspection.staging_row_count}</li>
                <li>- Lineage records: {detail.inspection.lineage_record_count}</li>
                <li>- Job runs: {detail.inspection.job_run_count}</li>
                <li>- Raw files: {detail.inspection.raw_file_count}</li>
                <li>- Validation results: {detail.inspection.validation_result_count}</li>
                <li>- Current publish version: {detail.inspection.publish_version ?? "none"}</li>
              </ul>
            </section>

            <section className="rounded-[2rem] border border-slate-200/80 bg-slate-950 p-7 text-slate-50 shadow-[0_18px_45px_rgba(15,23,42,0.14)]">
              <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-300">
                Manual fallback
              </h2>
              <p className="mt-4 text-sm leading-7 text-slate-100">
                {detail.actions.manual_fallback_message ?? "Manual fallback details unavailable."}
              </p>
              <Link href="/admin/ops/manual-upload" className="mt-4 inline-block text-sm font-semibold underline-offset-4 hover:underline">
                Open manual upload page
              </Link>
            </section>
          </aside>
        </section>

        <section className="grid gap-6 lg:grid-cols-2">
          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <div className="flex items-center justify-between">
              <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">
                Validation results
              </h2>
              <Link href={`/admin/ops/validation?import_batch_id=${detail.batch.import_batch_id}`} className="text-sm font-semibold text-slate-700 underline-offset-4 hover:underline">
                Open validation page
              </Link>
            </div>
            <div className="mt-5 space-y-3">
              {detail.validation_summary.findings.slice(0, 8).map((finding) => (
                <div key={finding.validation_result_id} className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                  <div className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">
                    {finding.severity} / {finding.validation_scope}
                  </div>
                  <div className="mt-2 font-semibold text-slate-950">{finding.validation_code}</div>
                  <p className="mt-2 text-sm leading-6 text-slate-600">{finding.message}</p>
                </div>
              ))}
            </div>
          </article>

          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-500">
              Job runs and raw files
            </h2>
            <div className="mt-5 grid gap-4">
              <div className="rounded-3xl border border-slate-200 bg-slate-50 p-5">
                <h3 className="text-sm font-semibold uppercase tracking-[0.2em] text-slate-500">Job runs</h3>
                <ul className="mt-3 space-y-2 text-sm leading-6 text-slate-700">
                  {detail.job_runs.map((jobRun) => (
                    <li key={jobRun.job_run_id}>
                      <span className="font-semibold">{jobRun.job_name}</span> / {jobRun.job_stage} / {jobRun.status}
                    </li>
                  ))}
                </ul>
              </div>
              <div className="rounded-3xl border border-slate-200 bg-slate-50 p-5">
                <h3 className="text-sm font-semibold uppercase tracking-[0.2em] text-slate-500">Raw files</h3>
                <ul className="mt-3 space-y-2 text-sm leading-6 text-slate-700">
                  {detail.source_files.map((sourceFile) => (
                    <li key={sourceFile.raw_file_id}>
                      <span className="font-semibold">{sourceFile.original_filename}</span>
                      {" "}({sourceFile.dataset_type})
                    </li>
                  ))}
                </ul>
              </div>
            </div>
          </article>
        </section>
      </section>
    </main>
  );
}
