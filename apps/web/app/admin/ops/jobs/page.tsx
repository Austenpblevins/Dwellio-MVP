import Link from "next/link";

import { AdminOpsNav } from "../../_lib/AdminOpsNav";
import { getImportBatches } from "../../_lib/api";

const DEFAULT_COUNTY = "harris";
const DEFAULT_TAX_YEAR = 2026;

function formatLabel(value: string): string {
  return value.replaceAll("_", " ");
}

export default async function AdminJobsPage({
  searchParams,
}: {
  searchParams?: Promise<{ county?: string; tax_year?: string; dataset_type?: string }>;
}) {
  const params = (await searchParams) ?? {};
  const countyId = params.county ?? DEFAULT_COUNTY;
  const taxYear = Number.parseInt(params.tax_year ?? String(DEFAULT_TAX_YEAR), 10) || DEFAULT_TAX_YEAR;
  const datasetType = params.dataset_type;

  const batches = await getImportBatches(countyId, taxYear, datasetType);

  return (
    <main className="min-h-screen bg-slate-100 px-6 py-10 text-slate-950">
      <section className="mx-auto flex max-w-7xl flex-col gap-6">
        <header className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-8 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
          <p className="text-xs font-semibold uppercase tracking-[0.28em] text-slate-500">
            Ingestion jobs
          </p>
          <h1 className="mt-3 text-4xl font-semibold tracking-tight text-slate-950">
            Import batch dashboard
          </h1>
          <p className="mt-4 text-base leading-7 text-slate-600">
            Inspect fetch, staging, normalize, publish, and rollback outcomes by county-year and dataset.
          </p>
          <div className="mt-6">
            <AdminOpsNav />
          </div>
        </header>

        <section className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
          <div className="overflow-x-auto">
            <table className="min-w-full text-left text-sm text-slate-700">
              <thead className="text-xs uppercase tracking-[0.18em] text-slate-500">
                <tr>
                  <th className="pb-3 pr-6">Dataset</th>
                  <th className="pb-3 pr-6">Filename</th>
                  <th className="pb-3 pr-6">Status</th>
                  <th className="pb-3 pr-6">Publish</th>
                  <th className="pb-3 pr-6">Rows</th>
                  <th className="pb-3 pr-6">Errors</th>
                  <th className="pb-3 pr-6">Latest Job</th>
                  <th className="pb-3 pr-6">Inspect</th>
                </tr>
              </thead>
              <tbody>
                {batches.batches.map((batch) => (
                  <tr key={batch.import_batch_id} className="border-t border-slate-200">
                    <td className="py-4 pr-6">
                      <div className="font-semibold text-slate-950">{formatLabel(batch.dataset_type)}</div>
                      <div className="text-xs text-slate-500">{batch.source_system_code}</div>
                    </td>
                    <td className="py-4 pr-6">{batch.source_filename}</td>
                    <td className="py-4 pr-6">{formatLabel(batch.status)}</td>
                    <td className="py-4 pr-6">{formatLabel(batch.publish_state ?? "draft")}</td>
                    <td className="py-4 pr-6">{batch.row_count ?? "-"}</td>
                    <td className="py-4 pr-6">
                      {batch.validation_error_count}
                      {batch.latest_job_error_message ? (
                        <div className="mt-1 text-xs text-rose-700">{batch.latest_job_error_message}</div>
                      ) : null}
                    </td>
                    <td className="py-4 pr-6">
                      {batch.latest_job_name ? `${batch.latest_job_name} / ${batch.latest_job_status}` : "none"}
                    </td>
                    <td className="py-4 pr-6">
                      <Link href={`/admin/ops/jobs/${batch.import_batch_id}`} className="font-semibold text-slate-700 underline-offset-4 hover:underline">
                        Open
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
