import { redirect } from "next/navigation";

import { AdminOpsNav } from "../../_lib/AdminOpsNav";
import { postAdminMutation } from "../../_lib/api";

async function registerManualImportAction(formData: FormData): Promise<void> {
  "use server";

  const countyId = String(formData.get("county_id") ?? "");
  const taxYear = Number.parseInt(String(formData.get("tax_year") ?? "0"), 10);
  const datasetType = String(formData.get("dataset_type") ?? "");
  const sourceFilePath = String(formData.get("source_file_path") ?? "");
  const sourceUrl = String(formData.get("source_url") ?? "");
  const dryRun = formData.get("dry_run") === "on";

  const result = await postAdminMutation("/admin/ops/manual-import/register", {
    county_id: countyId,
    tax_year: taxYear,
    dataset_type: datasetType,
    source_file_path: sourceFilePath,
    source_url: sourceUrl || null,
    dry_run: dryRun,
  });
  const params = new URLSearchParams({ message: result.message });
  redirect(`/admin/ops/manual-upload?${params.toString()}`);
}

export default async function AdminManualUploadPage({
  searchParams,
}: {
  searchParams?: Promise<{ message?: string }>;
}) {
  const params = (await searchParams) ?? {};

  return (
    <main className="min-h-screen bg-slate-100 px-6 py-10 text-slate-950">
      <section className="mx-auto flex max-w-6xl flex-col gap-6">
        <header className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-8 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
          <p className="text-xs font-semibold uppercase tracking-[0.28em] text-slate-500">Manual fallback</p>
          <h1 className="mt-3 text-4xl font-semibold tracking-tight text-slate-950">
            Register a manual import
          </h1>
          <p className="mt-4 text-base leading-7 text-slate-600">
            Use this internal control when county automation is unavailable or a fuller historical year must be backfilled manually.
          </p>
          <div className="mt-6">
            <AdminOpsNav />
          </div>
        </header>

        {params.message ? (
          <section className="rounded-[2rem] border border-emerald-200 bg-emerald-50 p-6 text-sm text-emerald-950">
            {params.message}
          </section>
        ) : null}

        <section className="grid gap-6 lg:grid-cols-[1.1fr,0.9fr]">
          <form action={registerManualImportAction} className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <div className="grid gap-4">
              <label className="grid gap-2 text-sm font-medium text-slate-700">
                County
                <input name="county_id" defaultValue="harris" className="rounded-2xl border border-slate-300 px-4 py-3" required />
              </label>
              <label className="grid gap-2 text-sm font-medium text-slate-700">
                Tax year
                <input name="tax_year" type="number" defaultValue={2025} className="rounded-2xl border border-slate-300 px-4 py-3" required />
              </label>
              <label className="grid gap-2 text-sm font-medium text-slate-700">
                Dataset type
                <input name="dataset_type" defaultValue="property_roll" className="rounded-2xl border border-slate-300 px-4 py-3" required />
              </label>
              <label className="grid gap-2 text-sm font-medium text-slate-700">
                Source file path
                <input name="source_file_path" className="rounded-2xl border border-slate-300 px-4 py-3" placeholder="/absolute/path/to/file.csv" required />
              </label>
              <label className="grid gap-2 text-sm font-medium text-slate-700">
                Source URL
                <input name="source_url" className="rounded-2xl border border-slate-300 px-4 py-3" placeholder="https://county.example/export" />
              </label>
              <label className="flex items-center gap-3 rounded-2xl bg-slate-100 px-4 py-3 text-sm text-slate-700">
                <input name="dry_run" type="checkbox" />
                Register in dry-run mode
              </label>
              <button type="submit" className="rounded-2xl bg-slate-950 px-5 py-3 text-sm font-semibold text-white">
                Register manual import
              </button>
            </div>
          </form>

          <aside className="rounded-[2rem] border border-slate-200/80 bg-slate-950 p-7 text-slate-50 shadow-[0_18px_45px_rgba(15,23,42,0.14)]">
            <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-300">
              Operator notes
            </h2>
            <ul className="mt-5 space-y-3 text-sm leading-7 text-slate-100">
              <li>- Use absolute file paths on the machine where the backend can read the file.</li>
              <li>- Registration writes import-batch and raw-file metadata into the standard ingestion backbone.</li>
              <li>- After registration, use the jobs dashboard to stage and publish that batch.</li>
              <li>- Prefer fuller prior years like 2025 when 2026 is still sparse.</li>
            </ul>
          </aside>
        </section>
      </section>
    </main>
  );
}
