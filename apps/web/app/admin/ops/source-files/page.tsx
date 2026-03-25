import { AdminOpsNav } from "../../_lib/AdminOpsNav";
import { getSourceFiles } from "../../_lib/api";

const DEFAULT_COUNTY = "harris";
const DEFAULT_TAX_YEAR = 2026;

export default async function AdminSourceFilesPage({
  searchParams,
}: {
  searchParams?: Promise<{ county?: string; tax_year?: string; dataset_type?: string }>;
}) {
  const params = (await searchParams) ?? {};
  const countyId = params.county ?? DEFAULT_COUNTY;
  const taxYear = Number.parseInt(params.tax_year ?? String(DEFAULT_TAX_YEAR), 10) || DEFAULT_TAX_YEAR;
  const sourceFiles = await getSourceFiles(countyId, taxYear, params.dataset_type);

  return (
    <main className="min-h-screen bg-slate-100 px-6 py-10 text-slate-950">
      <section className="mx-auto flex max-w-7xl flex-col gap-6">
        <header className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-8 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
          <p className="text-xs font-semibold uppercase tracking-[0.28em] text-slate-500">Source files</p>
          <h1 className="mt-3 text-4xl font-semibold tracking-tight text-slate-950">Raw archive visibility</h1>
          <p className="mt-4 text-base leading-7 text-slate-600">
            Review archived raw files, checksums, storage paths, and import-batch linkage for {countyId} {taxYear}.
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
                  <th className="pb-3 pr-6">File</th>
                  <th className="pb-3 pr-6">Dataset</th>
                  <th className="pb-3 pr-6">Storage Path</th>
                  <th className="pb-3 pr-6">Checksum</th>
                </tr>
              </thead>
              <tbody>
                {sourceFiles.source_files.map((sourceFile) => (
                  <tr key={sourceFile.raw_file_id} className="border-t border-slate-200">
                    <td className="py-4 pr-6">
                      <div className="font-semibold text-slate-950">{sourceFile.original_filename}</div>
                      <div className="text-xs text-slate-500">{sourceFile.source_system_code}</div>
                    </td>
                    <td className="py-4 pr-6">{sourceFile.dataset_type}</td>
                    <td className="py-4 pr-6 font-mono text-xs">{sourceFile.storage_path}</td>
                    <td className="py-4 pr-6 font-mono text-xs">{sourceFile.checksum}</td>
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
