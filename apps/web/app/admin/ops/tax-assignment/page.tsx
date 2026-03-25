import { AdminOpsNav } from "../../_lib/AdminOpsNav";
import { getTaxAssignmentIssues } from "../../_lib/api";

const DEFAULT_COUNTY = "harris";
const DEFAULT_TAX_YEAR = 2026;

export default async function AdminTaxAssignmentPage({
  searchParams,
}: {
  searchParams?: Promise<{ county?: string; tax_year?: string }>;
}) {
  const params = (await searchParams) ?? {};
  const countyId = params.county ?? DEFAULT_COUNTY;
  const taxYear = Number.parseInt(params.tax_year ?? String(DEFAULT_TAX_YEAR), 10) || DEFAULT_TAX_YEAR;
  const response = await getTaxAssignmentIssues(countyId, taxYear);

  return (
    <main className="min-h-screen bg-slate-100 px-6 py-10 text-slate-950">
      <section className="mx-auto flex max-w-7xl flex-col gap-6">
        <header className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-8 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
          <p className="text-xs font-semibold uppercase tracking-[0.28em] text-slate-500">Tax assignment QA</p>
          <h1 className="mt-3 text-4xl font-semibold tracking-tight text-slate-950">Assignment issue view</h1>
          <p className="mt-4 text-base leading-7 text-slate-600">
            Review missing or conflicting parcel-to-tax-unit assignments before trusting downstream tax calculations.
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
                  <th className="pb-3 pr-6">Account</th>
                  <th className="pb-3 pr-6">County</th>
                  <th className="pb-3 pr-6">City</th>
                  <th className="pb-3 pr-6">School</th>
                  <th className="pb-3 pr-6">MUD</th>
                </tr>
              </thead>
              <tbody>
                {response.issues.map((issue) => (
                  <tr key={`${issue.parcel_id}-${issue.tax_year}`} className="border-t border-slate-200">
                    <td className="py-4 pr-6 font-semibold text-slate-950">{issue.account_number}</td>
                    <td className="py-4 pr-6">
                      {issue.county_assignment_count}
                      {issue.missing_county_assignment ? " / missing" : ""}
                      {issue.conflicting_county_assignment ? " / conflict" : ""}
                    </td>
                    <td className="py-4 pr-6">
                      {issue.city_assignment_count}
                      {issue.missing_city_assignment ? " / missing" : ""}
                      {issue.conflicting_city_assignment ? " / conflict" : ""}
                    </td>
                    <td className="py-4 pr-6">
                      {issue.school_assignment_count}
                      {issue.missing_school_assignment ? " / missing" : ""}
                      {issue.conflicting_school_assignment ? " / conflict" : ""}
                    </td>
                    <td className="py-4 pr-6">
                      {issue.mud_assignment_count}
                      {issue.missing_mud_assignment ? " / missing" : ""}
                      {issue.conflicting_mud_assignment ? " / conflict" : ""}
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
