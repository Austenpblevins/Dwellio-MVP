import Link from "next/link";
import { notFound } from "next/navigation";

import { getParcelSummary, getQuote } from "../../../../_lib/public-api";
import { formatCurrency, formatLabel, formatNumber, formatRate } from "../../../../_lib/formatters";
import { buildTaxYearFallbackNotice } from "../../../../_lib/tax-year-fallback";
import type {
  ParcelDataCaveat,
  ParcelSummaryResponse,
  ParcelTaxRateComponent,
  QuoteResponse,
} from "../../../../_lib/public-types";

type ParcelPageProps = {
  params: Promise<{
    countyId: string;
    taxYear: string;
    accountNumber: string;
  }>;
};

function caveatTone(caveat: ParcelDataCaveat): string {
  if (caveat.severity === "critical") {
    return "border-rose-200 bg-rose-50 text-rose-950";
  }
  if (caveat.severity === "warning") {
    return "border-amber-200 bg-amber-50 text-amber-950";
  }
  return "border-slate-200 bg-slate-50 text-slate-800";
}

function ValueCard({
  label,
  value,
}: {
  label: string;
  value: number | null | undefined;
}) {
  return (
    <div className="rounded-[1.5rem] border border-slate-200 bg-white/90 p-5">
      <div className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">{label}</div>
      <div className="mt-3 text-2xl font-semibold text-slate-950">{formatCurrency(value)}</div>
    </div>
  );
}

function ExemptionBadge({ active, label }: { active: boolean | null | undefined; label: string }) {
  return (
    <span
      className={`rounded-full px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] ${
        active ? "bg-emerald-100 text-emerald-900" : "bg-slate-100 text-slate-500"
      }`}
    >
      {label}
    </span>
  );
}

function TaxRateRow({ component }: { component: ParcelTaxRateComponent }) {
  return (
    <tr className="border-t border-slate-200/80">
      <td className="py-3 pr-4 align-top text-sm font-semibold text-slate-900">
        {component.unit_name ?? "Unnamed unit"}
      </td>
      <td className="py-3 pr-4 align-top text-sm text-slate-600">
        {component.unit_type_code ? formatLabel(component.unit_type_code) : "Unknown"}
      </td>
      <td className="py-3 pr-4 align-top text-sm text-slate-600">
        {component.rate_component ? formatLabel(component.rate_component) : "Base"}
      </td>
      <td className="py-3 align-top text-sm font-semibold text-slate-900">
        {formatRate(component.rate_value)}
      </td>
    </tr>
  );
}

function QuoteTeaser({ quote }: { quote: QuoteResponse }) {
  const fallbackNotice = buildTaxYearFallbackNotice(quote);

  return (
    <section className="rounded-[2rem] border border-emerald-200 bg-emerald-50/90 p-6">
      <p className="text-xs font-semibold uppercase tracking-[0.24em] text-emerald-700">
        Quote-safe read model
      </p>
      {fallbackNotice ? (
        <div className="mt-4 rounded-[1.25rem] border border-amber-200 bg-amber-100/80 px-4 py-3 text-sm font-medium text-amber-950">
          {fallbackNotice}
        </div>
      ) : null}
      <div className="mt-4 grid gap-4 md:grid-cols-3">
        <div>
          <div className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-700">
            Defensible value
          </div>
          <div className="mt-2 text-2xl font-semibold text-emerald-950">
            {formatCurrency(quote.defensible_value_point)}
          </div>
        </div>
        <div>
          <div className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-700">
            Expected tax savings
          </div>
          <div className="mt-2 text-2xl font-semibold text-emerald-950">
            {formatCurrency(quote.expected_tax_savings_point)}
          </div>
        </div>
        <div>
          <div className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-700">
            Recommendation
          </div>
          <div className="mt-2 text-lg font-semibold text-emerald-950">
            {quote.protest_recommendation ? formatLabel(quote.protest_recommendation) : "Unavailable"}
          </div>
        </div>
      </div>
      {quote.explanation_bullets.length > 0 ? (
        <ul className="mt-5 grid gap-2 text-sm leading-7 text-emerald-950">
          {quote.explanation_bullets.slice(0, 3).map((bullet) => (
            <li key={bullet}>• {bullet}</li>
          ))}
        </ul>
      ) : null}
    </section>
  );
}

function OwnerConfidenceNote({ summary }: { summary: ParcelSummaryResponse }) {
  const ownerSummary = summary.owner_summary;
  if (!ownerSummary) {
    return null;
  }

  return (
    <div className="rounded-[1.5rem] bg-slate-100 p-5">
      <div className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">
        Owner summary
      </div>
      <div className="mt-3 text-xl font-semibold text-slate-950">
        {ownerSummary.display_name ?? "Unavailable"}
      </div>
      <p className="mt-3 text-sm leading-7 text-slate-600">
        {ownerSummary.privacy_mode === "masked_individual_name"
          ? "Individual owner names are masked on public pages."
          : ownerSummary.privacy_mode === "public_entity_name"
            ? "Entity owner names are shown from public appraisal records."
            : "Owner display is hidden because a stable public-safe owner label is not available."}
      </p>
      <p className="mt-2 text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">
        Confidence {formatLabel(ownerSummary.confidence_label)}
      </p>
    </div>
  );
}

export default async function ParcelPage({ params }: ParcelPageProps) {
  const route = await params;
  const taxYear = Number.parseInt(route.taxYear, 10);

  if (!Number.isInteger(taxYear)) {
    notFound();
  }

  let summary: ParcelSummaryResponse;
  try {
    summary = await getParcelSummary(route.countyId, taxYear, route.accountNumber);
  } catch (error) {
    if (error instanceof Error && error.message.includes("404")) {
      notFound();
    }
    throw error;
  }

  const quote = await getQuote(route.countyId, taxYear, route.accountNumber);
  const parcelFallbackNotice = buildTaxYearFallbackNotice(summary);

  return (
    <main className="px-5 py-8 md:px-8 md:py-10">
      <section className="mx-auto flex w-full max-w-6xl flex-col gap-6">
        <header className="rounded-[2.25rem] border border-[var(--card-border)] bg-[var(--surface)] p-6 shadow-[var(--shadow)] backdrop-blur md:p-8">
          <div className="flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
            <div className="max-w-4xl">
              <Link href={`/search?address=${encodeURIComponent(summary.address)}`} className="text-sm font-semibold uppercase tracking-[0.22em] text-[var(--muted)]">
                Back to results
              </Link>
              <h1 className="mt-4 font-[family:var(--font-display-stack)] text-4xl tracking-tight text-slate-950 md:text-5xl">
                {summary.address}
              </h1>
              <p className="mt-4 text-base leading-8 text-slate-600">
                {summary.county_id} parcel {summary.account_number} for tax year {summary.tax_year}.
                This public summary is backed by canonical parcel-year read models and avoids live
                request-time valuation generation.
              </p>
              {parcelFallbackNotice ? (
                <div className="mt-5 rounded-[1.25rem] border border-amber-200 bg-amber-100/80 px-4 py-3 text-sm font-medium text-amber-950">
                  {parcelFallbackNotice}
                </div>
              ) : null}
            </div>
            <div className="grid gap-3 rounded-[1.8rem] bg-slate-950 px-5 py-5 text-sm text-slate-100">
              <div>
                <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                  Completeness
                </div>
                <div className="mt-2 text-3xl font-semibold">{summary.completeness_score.toFixed(0)}%</div>
              </div>
              <div>
                <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                  Effective tax rate
                </div>
                <div className="mt-2 text-xl font-semibold">{formatRate(summary.effective_tax_rate)}</div>
              </div>
            </div>
          </div>
        </header>

        {summary.caveats.length > 0 ? (
          <section className="grid gap-4 md:grid-cols-2">
            {summary.caveats.map((caveat) => (
              <article
                key={caveat.code}
                className={`rounded-[1.7rem] border p-5 ${caveatTone(caveat)}`}
              >
                <div className="text-xs font-semibold uppercase tracking-[0.18em]">
                  {formatLabel(caveat.severity)}
                </div>
                <h2 className="mt-3 text-xl font-semibold">{caveat.title}</h2>
                <p className="mt-3 text-sm leading-7">{caveat.message}</p>
              </article>
            ))}
          </section>
        ) : null}

        <section className="grid gap-6 lg:grid-cols-[1.15fr,0.85fr]">
          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-6 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <p className="text-xs font-semibold uppercase tracking-[0.24em] text-[var(--muted)]">
              Value summary
            </p>
            <div className="mt-5 grid gap-4 md:grid-cols-2 xl:grid-cols-3">
              <ValueCard label="Market value" value={summary.value_summary?.market_value} />
              <ValueCard label="Assessed value" value={summary.value_summary?.assessed_value} />
              <ValueCard label="Appraised value" value={summary.value_summary?.appraised_value} />
              <ValueCard label="Certified value" value={summary.value_summary?.certified_value} />
              <ValueCard label="Notice value" value={summary.value_summary?.notice_value} />
              <ValueCard label="Estimated annual tax" value={summary.tax_summary?.estimated_annual_tax} />
            </div>
          </article>

          <article className="grid gap-6">
            <OwnerConfidenceNote summary={summary} />
            <section className="rounded-[1.5rem] border border-slate-200 bg-white/90 p-5">
              <div className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">
                Parcel facts
              </div>
              <dl className="mt-4 grid grid-cols-2 gap-4 text-sm text-slate-700">
                <div>
                  <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Living area</dt>
                  <dd className="mt-2 font-semibold text-slate-950">
                    {summary.living_area_sf ? `${formatNumber(summary.living_area_sf)} sf` : "Unavailable"}
                  </dd>
                </div>
                <div>
                  <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Year built</dt>
                  <dd className="mt-2 font-semibold text-slate-950">{formatNumber(summary.year_built)}</dd>
                </div>
                <div>
                  <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Bedrooms</dt>
                  <dd className="mt-2 font-semibold text-slate-950">{formatNumber(summary.bedrooms)}</dd>
                </div>
                <div>
                  <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Bathrooms</dt>
                  <dd className="mt-2 font-semibold text-slate-950">
                    {summary.full_baths === null && summary.half_baths === null
                      ? "Unavailable"
                      : `${formatNumber(summary.full_baths)} full / ${formatNumber(summary.half_baths)} half`}
                  </dd>
                </div>
                <div>
                  <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Land</dt>
                  <dd className="mt-2 font-semibold text-slate-950">
                    {summary.land_sf ? `${formatNumber(summary.land_sf)} sf` : formatNumber(summary.land_acres)}
                  </dd>
                </div>
                <div>
                  <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">School district</dt>
                  <dd className="mt-2 font-semibold text-slate-950">
                    {summary.school_district_name ?? "Unavailable"}
                  </dd>
                </div>
              </dl>
            </section>
          </article>
        </section>

        <section className="grid gap-6 lg:grid-cols-[0.92fr,1.08fr]">
          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-6 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <p className="text-xs font-semibold uppercase tracking-[0.24em] text-[var(--muted)]">
              Exemptions
            </p>
            <div className="mt-5 text-3xl font-semibold text-slate-950">
              {formatCurrency(summary.exemption_summary?.exemption_value_total)}
            </div>
            <div className="mt-5 flex flex-wrap gap-2">
              <ExemptionBadge label="Homestead" active={summary.exemption_summary?.homestead_flag} />
              <ExemptionBadge label="Over 65" active={summary.exemption_summary?.over65_flag} />
              <ExemptionBadge label="Disabled" active={summary.exemption_summary?.disabled_flag} />
              <ExemptionBadge
                label="Disabled veteran"
                active={summary.exemption_summary?.disabled_veteran_flag}
              />
              <ExemptionBadge label="Freeze" active={summary.exemption_summary?.freeze_flag} />
            </div>
            <div className="mt-5 text-sm leading-7 text-slate-600">
              Codes {summary.exemption_summary?.exemption_type_codes.join(", ") || "Unavailable"}
            </div>
          </article>

          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-6 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <div className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.24em] text-[var(--muted)]">
                  Tax rate breakdown
                </p>
                <h2 className="mt-3 text-3xl font-semibold tracking-tight text-slate-950">
                  {formatRate(summary.tax_summary?.effective_tax_rate)}
                </h2>
              </div>
              <div className="rounded-[1.2rem] bg-slate-100 px-4 py-3 text-sm text-slate-700">
                Taxable value {formatCurrency(summary.tax_summary?.estimated_taxable_value)}
              </div>
            </div>
            <div className="mt-6 overflow-x-auto">
              <table className="min-w-full text-left">
                <thead className="text-xs uppercase tracking-[0.18em] text-slate-500">
                  <tr>
                    <th className="pb-3 pr-4">Unit</th>
                    <th className="pb-3 pr-4">Type</th>
                    <th className="pb-3 pr-4">Component</th>
                    <th className="pb-3">Rate</th>
                  </tr>
                </thead>
                <tbody>
                  {summary.tax_summary?.component_breakdown.length ? (
                    summary.tax_summary.component_breakdown.map((component) => (
                      <TaxRateRow
                        key={`${component.unit_code ?? "unknown"}-${component.rate_component ?? "base"}`}
                        component={component}
                      />
                    ))
                  ) : (
                    <tr>
                      <td colSpan={4} className="py-6 text-sm text-slate-500">
                        Component tax-rate data is not available yet for this parcel-year.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </article>
        </section>

        {quote ? <QuoteTeaser quote={quote} /> : null}
      </section>
    </main>
  );
}
