import Link from "next/link";
import { notFound } from "next/navigation";

import { LeadCaptureCard } from "../../../../_components/LeadCaptureCard";
import { deriveLeadFunnelExperience } from "../../../../_lib/lead-funnel";
import { formatCurrency, formatLabel, formatNumber, formatRate } from "../../../../_lib/formatters";
import { isSupportedCounty } from "../../../../_lib/public-config";
import {
  getParcelSummary,
  getQuote,
  getQuoteExplanation,
} from "../../../../_lib/public-api";
import { buildTaxYearFallbackNotice } from "../../../../_lib/tax-year-fallback";
import type {
  ParcelDataCaveat,
  ParcelSummaryResponse,
  ParcelTaxRateComponent,
  QuoteExplanationResponse,
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

function ExperienceStatCard({
  label,
  value,
  footnote,
}: {
  label: string;
  value: string;
  footnote: string;
}) {
  return (
    <article className="rounded-[1.45rem] border border-white/70 bg-white/80 p-5">
      <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">{label}</div>
      <div className="mt-3 text-3xl font-semibold tracking-tight text-slate-950">{value}</div>
      <p className="mt-3 text-sm leading-6 text-slate-600">{footnote}</p>
    </article>
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

function DetailedQuoteSection({
  quote,
  explanation,
}: {
  quote: QuoteResponse;
  explanation: QuoteExplanationResponse | null;
}) {
  const bullets =
    explanation && explanation.explanation_bullets.length > 0
      ? explanation.explanation_bullets
      : quote.explanation_bullets;
  const explanationJson =
    explanation && Object.keys(explanation.explanation_json).length > 0
      ? explanation.explanation_json
      : quote.explanation_json;

  return (
    <section
      id="quote-details"
      className="rounded-[2rem] border border-emerald-200 bg-emerald-50/85 p-6"
    >
      <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div className="max-w-3xl">
          <p className="text-xs font-semibold uppercase tracking-[0.24em] text-emerald-700">
            Detailed quote section
          </p>
          <h2 className="mt-3 font-[family:var(--font-display-stack)] text-3xl tracking-tight text-emerald-950">
            Explanation and recommendation
          </h2>
          <p className="mt-4 text-sm leading-7 text-emerald-950/80">
            This section stays on quote-safe public outputs only. It does not expose raw comps or
            restricted listing artifacts.
          </p>
        </div>
        <div className="rounded-[1.2rem] bg-white/70 px-4 py-3 text-sm text-emerald-950">
          Recommendation{" "}
          <span className="font-semibold">
            {quote.protest_recommendation
              ? formatLabel(quote.protest_recommendation)
              : "Unavailable"}
          </span>
        </div>
      </div>

      <div className="mt-6 grid gap-6 lg:grid-cols-[0.95fr,1.05fr]">
        <article className="rounded-[1.6rem] bg-white/85 p-5">
          <div className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-700">
            Explanation bullets
          </div>
          {bullets.length > 0 ? (
            <ul className="mt-4 grid gap-3 text-sm leading-7 text-slate-700">
              {bullets.map((bullet) => (
                <li key={bullet}>• {bullet}</li>
              ))}
            </ul>
          ) : (
            <p className="mt-4 text-sm leading-7 text-slate-600">
              Dwellio has a quote row for this parcel-year, but detailed explanation bullets are
              still sparse.
            </p>
          )}
        </article>

        <article className="rounded-[1.6rem] bg-white/85 p-5">
          <div className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-700">
            Quote basis
          </div>
          <div className="mt-4 grid gap-3 text-sm leading-7 text-slate-700">
            <div>
              <span className="font-semibold text-slate-950">Current notice value:</span>{" "}
              {formatCurrency(quote.current_notice_value)}
            </div>
            <div>
              <span className="font-semibold text-slate-950">Defensible value:</span>{" "}
              {formatCurrency(quote.defensible_value_point)}
            </div>
            <div>
              <span className="font-semibold text-slate-950">Expected tax savings:</span>{" "}
              {formatCurrency(quote.expected_tax_savings_point)}
            </div>
            {Object.keys(explanationJson).length > 0 ? (
              <pre className="overflow-x-auto rounded-[1.2rem] bg-slate-950 px-4 py-4 text-xs leading-6 text-slate-100">
                {JSON.stringify(explanationJson, null, 2)}
              </pre>
            ) : null}
          </div>
        </article>
      </div>
    </section>
  );
}

function UnsupportedCountyFallback({
  countyId,
  taxYear,
  accountNumber,
}: {
  countyId: string;
  taxYear: number;
  accountNumber: string;
}) {
  const experience = deriveLeadFunnelExperience({
    countyId,
    propertyTypeCode: null,
    quote: null,
  });

  return (
    <main className="px-5 py-8 md:px-8 md:py-10">
      <section className="mx-auto flex w-full max-w-6xl flex-col gap-6">
        <header className="rounded-[2.25rem] border border-[var(--card-border)] bg-[var(--surface)] p-6 shadow-[var(--shadow)] backdrop-blur md:p-8">
          <Link href="/" className="text-sm font-semibold uppercase tracking-[0.22em] text-[var(--muted)]">
            Back to search
          </Link>
          <div className="mt-5 grid gap-6 lg:grid-cols-[1.05fr,0.95fr] lg:items-end">
            <div className="max-w-4xl">
              <p className="text-xs font-semibold uppercase tracking-[0.3em] text-[var(--muted)]">
                {experience.eyebrow}
              </p>
              <h1 className="mt-4 font-[family:var(--font-display-stack)] text-4xl tracking-tight text-slate-950 md:text-5xl">
                {experience.title}
              </h1>
              <p className="mt-4 text-base leading-8 text-slate-600">{experience.description}</p>
            </div>
            <div className="rounded-[1.8rem] bg-slate-950 px-5 py-5 text-sm text-slate-100">
              <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                Requested parcel-year
              </div>
              <div className="mt-2 text-lg font-semibold">{countyId}</div>
              <div>Tax year {taxYear}</div>
              <div>Account {accountNumber}</div>
            </div>
          </div>
        </header>

        <section className="grid gap-6 lg:grid-cols-[0.95fr,1.05fr]">
          <article className="rounded-[2rem] border border-slate-200/80 bg-white/95 p-6 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
            <p className="text-xs font-semibold uppercase tracking-[0.24em] text-[var(--muted)]">
              Current public scope
            </p>
            <ul className="mt-5 grid gap-3 text-sm leading-7 text-slate-700">
              <li>Harris and Fort Bend counties only.</li>
              <li>Single-family public instant-quote MVP only.</li>
              <li>Public routes stay on canonical `/search`, `/quote`, and `/lead` contracts.</li>
            </ul>
          </article>

          <LeadCaptureCard
            countyId={countyId}
            taxYear={taxYear}
            accountNumber={accountNumber}
            experience={experience}
          />
        </section>
      </section>
    </main>
  );
}

export default async function ParcelPage({ params }: ParcelPageProps) {
  const route = await params;
  const taxYear = Number.parseInt(route.taxYear, 10);

  if (!Number.isInteger(taxYear)) {
    notFound();
  }

  if (!isSupportedCounty(route.countyId)) {
    return (
      <UnsupportedCountyFallback
        countyId={route.countyId}
        taxYear={taxYear}
        accountNumber={route.accountNumber}
      />
    );
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
  const explanation = quote
    ? await getQuoteExplanation(route.countyId, taxYear, route.accountNumber)
    : null;
  const parcelFallbackNotice = buildTaxYearFallbackNotice(summary);
  const quoteFallbackNotice = quote ? buildTaxYearFallbackNotice(quote) : null;
  const experience = deriveLeadFunnelExperience({
    countyId: route.countyId,
    propertyTypeCode: summary.property_type_code,
    quote,
  });

  const heroTone =
    experience.tone === "emerald"
      ? "border-emerald-200 bg-emerald-50/85"
      : experience.tone === "amber"
        ? "border-amber-200 bg-amber-50/85"
        : "border-slate-200 bg-white/90";

  return (
    <main className="px-5 py-8 md:px-8 md:py-10">
      <section className="mx-auto flex w-full max-w-6xl flex-col gap-6">
        <header className="rounded-[2.25rem] border border-[var(--card-border)] bg-[var(--surface)] p-6 shadow-[var(--shadow)] backdrop-blur md:p-8">
          <div className="flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
            <div className="max-w-4xl">
              <Link
                href={`/search?address=${encodeURIComponent(summary.address)}`}
                className="text-sm font-semibold uppercase tracking-[0.22em] text-[var(--muted)]"
              >
                Back to results
              </Link>
              <h1 className="mt-4 font-[family:var(--font-display-stack)] text-4xl tracking-tight text-slate-950 md:text-5xl">
                {summary.address}
              </h1>
              <p className="mt-4 text-base leading-8 text-slate-600">
                {summary.county_id} parcel {summary.account_number} for tax year {summary.tax_year}.
                This public parcel page is also Dwellio&apos;s progressive quote-to-lead funnel for
                the same parcel-year identity.
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
                <div className="mt-2 text-3xl font-semibold">
                  {summary.completeness_score.toFixed(0)}%
                </div>
              </div>
              <div>
                <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                  Effective tax rate
                </div>
                <div className="mt-2 text-xl font-semibold">
                  {formatRate(summary.effective_tax_rate)}
                </div>
              </div>
            </div>
          </div>
        </header>

        <section className="grid gap-6 lg:grid-cols-[1.02fr,0.98fr]">
          <article className={`rounded-[2rem] border p-6 ${heroTone}`}>
            <p className="text-xs font-semibold uppercase tracking-[0.24em] text-[var(--muted)]">
              {experience.eyebrow}
            </p>
            <h2 className="mt-4 font-[family:var(--font-display-stack)] text-4xl tracking-tight text-slate-950">
              {experience.title}
            </h2>
            <p className="mt-4 max-w-3xl text-sm leading-7 text-slate-700">
              {experience.description}
            </p>

            {quoteFallbackNotice ? (
              <div className="mt-5 rounded-[1.2rem] border border-amber-200 bg-white/80 px-4 py-3 text-sm font-medium text-amber-950">
                {quoteFallbackNotice}
              </div>
            ) : null}

            <div className="mt-6 grid gap-4 md:grid-cols-3">
              <ExperienceStatCard
                label={quote ? "Defensible value" : "Notice value"}
                value={formatCurrency(quote?.defensible_value_point ?? summary.notice_value)}
                footnote={
                  quote
                    ? "Public-safe protest value pulled from the quote read model."
                    : "Current public notice value while quote-ready data is pending or unsupported."
                }
              />
              <ExperienceStatCard
                label={quote ? "Expected tax savings" : "Property type"}
                value={
                  quote
                    ? formatCurrency(quote.expected_tax_savings_point)
                    : formatLabel(summary.property_type_code ?? "unknown")
                }
                footnote={
                  quote
                    ? "Expected savings estimate when a quote row exists."
                    : "The public funnel still keeps lead capture open even when quote coverage is limited."
                }
              />
              <ExperienceStatCard
                label={quote ? "Recommendation" : "Next best step"}
                value={
                  quote && quote.protest_recommendation
                    ? formatLabel(quote.protest_recommendation)
                    : "Save this parcel-year"
                }
                footnote={
                  quote
                    ? "Recommendation stays tied to this parcel-year quote-safe row."
                    : "Leave your email so Dwellio can follow up without losing parcel context."
                }
              />
            </div>

            <div className="mt-6 flex flex-col gap-3 sm:flex-row">
              <a
                href="#lead-capture"
                className="inline-flex items-center justify-center rounded-[1.2rem] bg-slate-950 px-5 py-3 text-sm font-semibold text-white transition hover:bg-[var(--accent-strong)]"
              >
                {experience.primaryCtaLabel}
              </a>
              <a
                href={experience.showDetailedQuote ? "#quote-details" : "#parcel-facts"}
                className="inline-flex items-center justify-center rounded-[1.2rem] border border-slate-300 bg-white/80 px-5 py-3 text-sm font-semibold text-slate-700 transition hover:border-[var(--accent)] hover:text-[var(--accent-strong)]"
              >
                {experience.secondaryCtaLabel}
              </a>
            </div>
          </article>

          <LeadCaptureCard
            countyId={summary.county_id}
            taxYear={summary.tax_year}
            accountNumber={summary.account_number}
            ownerName={summary.owner_name}
            experience={experience}
          />
        </section>

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

        {quote && experience.showDetailedQuote ? (
          <DetailedQuoteSection quote={quote} explanation={explanation} />
        ) : null}

        <section id="parcel-facts" className="grid gap-6 lg:grid-cols-[1.15fr,0.85fr]">
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
              <ValueCard
                label="Estimated annual tax"
                value={summary.tax_summary?.estimated_annual_tax}
              />
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
                    {summary.living_area_sf
                      ? `${formatNumber(summary.living_area_sf)} sf`
                      : "Unavailable"}
                  </dd>
                </div>
                <div>
                  <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Year built</dt>
                  <dd className="mt-2 font-semibold text-slate-950">
                    {formatNumber(summary.year_built)}
                  </dd>
                </div>
                <div>
                  <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">Bedrooms</dt>
                  <dd className="mt-2 font-semibold text-slate-950">
                    {formatNumber(summary.bedrooms)}
                  </dd>
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
                    {summary.land_sf
                      ? `${formatNumber(summary.land_sf)} sf`
                      : formatNumber(summary.land_acres)}
                  </dd>
                </div>
                <div>
                  <dt className="text-xs uppercase tracking-[0.18em] text-slate-500">
                    School district
                  </dt>
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
      </section>
    </main>
  );
}
