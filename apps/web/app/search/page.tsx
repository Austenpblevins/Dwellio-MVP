import Link from "next/link";

import { buildQuoteFunnelHref } from "../_lib/lead-funnel";
import { searchParcels } from "../_lib/public-api";
import { formatLabel } from "../_lib/formatters";
import type { ParcelSearchResult } from "../_lib/public-types";

type SearchPageProps = {
  searchParams?: Promise<{ address?: string }>;
};

function toneForConfidence(label: string): string {
  if (label === "very_high" || label === "high") {
    return "bg-emerald-100 text-emerald-900";
  }
  if (label === "medium") {
    return "bg-amber-100 text-amber-900";
  }
  return "bg-slate-200 text-slate-800";
}

function ResultCard({ result }: { result: ParcelSearchResult }) {
  const parcelHref = buildQuoteFunnelHref(result);

  return (
    <article className="rounded-[1.8rem] border border-slate-200/80 bg-white/95 p-6 shadow-[0_18px_45px_rgba(15,23,42,0.08)]">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div className="max-w-3xl">
          <div className="flex flex-wrap items-center gap-3">
            <span className="rounded-full bg-slate-950 px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] text-white">
              {result.county_id}
            </span>
            <span
              className={`rounded-full px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] ${toneForConfidence(result.confidence_label)}`}
            >
              {formatLabel(result.confidence_label)}
            </span>
          </div>
          <h2 className="mt-4 font-[family:var(--font-display-stack)] text-3xl tracking-tight text-slate-950">
            {result.address}
          </h2>
          <p className="mt-3 text-sm leading-7 text-slate-600">
            Account {result.account_number}
            {result.tax_year !== null ? ` for ${result.tax_year}` : ""}. Match basis{" "}
            {formatLabel(result.match_basis)}. Opening the parcel route keeps the flow on the
            canonical quote-safe public pages.
          </p>
          <div className="mt-4 flex flex-wrap gap-3 text-sm text-slate-700">
            <span className="rounded-full bg-slate-100 px-3 py-1">
              Owner {result.owner_name ?? "Unavailable"}
            </span>
            <span className="rounded-full bg-slate-100 px-3 py-1">
              Match score {result.match_score.toFixed(2)}
            </span>
          </div>
        </div>
        {parcelHref ? (
          <Link
            href={parcelHref}
            className="inline-flex items-center justify-center rounded-[1.2rem] bg-[var(--accent-strong)] px-5 py-3 text-sm font-semibold text-white transition hover:bg-[var(--accent)]"
          >
            Open quote funnel
          </Link>
        ) : (
          <span className="rounded-[1.2rem] bg-slate-100 px-5 py-3 text-sm font-semibold text-slate-500">
            Parcel year not available yet
          </span>
        )}
      </div>
    </article>
  );
}

export default async function SearchPage({ searchParams }: SearchPageProps) {
  const params = (await searchParams) ?? {};
  const address = params.address?.trim() ?? "";

  let results: ParcelSearchResult[] = [];
  let errorMessage: string | null = null;

  if (address.length >= 3) {
    try {
      const response = await searchParcels(address);
      results = response.results;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : "Search is unavailable.";
    }
  }

  return (
    <main className="px-5 py-8 md:px-8 md:py-10">
      <section className="mx-auto flex w-full max-w-6xl flex-col gap-6">
        <header className="rounded-[2.25rem] border border-[var(--card-border)] bg-[var(--surface)] p-6 shadow-[var(--shadow)] backdrop-blur md:p-8">
          <Link href="/" className="text-sm font-semibold uppercase tracking-[0.22em] text-[var(--muted)]">
            Back to search
          </Link>
          <div className="mt-5 flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
            <div className="max-w-3xl">
              <p className="text-xs font-semibold uppercase tracking-[0.3em] text-[var(--muted)]">
                Search results
              </p>
              <h1 className="mt-4 font-[family:var(--font-display-stack)] text-4xl tracking-tight text-slate-950 md:text-5xl">
                {address ? `Results for “${address}”` : "Enter an address to begin"}
              </h1>
              <p className="mt-4 text-base leading-8 text-slate-600">
                Search results come from Dwellio&apos;s public parcel read model and carry parcel-year
                identity so the next page can stay tied to a specific quote-safe parcel-year flow.
              </p>
            </div>
            <form action="/search" className="flex w-full max-w-xl flex-col gap-3 sm:flex-row">
              <input
                type="text"
                name="address"
                defaultValue={address}
                minLength={3}
                required
                placeholder="Search another address"
                className="min-w-0 flex-1 rounded-[1.2rem] border border-slate-300 bg-white px-4 py-3 text-base text-slate-950 placeholder:text-slate-400 focus:border-[var(--accent)] focus:outline-none"
              />
              <button
                type="submit"
                className="rounded-[1.2rem] bg-slate-950 px-5 py-3 text-sm font-semibold text-white transition hover:bg-[var(--accent-strong)]"
              >
                Search
              </button>
            </form>
          </div>
        </header>

        {errorMessage ? (
          <section className="rounded-[2rem] border border-rose-200 bg-rose-50 p-6 text-rose-950">
            <h2 className="text-xl font-semibold">Parcel search needs the API backend</h2>
            <p className="mt-3 text-sm leading-7">
              The public search page could not reach the API. Confirm the FastAPI service is running
              and `DWELLIO_API_BASE_URL` points to it. Error: {errorMessage}
            </p>
          </section>
        ) : null}

        {!errorMessage && address.length < 3 ? (
          <section className="rounded-[2rem] border border-slate-200 bg-white/90 p-6 text-slate-700">
            Enter at least three characters to search public parcel records.
          </section>
        ) : null}

        {!errorMessage && address.length >= 3 && results.length === 0 ? (
          <section className="rounded-[2rem] border border-slate-200 bg-white/90 p-6 text-slate-700">
            No parcel matches were found for that query yet. Try the notice address, a simpler
            street form, or the account number.
          </section>
        ) : null}

        <section className="grid gap-5">
          {results.map((result) => (
            <ResultCard
              key={`${result.county_id}-${result.tax_year ?? "unknown"}-${result.account_number}`}
              result={result}
            />
          ))}
        </section>
      </section>
    </main>
  );
}
