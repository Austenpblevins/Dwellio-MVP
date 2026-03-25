import Link from "next/link";

const quickSearches = [
  "101 Main St Houston TX 77002",
  "123 Oak Meadow Dr Katy TX 77494",
  "1001 Pine Hollow Ln Sugar Land TX 77479",
];

export default function HomePage() {
  return (
    <main className="px-5 py-8 text-slate-950 md:px-8 md:py-10">
      <section className="mx-auto flex min-h-[calc(100vh-4rem)] w-full max-w-6xl flex-col gap-8 rounded-[2.5rem] border border-[var(--card-border)] bg-[var(--surface)] p-6 shadow-[var(--shadow)] backdrop-blur md:p-8">
        <header className="grid gap-8 lg:grid-cols-[1.1fr,0.9fr] lg:items-end">
          <div className="max-w-3xl">
            <p className="text-xs font-semibold uppercase tracking-[0.35em] text-[var(--muted)]">
              Dwellio Public Quote Funnel
            </p>
            <h1 className="mt-4 max-w-3xl font-[family:var(--font-display-stack)] text-5xl leading-[0.98] tracking-tight text-slate-950 md:text-7xl">
              Start with your notice address and see whether a protest is worth your time.
            </h1>
            <p className="mt-5 max-w-2xl text-base leading-8 text-slate-600 md:text-lg">
              Dwellio resolves the parcel first, reads quote-safe public models second, and keeps
              the lead step lightweight when deeper quote data is missing or still rolling out.
            </p>
            <div className="mt-6 flex flex-wrap gap-3 text-sm text-slate-700">
              <span className="rounded-full bg-white/80 px-4 py-2">Harris + Fort Bend</span>
              <span className="rounded-full bg-white/80 px-4 py-2">Single-family homes</span>
              <span className="rounded-full bg-white/80 px-4 py-2">Public-safe quote data only</span>
            </div>
          </div>

          <section className="rounded-[2rem] border border-slate-200/80 bg-[var(--surface-strong)] p-6 md:p-8">
            <p className="text-xs font-semibold uppercase tracking-[0.3em] text-[var(--muted)]">
              Search entry
            </p>
            <h2 className="mt-4 font-[family:var(--font-display-stack)] text-3xl tracking-tight md:text-4xl">
              Enter the parcel you want Dwellio to review.
            </h2>
            <form action="/search" className="mt-8 flex flex-col gap-4">
              <label
                htmlFor="address"
                className="text-sm font-semibold uppercase tracking-[0.2em] text-slate-500"
              >
                Address or account number
              </label>
              <input
                id="address"
                name="address"
                required
                minLength={3}
                placeholder="101 Main St Houston TX 77002"
                className="rounded-[1.35rem] border border-slate-300 bg-white px-5 py-4 text-base text-slate-950 outline-none ring-0 placeholder:text-slate-400 focus:border-[var(--accent)]"
              />
              <button
                type="submit"
                className="inline-flex items-center justify-center rounded-[1.35rem] bg-[var(--accent-strong)] px-5 py-4 text-base font-semibold text-white transition hover:bg-[var(--accent)]"
              >
                Find my parcel and quote signal
              </button>
            </form>
          </section>
        </header>

        <section className="grid gap-6 lg:grid-cols-[0.95fr,1.05fr]">
          <article className="rounded-[2rem] border border-slate-200/80 bg-white/90 p-6">
            <p className="text-xs font-semibold uppercase tracking-[0.25em] text-[var(--muted)]">
              How the funnel works
            </p>
            <ol className="mt-5 grid gap-4 text-sm leading-7 text-slate-700">
              <li>
                1. Search the parcel tied to your notice address or account number.
              </li>
              <li>
                2. Review the parcel-year facts and any available quote-safe protest signal.
              </li>
              <li>
                3. Leave just your email to save the parcel-year context and keep moving.
              </li>
            </ol>
          </article>

          <article className="grid gap-4">
            <section className="rounded-[2rem] border border-slate-200/80 bg-white/90 p-6">
              <p className="text-xs font-semibold uppercase tracking-[0.25em] text-[var(--muted)]">
                What you can review
              </p>
              <ul className="mt-5 grid gap-3 text-sm leading-7 text-slate-700">
                <li>Notice value, defensible value, and expected savings when a quote row exists.</li>
                <li>Parcel facts, exemptions, tax-rate components, and public caveats.</li>
                <li>Recommendation and explanation bullets from the quote-safe read model.</li>
              </ul>
            </section>
            <section className="rounded-[2rem] border border-amber-200 bg-amber-50/90 p-6 text-amber-950">
              <p className="text-xs font-semibold uppercase tracking-[0.25em] text-amber-700">
                Public-safe by design
              </p>
              <p className="mt-4 text-sm leading-7">
                Restricted MLS or internal evidence never appears on public pages. If quote-ready
                data is missing, Dwellio keeps the lead path open instead of faking a live value.
              </p>
            </section>
          </article>
        </section>

        <section className="border-t border-slate-200/70 pt-8">
          <div className="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">
            Quick examples
          </div>
          <div className="mt-4 flex flex-wrap gap-3">
            {quickSearches.map((query) => (
              <Link
                key={query}
                href={`/search?address=${encodeURIComponent(query)}`}
                className="rounded-full border border-slate-300 bg-white px-4 py-2 text-sm text-slate-700 transition hover:border-[var(--accent)] hover:text-[var(--accent-strong)]"
              >
                {query}
              </Link>
            ))}
          </div>
        </section>
      </section>
    </main>
  );
}
