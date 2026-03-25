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
        <header className="flex flex-col gap-5 border-b border-slate-200/70 pb-8 lg:flex-row lg:items-end lg:justify-between">
          <div className="max-w-3xl">
            <p className="text-xs font-semibold uppercase tracking-[0.35em] text-[var(--muted)]">
              Public Parcel Search
            </p>
            <h1 className="mt-4 max-w-3xl font-[family:var(--font-display-stack)] text-5xl leading-[1.02] tracking-tight text-slate-950 md:text-7xl">
              Clean county tax data for the parcel you actually own.
            </h1>
            <p className="mt-5 max-w-2xl text-base leading-8 text-slate-600 md:text-lg">
              Search by address or account number to view a public-safe parcel summary built from
              canonical read models, with values, exemptions, tax-rate components, and caveats in
              one place.
            </p>
          </div>
          <div className="rounded-[2rem] bg-slate-950 px-5 py-5 text-sm text-slate-100">
            <div className="text-xs font-semibold uppercase tracking-[0.25em] text-slate-400">
              Current scope
            </div>
            <div className="mt-3">Harris + Fort Bend</div>
            <div className="text-slate-400">Single-family public parcel summary MVP</div>
          </div>
        </header>

        <section className="grid gap-8 lg:grid-cols-[1.2fr,0.8fr]">
          <article className="rounded-[2rem] border border-slate-200/80 bg-[var(--surface-strong)] p-6 md:p-8">
            <p className="text-xs font-semibold uppercase tracking-[0.3em] text-[var(--muted)]">
              Search
            </p>
            <h2 className="mt-4 font-[family:var(--font-display-stack)] text-3xl tracking-tight md:text-4xl">
              Start with the address on your notice.
            </h2>
            <form action="/search" className="mt-8 flex flex-col gap-4">
              <label htmlFor="address" className="text-sm font-semibold uppercase tracking-[0.2em] text-slate-500">
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
                Search parcel records
              </button>
            </form>

            <div className="mt-8">
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
            </div>
          </article>

          <article className="grid gap-4">
            <section className="rounded-[2rem] border border-slate-200/80 bg-white/90 p-6">
              <p className="text-xs font-semibold uppercase tracking-[0.25em] text-[var(--muted)]">
                You can review
              </p>
              <ul className="mt-5 grid gap-3 text-sm leading-7 text-slate-700">
                <li>Market, assessed, appraised, certified, and notice values.</li>
                <li>Homestead and exemption flags with exemption totals when available.</li>
                <li>Component tax-rate breakdowns and total effective rate.</li>
                <li>Privacy-safe owner summary and data-quality caveats.</li>
              </ul>
            </section>
            <section className="rounded-[2rem] border border-amber-200 bg-amber-50/90 p-6 text-amber-950">
              <p className="text-xs font-semibold uppercase tracking-[0.25em] text-amber-700">
                Public-safe by design
              </p>
              <p className="mt-4 text-sm leading-7">
                Restricted MLS or internal-only evidence never appears in these public pages. When
                data is incomplete, the parcel page explains what is limited instead of inventing a
                value.
              </p>
            </section>
          </article>
        </section>
      </section>
    </main>
  );
}
