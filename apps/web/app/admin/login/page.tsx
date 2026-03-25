import { redirect } from "next/navigation";

import { clearAdminTokenCookie, getExpectedAdminToken, setAdminTokenCookie } from "../_lib/auth";

async function loginAction(formData: FormData): Promise<void> {
  "use server";

  const submittedToken = String(formData.get("admin_token") ?? "");
  if (submittedToken !== getExpectedAdminToken()) {
    redirect("/admin/login?error=invalid");
  }

  await setAdminTokenCookie(submittedToken);
  redirect("/admin/ops");
}

async function logoutAction(): Promise<void> {
  "use server";

  await clearAdminTokenCookie();
  redirect("/admin/login");
}

export default async function AdminLoginPage({
  searchParams,
}: {
  searchParams?: Promise<{ error?: string }>;
}) {
  const params = (await searchParams) ?? {};
  const hasError = params.error === "invalid";

  return (
    <main className="min-h-screen bg-[radial-gradient(circle_at_top,#efe6d4_0%,#e7eef5_45%,#f8fafc_100%)] px-6 py-12 text-slate-950">
      <section className="mx-auto grid max-w-5xl gap-8 md:grid-cols-[1.1fr,0.9fr]">
        <div className="rounded-[2rem] border border-slate-200/80 bg-white/90 p-10 shadow-[0_18px_50px_rgba(15,23,42,0.08)]">
          <p className="text-xs font-semibold uppercase tracking-[0.28em] text-slate-500">
            Internal operations
          </p>
          <h1 className="mt-4 text-4xl font-semibold tracking-tight text-slate-950">
            Admin ops sign-in
          </h1>
          <p className="mt-4 max-w-xl text-base leading-7 text-slate-600">
            Use the shared internal admin token to open ingestion, validation, manual fallback,
            and readiness tooling. This gate is intentionally small and temporary until broader
            platform auth is wired in.
          </p>
          <form action={loginAction} className="mt-8 grid gap-4">
            <label className="grid gap-2 text-sm font-medium text-slate-700">
              Admin token
              <input
                type="password"
                name="admin_token"
                className="rounded-2xl border border-slate-300 bg-white px-4 py-3 text-sm text-slate-950 outline-none ring-0"
                placeholder="Enter DWELLIO_ADMIN_API_TOKEN"
                required
              />
            </label>
            {hasError ? (
              <p className="rounded-2xl bg-rose-50 px-4 py-3 text-sm text-rose-900">
                The provided admin token was not accepted.
              </p>
            ) : null}
            <button
              type="submit"
              className="rounded-2xl bg-slate-950 px-5 py-3 text-sm font-semibold text-white transition hover:bg-slate-800"
            >
              Continue to admin ops
            </button>
          </form>
        </div>

        <aside className="rounded-[2rem] border border-slate-200/80 bg-slate-950 p-10 text-slate-50 shadow-[0_18px_50px_rgba(15,23,42,0.16)]">
          <h2 className="text-lg font-semibold uppercase tracking-[0.22em] text-slate-300">
            What this unlocks
          </h2>
          <ul className="mt-5 space-y-3 text-sm leading-7 text-slate-100">
            <li>- import batch inspection and failure review</li>
            <li>- validation warnings and errors</li>
            <li>- raw source file visibility</li>
            <li>- manual fallback registration</li>
            <li>- publish and rollback controls</li>
            <li>- parcel completeness and tax assignment QA</li>
          </ul>
          <form action={logoutAction} className="mt-8">
            <button
              type="submit"
              className="rounded-2xl border border-white/20 px-4 py-3 text-sm font-semibold text-white transition hover:bg-white/10"
            >
              Clear admin cookie
            </button>
          </form>
        </aside>
      </section>
    </main>
  );
}
