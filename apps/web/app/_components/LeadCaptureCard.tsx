"use client";

import { useActionState, useEffect, useState } from "react";
import { useFormStatus } from "react-dom";

import {
  ATTRIBUTION_STORAGE_KEY,
  type AttributionSnapshot,
  type LeadFunnelExperience,
} from "../_lib/lead-funnel";
import {
  initialLeadCaptureFormState,
  submitLeadCaptureAction,
} from "../_lib/lead-capture-action";

type LeadCaptureCardProps = {
  countyId: string;
  taxYear: number;
  accountNumber: string;
  ownerName?: string | null;
  experience: LeadFunnelExperience;
};

const EMPTY_ATTRIBUTION: AttributionSnapshot = {
  anonymousSessionId: null,
  utmSource: null,
  utmMedium: null,
  utmCampaign: null,
  utmTerm: null,
  utmContent: null,
};

function getInitialAttributionSnapshot(): AttributionSnapshot {
  if (typeof window === "undefined") {
    return EMPTY_ATTRIBUTION;
  }

  const raw = window.sessionStorage.getItem(ATTRIBUTION_STORAGE_KEY);
  if (raw) {
    try {
      return JSON.parse(raw) as AttributionSnapshot;
    } catch {
      // Fall through to regenerate a clean snapshot.
    }
  }

  return {
    ...EMPTY_ATTRIBUTION,
    anonymousSessionId: crypto.randomUUID(),
  };
}

function SubmitButton() {
  const { pending } = useFormStatus();

  return (
    <button
      type="submit"
      className="inline-flex w-full items-center justify-center rounded-[1.2rem] bg-[var(--accent-strong)] px-4 py-3 text-sm font-semibold text-white transition hover:bg-[var(--accent)] disabled:cursor-not-allowed disabled:opacity-70"
      disabled={pending}
    >
      {pending ? "Saving your request..." : "Save my quote follow-up"}
    </button>
  );
}

export function LeadCaptureCard({
  countyId,
  taxYear,
  accountNumber,
  ownerName,
  experience,
}: LeadCaptureCardProps) {
  const [state, formAction] = useActionState(submitLeadCaptureAction, initialLeadCaptureFormState);
  const [expanded, setExpanded] = useState(experience.status !== "quote_ready");
  const [attribution] = useState<AttributionSnapshot>(getInitialAttributionSnapshot);

  useEffect(() => {
    window.sessionStorage.setItem(ATTRIBUTION_STORAGE_KEY, JSON.stringify(attribution));
  }, [attribution]);

  const cardTone =
    experience.tone === "emerald"
      ? "border-emerald-200 bg-emerald-50/90"
      : experience.tone === "amber"
        ? "border-amber-200 bg-amber-50/90"
        : "border-slate-200 bg-white/95";

  return (
    <section
      id="lead-capture"
      className={`rounded-[2rem] border p-6 shadow-[0_18px_45px_rgba(15,23,42,0.08)] ${cardTone}`}
    >
      <p className="text-xs font-semibold uppercase tracking-[0.24em] text-[var(--muted)]">
        Soft-gated lead capture
      </p>
      <h2 className="mt-4 font-[family:var(--font-display-stack)] text-3xl tracking-tight text-slate-950">
        {experience.leadTitle}
      </h2>
      <p className="mt-4 text-sm leading-7 text-slate-700">{experience.leadDescription}</p>

      {state.status === "success" ? (
        <div className="mt-6 rounded-[1.4rem] border border-emerald-200 bg-white/90 p-5 text-emerald-950">
          <div className="text-xs font-semibold uppercase tracking-[0.2em] text-emerald-700">
            Saved
          </div>
          <p className="mt-3 text-sm leading-7">{state.message}</p>
        </div>
      ) : null}

      {state.status === "error" ? (
        <div className="mt-6 rounded-[1.4rem] border border-rose-200 bg-rose-50 p-5 text-rose-950">
          <div className="text-xs font-semibold uppercase tracking-[0.2em] text-rose-700">
            Lead capture unavailable
          </div>
          <p className="mt-3 text-sm leading-7">{state.message}</p>
        </div>
      ) : null}

      {!expanded ? (
        <button
          type="button"
          onClick={() => setExpanded(true)}
          className="mt-6 inline-flex w-full items-center justify-center rounded-[1.2rem] bg-slate-950 px-4 py-3 text-sm font-semibold text-white transition hover:bg-[var(--accent-strong)]"
        >
          {experience.primaryCtaLabel}
        </button>
      ) : null}

      {expanded ? (
        <form action={formAction} className="mt-6 grid gap-4">
          <input type="hidden" name="countyId" value={countyId} />
          <input type="hidden" name="taxYear" value={taxYear} />
          <input type="hidden" name="accountNumber" value={accountNumber} />
          <input type="hidden" name="ownerName" value={ownerName ?? ""} />
          <input type="hidden" name="sourceChannel" value="web_quote_funnel" />
          <input
            type="hidden"
            name="funnelStage"
            value={experience.status === "quote_ready" ? "quote_gate" : "quote_support"}
          />
          <input type="hidden" name="anonymousSessionId" value={attribution.anonymousSessionId ?? ""} />
          <input type="hidden" name="utmSource" value={attribution.utmSource ?? ""} />
          <input type="hidden" name="utmMedium" value={attribution.utmMedium ?? ""} />
          <input type="hidden" name="utmCampaign" value={attribution.utmCampaign ?? ""} />
          <input type="hidden" name="utmTerm" value={attribution.utmTerm ?? ""} />
          <input type="hidden" name="utmContent" value={attribution.utmContent ?? ""} />

          <label className="grid gap-2">
            <span className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">
              Email
            </span>
            <input
              type="email"
              name="email"
              required
              placeholder="you@example.com"
              className="rounded-[1.2rem] border border-slate-300 bg-white px-4 py-3 text-base text-slate-950 placeholder:text-slate-400 focus:border-[var(--accent)] focus:outline-none"
            />
          </label>

          <div className="grid gap-4 sm:grid-cols-2">
            <label className="grid gap-2">
              <span className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">
                First name
              </span>
              <input
                type="text"
                name="firstName"
                placeholder="Optional"
                className="rounded-[1.2rem] border border-slate-300 bg-white px-4 py-3 text-base text-slate-950 placeholder:text-slate-400 focus:border-[var(--accent)] focus:outline-none"
              />
            </label>
            <label className="grid gap-2">
              <span className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">
                Phone
              </span>
              <input
                type="tel"
                name="phone"
                placeholder="Optional"
                className="rounded-[1.2rem] border border-slate-300 bg-white px-4 py-3 text-base text-slate-950 placeholder:text-slate-400 focus:border-[var(--accent)] focus:outline-none"
              />
            </label>
          </div>

          <label className="flex items-start gap-3 rounded-[1.2rem] bg-white/80 px-4 py-3 text-sm leading-6 text-slate-700">
            <input
              type="checkbox"
              name="consentToContact"
              className="mt-1 h-4 w-4 rounded border-slate-300 text-[var(--accent-strong)]"
            />
            <span>
              I&apos;m okay with Dwellio following up about this parcel-year quote or county support.
            </span>
          </label>

          <SubmitButton />
        </form>
      ) : null}
    </section>
  );
}
