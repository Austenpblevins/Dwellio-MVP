import type {
  LeadContextStatus,
  LeadCreateRequest,
  ParcelSearchResult,
  QuoteResponse,
} from "./public-types.ts";
import { isSupportedCounty, isSupportedPropertyType } from "./public-config.ts";

export const ATTRIBUTION_STORAGE_KEY = "dwellio.public.attribution";

export type AttributionSnapshot = {
  anonymousSessionId: string | null;
  utmSource: string | null;
  utmMedium: string | null;
  utmCampaign: string | null;
  utmTerm: string | null;
  utmContent: string | null;
};

export type LeadFunnelExperience = {
  status: LeadContextStatus;
  eyebrow: string;
  title: string;
  description: string;
  primaryCtaLabel: string;
  secondaryCtaLabel: string;
  leadTitle: string;
  leadDescription: string;
  tone: "emerald" | "amber" | "slate";
  showDetailedQuote: boolean;
};

export type LeadCaptureInput = {
  countyId: string;
  taxYear: number;
  accountNumber: string;
  ownerName?: string | null;
  email: string;
  phone?: string | null;
  consentToContact: boolean;
  sourceChannel: string;
  anonymousSessionId?: string | null;
  funnelStage: string;
  attribution?: Partial<AttributionSnapshot>;
};

const EMPTY_ATTRIBUTION: AttributionSnapshot = {
  anonymousSessionId: null,
  utmSource: null,
  utmMedium: null,
  utmCampaign: null,
  utmTerm: null,
  utmContent: null,
};

export function buildQuoteFunnelHref(result: ParcelSearchResult): string | null {
  if (result.tax_year === null) {
    return null;
  }

  return `/parcel/${result.county_id}/${result.tax_year}/${result.account_number}`;
}

export function deriveLeadFunnelExperience(input: {
  countyId: string;
  propertyTypeCode?: string | null;
  quote: QuoteResponse | null;
}): LeadFunnelExperience {
  if (!isSupportedCounty(input.countyId)) {
    return {
      status: "unsupported_county",
      eyebrow: "County rollout",
      title: "This county is not in Dwellio's public quote scope yet.",
      description:
        "Dwellio's MVP public flow currently supports Harris and Fort Bend single-family homes. You can still leave your email so we can notify you when coverage expands.",
      primaryCtaLabel: "Get county updates",
      secondaryCtaLabel: "See current coverage",
      leadTitle: "Join the county rollout list",
      leadDescription:
        "Leave an email and we will let you know when this county is available for public quote requests.",
      tone: "slate",
      showDetailedQuote: false,
    };
  }

  if (
    input.propertyTypeCode !== undefined &&
    input.propertyTypeCode !== null &&
    !isSupportedPropertyType(input.propertyTypeCode)
  ) {
    return {
      status: "unsupported_property_type",
      eyebrow: "Property type",
      title: "This property is outside the current instant-quote scope.",
      description:
        "The MVP public funnel is limited to single-family residential parcels. You can still leave your email for a manual follow-up as broader property support comes online.",
      primaryCtaLabel: "Request manual follow-up",
      secondaryCtaLabel: "Review parcel facts",
      leadTitle: "Stay in the loop for broader property support",
      leadDescription:
        "Email is enough for now. We'll keep your parcel-year context and reach out when this property type is supported.",
      tone: "amber",
      showDetailedQuote: false,
    };
  }

  if (input.quote === null) {
    return {
      status: "missing_quote_ready_row",
      eyebrow: "Quote refresh pending",
      title: "Parcel facts are available, but the quote-ready row is not yet published.",
      description:
        "Dwellio only serves precomputed public quotes. When a quote-safe row is missing, we keep the flow open so you can leave an email instead of hitting a dead end.",
      primaryCtaLabel: "Email me when the quote is ready",
      secondaryCtaLabel: "Review parcel facts",
      leadTitle: "Get notified when this parcel's quote is ready",
      leadDescription:
        "We'll save your parcel-year context and send an update when a quote-safe read-model row is available.",
      tone: "amber",
      showDetailedQuote: false,
    };
  }

  return {
    status: "quote_ready",
    eyebrow: "Instant protest signal",
    title: "See the protest value Dwellio would start from.",
    description:
      "This public quote is read from precomputed parcel-year data so the page stays fast, consistent, and explainable.",
    primaryCtaLabel: "Email my protest snapshot",
    secondaryCtaLabel: "See how the value was supported",
    leadTitle: "Email yourself the quote and next steps",
    leadDescription:
      "Email is required so we can send the parcel-year snapshot. Phone and first name stay optional for this soft gate.",
    tone: "emerald",
    showDetailedQuote: true,
  };
}

export function buildLeadCapturePayload(input: LeadCaptureInput): LeadCreateRequest {
  const email = normalizeOptionalText(input.email);
  if (!email) {
    throw new Error("Email is required before Dwellio can save this lead.");
  }

  const attribution = mergeAttributionSnapshot(EMPTY_ATTRIBUTION, input.attribution);

  return {
    county_id: input.countyId,
    tax_year: input.taxYear,
    account_number: input.accountNumber,
    owner_name: normalizeOptionalText(input.ownerName),
    email,
    phone: normalizeOptionalText(input.phone),
    consent_to_contact: input.consentToContact,
    source_channel: input.sourceChannel,
    anonymous_session_id:
      normalizeOptionalText(input.anonymousSessionId) ?? attribution.anonymousSessionId ?? undefined,
    funnel_stage: normalizeOptionalText(input.funnelStage),
    utm_source: attribution.utmSource ?? undefined,
    utm_medium: attribution.utmMedium ?? undefined,
    utm_campaign: attribution.utmCampaign ?? undefined,
    utm_term: attribution.utmTerm ?? undefined,
    utm_content: attribution.utmContent ?? undefined,
  };
}

export function extractAttributionFromSearchParams(
  searchParams: URLSearchParams,
): Partial<AttributionSnapshot> {
  return {
    utmSource: normalizeOptionalText(searchParams.get("utm_source")),
    utmMedium: normalizeOptionalText(searchParams.get("utm_medium")),
    utmCampaign: normalizeOptionalText(searchParams.get("utm_campaign")),
    utmTerm: normalizeOptionalText(searchParams.get("utm_term")),
    utmContent: normalizeOptionalText(searchParams.get("utm_content")),
  };
}

export function mergeAttributionSnapshot(
  existing: Partial<AttributionSnapshot> | null | undefined,
  incoming: Partial<AttributionSnapshot> | null | undefined,
): AttributionSnapshot {
  return {
    anonymousSessionId:
      normalizeOptionalText(incoming?.anonymousSessionId) ??
      normalizeOptionalText(existing?.anonymousSessionId) ??
      null,
    utmSource:
      normalizeOptionalText(incoming?.utmSource) ??
      normalizeOptionalText(existing?.utmSource) ??
      null,
    utmMedium:
      normalizeOptionalText(incoming?.utmMedium) ??
      normalizeOptionalText(existing?.utmMedium) ??
      null,
    utmCampaign:
      normalizeOptionalText(incoming?.utmCampaign) ??
      normalizeOptionalText(existing?.utmCampaign) ??
      null,
    utmTerm:
      normalizeOptionalText(incoming?.utmTerm) ??
      normalizeOptionalText(existing?.utmTerm) ??
      null,
    utmContent:
      normalizeOptionalText(incoming?.utmContent) ??
      normalizeOptionalText(existing?.utmContent) ??
      null,
  };
}

export function normalizeOptionalText(value: string | null | undefined): string | undefined {
  const normalized = value?.trim();
  return normalized ? normalized : undefined;
}
