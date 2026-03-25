"use server";

import { buildLeadCapturePayload, normalizeOptionalText } from "./lead-funnel";
import { createLead } from "./public-api";
import type { LeadContextStatus } from "./public-types";

export type LeadCaptureFormState = {
  status: "idle" | "success" | "error";
  message?: string;
  contextStatus?: LeadContextStatus;
  leadId?: string;
  firstName?: string;
};

export const initialLeadCaptureFormState: LeadCaptureFormState = {
  status: "idle",
};

export async function submitLeadCaptureAction(
  _previousState: LeadCaptureFormState,
  formData: FormData,
): Promise<LeadCaptureFormState> {
  const firstName = normalizeOptionalText(formData.get("firstName")?.toString());

  try {
    const request = buildLeadCapturePayload({
      countyId: requiredField(formData, "countyId"),
      taxYear: Number.parseInt(requiredField(formData, "taxYear"), 10),
      accountNumber: requiredField(formData, "accountNumber"),
      ownerName: normalizeOptionalText(formData.get("ownerName")?.toString()) ?? null,
      email: requiredField(formData, "email"),
      phone: normalizeOptionalText(formData.get("phone")?.toString()) ?? null,
      consentToContact: formData.get("consentToContact") === "on",
      sourceChannel: requiredField(formData, "sourceChannel"),
      anonymousSessionId: normalizeOptionalText(formData.get("anonymousSessionId")?.toString()) ?? null,
      funnelStage: requiredField(formData, "funnelStage"),
      attribution: {
        anonymousSessionId: normalizeOptionalText(formData.get("anonymousSessionId")?.toString()) ?? null,
        utmSource: normalizeOptionalText(formData.get("utmSource")?.toString()) ?? null,
        utmMedium: normalizeOptionalText(formData.get("utmMedium")?.toString()) ?? null,
        utmCampaign: normalizeOptionalText(formData.get("utmCampaign")?.toString()) ?? null,
        utmTerm: normalizeOptionalText(formData.get("utmTerm")?.toString()) ?? null,
        utmContent: normalizeOptionalText(formData.get("utmContent")?.toString()) ?? null,
      },
    });

    const response = await createLead(request);
    return {
      status: "success",
      message: buildSuccessMessage(response.context_status, firstName),
      contextStatus: response.context_status,
      leadId: response.lead_id,
      firstName,
    };
  } catch (error) {
    return {
      status: "error",
      message:
        error instanceof Error
          ? error.message
          : "Dwellio could not save this lead right now. Please try again shortly.",
      firstName,
    };
  }
}

function requiredField(formData: FormData, fieldName: string): string {
  const value = normalizeOptionalText(formData.get(fieldName)?.toString());
  if (!value) {
    throw new Error(`Missing required field: ${fieldName}`);
  }
  return value;
}

function buildSuccessMessage(contextStatus: LeadContextStatus, firstName?: string): string {
  const greeting = firstName ? `${firstName}, ` : "";
  switch (contextStatus) {
    case "quote_ready":
      return `${greeting}your protest snapshot request is in. Dwellio has the parcel-year quote context and can follow up from this quote-safe record.`;
    case "missing_quote_ready_row":
      return `${greeting}we saved your parcel-year context and will follow up when the quote-ready row is published.`;
    case "unsupported_property_type":
      return `${greeting}we saved your info and will follow up when broader property support is available.`;
    case "unsupported_county":
      return `${greeting}we saved your info and will reach out when Dwellio opens this county.`;
  }
}
