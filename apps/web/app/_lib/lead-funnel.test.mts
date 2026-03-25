import test from "node:test";
import assert from "node:assert/strict";

import {
  buildLeadCapturePayload,
  buildQuoteFunnelHref,
  deriveLeadFunnelExperience,
  extractAttributionFromSearchParams,
  mergeAttributionSnapshot,
} from "./lead-funnel.ts";

test("buildQuoteFunnelHref maps search results into the parcel-based quote path", () => {
  assert.equal(
    buildQuoteFunnelHref({
      county_id: "harris",
      tax_year: 2026,
      account_number: "1001001001001",
      parcel_id: "parcel-1",
      address: "101 Main St, Houston, TX 77002",
      situs_zip: "77002",
      owner_name: "A. Example",
      match_basis: "address_exact",
      match_score: 0.98,
      confidence_label: "very_high",
    }),
    "/parcel/harris/2026/1001001001001",
  );
});

test("deriveLeadFunnelExperience returns unsupported and missing states without changing route contracts", () => {
  assert.equal(
    deriveLeadFunnelExperience({
      countyId: "dallas",
      propertyTypeCode: "sfr",
      quote: null,
    }).status,
    "unsupported_county",
  );

  assert.equal(
    deriveLeadFunnelExperience({
      countyId: "harris",
      propertyTypeCode: "condo",
      quote: null,
    }).status,
    "unsupported_property_type",
  );

  assert.equal(
    deriveLeadFunnelExperience({
      countyId: "harris",
      propertyTypeCode: "sfr",
      quote: null,
    }).status,
    "missing_quote_ready_row",
  );
});

test("buildLeadCapturePayload keeps email required while preserving quote-funnel attribution fields", () => {
  const request = buildLeadCapturePayload({
    countyId: "harris",
    taxYear: 2026,
    accountNumber: "1001001001001",
    ownerName: "A. Example",
    email: "alex@example.com",
    phone: "7135550101",
    consentToContact: true,
    sourceChannel: "web_quote_funnel",
    anonymousSessionId: "anon-123",
    funnelStage: "quote_gate",
    attribution: {
      utmSource: "google",
      utmMedium: "cpc",
      utmCampaign: "spring",
    },
  });

  assert.deepEqual(request, {
    county_id: "harris",
    tax_year: 2026,
    account_number: "1001001001001",
    owner_name: "A. Example",
    email: "alex@example.com",
    phone: "7135550101",
    consent_to_contact: true,
    source_channel: "web_quote_funnel",
    anonymous_session_id: "anon-123",
    funnel_stage: "quote_gate",
    utm_source: "google",
    utm_medium: "cpc",
    utm_campaign: "spring",
    utm_term: null,
    utm_content: null,
  });
});

test("attribution helpers merge stored values with current-page utm params", () => {
  const incoming = extractAttributionFromSearchParams(
    new URLSearchParams("utm_source=newsletter&utm_campaign=tax-season"),
  );

  assert.deepEqual(
    mergeAttributionSnapshot(
      {
        anonymousSessionId: "anon-123",
        utmMedium: "email",
      },
      incoming,
    ),
    {
      anonymousSessionId: "anon-123",
      utmSource: "newsletter",
      utmMedium: "email",
      utmCampaign: "tax-season",
      utmTerm: null,
      utmContent: null,
    },
  );
});
