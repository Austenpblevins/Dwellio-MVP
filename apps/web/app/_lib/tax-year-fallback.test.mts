import test from "node:test";
import assert from "node:assert/strict";

import { buildTaxYearFallbackNotice } from "./tax-year-fallback.ts";

test("buildTaxYearFallbackNotice returns notice when fallback is applied", () => {
  assert.equal(
    buildTaxYearFallbackNotice({
      requested_tax_year: 2026,
      served_tax_year: 2025,
      tax_year_fallback_applied: true,
    }),
    "2026 data is not yet available for this county. Showing 2025 data.",
  );
});

test("buildTaxYearFallbackNotice hides notice for exact-year responses", () => {
  assert.equal(
    buildTaxYearFallbackNotice({
      requested_tax_year: 2026,
      served_tax_year: 2026,
      tax_year_fallback_applied: false,
    }),
    null,
  );
});
