import type { TaxYearFallbackMetadata } from "./public-types";

export function buildTaxYearFallbackNotice(
  metadata: Pick<
    TaxYearFallbackMetadata,
    "requested_tax_year" | "served_tax_year" | "tax_year_fallback_applied"
  >,
): string | null {
  if (!metadata.tax_year_fallback_applied) {
    return null;
  }

  return `${metadata.requested_tax_year} data is not yet available for this county. Showing ${metadata.served_tax_year} data.`;
}
