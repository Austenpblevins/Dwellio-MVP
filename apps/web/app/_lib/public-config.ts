const DEFAULT_API_BASE_URL = "http://127.0.0.1:8000";

export const SUPPORTED_PUBLIC_COUNTIES = ["harris", "fort_bend"] as const;
export const SUPPORTED_PUBLIC_PROPERTY_TYPES = ["sfr"] as const;

export type PublicRuntimeConfig = {
  apiBaseUrl: string;
  isDefaultApiBaseUrl: boolean;
  supportedCounties: readonly string[];
  supportedPropertyTypes: readonly string[];
};

export function getPublicRuntimeConfig(): PublicRuntimeConfig {
  const configuredApiBaseUrl =
    process.env.NEXT_PUBLIC_DWELLIO_API_BASE_URL ?? process.env.DWELLIO_API_BASE_URL ?? null;

  return {
    apiBaseUrl: configuredApiBaseUrl ?? DEFAULT_API_BASE_URL,
    isDefaultApiBaseUrl: configuredApiBaseUrl === null,
    supportedCounties: SUPPORTED_PUBLIC_COUNTIES,
    supportedPropertyTypes: SUPPORTED_PUBLIC_PROPERTY_TYPES,
  };
}

export function isSupportedCounty(countyId: string): boolean {
  return SUPPORTED_PUBLIC_COUNTIES.includes(countyId as (typeof SUPPORTED_PUBLIC_COUNTIES)[number]);
}

export function isSupportedPropertyType(propertyTypeCode: string | null | undefined): boolean {
  return (
    propertyTypeCode !== null &&
    propertyTypeCode !== undefined &&
    SUPPORTED_PUBLIC_PROPERTY_TYPES.includes(
      propertyTypeCode as (typeof SUPPORTED_PUBLIC_PROPERTY_TYPES)[number],
    )
  );
}
