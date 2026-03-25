import type { ParcelSummaryResponse, QuoteResponse, SearchResponse } from "./public-types";

function getApiBaseUrl(): string {
  return process.env.DWELLIO_API_BASE_URL ?? "http://127.0.0.1:8000";
}

async function publicFetch<T>(path: string): Promise<T> {
  const response = await fetch(`${getApiBaseUrl()}${path}`, {
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`Public API returned ${response.status}`);
  }

  return (await response.json()) as T;
}

export async function searchParcels(address: string): Promise<SearchResponse> {
  const params = new URLSearchParams({ address });
  return publicFetch<SearchResponse>(`/search?${params.toString()}`);
}

export async function getParcelSummary(
  countyId: string,
  taxYear: number,
  accountNumber: string,
): Promise<ParcelSummaryResponse> {
  return publicFetch<ParcelSummaryResponse>(`/parcel/${countyId}/${taxYear}/${accountNumber}`);
}

export async function getQuote(
  countyId: string,
  taxYear: number,
  accountNumber: string,
): Promise<QuoteResponse | null> {
  try {
    return await publicFetch<QuoteResponse>(`/quote/${countyId}/${taxYear}/${accountNumber}`);
  } catch (error) {
    if (error instanceof Error && error.message.includes("404")) {
      return null;
    }
    throw error;
  }
}
