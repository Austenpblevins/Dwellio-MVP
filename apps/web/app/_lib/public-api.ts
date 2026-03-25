import { getPublicRuntimeConfig } from "./public-config";
import type {
  LeadCreateRequest,
  LeadCreateResponse,
  ParcelSummaryResponse,
  QuoteExplanationResponse,
  QuoteResponse,
  SearchResponse,
} from "./public-types";

async function publicFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const config = getPublicRuntimeConfig();
  const response = await fetch(`${config.apiBaseUrl}${path}`, {
    cache: "no-store",
    ...init,
    headers: {
      ...(init?.body ? { "content-type": "application/json" } : {}),
      ...init?.headers,
    },
  });

  if (!response.ok) {
    let detail = `Public API returned ${response.status}.`;
    try {
      const payload = (await response.json()) as { detail?: string };
      if (payload.detail) {
        detail = payload.detail;
      }
    } catch {
      // Fall back to the generic error message when the response body is not JSON.
    }

    throw new Error(
      `${detail} (${config.apiBaseUrl}${path}${
        config.isDefaultApiBaseUrl ? "; using default API base URL" : ""
      })`,
    );
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

export async function getQuoteExplanation(
  countyId: string,
  taxYear: number,
  accountNumber: string,
): Promise<QuoteExplanationResponse | null> {
  try {
    return await publicFetch<QuoteExplanationResponse>(
      `/quote/${countyId}/${taxYear}/${accountNumber}/explanation`,
    );
  } catch (error) {
    if (error instanceof Error && error.message.includes("404")) {
      return null;
    }
    throw error;
  }
}

export async function createLead(request: LeadCreateRequest): Promise<LeadCreateResponse> {
  return publicFetch<LeadCreateResponse>("/lead", {
    method: "POST",
    body: JSON.stringify(request),
  });
}
