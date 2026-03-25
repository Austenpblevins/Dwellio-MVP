import { requireAdminToken } from "./auth";
import type {
  AdminCaseDetail,
  AdminCaseListResponse,
  AdminCompletenessIssuesResponse,
  AdminCountyYearReadinessDashboard,
  AdminEvidencePacketDetail,
  AdminEvidencePacketListResponse,
  AdminImportBatchDetail,
  AdminImportBatchListResponse,
  AdminMutationResult,
  AdminSourceFilesResponse,
  AdminTaxAssignmentIssuesResponse,
  AdminValidationResultsResponse,
} from "./types";

function getApiBaseUrl(): string {
  return process.env.DWELLIO_API_BASE_URL ?? "http://127.0.0.1:8000";
}

async function adminFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const token = await requireAdminToken();
  const response = await fetch(`${getApiBaseUrl()}${path}`, {
    ...init,
    cache: "no-store",
    headers: {
      "content-type": "application/json",
      "x-dwellio-admin-token": token,
      ...(init?.headers ?? {}),
    },
  });

  if (!response.ok) {
    throw new Error(`Admin API returned ${response.status}`);
  }

  return (await response.json()) as T;
}

export async function getReadinessDashboard(
  countyId: string,
  taxYears: number[],
): Promise<AdminCountyYearReadinessDashboard> {
  const params = new URLSearchParams();
  for (const taxYear of taxYears) {
    params.append("tax_years", String(taxYear));
  }
  return adminFetch(`/admin/readiness/${countyId}?${params.toString()}`);
}

export async function getImportBatches(
  countyId: string,
  taxYear?: number,
  datasetType?: string,
): Promise<AdminImportBatchListResponse> {
  const params = new URLSearchParams({ county_id: countyId });
  if (taxYear !== undefined) {
    params.set("tax_year", String(taxYear));
  }
  if (datasetType) {
    params.set("dataset_type", datasetType);
  }
  return adminFetch(`/admin/ops/import-batches?${params.toString()}`);
}

export async function getImportBatchDetail(importBatchId: string): Promise<AdminImportBatchDetail> {
  return adminFetch(`/admin/ops/import-batches/${importBatchId}`);
}

export async function getValidationResults(
  importBatchId: string,
  severity?: string,
): Promise<AdminValidationResultsResponse> {
  const params = new URLSearchParams();
  if (severity) {
    params.set("severity", severity);
  }
  const suffix = params.toString() ? `?${params.toString()}` : "";
  return adminFetch(`/admin/ops/validation/${importBatchId}${suffix}`);
}

export async function getSourceFiles(
  countyId: string,
  taxYear?: number,
  datasetType?: string,
): Promise<AdminSourceFilesResponse> {
  const params = new URLSearchParams({ county_id: countyId });
  if (taxYear !== undefined) {
    params.set("tax_year", String(taxYear));
  }
  if (datasetType) {
    params.set("dataset_type", datasetType);
  }
  return adminFetch(`/admin/ops/source-files?${params.toString()}`);
}

export async function getCompletenessIssues(
  countyId: string,
  taxYear: number,
): Promise<AdminCompletenessIssuesResponse> {
  return adminFetch(`/admin/ops/completeness/${countyId}/${taxYear}`);
}

export async function getTaxAssignmentIssues(
  countyId: string,
  taxYear: number,
): Promise<AdminTaxAssignmentIssuesResponse> {
  return adminFetch(`/admin/ops/tax-assignment/${countyId}/${taxYear}`);
}

export async function postAdminMutation<TPayload extends object>(
  path: string,
  payload: TPayload,
): Promise<AdminMutationResult> {
  return adminFetch<AdminMutationResult>(path, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function getAdminCases(
  countyId?: string,
  taxYear?: number,
  caseStatus?: string,
): Promise<AdminCaseListResponse> {
  const params = new URLSearchParams();
  if (countyId) {
    params.set("county_id", countyId);
  }
  if (taxYear !== undefined) {
    params.set("tax_year", String(taxYear));
  }
  if (caseStatus) {
    params.set("case_status", caseStatus);
  }
  const suffix = params.toString() ? `?${params.toString()}` : "";
  return adminFetch(`/admin/cases${suffix}`);
}

export async function getAdminCaseDetail(caseId: string): Promise<AdminCaseDetail> {
  return adminFetch(`/admin/cases/${caseId}`);
}

export async function getAdminPackets(
  countyId?: string,
  taxYear?: number,
  packetStatus?: string,
): Promise<AdminEvidencePacketListResponse> {
  const params = new URLSearchParams();
  if (countyId) {
    params.set("county_id", countyId);
  }
  if (taxYear !== undefined) {
    params.set("tax_year", String(taxYear));
  }
  if (packetStatus) {
    params.set("packet_status", packetStatus);
  }
  const suffix = params.toString() ? `?${params.toString()}` : "";
  return adminFetch(`/admin/packets${suffix}`);
}

export async function getAdminPacketDetail(
  packetId: string,
): Promise<AdminEvidencePacketDetail> {
  return adminFetch(`/admin/packets/${packetId}`);
}
