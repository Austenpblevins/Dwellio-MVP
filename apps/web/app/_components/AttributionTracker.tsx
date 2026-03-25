"use client";

import { useEffect } from "react";
import { useSearchParams } from "next/navigation";

import {
  ATTRIBUTION_STORAGE_KEY,
  extractAttributionFromSearchParams,
  mergeAttributionSnapshot,
} from "../_lib/lead-funnel";

export function AttributionTracker() {
  const searchParams = useSearchParams();

  useEffect(() => {
    const incoming = extractAttributionFromSearchParams(new URLSearchParams(searchParams.toString()));
    const existing = readStoredAttribution();
    const merged = mergeAttributionSnapshot(existing, {
      anonymousSessionId: existing?.anonymousSessionId ?? crypto.randomUUID(),
      ...incoming,
    });

    window.sessionStorage.setItem(ATTRIBUTION_STORAGE_KEY, JSON.stringify(merged));
  }, [searchParams]);

  return null;
}

function readStoredAttribution() {
  const raw = window.sessionStorage.getItem(ATTRIBUTION_STORAGE_KEY);
  if (!raw) {
    return undefined;
  }

  try {
    return JSON.parse(raw) as Record<string, string | null>;
  } catch {
    return undefined;
  }
}
