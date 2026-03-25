import test from "node:test";
import assert from "node:assert/strict";

import { getPublicRuntimeConfig, isSupportedCounty, isSupportedPropertyType } from "./public-config.ts";

test("getPublicRuntimeConfig falls back to the local API base URL when no env var is set", () => {
  const originalPublic = process.env.NEXT_PUBLIC_DWELLIO_API_BASE_URL;
  const originalServer = process.env.DWELLIO_API_BASE_URL;

  delete process.env.NEXT_PUBLIC_DWELLIO_API_BASE_URL;
  delete process.env.DWELLIO_API_BASE_URL;

  try {
    const config = getPublicRuntimeConfig();
    assert.equal(config.apiBaseUrl, "http://127.0.0.1:8000");
    assert.equal(config.isDefaultApiBaseUrl, true);
  } finally {
    if (originalPublic === undefined) {
      delete process.env.NEXT_PUBLIC_DWELLIO_API_BASE_URL;
    } else {
      process.env.NEXT_PUBLIC_DWELLIO_API_BASE_URL = originalPublic;
    }

    if (originalServer === undefined) {
      delete process.env.DWELLIO_API_BASE_URL;
    } else {
      process.env.DWELLIO_API_BASE_URL = originalServer;
    }
  }
});

test("public scope helpers stay aligned to the MVP county and property boundaries", () => {
  assert.equal(isSupportedCounty("harris"), true);
  assert.equal(isSupportedCounty("dallas"), false);
  assert.equal(isSupportedPropertyType("sfr"), true);
  assert.equal(isSupportedPropertyType("condo"), false);
});
