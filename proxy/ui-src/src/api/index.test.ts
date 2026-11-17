import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// api/index.ts resolves the proxy token once, at module load, from the URL and
// localStorage. Each test therefore sets up window.location and storage, then
// imports a fresh copy of the module via resetModules() + dynamic import.

const TOKEN_KEY = "nxp_token";

function setUrl(search: string) {
  // jsdom allows overwriting window.location via defineProperty.
  Object.defineProperty(window, "location", {
    configurable: true,
    value: new URL(`http://127.0.0.1:8080/${search}`),
  });
}

async function loadApi() {
  vi.resetModules();
  return import("./index");
}

describe("api token handling", () => {
  beforeEach(() => {
    localStorage.clear();
    setUrl("");
    vi.spyOn(window.history, "replaceState").mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("prefers a token from the URL over the stored one and persists it", async () => {
    localStorage.setItem(TOKEN_KEY, "stored-token");
    setUrl("?token=url-token");

    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(new Response("{}", { status: 200 }));

    const { metadata } = await loadApi();
    await metadata.config();

    // The URL token is saved and sent, not the previously stored one.
    expect(localStorage.getItem(TOKEN_KEY)).toBe("url-token");
    const headers = (fetchMock.mock.calls[0][1] as RequestInit).headers as Headers;
    expect(headers.get("Authorization")).toBe("Bearer url-token");
  });

  it("clears the stored token on 401 only if it is the token that failed", async () => {
    setUrl("?token=url-token");
    vi.spyOn(globalThis, "fetch").mockImplementation(async () => {
      // Simulate another tab replacing the token before this 401 lands.
      localStorage.setItem(TOKEN_KEY, "a-newer-token");
      return new Response(null, { status: 401 });
    });

    const { metadata } = await loadApi();
    await expect(metadata.config()).rejects.toThrow();

    // The newer token must survive: only the failed token should be removed.
    expect(localStorage.getItem(TOKEN_KEY)).toBe("a-newer-token");
  });

  it("marks the token as expired on 401", async () => {
    setUrl("?token=url-token");
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(null, { status: 401 }));

    const { metadata, tokenStatus } = await loadApi();
    const listener = vi.fn();
    tokenStatus.subscribe(listener);

    expect(tokenStatus.isExpired()).toBe(false);
    await expect(metadata.config()).rejects.toThrow();

    expect(tokenStatus.isExpired()).toBe(true);
    expect(listener).toHaveBeenCalledOnce();
  });
});
