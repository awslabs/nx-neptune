import { defineConfig, devices } from "@playwright/test";
import { LIVE_PORT } from "./ui-tests-live/config";

// Live browser tests: they drive the real UI against a real proxy and real AWS, and create
// real graphs. Run on demand with `make ui-browser-live`; CI does not run them.
// No screenshots, traces or videos are recorded, so no resource names are written to disk.
export default defineConfig({
  testDir: "./ui-tests-live",
  fullyParallel: false,
  workers: 1, // one graph at a time
  retries: 0,
  timeout: 30 * 60_000,
  expect: { timeout: 15_000 },
  reporter: "list",
  globalSetup: "./ui-tests-live/global-setup.ts",
  use: {
    baseURL: `http://127.0.0.1:${LIVE_PORT}`,
    screenshot: "off",
    trace: "off",
    video: "off",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
});
