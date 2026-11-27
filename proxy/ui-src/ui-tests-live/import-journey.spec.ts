import { test, expect, type Page } from "@playwright/test";
import { importConfig, loadLiveEnv, type ImportConfig } from "./config";

// Import journey, clicked through the real UI against a real proxy and real AWS:
//   create a project -> fill in the import form -> Execute -> wait for "Graph ready"
//   -> see the graph AVAILABLE on the Graphs page -> delete it from the UI.
// Every step asserts what the user sees. The API is never called to help a step along.
// Graphs a failed run leaves behind are deleted by the teardown in global-setup.ts.

loadLiveEnv();
const { config, missing } = importConfig();
const skipReason = `Set ${missing.join(", ")} in ui-tests-live/.env.local (see .env.example)`;

const READY_TIMEOUT = 15 * 60_000; // an import can take minutes
const DELETE_TIMEOUT = 15 * 60_000;

async function importJourney(page: Page, cfg: ImportConfig, opts: { memory: number; graphName: string }) {
  const runId = process.env.E2E_RUN_ID!;

  // Open the app with the proxy's per-run token (kept in the environment, never printed).
  await page.goto(process.env.E2E_LAUNCH_URL!);

  // 1. A project to import into.
  await page.goto("/projects");
  await page.getByPlaceholder("Project name").fill(`e2e ${opts.graphName}`);
  await page.getByRole("button", { name: "Create", exact: true }).click();
  await expect(page).toHaveURL(/\/projections\?project=/);

  // 2. The import form.
  await page.getByRole("link", { name: "+ Add Graph" }).click();
  await expect(page.getByRole("heading", { name: "Import" })).toBeVisible();

  if (cfg.catalog !== "AwsDataCatalog") await page.getByLabel("Catalog").selectOption(cfg.catalog);
  await page.getByLabel("Database").selectOption({ label: cfg.database });
  await page.getByLabel("S3 Staging Bucket").selectOption(`s3://${cfg.bucket}/`);
  await page.getByLabel(/Memory/).selectOption(String(opts.memory));
  await page.getByPlaceholder("my-graph").fill(opts.graphName);

  await page.getByRole("button", { name: /Athena SQL/ }).click();
  await page.getByPlaceholder(/~label", col1/).fill(cfg.nodeQuery);
  if (cfg.edgeQuery) await page.getByPlaceholder(/~from/).fill(cfg.edgeQuery);

  // 3. Run it and wait for the result. The Import page shows a red box when the run fails,
  //    so a failure ends the test now instead of after the full timeout.
  await page.getByRole("button", { name: "Execute" }).click();
  const ready = page.getByText("✓ Graph ready");
  const failed = page.locator("div.border-red-200");
  await expect(ready.or(failed).first()).toBeVisible({ timeout: READY_TIMEOUT });
  await expect(ready, "the import should finish with a ready graph").toBeVisible();

  // 4. The graph is listed as available.
  await page.getByRole("link", { name: "Graphs" }).click();
  const row = page.getByRole("row", { name: new RegExp(runId) });
  await expect(row).toContainText("AVAILABLE", { timeout: 2 * 60_000 });

  // 5. Delete it from the UI (this also cleans up after the test).
  page.once("dialog", (dialog) => dialog.accept());
  await row.getByTitle("Delete graph").click();
  await expect(row).toHaveCount(0, { timeout: DELETE_TIMEOUT });
}

test.describe("Graph sessions", () => {
  test.skip(!config, skipReason);

  test("imports into a 4 NCU graph session", async ({ page }) => {
    await importJourney(page, config!, { memory: 4, graphName: `gs-${process.env.E2E_RUN_ID}` });
  });
});

test.describe("Provisioned graphs", () => {
  test.skip(!config, skipReason);

  test("imports into a 16 NCU provisioned graph", async ({ page }) => {
    await importJourney(page, config!, { memory: 16, graphName: `pg-${process.env.E2E_RUN_ID}` });
  });
});
