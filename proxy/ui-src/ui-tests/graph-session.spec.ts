import type { Page } from "@playwright/test";
import { test, expect, importFormReady } from "./fixtures";

// UI behaviour added for graph sessions (4 and 8 NCU graphs): the Import memory
// selector lists the sizes the proxy reports, including 4 and 8.
// (Skipping summaries for session graphs is covered by Graphs.test.tsx and Projections.test.tsx.)

async function openImport(page: Page) {
  await page.goto("/import?project=p1");
  await importFormReady(page);
}

test.describe("Import memory selector", () => {
  test("offers 4 and 8 NCU", async ({ page, api }) => {
    await openImport(page);
    await expect(page.getByRole("option", { name: /^4 NCU/ })).toBeAttached();
    await expect(page.getByRole("option", { name: /^8 NCU/ })).toBeAttached();
    await expect(page.getByRole("option", { name: "16 NCU", exact: true })).toBeAttached();
    await expect(page.getByRole("option", { name: "4096 NCU", exact: true })).toBeAttached();
  });

  test("shows graph session info only while a session size is selected", async ({ page, api }) => {
    await openImport(page);
    const info = page.getByLabel("Graph session info");
    await expect(info).toHaveCount(0); // default 16 is a provisioned size

    await page.getByLabel(/Memory/).selectOption("4");
    await expect(info).toBeVisible();

    await page.getByLabel(/Memory/).selectOption("32");
    await expect(info).toHaveCount(0);
  });

  test("saves the chosen session size on the draft projection", async ({ page, api }) => {
    await openImport(page);
    await page.getByLabel(/Memory/).selectOption("8");
    await page.getByLabel("Database").selectOption("finance");

    await expect.poll(() => api.find("POST", "/projection").length).toBeGreaterThan(0);
    expect(api.find("POST", "/projection")[0].body).toMatchObject({ graph_memory_gb: 8, project_id: "p1" });
  });

  test("falls back to 16 and above when the proxy sends no size lists", async ({ page, useApi }) => {
    await useApi({ config: { region: "us-west-2", graph_prefix: "nxp-" } });
    await openImport(page);
    await expect(page.getByRole("option", { name: "16 NCU", exact: true })).toBeAttached();
    await expect(page.getByRole("option", { name: /^4 NCU/ })).toHaveCount(0);
  });
});
