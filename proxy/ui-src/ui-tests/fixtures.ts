import { test as base, expect, type Page } from "@playwright/test";
import { createMockApi, type MockApi, type MockState } from "./mockApi";

// `api` is the fake proxy API. It is installed before the test body runs, and the
// test fails if the UI made a request the mock has no handler for.
// To start a test from different data, call `useApi({ projects: [] })` first.
type Fixtures = {
  api: MockApi;
  useApi: (overrides: Partial<MockState>) => Promise<MockApi>;
};

export const test = base.extend<Fixtures>({
  useApi: async ({ page }, use) => {
    let installed: MockApi | undefined;
    await use(async (overrides) => {
      installed = await createMockApi(page, overrides);
      return installed;
    });
    if (installed) expect(installed.unmocked, "API calls with no mock").toEqual([]);
  },
  api: async ({ page }, use) => {
    const api = await createMockApi(page);
    await use(api);
    expect(api.unmocked, "API calls with no mock").toEqual([]);
  },
});

export { expect };

/** Wait until the Import form has loaded its metadata (databases and buckets). */
export async function importFormReady(page: Page) {
  await expect(page.getByRole("option", { name: "finance" })).toBeAttached();
  await expect(page.getByRole("option", { name: "s3://e2e-staging/" })).toBeAttached();
}
