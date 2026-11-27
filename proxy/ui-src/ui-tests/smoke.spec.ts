import { test, expect } from "./fixtures";

// One basic flow to prove the setup works. Add flow tests next to this file.
test.describe("Start page", () => {
  test("opens Projections when a project exists", async ({ page, api }) => {
    await page.goto("/");
    await expect(page).toHaveURL(/\/projections$/);
    await expect(page.getByRole("heading", { name: "Projections", exact: true })).toBeVisible();
    await expect(page.getByRole("link", { name: "Alpha" })).toBeVisible();
  });
});
