import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

// Test config kept separate from vite.config.ts so the production build is untouched.
export default defineConfig({
  plugins: [react()],
  test: {
    environment: "jsdom",
    globals: true,
    // Only unit tests in src. ui-tests/ holds Playwright specs, run by `npx playwright test`.
    include: ["src/**/*.{test,spec}.{ts,tsx}"],
    setupFiles: "./src/test/setup.ts",
    css: false,
    restoreMocks: true,
  },
});
