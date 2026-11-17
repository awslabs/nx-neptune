import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

// Test config kept separate from vite.config.ts so the production build is untouched.
export default defineConfig({
  plugins: [react()],
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: "./src/test/setup.ts",
    css: false,
    restoreMocks: true,
  },
});
