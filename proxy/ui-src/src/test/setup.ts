import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach } from "vitest";

// Unmount rendered components and clear persisted token state between tests.
afterEach(() => {
  cleanup();
  localStorage.clear();
});
