import { existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

// Settings for the live browser tests. Real resource names come from the environment
// (the file named by E2E_ENV_FILE) and are never printed or written anywhere.
// Error messages list the NAMES of missing keys, never their values.

export const LIVE_DIR = path.dirname(fileURLToPath(import.meta.url));
export const LIVE_PORT = Number(process.env.E2E_PORT ?? 18080);
// The test proxy only manages graphs with this prefix, so it ignores everything else in the account.
export const GRAPH_PREFIX = "nxp-e2e-";

/**
 * Load the settings file named by E2E_ENV_FILE. There is no search: if E2E_ENV_FILE is not set,
 * only variables already in the shell are used. If it is set but the file does not exist, fail
 * (a wrong path should not silently skip the tests). Variables already set in the shell win,
 * unless they are empty.
 */
export function loadLiveEnv(): void {
  // A variable that is set but empty is a leftover (for example from a shell export that failed),
  // never a real setting, so treat it as unset and let the file supply it.
  for (const key of Object.keys(process.env)) {
    if (key.startsWith("E2E_") && process.env[key] === "") delete process.env[key];
  }
  const file = process.env.E2E_ENV_FILE?.trim();
  if (!file) return;
  if (!existsSync(file)) throw new Error(`E2E_ENV_FILE points to a file that does not exist: ${file}`);
  process.loadEnvFile(file);
}

export interface ImportConfig {
  bucket: string;
  catalog: string;
  database: string;
  nodeQuery: string;
  edgeQuery?: string;
}

/** Settings for the import test cases, or the names of the keys that are missing. */
export function importConfig(): { config?: ImportConfig; missing: string[] } {
  const missing: string[] = [];
  // Accept "my-bucket", "s3://my-bucket" and "s3://my-bucket/": the tests need the bare name.
  const bucket = process.env.E2E_S3_BUCKET?.trim().replace(/^s3:\/\//, "").replace(/\/+$/, "");
  const database = process.env.E2E_DATABASE?.trim();
  if (!bucket) missing.push("E2E_S3_BUCKET");
  if (!database) missing.push("E2E_DATABASE");

  const nodeQuery = process.env.E2E_IMPORT_NODE_QUERY?.trim();
  if (!nodeQuery) missing.push("E2E_IMPORT_NODE_QUERY");
  const edgeQuery = process.env.E2E_IMPORT_EDGE_QUERY?.trim() || undefined;

  if (missing.length) return { missing };
  return {
    missing,
    config: {
      bucket: bucket!,
      catalog: process.env.E2E_CATALOG?.trim() || "AwsDataCatalog",
      database: database!,
      nodeQuery: nodeQuery!,
      edgeQuery,
    },
  };
}
