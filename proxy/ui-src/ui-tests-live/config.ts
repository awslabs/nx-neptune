import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

// Settings for the live browser tests. Real resource names come from the environment
// (or the git-ignored ui-tests-live/.env.local) and are never printed or written anywhere.
// Error messages list the NAMES of missing keys, never their values.

export const LIVE_DIR = path.dirname(fileURLToPath(import.meta.url));
export const LIVE_PORT = Number(process.env.E2E_PORT ?? 18080);
// The test proxy only manages graphs with this prefix, so it ignores everything else in the account.
export const GRAPH_PREFIX = "nxp-e2e-";

/** Load ui-tests-live/.env.local. Variables already set in the shell win. */
export function loadLiveEnv(): void {
  const file = path.join(LIVE_DIR, ".env.local");
  if (existsSync(file)) process.loadEnvFile(file);
}

export interface ImportConfig {
  bucket: string;
  catalog: string;
  database: string;
  nodeQuery: string;
  edgeQuery?: string;
}

function readQuery(key: string): string | undefined {
  const file = process.env[key]?.trim();
  if (!file) return undefined;
  const full = path.resolve(LIVE_DIR, file);
  return existsSync(full) ? readFileSync(full, "utf8").trim() : undefined;
}

/** Settings for the import test cases, or the names of the keys that are missing. */
export function importConfig(): { config?: ImportConfig; missing: string[] } {
  const missing: string[] = [];
  const bucket = process.env.E2E_S3_BUCKET?.trim();
  const database = process.env.E2E_DATABASE?.trim();
  if (!bucket) missing.push("E2E_S3_BUCKET");
  if (!database) missing.push("E2E_DATABASE");

  const nodeQuery = readQuery("E2E_IMPORT_NODE_QUERY_FILE");
  if (!nodeQuery) missing.push("E2E_IMPORT_NODE_QUERY_FILE (set, and the file must exist)");

  const edgeKey = process.env.E2E_IMPORT_EDGE_QUERY_FILE?.trim();
  const edgeQuery = edgeKey ? readQuery("E2E_IMPORT_EDGE_QUERY_FILE") : undefined;
  if (edgeKey && !edgeQuery) missing.push("E2E_IMPORT_EDGE_QUERY_FILE (set, but the file does not exist)");

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
