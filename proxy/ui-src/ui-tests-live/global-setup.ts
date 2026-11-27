import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { GRAPH_PREFIX, LIVE_PORT, importConfig, loadLiveEnv } from "./config";

// Starts a real proxy for the live tests and removes the graphs they leave behind.
//  - The proxy listens on 127.0.0.1:LIVE_PORT (not 8080) and uses a temporary database,
//    so a dev proxy and its data are not touched.
//  - Its per-run token is read from the launch URL it prints. The URL goes to the tests through
//    the environment only. It is never printed.
//  - Public connectivity on and deletion protection off, as for `make dev`, so the proxy can
//    reach and delete the graphs it creates.

const HEADERS = { "X-Requested-With": "nx-neptune" };

async function api(origin: string, token: string, method: string, apiPath: string) {
  const res = await fetch(`${origin}/api/v0${apiPath}`, {
    method,
    headers: { ...HEADERS, Authorization: `Bearer ${token}` },
  });
  if (!res.ok) throw new Error(`${method} ${apiPath} failed: ${res.status}`);
  return res.status === 204 ? undefined : res.json();
}

export default async function globalSetup() {
  loadLiveEnv();
  const { missing } = importConfig();
  if (missing.length) {
    console.log(`Live tests are skipped, so no proxy is started. Missing: ${missing.join(", ")}`);
    return;
  }

  const runId = randomBytes(3).toString("hex");
  process.env.E2E_RUN_ID = runId;
  const dbDir = mkdtempSync(path.join(tmpdir(), "nx-e2e-"));

  const child = spawn(process.env.E2E_PYTHON || "python", ["-u", "-m", "nx_neptune_proxy"], {
    env: {
      ...process.env,
      PORT: String(LIVE_PORT),
      GRAPH_PREFIX,
      NETWORKX_PUBLIC_CONNECTIVITY: "true",
      NETWORKX_DELETION_PROTECTION: "false",
      NX_NEPTUNE_DB_PATH: path.join(dbDir, "proxy.db"),
      LOG_FORMAT: "text",
      OPEN_BROWSER: "false",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });

  let stderrTail = "";
  child.stderr.on("data", (d) => (stderrTail = (stderrTail + d.toString()).slice(-2000)));

  const launchUrl = await new Promise<string>((resolve, reject) => {
    let out = "";
    const timer = setTimeout(
      () => reject(new Error(`The test proxy printed no launch URL in 60 s.\n${stderrTail}`)),
      60_000,
    );
    child.stdout.on("data", (d) => {
      out += d.toString();
      const m = out.match(/http:\/\/127\.0\.0\.1:\d+\/\?token=[\w-]+/);
      if (m) {
        clearTimeout(timer);
        resolve(m[0]);
      }
    });
    child.on("exit", (code) => {
      clearTimeout(timer);
      reject(
        new Error(
          `The test proxy exited with code ${code}. Is port ${LIVE_PORT} free, and is nx_neptune_proxy ` +
            `installed for E2E_PYTHON?\n${stderrTail}`,
        ),
      );
    });
  });
  process.env.E2E_LAUNCH_URL = launchUrl;

  // Teardown: delete the graphs this run left behind (only names that carry this run's id).
  return async () => {
    const origin = new URL(launchUrl).origin;
    const token = new URL(launchUrl).searchParams.get("token")!;
    try {
      const { graphs } = await api(origin, token, "GET", "/metadata/neptune/graph-analytics");
      for (const g of graphs as { id: string; name: string; status: string }[]) {
        if (g.name.includes(runId) && g.status !== "DELETING") {
          console.log(`Cleaning up leftover graph ${g.id}`);
          await api(origin, token, "DELETE", `/metadata/neptune/graph-analytics/${g.id}`);
        }
      }
    } catch (e) {
      console.warn(`Cleanup could not finish (${(e as Error).message}). Check for graphs named *${runId}*.`);
    } finally {
      child.removeAllListeners("exit");
      // Wait for the proxy to exit, so the port is free for an immediate re-run.
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, 10_000);
        child.once("exit", () => {
          clearTimeout(timer);
          resolve();
        });
        child.kill();
      });
      rmSync(dbDir, { recursive: true, force: true });
    }
  };
}
