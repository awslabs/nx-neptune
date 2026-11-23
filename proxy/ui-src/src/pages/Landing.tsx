import { useEffect, useState } from "react";
import { useNavigate } from "react-router";
import { projectApi } from "../api";

/**
 * Decides where to send a visitor, then gets out of the way.
 *
 * This renders a skeleton rather than `null` while it waits. The decision needs
 * an API round trip, and a blank page is the first thing a new user sees of the
 * product — with nothing on screen to suggest work is happening, any delay reads
 * as broken rather than as loading.
 */
export function Landing() {
  const navigate = useNavigate();
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    projectApi
      .list()
      .then((all) => {
        const projects = all.filter((p) => p.status !== "deleting");
        navigate(projects.length === 0 ? "/projects" : "/projections", {
          replace: true,
        });
      })
      // Without this the page sat on the skeleton forever when the call failed,
      // which looks identical to a slow network.
      .catch((e) => setError(e instanceof Error ? e.message : "Could not load projects"));
  }, [navigate]);

  if (error) {
    return (
      <div className="p-6">
        <p className="text-sm text-red-700">{error}</p>
        <button
          onClick={() => window.location.reload()}
          className="mt-2 text-sm text-blue-600 hover:text-blue-800"
        >
          Retry
        </button>
      </div>
    );
  }

  return (
    <div className="space-y-4 p-6" aria-busy="true" aria-label="Loading">
      <div className="h-6 w-40 animate-pulse rounded bg-gray-200" />
      <div className="h-24 w-full animate-pulse rounded-lg bg-gray-100" />
      <div className="h-24 w-full animate-pulse rounded-lg bg-gray-100" />
    </div>
  );
}
