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
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    // The redirect must not fire after the user has gone somewhere themselves.
    // A slow call plus a sidebar click meant a late `replace: true` navigation
    // yanked them back out of the page they had chosen.
    let cancelled = false;
    projectApi
      .list()
      .then((all) => {
        if (cancelled) return;
        const projects = all.filter((p) => p.status !== "deleting");
        navigate(projects.length === 0 ? "/projects" : "/projections", {
          replace: true,
        });
      })
      // Without this the page sat on the skeleton forever when the call failed,
      // which looks identical to a slow network.
      .catch((e) => {
        if (cancelled) return;
        setError(e instanceof Error ? e.message : "Could not load projects");
      });
    return () => {
      cancelled = true;
    };
  }, [navigate, attempt]);

  if (error) {
    return (
      <div className="p-6" role="alert">
        <p className="text-sm text-red-700">{error}</p>
        <button
          // Re-runs the effect rather than reloading the app, which would throw
          // away everything else the session had.
          onClick={() => {
            setError(null);
            setAttempt((n) => n + 1);
          }}
          className="mt-2 text-sm text-blue-600 hover:text-blue-800"
        >
          Retry
        </button>
      </div>
    );
  }

  return (
    // `role="status"`, because `aria-label` on a plain div maps to `role=generic`
    // where ARIA prohibits it: the name is dropped and nothing is announced.
    <div className="space-y-4 p-6" role="status" aria-busy="true" aria-label="Loading">
      <div className="h-6 w-40 animate-pulse rounded bg-gray-200" />
      <div className="h-24 w-full animate-pulse rounded-lg bg-gray-100" />
      <div className="h-24 w-full animate-pulse rounded-lg bg-gray-100" />
    </div>
  );
}
