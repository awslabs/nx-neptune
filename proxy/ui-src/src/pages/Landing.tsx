import { useEffect } from "react";
import { useNavigate } from "react-router";
import { projectApi } from "../api";

export function Landing() {
  const navigate = useNavigate();

  useEffect(() => {
    projectApi.list().then(all => {
      const projects = all.filter(p => p.status !== "deleting");
      if (projects.length === 0) {
        navigate("/projects", { replace: true });
      } else {
        navigate(`/sessions?project=${projects[0].id}`, { replace: true });
      }
    });
  }, [navigate]);

  return null;
}
