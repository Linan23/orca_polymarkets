import type { ReactNode } from "react";
import { Navigate, useLocation } from "react-router-dom";
import TopNavbar from "../homepage/TopNavbar";
import { useAuth } from "./AuthContext";

function currentLocationPath(pathname: string, search: string, hash: string) {
  return `${pathname}${search}${hash}`;
}

export default function RequireAuth({ children }: { children: ReactNode }) {
  const { loading, isAuthenticated } = useAuth();
  const location = useLocation();

  if (loading) {
    return (
      <div className="page">
        <TopNavbar />
        <section className="analytics-section">
          <div className="status-panel">Checking account session...</div>
        </section>
      </div>
    );
  }

  if (!isAuthenticated) {
    const returnTo = encodeURIComponent(
      currentLocationPath(location.pathname, location.search, location.hash),
    );
    return <Navigate to={`/login?returnTo=${returnTo}`} replace />;
  }

  return <>{children}</>;
}
