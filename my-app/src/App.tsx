import { lazy, Suspense, useEffect } from "react";
import { Routes, Route, Navigate } from "react-router-dom";
import RequireAuth from "./auth/RequireAuth";
import PageTitle from "./components/PageTitle";
import ScrollToTop from "./components/ScrollToTop";
import { prefetchCommonRoutes, routeLoaders } from "./lib/routePrefetch";

const HomePage = lazy(routeLoaders["/"]);
const LoginPage = lazy(routeLoaders["/login"]);
const MarketProfile = lazy(routeLoaders["/markets"]);
const UserProfile = lazy(routeLoaders["/users"]);
const FollowingPage = lazy(routeLoaders["/following"]);
const LeaderboardPage = lazy(routeLoaders["/leaderboard"]);
const DefinitionsPage = lazy(routeLoaders["/definitions"]);
const AboutUsPage = lazy(routeLoaders["/about"]);

function RouteFallback() {
  return (
    <div className="page">
      <section className="status-panel">Loading dashboard...</section>
    </div>
  );
}

export default function App() {
  useEffect(() => {
    const windowWithIdle = window as Window & {
      requestIdleCallback?: (callback: () => void) => number;
      cancelIdleCallback?: (handle: number) => void;
    };
    if (windowWithIdle.requestIdleCallback) {
      const handle = windowWithIdle.requestIdleCallback(prefetchCommonRoutes);
      return () => windowWithIdle.cancelIdleCallback?.(handle);
    }
    const handle = window.setTimeout(prefetchCommonRoutes, 1500);
    return () => window.clearTimeout(handle);
  }, []);

  return (
    <>
      <PageTitle />
      <ScrollToTop />
      <Suspense fallback={<RouteFallback />}>
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/analytics" element={<Navigate to="/" replace />} />

          <Route
            path="/following"
            element={
              <RequireAuth>
                <FollowingPage />
              </RequireAuth>
            }
          />

          <Route path="/leaderboard" element={<LeaderboardPage />} />
          <Route path="/definitions" element={<DefinitionsPage />} />
          <Route path="/about" element={<AboutUsPage />} />
          <Route path="/login" element={<LoginPage />} />
          <Route path="/markets/:marketId" element={<MarketProfile />} />
          <Route path="/users/:userId" element={<UserProfile />} />
        </Routes>
      </Suspense>
    </>
  );
}
