export const routeLoaders = {
  "/": () => import("../pages/HomePage"),
  "/about": () => import("../pages/AboutUsPage"),
  "/definitions": () => import("../pages/DefinitionsPage"),
  "/following": () => import("../pages/FollowingPage"),
  "/leaderboard": () => import("../pages/LeaderboardPage"),
  "/login": () => import("../pages/LoginPage"),
  "/markets": () => import("../pages/MarketProfile"),
  "/users": () => import("../pages/UserProfile"),
};

const prefetchedRoutes = new Set<keyof typeof routeLoaders>();

export function prefetchRoute(path: string) {
  const key: keyof typeof routeLoaders = path.startsWith("/markets/")
    ? "/markets"
    : path.startsWith("/users/")
      ? "/users"
      : (path as keyof typeof routeLoaders);
  const loader = routeLoaders[key];
  if (!loader || prefetchedRoutes.has(key)) return;
  prefetchedRoutes.add(key);
  void loader();
}

export function prefetchCommonRoutes() {
  ["/leaderboard", "/following", "/definitions", "/about", "/login"].forEach(prefetchRoute);
}
