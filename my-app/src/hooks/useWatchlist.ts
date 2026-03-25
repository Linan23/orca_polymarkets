import { useCallback } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";

function returnToPath(pathname: string, search: string, hash: string) {
  return `${pathname}${search}${hash}`;
}

export function useWatchlist() {
  const {
    loading,
    isAuthenticated,
    watchlist,
    toggleUserFollow,
    toggleMarketFollow,
    removeUserFollow,
    removeMarketFollow,
  } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();

  const redirectToLogin = useCallback(() => {
    const returnTo = encodeURIComponent(
      returnToPath(location.pathname, location.search, location.hash),
    );
    navigate(`/login?returnTo=${returnTo}`);
  }, [location.hash, location.pathname, location.search, navigate]);

  return {
    watchlist,
    isUserFollowed: (userId: number) => watchlist.users.includes(userId),
    isMarketFollowed: (marketSlug: string) => watchlist.markets.includes(marketSlug.trim().toLowerCase()),
    toggleUser: async (userId: number) => {
      if (loading) return;
      if (!isAuthenticated) {
        redirectToLogin();
        return;
      }
      await toggleUserFollow(userId);
    },
    toggleMarket: async (marketSlug: string) => {
      if (loading) return;
      if (!isAuthenticated) {
        redirectToLogin();
        return;
      }
      await toggleMarketFollow(marketSlug);
    },
    removeUser: async (userId: number) => {
      if (loading) return;
      if (!isAuthenticated) {
        redirectToLogin();
        return;
      }
      await removeUserFollow(userId);
    },
    removeMarket: async (marketSlug: string) => {
      if (loading) return;
      if (!isAuthenticated) {
        redirectToLogin();
        return;
      }
      await removeMarketFollow(marketSlug);
    },
  };
}
