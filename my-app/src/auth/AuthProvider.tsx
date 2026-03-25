import {
  useCallback,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { AuthContext, type AuthContextValue } from "./AuthContext";
import {
  ApiError,
  type AccountPreferences,
  type AccountPreferencesPatch,
  type AuthSession,
  type LoginPayload,
  type SignUpPayload,
  type WatchlistState,
  fetchAuthSession,
  followMarketAccount,
  followUserAccount,
  importLocalWatchlist,
  loginAccount,
  logoutAccount,
  patchAccountPreferences,
  signUpAccount,
  unfollowMarketAccount,
  unfollowUserAccount,
} from "../lib/api";
import { clearLegacyWatchlist, readLegacyWatchlist } from "../lib/watchlist";

const EMPTY_WATCHLIST: WatchlistState = {
  users: [],
  markets: [],
};

const DEFAULT_PREFERENCES: AccountPreferences = {
  homepage: {
    research_timeframe: "all",
  },
  user_profile: {
    analytics_timeframe: "30d",
  },
  leaderboard: {
    active_board: "market",
    user_filters: {
      board: "all",
      platform: "all",
      min_trades: 0,
      sort: "trust",
    },
    market_filters: {
      min_whales: 0,
      sort: "trusted",
    },
  },
};

async function hydrateSessionWithLegacyWatchlist(session: AuthSession): Promise<AuthSession> {
  const legacy = readLegacyWatchlist();
  if (legacy.users.length === 0 && legacy.markets.length === 0) {
    return session;
  }

  try {
    const imported = await importLocalWatchlist(legacy);
    clearLegacyWatchlist();
    return {
      ...session,
      watchlist: imported.watchlist,
    };
  } catch {
    return session;
  }
}

function insertUserId(values: number[], userId: number) {
  if (values.includes(userId)) return values;
  return [userId, ...values];
}

function removeUserId(values: number[], userId: number) {
  return values.filter((value) => value !== userId);
}

function insertMarketSlug(values: string[], marketSlug: string) {
  if (values.includes(marketSlug)) return values;
  return [marketSlug, ...values];
}

function removeMarketSlug(values: string[], marketSlug: string) {
  return values.filter((value) => value !== marketSlug);
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [loading, setLoading] = useState(true);
  const [session, setSession] = useState<AuthSession | null>(null);

  const applySession = useCallback(async (nextSession: AuthSession) => {
    const hydrated = await hydrateSessionWithLegacyWatchlist(nextSession);
    setSession(hydrated);
  }, []);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const nextSession = await fetchAuthSession();
      await applySession(nextSession);
    } catch (error) {
      if (!(error instanceof ApiError) || error.status !== 401) {
        console.error(error);
      }
      setSession(null);
    } finally {
      setLoading(false);
    }
  }, [applySession]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const login = useCallback(
    async (payload: LoginPayload) => {
      setLoading(true);
      try {
        const nextSession = await loginAccount(payload);
        await applySession(nextSession);
      } finally {
        setLoading(false);
      }
    },
    [applySession],
  );

  const signup = useCallback(
    async (payload: SignUpPayload) => {
      setLoading(true);
      try {
        const nextSession = await signUpAccount(payload);
        await applySession(nextSession);
      } finally {
        setLoading(false);
      }
    },
    [applySession],
  );

  const logout = useCallback(async () => {
    await logoutAccount();
    setSession(null);
  }, []);

  const toggleUserFollow = useCallback(
    async (userId: number) => {
      if (!session) return;
      const wasFollowed = session.watchlist.users.includes(userId);
      setSession((current) => (
        current
          ? {
              ...current,
              watchlist: {
                ...current.watchlist,
                users: wasFollowed
                  ? removeUserId(current.watchlist.users, userId)
                  : insertUserId(current.watchlist.users, userId),
              },
            }
          : current
      ));
      try {
        const watchlist = wasFollowed
          ? await unfollowUserAccount(userId)
          : await followUserAccount(userId);
        setSession((current) => (current ? { ...current, watchlist } : current));
      } catch (error) {
        await refresh();
        throw error;
      }
    },
    [refresh, session],
  );

  const toggleMarketFollow = useCallback(
    async (marketSlug: string) => {
      if (!session) return;
      const normalized = marketSlug.trim().toLowerCase();
      if (!normalized) return;
      const wasFollowed = session.watchlist.markets.includes(normalized);
      setSession((current) => (
        current
          ? {
              ...current,
              watchlist: {
                ...current.watchlist,
                markets: wasFollowed
                  ? removeMarketSlug(current.watchlist.markets, normalized)
                  : insertMarketSlug(current.watchlist.markets, normalized),
              },
            }
          : current
      ));
      try {
        const watchlist = wasFollowed
          ? await unfollowMarketAccount(normalized)
          : await followMarketAccount(normalized);
        setSession((current) => (current ? { ...current, watchlist } : current));
      } catch (error) {
        await refresh();
        throw error;
      }
    },
    [refresh, session],
  );

  const removeUserFollow = useCallback(async (userId: number) => {
    if (!session) return;
    setSession((current) => (
      current
        ? {
            ...current,
            watchlist: {
              ...current.watchlist,
              users: removeUserId(current.watchlist.users, userId),
            },
          }
        : current
    ));
    try {
      const watchlist = await unfollowUserAccount(userId);
      setSession((current) => (current ? { ...current, watchlist } : current));
    } catch (error) {
      await refresh();
      throw error;
    }
  }, [refresh, session]);

  const removeMarketFollow = useCallback(async (marketSlug: string) => {
    if (!session) return;
    const normalized = marketSlug.trim().toLowerCase();
    setSession((current) => (
      current
        ? {
            ...current,
            watchlist: {
              ...current.watchlist,
              markets: removeMarketSlug(current.watchlist.markets, normalized),
            },
          }
        : current
    ));
    try {
      const watchlist = await unfollowMarketAccount(normalized);
      setSession((current) => (current ? { ...current, watchlist } : current));
    } catch (error) {
      await refresh();
      throw error;
    }
  }, [refresh, session]);

  const updatePreferences = useCallback(async (patch: AccountPreferencesPatch) => {
    if (!session) return;
    const preferences = await patchAccountPreferences(patch);
    setSession((current) => (current ? { ...current, preferences } : current));
  }, [session]);

  const value = useMemo<AuthContextValue>(
    () => ({
      loading,
      isAuthenticated: Boolean(session),
      account: session?.account ?? null,
      watchlist: session?.watchlist ?? EMPTY_WATCHLIST,
      preferences: session?.preferences ?? DEFAULT_PREFERENCES,
      refresh,
      login,
      signup,
      logout,
      toggleUserFollow,
      toggleMarketFollow,
      removeUserFollow,
      removeMarketFollow,
      updatePreferences,
    }),
    [
      loading,
      session,
      refresh,
      login,
      signup,
      logout,
      toggleUserFollow,
      toggleMarketFollow,
      removeUserFollow,
      removeMarketFollow,
      updatePreferences,
    ],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}
