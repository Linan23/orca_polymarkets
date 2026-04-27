export type WatchlistState = {
  users: number[];
  markets: string[];
};

export type AccountRole = "viewer" | "moderator" | "admin";

export type LeaderboardActiveBoard = "market" | "user";
export type LeaderboardUserBoardFilter = "all" | "trusted" | "whale" | "potential" | "standard";
export type LeaderboardUserPlatformFilter = "all" | "polymarket";
export type LeaderboardUserSort = "trust" | "profitability" | "trades";
export type LeaderboardMarketSort = "trusted" | "whales" | "volume";

export type AccountPreferences = {
  homepage: {
    research_timeframe: AnalyticsTimeframe;
  };
  user_profile: {
    analytics_timeframe: AnalyticsTimeframe;
  };
  leaderboard: {
    active_board: LeaderboardActiveBoard;
    user_filters: {
      board: LeaderboardUserBoardFilter;
      platform: LeaderboardUserPlatformFilter;
      min_trades: number;
      sort: LeaderboardUserSort;
    };
    market_filters: {
      min_whales: number;
      sort: LeaderboardMarketSort;
    };
  };
};

export type AccountPreferencesPatch = {
  homepage?: Partial<AccountPreferences["homepage"]>;
  user_profile?: Partial<AccountPreferences["user_profile"]>;
  leaderboard?: {
    active_board?: AccountPreferences["leaderboard"]["active_board"];
    user_filters?: Partial<AccountPreferences["leaderboard"]["user_filters"]>;
    market_filters?: Partial<AccountPreferences["leaderboard"]["market_filters"]>;
  };
};

export type AuthAccount = {
  account_id: number;
  email: string;
  display_name: string;
  role: AccountRole;
  created_at: string | null;
  last_login_at: string | null;
};

export type AuthSession = {
  account: AuthAccount;
  watchlist: WatchlistState;
  preferences: AccountPreferences;
};

export type SignUpPayload = {
  display_name: string;
  email: string;
  password: string;
};

export type LoginPayload = {
  email: string;
  password: string;
};

export type DashboardMarketRow = {
  market_id: number;
  market_contract_id: number;
  market_slug: string;
  market_url: string | null;
  question: string;
  price: number | null;
  volume: number | null;
  odds: number | null;
  orderbook_depth: number | null;
  whale_count: number;
  trusted_whale_count: number;
  whale_market_focus: string | null;
  read_time: string | null;
};

export type WhaleScoreRow = {
  user_id: number;
  external_user_ref: string;
  wallet_address: string | null;
  preferred_username: string | null;
  display_label: string | null;
  platform_name: string;
  snapshot_time: string | null;
  scoring_version: string;
  trust_score: number;
  profitability_score: number;
  sample_trade_count: number;
  is_whale: boolean;
  is_trusted_whale: boolean;
};

export type LeaderboardRow = {
  leaderboard_id: number;
  user_id: number;
  external_user_ref: string | null;
  board_type: string;
  rank: number;
  score_metric: string;
  score_value: number | null;
};

export type WhaleProfile = {
  user_id: number;
  external_user_ref: string;
  wallet_address: string | null;
  preferred_username: string | null;
  display_label: string | null;
  is_likely_insider: boolean;
  latest_whale_score: {
    snapshot_time: string | null;
    scoring_version: string;
    trust_score: number;
    profitability_score: number;
    sample_trade_count: number;
    is_whale: boolean;
    is_trusted_whale: boolean;
  } | null;
  resolved_performance: {
    resolved_market_count: number;
    winning_market_count: number;
    realized_pnl: number;
    realized_roi: number;
    excluded_market_count: number;
    win_rate: number | null;
  };
  dashboard_profile: {
    dashboard_id: number;
    historical_actions_summary: Record<string, unknown> | null;
    insider_stats: Record<string, unknown> | null;
    trusted_traders_summary: Record<string, unknown> | null;
    total_volume: number;
    total_shares: number;
    created_at: string | null;
  } | null;
};

export type MarketProfile = {
  dashboard_id: number;
  market_id: number;
  market_contract_id: number;
  market_slug: string;
  market_url: string | null;
  question: string;
  price: number | null;
  volume: number | null;
  odds: number | null;
  orderbook_depth: number | null;
  whale_count: number;
  trusted_whale_count: number;
  whale_market_focus: string | null;
  read_time: string | null;
  market_status_label: "Open" | "Closed";
  realtime_source: string;
  snapshot_time: string | null;
  realtime_payload: Record<string, unknown>;
};

export type HomeSummaryPlatformCoverage = {
  platform_name: string;
  user_count: number;
  market_count: number;
  transaction_count: number;
  orderbook_snapshot_count: number;
};

export type HomeSummary = {
  scoring_version: string | null;
  whales_detected: number;
  trusted_whales: number;
  resolved_markets_available: number;
  resolved_markets_observed: number;
  profitability_users: number;
  top_trusted_whale: {
    user_id: number;
    external_user_ref: string;
    wallet_address: string | null;
    preferred_username: string | null;
    display_label: string | null;
    trust_score: number;
    profitability_score: number;
    sample_trade_count: number;
  } | null;
  most_whale_concentrated_market: {
    market_slug: string;
    question: string;
    whale_count: number;
    trusted_whale_count: number;
    price: number | null;
  } | null;
  latest_ingestion: {
    scrape_run_id: number;
    job_name: string;
    endpoint_name: string;
    status: string;
    started_at: string | null;
    finished_at: string | null;
    records_written: number;
    error_count: number;
    error_summary: string | null;
  } | null;
  platform_coverage: HomeSummaryPlatformCoverage[];
};

export type TopProfitableUserRow = {
  user_id: number;
  external_user_ref: string;
  wallet_address: string | null;
  preferred_username: string | null;
  display_label: string | null;
  platform_name: string;
  resolved_market_count: number;
  winning_market_count: number;
  realized_pnl: number;
  realized_roi: number;
  win_rate: number | null;
  trust_score: number;
  profitability_score: number;
  sample_trade_count: number;
  latest_trade_time: string | null;
  is_whale: boolean;
  is_trusted_whale: boolean;
};

export type MarketConcentrationRow = {
  market_id: number;
  market_contract_id: number;
  platform_name: string;
  market_slug: string;
  market_url: string | null;
  question: string;
  price: number | null;
  volume: number | null;
  whale_count: number;
  trusted_whale_count: number;
  orderbook_depth: number | null;
  read_time: string | null;
  last_entry_time: string | null;
  market_status_label: "Open" | "Closed";
  whale_bias_label: string;
};

export type RecentWhaleEntryRow = {
  market_id: number;
  market_contract_id: number;
  platform_name: string;
  market_slug: string;
  market_url: string | null;
  question: string;
  price: number | null;
  volume: number | null;
  whale_count: number;
  trusted_whale_count: number;
  entry_trade_count: number;
  total_entry_notional: number;
  latest_entry_time: string | null;
  market_status_label: "Open" | "Closed";
  whale_bias_label: string;
};

export type AnalyticsTimeframe = "7d" | "30d" | "90d" | "all";

export type WhaleEntryBehaviorRow = {
  user_id: number;
  external_user_ref: string;
  wallet_address: string | null;
  preferred_username: string | null;
  display_label: string | null;
  trust_score: number;
  profitability_score: number;
  is_whale: boolean;
  is_trusted_whale: boolean;
  entry_trade_count: number;
  distinct_markets: number;
  total_entry_shares: number;
  total_entry_notional: number;
  weighted_avg_entry_price: number;
  weighted_current_price: number | null;
  avg_entry_shares: number;
  min_entry_price: number;
  max_entry_price: number;
  yes_entry_trade_count: number;
  no_entry_trade_count: number;
  last_entry_time: string | null;
  entry_edge: number | null;
};

export type UserActivitySummary = {
  trade_count: number;
  distinct_markets: number;
  active_days: number;
  total_notional: number;
  latest_trade_time: string | null;
};

export type TagExposureSlice = {
  label: string;
  total_notional: number;
  trade_count: number;
  percentage: number;
};

export type OutcomeBias = {
  label: "yes" | "no" | "other";
  trade_count: number;
  total_notional: number;
  percentage: number;
};

export type HourlyActivityBucket = {
  hour_utc: number;
  trade_count: number;
  total_notional: number;
};

export type RecentTradeRow = {
  transaction_id: number;
  transaction_time: string | null;
  transaction_type: string;
  market_contract_id: number;
  market_slug: string;
  question: string;
  outcome_label: string | null;
  price: number | null;
  shares: number | null;
  notional_value: number | null;
};

export type CurrentPositionRow = {
  position_snapshot_id: number;
  market_contract_id: number;
  market_slug: string;
  question: string;
  snapshot_time: string | null;
  position_size: number | null;
  avg_entry_price: number | null;
  current_mark_price: number | null;
  market_value: number | null;
  cash_pnl: number | null;
  realized_pnl: number | null;
  unrealized_pnl: number | null;
  is_redeemable: boolean;
  is_mergeable: boolean;
};

export type UserActivityInsights = {
  user_id: number;
  timeframe: AnalyticsTimeframe;
  summary: UserActivitySummary;
  tag_exposure: TagExposureSlice[];
  outcome_bias: OutcomeBias[];
  hourly_activity_utc: HourlyActivityBucket[];
  recent_trades: RecentTradeRow[];
  current_positions: CurrentPositionRow[];
};

export type FollowingOverviewRequest = {
  user_ids: number[];
  market_slugs: string[];
};

export type FollowingSummary = {
  followed_trader_count: number;
  followed_market_count: number;
  active_followed_traders_24h: number;
  markets_entered_24h: number;
  recent_closed_followed_market_count: number;
};

export type FollowingInflowRow = {
  market_slug: string;
  question: string;
  distinct_trader_count: number;
  total_notional: number;
  total_shares: number;
  latest_trade_time: string | null;
  market_status_label: "Open" | "Closed";
};

export type FollowingMarketFocusRecentRow = {
  market_slug: string;
  question: string;
  trader_count: number;
  total_focus_value: number;
  latest_activity_time: string | null;
  market_status_label: "Open" | "Closed";
};

export type FollowingClosedMarketRow = {
  market_slug: string;
  question: string;
  closed_time: string | null;
  result_label: string;
  market_status_label: "Closed";
};

export type FollowedTraderFocusRow = {
  user_id: number;
  external_user_ref: string;
  wallet_address: string | null;
  preferred_username: string | null;
  display_label: string | null;
  main_market_slug: string;
  main_market_question: string;
  focus_value: number;
  focus_source: "position" | "recent_flow" | "lifetime_flow";
  share_percentage: number;
  latest_activity_time: string | null;
  market_status_label: "Open" | "Closed";
};

export type FollowingOverview = {
  summary: FollowingSummary;
  inflow_24h: FollowingInflowRow[];
  market_focus_recent: FollowingMarketFocusRecentRow[];
  recent_closed_markets: FollowingClosedMarketRow[];
  trader_focus: FollowedTraderFocusRow[];
};

export type FollowingUserCard = {
  user_id: number;
  external_user_ref: string;
  wallet_address: string | null;
  preferred_username: string | null;
  display_label: string | null;
  is_likely_insider: boolean;
  latest_whale_score: WhaleProfile["latest_whale_score"];
};

export type FollowingMarketCard = {
  market_slug: string;
  question: string;
  price: number | null;
  whale_count: number;
  trusted_whale_count: number;
  market_status_label: "Open" | "Closed";
};

export type FollowingDashboard = {
  overview: FollowingOverview;
  users: FollowingUserCard[];
  markets: FollowingMarketCard[];
};

const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL ?? "").replace(/\/$/, "");
const HOME_SUMMARY_CLIENT_CACHE_MS = 60_000;

let homeSummaryClientCache: { expiresAt: number; value: HomeSummary } | null = null;

export class ApiError extends Error {
  status: number;
  payload: unknown;

  constructor(status: number, message: string, payload: unknown) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.payload = payload;
  }
}

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    credentials: "include",
    ...init,
  });
  if (!response.ok) {
    let payload: unknown = null;
    try {
      payload = await response.json();
    } catch {
      payload = null;
    }
    const detail =
      payload && typeof payload === "object" && "detail" in payload && typeof payload.detail === "string"
        ? payload.detail
        : `Request failed (${response.status}): ${path}`;
    throw new ApiError(response.status, detail, payload);
  }
  return (await response.json()) as T;
}

export async function fetchAuthSession(): Promise<AuthSession> {
  const payload = await fetchJson<{ session: AuthSession }>("/api/auth/me");
  return payload.session;
}

export async function signUpAccount(payload: SignUpPayload): Promise<AuthSession> {
  const response = await fetchJson<{ session: AuthSession }>("/api/auth/signup", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return response.session;
}

export async function loginAccount(payload: LoginPayload): Promise<AuthSession> {
  const response = await fetchJson<{ session: AuthSession }>("/api/auth/login", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return response.session;
}

export async function logoutAccount(): Promise<void> {
  await fetchJson<{ ok: boolean }>("/api/auth/logout", {
    method: "POST",
  });
}

export async function followUserAccount(userId: number): Promise<WatchlistState> {
  const payload = await fetchJson<{ watchlist: WatchlistState }>(`/api/account/follow/users/${userId}`, {
    method: "POST",
  });
  return payload.watchlist;
}

export async function unfollowUserAccount(userId: number): Promise<WatchlistState> {
  const payload = await fetchJson<{ watchlist: WatchlistState }>(`/api/account/follow/users/${userId}`, {
    method: "DELETE",
  });
  return payload.watchlist;
}

export async function followMarketAccount(marketSlug: string): Promise<WatchlistState> {
  const payload = await fetchJson<{ watchlist: WatchlistState }>(
    `/api/account/follow/markets/${encodeURIComponent(marketSlug)}`,
    {
      method: "POST",
    },
  );
  return payload.watchlist;
}

export async function unfollowMarketAccount(marketSlug: string): Promise<WatchlistState> {
  const payload = await fetchJson<{ watchlist: WatchlistState }>(
    `/api/account/follow/markets/${encodeURIComponent(marketSlug)}`,
    {
      method: "DELETE",
    },
  );
  return payload.watchlist;
}

export async function patchAccountPreferences(payload: AccountPreferencesPatch): Promise<AccountPreferences> {
  const response = await fetchJson<{ preferences: AccountPreferences }>("/api/account/preferences", {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return response.preferences;
}

export async function importLocalWatchlist(payload: WatchlistState): Promise<{
  watchlist: WatchlistState;
  imported: {
    users: number;
    markets: number;
  };
}> {
  return fetchJson("/api/account/watchlist/import-local", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      user_ids: payload.users,
      market_slugs: payload.markets,
    }),
  });
}

export async function fetchDashboardMarkets(limit = 10): Promise<DashboardMarketRow[]> {
  const payload = await fetchJson<{ markets: { items: DashboardMarketRow[] } | null }>(
    `/api/dashboards/latest/markets?limit=${limit}`,
  );
  return payload.markets?.items ?? [];
}

export async function fetchLatestWhales(options?: {
  limit?: number;
  tier?: LeaderboardUserBoardFilter;
  whalesOnly?: boolean;
  trustedOnly?: boolean;
}): Promise<WhaleScoreRow[]> {
  const params = new URLSearchParams();
  params.set("limit", String(options?.limit ?? 10));
  if (options?.tier && options.tier !== "all") params.set("tier", options.tier);
  if (options?.whalesOnly) params.set("whales_only", "true");
  if (options?.trustedOnly) params.set("trusted_only", "true");
  const payload = await fetchJson<{ whales: { items: WhaleScoreRow[] } | null }>(
    `/api/whales/latest?${params.toString()}`,
  );
  return payload.whales?.items ?? [];
}

export async function fetchTrustedLeaderboard(): Promise<LeaderboardRow[]> {
  const payload = await fetchJson<{ leaderboard: { rows: LeaderboardRow[] } | null }>(
    "/api/leaderboards/trusted/latest",
  );
  return payload.leaderboard?.rows ?? [];
}

export async function fetchUserWhaleProfile(userId: number): Promise<WhaleProfile> {
  const payload = await fetchJson<{ profile: WhaleProfile }>(`/api/users/${userId}/whale-profile`);
  return payload.profile;
}

export async function fetchMarketProfile(marketSlug: string): Promise<MarketProfile> {
  const payload = await fetchJson<{ profile: MarketProfile }>(
    `/api/markets/${encodeURIComponent(marketSlug)}/profile`,
  );
  return payload.profile;
}

export async function fetchHomeSummary(): Promise<HomeSummary> {
  const now = Date.now();
  if (homeSummaryClientCache && homeSummaryClientCache.expiresAt > now) {
    return homeSummaryClientCache.value;
  }
  const payload = await fetchJson<{ summary: HomeSummary }>("/api/home/summary");
  homeSummaryClientCache = {
    expiresAt: Date.now() + HOME_SUMMARY_CLIENT_CACHE_MS,
    value: payload.summary,
  };
  return payload.summary;
}

export async function fetchTopProfitableUsers(
  limit = 5,
  timeframe: AnalyticsTimeframe = "all",
): Promise<TopProfitableUserRow[]> {
  const payload = await fetchJson<{ analytics: { items: TopProfitableUserRow[] } | null }>(
    `/api/analytics/top-profitable-users?limit=${limit}&timeframe=${timeframe}`,
  );
  return payload.analytics?.items ?? [];
}

export async function fetchMarketWhaleConcentration(
  limit = 5,
  timeframe: AnalyticsTimeframe = "all",
): Promise<MarketConcentrationRow[]> {
  const payload = await fetchJson<{ analytics: { items: MarketConcentrationRow[] } | null }>(
    `/api/analytics/market-whale-concentration?limit=${limit}&timeframe=${timeframe}`,
  );
  return payload.analytics?.items ?? [];
}

export async function fetchWhaleEntryBehavior(
  limit = 5,
  timeframe: AnalyticsTimeframe = "all",
): Promise<WhaleEntryBehaviorRow[]> {
  const payload = await fetchJson<{ analytics: { items: WhaleEntryBehaviorRow[] } | null }>(
    `/api/analytics/whale-entry-behavior?limit=${limit}&timeframe=${timeframe}`,
  );
  return payload.analytics?.items ?? [];
}

export async function fetchRecentWhaleEntries(
  limit = 5,
  timeframe: AnalyticsTimeframe = "all",
): Promise<RecentWhaleEntryRow[]> {
  const payload = await fetchJson<{ analytics: { items: RecentWhaleEntryRow[] } | null }>(
    `/api/analytics/recent-whale-entries?limit=${limit}&timeframe=${timeframe}`,
  );
  return payload.analytics?.items ?? [];
}

export async function fetchUserActivityInsights(
  userId: number,
  timeframe: AnalyticsTimeframe = "all",
): Promise<UserActivityInsights> {
  const payload = await fetchJson<{ insights: UserActivityInsights }>(
    `/api/users/${userId}/activity-insights?timeframe=${timeframe}`,
  );
  return payload.insights;
}

export async function fetchFollowingOverview(
  payload: FollowingOverviewRequest,
): Promise<FollowingOverview> {
  const response = await fetchJson<{ overview: FollowingOverview }>(
    "/api/following/overview",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    },
  );
  return response.overview;
}

export async function fetchFollowingDashboard(
  payload: FollowingOverviewRequest,
): Promise<FollowingDashboard> {
  const response = await fetchJson<{ dashboard: FollowingDashboard }>(
    "/api/following/dashboard",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    },
  );
  return response.dashboard;
}
