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
  avg_entry_shares: number;
  min_entry_price: number;
  max_entry_price: number;
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

const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL ?? "").replace(/\/$/, "");

async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`);
  if (!response.ok) {
    throw new Error(`Request failed (${response.status}): ${path}`);
  }
  return (await response.json()) as T;
}

export async function fetchDashboardMarkets(limit = 10): Promise<DashboardMarketRow[]> {
  const payload = await fetchJson<{ markets: { items: DashboardMarketRow[] } | null }>(
    `/api/dashboards/latest/markets?limit=${limit}`,
  );
  return payload.markets?.items ?? [];
}

export async function fetchLatestWhales(options?: {
  limit?: number;
  whalesOnly?: boolean;
  trustedOnly?: boolean;
}): Promise<WhaleScoreRow[]> {
  const params = new URLSearchParams();
  params.set("limit", String(options?.limit ?? 10));
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
  const payload = await fetchJson<{ summary: HomeSummary }>("/api/home/summary");
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

export async function fetchUserActivityInsights(
  userId: number,
  timeframe: AnalyticsTimeframe = "all",
): Promise<UserActivityInsights> {
  const payload = await fetchJson<{ insights: UserActivityInsights }>(
    `/api/users/${userId}/activity-insights?timeframe=${timeframe}`,
  );
  return payload.insights;
}
