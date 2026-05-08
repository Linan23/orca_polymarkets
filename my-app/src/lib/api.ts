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

export type MarketProfileMlPredictionCase = {
  window: "12h" | "24h" | string;
  market_slug: string;
  question: string;
  side_label: string;
  observation_time: string;
  event_category: string;
  focus_category: string;
  focused_fit_category: string;
  market_family: string;
  current_odds_pct: number | null;
  predicted_future_odds_pct: number | null;
  predicted_delta_pts: number | null;
  predicted_direction: string;
  model_predicted_future_odds_pct?: number | null;
  model_predicted_delta_pts?: number | null;
  prediction_source: string;
  prediction_status?: string;
  display_tier: "show" | "review" | "hidden" | string;
  display_reasons: string[];
  review_reasons: string[];
  direction_signal_tier: "strong" | "watch" | "abstain" | string;
  direction_signal_tier_reason: string;
  historical_validation_tier?: string | null;
  historical_validation_reason?: string | null;
  historical_validation_direction_match_pct?: number | null;
  historical_validation_sample_size?: number | null;
  validation_accuracy_pct?: number | null;
  direction_signal_accuracy_pct?: number | null;
  model_confidence_pct?: number | null;
  accuracy_source?: string | null;
  direction_signal_predicted_direction: string;
  direction_signal_confidence: number;
  reliability_warnings: string[];
  overlay_future_odds_pct: number | null;
  overlay_delta_pts: number | null;
  overlay_direction: string;
  interval_low_future_odds_pct: number | null;
  interval_high_future_odds_pct: number | null;
  whale_entry_time?: string | null;
  whale_entry_age_hours?: number | null;
  whale_entry_odds_pct?: number | null;
  whale_entry_notional?: number | null;
  whale_entry_weighted_notional?: number | null;
  whale_entry_trust_score?: number | null;
  whale_entry_is_trusted?: boolean | null;
  prediction_start_time?: string | null;
  prediction_target_time?: string | null;
  prediction_window_hours?: number;
  prediction_timeline_source?: string;
  live_window_features?: Record<string, Record<string, number | string | null> | undefined>;
  actual_future_odds_pct?: number;
  actual_delta_pts?: number;
  actual_direction?: string;
  prediction_signed_error_pts?: number | null;
  prediction_absolute_error_pts?: number | null;
  prediction_direction_match?: boolean | null;
  prediction_validation_status?: string | null;
  actual_source?: string | null;
  actual_observed_at?: string | null;
  trend_fit_error_type: string;
  trend_shape_score: number;
  whale_anchor: Record<string, number | string | null>;
  crypto_segment_direction_gate_tier?: string;
  crypto_segment_direction_gate_reason?: string;
  crypto_direction_source_selector_reason?: string;
  live_polymarket_updated_at?: string | null;
  live_polymarket_closed?: boolean | null;
  latest_completed_validation?: MarketProfileMlPredictionValidationReference | null;
  prediction_validation_comparison_type?: string | null;
  local_backtest_only?: boolean;
};

export type MarketProfileMlPredictionValidationReference = {
  ml_market_prediction_snapshot_id: number;
  window: "12h" | "24h" | string;
  side_label: string;
  observation_time: string | null;
  prediction_start_time?: string | null;
  prediction_target_time: string | null;
  prediction_generated_at: string | null;
  prediction_window_hours: number;
  current_odds_pct: number | null;
  model_predicted_future_odds_pct: number | null;
  model_predicted_delta_pts: number | null;
  predicted_direction?: string | null;
  actual_future_odds_pct: number | null;
  actual_delta_pts: number | null;
  actual_direction?: string | null;
  prediction_signed_error_pts?: number | null;
  prediction_absolute_error_pts?: number | null;
  prediction_direction_match?: boolean | null;
  prediction_validation_status?: string | null;
  actual_source?: string | null;
  actual_observed_at?: string | null;
  comparison_type?: string | null;
};

export type MarketOutcomeProbability = {
  label: string;
  probability: number | null;
};

export type MarketProfileWhaleEntryAnchor = {
  available?: boolean;
  reason?: string;
  event_type?: string;
  event_time?: string | null;
  age_hours?: number | null;
  odds_pct?: number | null;
  notional_value?: number | null;
  weighted_notional?: number | null;
  trust_score?: number | null;
  is_trusted_whale?: boolean | null;
  side_label?: string | null;
  market_slug?: string | null;
  source?: string;
};

export type MarketProfileLiveWhaleSequenceItem = {
  market_slug?: string;
  side_label?: string;
  market_status?: string;
  current_market_price_pct?: number | null;
  signal?: Record<string, number | string | boolean | null>;
  entry_anchor?: MarketProfileWhaleEntryAnchor | null;
  exit_anchor?: MarketProfileWhaleEntryAnchor | null;
  window_features?: Record<string, Record<string, number | string | null> | undefined>;
};

export type MarketProfileMlPredictionTrend = {
  available: boolean;
  reason?: string;
  market_slug?: string;
  question?: string;
  generated_at?: string | null;
  report_status?: string | null;
  model_name?: string | null;
  source?: string;
  source_path?: string;
  production_use?: boolean;
  local_backtest_only?: boolean;
  server_ready_shape?: boolean;
  prediction_status?: string;
  outcome_probabilities?: MarketOutcomeProbability[] | null;
  primary_side_label?: string | null;
  live_polymarket_updated_at?: string | null;
  live_polymarket_closed?: boolean | null;
  recent_12h_validation?: MarketProfileMlPredictionValidationSummary | null;
  prediction_anchor?: MarketProfileWhaleEntryAnchor;
  live_whale_sequence?: {
    available?: boolean;
    reason?: string;
    semi_live?: boolean;
    as_of?: string | null;
    generated_at?: string | null;
    lookback_hours?: number;
    sequence_count?: number;
    queried_event_rows?: number;
    source?: string;
    items?: MarketProfileLiveWhaleSequenceItem[];
  };
  windows?: {
    "12h"?: MarketProfileMlPredictionCase[];
    "24h"?: MarketProfileMlPredictionCase[];
    [key: string]: MarketProfileMlPredictionCase[] | undefined;
  };
};

export type MarketProfileMlPredictionValidationSummary = {
  window_hours: number;
  sample_size: number;
  sample_limit: number;
  direction_match_count: number;
  direction_match_rate_pct: number;
  avg_absolute_error_pts: number | null;
  latest_validated_at?: string | null;
  latest_actual_observed_at?: string | null;
  summary_label?: string | null;
};

export type MarketProfileTopWhale = {
  user_id: number;
  external_user_ref: string;
  wallet_address: string | null;
  preferred_username: string | null;
  display_label: string | null;
  trust_score: number;
  profitability_score: number;
  sample_trade_count: number;
  is_whale: boolean;
  is_trusted_whale: boolean;
  trade_count: number;
  buy_trade_count: number;
  sell_trade_count: number;
  total_notional: number;
  total_shares: number;
  avg_trade_price: number;
  latest_trade_time: string | null;
  latest_side: string | null;
  latest_outcome_label: string | null;
  latest_trade_price: number | null;
};

export type MarketProfileTopWhales = {
  market_slug: string;
  snapshot_time: string | null;
  scoring_version: string | null;
  count: number;
  items: MarketProfileTopWhale[];
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
  realized_pnl: number | null;
  sample_trade_count: number;
  is_whale: boolean;
  is_trusted_whale: boolean;
  whale_status: boolean;
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
  whale_status: boolean;
  latest_whale_score: {
    snapshot_time: string | null;
    scoring_version: string;
    trust_score: number;
    profitability_score: number;
    sample_trade_count: number;
    is_whale: boolean;
    is_trusted_whale: boolean;
    whale_status: boolean;
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
    whale_status: boolean;
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
  selected_side_label?: string | null;
  primary_side_label?: string | null;
  outcome_probabilities?: MarketOutcomeProbability[] | null;
  market_status_label: "Open" | "Closed";
  realtime_source: string;
  snapshot_time: string | null;
  realtime_payload: Record<string, unknown>;
  ml_prediction_trend?: MarketProfileMlPredictionTrend;
  top_whales?: MarketProfileTopWhales;
};

export type HomeSummaryPlatformCoverage = {
  platform_name: string;
  user_count: number;
  market_count: number;
  transaction_count: number;
  orderbook_snapshot_count: number;
};

export type HomeSummaryMarketCategoryCoverage = {
  category_name: string;
  market_count: number;
};

export type HomeSummary = {
  scoring_version: string | null;
  is_stale?: boolean;
  stale_as_of?: string | null;
  freshness_source?: string | null;
  last_successful_ingest_at?: string | null;
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
  market_category_coverage?: HomeSummaryMarketCategoryCoverage[];
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
  whale_status: boolean;
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
  whale_status: boolean;
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
  main_market_category?: string | null;
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
  whale_status: boolean;
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

export type DashboardHomePayload = {
  summary: HomeSummary;
  research: {
    top_profitable_users: { items: TopProfitableUserRow[] } | null;
    recent_whale_entries: { items: RecentWhaleEntryRow[] } | null;
    market_whale_concentration: { items: MarketConcentrationRow[] } | null;
    whale_entry_behavior: { items: WhaleEntryBehaviorRow[] } | null;
  };
  market_leaderboard: { items: DashboardMarketRow[] } | null;
  whale_leaderboard: { items: WhaleScoreRow[] } | null;
};

export type MarketProfileFullPayload = {
  profile: MarketProfile;
  ml_prediction_trend: MarketProfileMlPredictionTrend;
  top_whales: MarketProfileTopWhales;
};

export type UserProfileFullPayload = {
  profile: WhaleProfile;
  insights: UserActivityInsights;
};

export type PolymarketTweet = {
  id: string;
  text: string;
  created_at: string | null;
  url: string;
};

export type PolymarketTweetFeed = {
  available: boolean;
  source: string;
  reason?: string | null;
  account: {
    id?: string;
    name: string;
    username: string;
    profile_image_url?: string | null;
    profile_url: string;
  };
  items: PolymarketTweet[];
};

const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL ?? "").replace(/\/$/, "");
const HOME_SUMMARY_CLIENT_CACHE_MS = 60_000;
const DASHBOARD_READ_CLIENT_CACHE_MS = 300_000;
const SESSION_CACHE_PREFIX = "orca:dashboard-read:";

const clientReadCache = new Map<string, { expiresAt: number; value: unknown }>();
const clientReadInflight = new Map<string, Promise<unknown>>();

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

function sessionCacheKey(path: string) {
  return `${SESSION_CACHE_PREFIX}${path}`;
}

function readStoredCacheEntry<T>(path: string): { expiresAt: number; value: T } | null {
  if (typeof window === "undefined") return null;
  try {
    const raw = window.sessionStorage.getItem(sessionCacheKey(path));
    if (!raw) return null;
    const parsed = JSON.parse(raw) as { expiresAt?: unknown; value?: unknown };
    if (typeof parsed.expiresAt !== "number" || !("value" in parsed)) {
      window.sessionStorage.removeItem(sessionCacheKey(path));
      return null;
    }
    return { expiresAt: parsed.expiresAt, value: parsed.value as T };
  } catch {
    return null;
  }
}

function writeStoredCacheEntry<T>(path: string, value: T, ttlMs: number) {
  if (typeof window === "undefined") return;
  const entry = { expiresAt: Date.now() + ttlMs, value };
  try {
    window.sessionStorage.setItem(sessionCacheKey(path), JSON.stringify(entry));
  } catch {
    // Session storage is an opportunistic speed path; failed writes should not block reads.
  }
}

export function peekCachedApiResponse<T>(path: string, options?: { allowStale?: boolean }): T | null {
  const now = Date.now();
  const memoryEntry = clientReadCache.get(path);
  if (memoryEntry && (options?.allowStale || memoryEntry.expiresAt > now)) {
    return memoryEntry.value as T;
  }

  const storedEntry = readStoredCacheEntry<T>(path);
  if (!storedEntry) return null;
  if (!options?.allowStale && storedEntry.expiresAt <= now) return null;
  clientReadCache.set(path, { expiresAt: storedEntry.expiresAt, value: storedEntry.value });
  return storedEntry.value;
}

async function fetchCachedJson<T>(path: string, ttlMs = DASHBOARD_READ_CLIENT_CACHE_MS): Promise<T> {
  const now = Date.now();
  const cached = clientReadCache.get(path);
  if (cached && cached.expiresAt > now) {
    return cached.value as T;
  }
  const stored = readStoredCacheEntry<T>(path);
  if (stored && stored.expiresAt > now) {
    clientReadCache.set(path, stored);
    return stored.value;
  }

  const inflight = clientReadInflight.get(path);
  if (inflight) {
    return (await inflight) as T;
  }

  const promise = fetchJson<T>(path);
  clientReadInflight.set(path, promise);
  let value: T;
  try {
    value = await promise;
  } finally {
    clientReadInflight.delete(path);
  }
  clientReadCache.set(path, { expiresAt: Date.now() + ttlMs, value });
  writeStoredCacheEntry(path, value, ttlMs);
  return value;
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

function dashboardHomePath(timeframe: AnalyticsTimeframe = "all", limit = 5) {
  const params = new URLSearchParams();
  params.set("timeframe", timeframe);
  params.set("limit", String(limit));
  return `/api/dashboard/home?${params.toString()}`;
}

function marketProfileFullPath(marketSlug: string, topWhalesLimit = 5) {
  const params = new URLSearchParams();
  params.set("top_whales_limit", String(topWhalesLimit));
  return `/api/markets/${encodeURIComponent(marketSlug)}/profile/full?${params.toString()}`;
}

function userProfileFullPath(userId: number, timeframe: AnalyticsTimeframe = "30d") {
  const params = new URLSearchParams();
  params.set("timeframe", timeframe);
  return `/api/users/${userId}/profile/full?${params.toString()}`;
}

export function getCachedDashboardHome(
  timeframe: AnalyticsTimeframe = "all",
  limit = 5,
): DashboardHomePayload | null {
  return peekCachedApiResponse<DashboardHomePayload>(dashboardHomePath(timeframe, limit), { allowStale: true });
}

export function getCachedMarketProfileFull(marketSlug: string, topWhalesLimit = 5): MarketProfileFullPayload | null {
  return peekCachedApiResponse<MarketProfileFullPayload>(marketProfileFullPath(marketSlug, topWhalesLimit), {
    allowStale: true,
  });
}

export function getCachedUserProfileFull(
  userId: number,
  timeframe: AnalyticsTimeframe = "30d",
): UserProfileFullPayload | null {
  return peekCachedApiResponse<UserProfileFullPayload>(userProfileFullPath(userId, timeframe), { allowStale: true });
}

export async function fetchDashboardHome(
  timeframe: AnalyticsTimeframe = "all",
  limit = 5,
): Promise<DashboardHomePayload> {
  return fetchCachedJson<DashboardHomePayload>(dashboardHomePath(timeframe, limit));
}

export async function fetchPolymarketTweetFeed(limit = 6): Promise<PolymarketTweetFeed> {
  const params = new URLSearchParams();
  params.set("limit", String(limit));
  return fetchCachedJson<PolymarketTweetFeed>(`/api/social/polymarket-tweets?${params.toString()}`);
}

export async function fetchMarketProfileFull(marketSlug: string, topWhalesLimit = 5): Promise<MarketProfileFullPayload> {
  return fetchCachedJson<MarketProfileFullPayload>(
    marketProfileFullPath(marketSlug, topWhalesLimit),
    120_000,
  );
}

export async function fetchUserProfileFull(
  userId: number,
  timeframe: AnalyticsTimeframe = "30d",
): Promise<UserProfileFullPayload> {
  return fetchCachedJson<UserProfileFullPayload>(userProfileFullPath(userId, timeframe), 60_000);
}

export async function fetchDashboardMarkets(limit = 10): Promise<DashboardMarketRow[]> {
  const payload = await fetchCachedJson<{ markets: { items: DashboardMarketRow[] } | null }>(
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
  const payload = await fetchCachedJson<{ whales: { items: WhaleScoreRow[] } | null }>(
    `/api/whales/latest?${params.toString()}`,
  );
  return payload.whales?.items ?? [];
}

export async function fetchTrustedLeaderboard(): Promise<LeaderboardRow[]> {
  const payload = await fetchCachedJson<{ leaderboard: { rows: LeaderboardRow[] } | null }>(
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

export async function fetchMarketProfileMlTrend(marketSlug: string): Promise<MarketProfileMlPredictionTrend> {
  const payload = await fetchCachedJson<{ ml_prediction_trend: MarketProfileMlPredictionTrend }>(
    `/api/markets/${encodeURIComponent(marketSlug)}/ml-trend`,
  );
  return payload.ml_prediction_trend;
}

export async function fetchMarketProfileTopWhales(marketSlug: string, limit = 5): Promise<MarketProfileTopWhales> {
  const params = new URLSearchParams();
  params.set("limit", String(limit));
  const payload = await fetchCachedJson<{ top_whales: MarketProfileTopWhales }>(
    `/api/markets/${encodeURIComponent(marketSlug)}/top-whales?${params.toString()}`,
  );
  return payload.top_whales;
}

export async function fetchHomeSummary(): Promise<HomeSummary> {
  const payload = await fetchCachedJson<{ summary: HomeSummary }>(
    "/api/home/summary",
    HOME_SUMMARY_CLIENT_CACHE_MS,
  );
  return payload.summary;
}

export async function fetchTopProfitableUsers(
  limit = 5,
  timeframe: AnalyticsTimeframe = "all",
): Promise<TopProfitableUserRow[]> {
  const payload = await fetchCachedJson<{ analytics: { items: TopProfitableUserRow[] } | null }>(
    `/api/analytics/top-profitable-users?limit=${limit}&timeframe=${timeframe}`,
  );
  return payload.analytics?.items ?? [];
}

export async function fetchMarketWhaleConcentration(
  limit = 5,
  timeframe: AnalyticsTimeframe = "all",
): Promise<MarketConcentrationRow[]> {
  const payload = await fetchCachedJson<{ analytics: { items: MarketConcentrationRow[] } | null }>(
    `/api/analytics/market-whale-concentration?limit=${limit}&timeframe=${timeframe}`,
  );
  return payload.analytics?.items ?? [];
}

export async function fetchWhaleEntryBehavior(
  limit = 5,
  timeframe: AnalyticsTimeframe = "all",
): Promise<WhaleEntryBehaviorRow[]> {
  const payload = await fetchCachedJson<{ analytics: { items: WhaleEntryBehaviorRow[] } | null }>(
    `/api/analytics/whale-entry-behavior?limit=${limit}&timeframe=${timeframe}`,
  );
  return payload.analytics?.items ?? [];
}

export async function fetchRecentWhaleEntries(
  limit = 5,
  timeframe: AnalyticsTimeframe = "all",
): Promise<RecentWhaleEntryRow[]> {
  const payload = await fetchCachedJson<{ analytics: { items: RecentWhaleEntryRow[] } | null }>(
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
