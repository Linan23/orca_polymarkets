import { useCallback, useMemo, useState } from "react";
import { useAuth } from "../auth/AuthContext";
import { Link } from "react-router-dom";
import FollowButton from "../components/FollowButton";
import { useApiData } from "../hooks/useApiData";
import { useWatchlist } from "../hooks/useWatchlist";
import {
  type AnalyticsTimeframe,
  fetchMarketWhaleConcentration,
  fetchRecentWhaleEntries,
  fetchTopProfitableUsers,
  fetchWhaleEntryBehavior,
  type MarketConcentrationRow,
  type RecentWhaleEntryRow,
  type TopProfitableUserRow,
  type WhaleEntryBehaviorRow,
} from "../lib/api";
import {
  deriveUserIdentity,
  deriveWhaleTierLabel,
  deriveWhaleTierPillClass,
  matchesUserIdentityQuery,
} from "../lib/userIdentity";

const RESEARCH_CARD_LIMIT = 5;

function formatPercent(value: number | null) {
  if (value === null) return "--";
  return `${(value * 100).toFixed(1)}%`;
}

function formatCurrency(value: number) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  }).format(value);
}

function formatNumber(value: number | null, maximumFractionDigits = 0) {
  if (value === null || Number.isNaN(value)) return "--";
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits,
  }).format(value);
}

function formatDateTime(value: string | null) {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

function formatPointDelta(value: number | null) {
  if (value === null) return "--";
  const points = value * 100;
  return `${points > 0 ? "+" : ""}${points.toFixed(1)} pts`;
}

function leanClass(label: string) {
  if (label.toLowerCase().includes("yes")) return "signal-yes";
  if (label.toLowerCase().includes("no")) return "signal-no";
  if (label.toLowerCase().includes("balanced")) return "signal-neutral";
  return "signal-neutral";
}

function marketStatusClass(status: "Open" | "Closed") {
  return status === "Closed" ? "signal-closed" : "signal-open";
}

function userEntryBiasLabel(user: WhaleEntryBehaviorRow) {
  if (user.yes_entry_trade_count > user.no_entry_trade_count) return "Mostly Buys Yes";
  if (user.no_entry_trade_count > user.yes_entry_trade_count) return "Mostly Buys No";
  return "Buys Yes and No Evenly";
}

function downloadText(filename: string, content: string, mimeType: string) {
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  URL.revokeObjectURL(url);
}

function csvCell(value: unknown) {
  const text = value === null || value === undefined ? "" : String(value);
  return `"${text.replace(/"/g, '""')}"`;
}

function downloadRowsAsCsv(filename: string, rows: Array<Record<string, unknown>>) {
  if (rows.length === 0) return;
  const columns = Object.keys(rows[0]);
  const lines = [
    columns.join(","),
    ...rows.map((row) => columns.map((column) => csvCell(row[column])).join(",")),
  ];
  downloadText(filename, `${lines.join("\n")}\n`, "text/csv;charset=utf-8");
}

function downloadRowsAsJson(filename: string, rows: Array<Record<string, unknown>>) {
  downloadText(filename, `${JSON.stringify(rows, null, 2)}\n`, "application/json;charset=utf-8");
}

function PlaceholderRows({ count }: { count: number }) {
  if (count <= 0) return null;
  return (
    <>
      {Array.from({ length: count }, (_, index) => (
        <article key={`placeholder-${index}`} className="leaderboard-row leaderboard-row-shell leaderboard-row-placeholder">
          <div className="leaderboard-rank">-</div>
          <div className="leaderboard-main">
            <div className="leaderboard-main-top">
              <div>
                <div className="leaderboard-name leaderboard-name-placeholder">No additional result</div>
                <div className="leaderboard-subtext">This slot is reserved to keep the top-5 layout aligned.</div>
              </div>
            </div>
          </div>
        </article>
      ))}
    </>
  );
}

function ResearchUserRows({
  items,
  isUserFollowed,
  onToggleUser,
}: {
  items: TopProfitableUserRow[];
  isUserFollowed: (userId: number) => boolean;
  onToggleUser: (userId: number) => void;
}) {
  return (
    <div className="leaderboard-list">
      {items.map((user, index) => {
        const identity = deriveUserIdentity(user);
        const avgResolvedPnl =
          user.resolved_market_count > 0 ? user.realized_pnl / user.resolved_market_count : null;
        return (
          <article key={`${user.user_id}-${user.external_user_ref}`} className="leaderboard-row leaderboard-row-shell">
            <div className="leaderboard-rank">{index + 1}</div>
            <div className="leaderboard-main">
              <div className="leaderboard-main-top">
                <div>
                  <Link to={`/users/${user.user_id}`} className="leaderboard-name">
                    {identity.primary}
                  </Link>
                  <div className="leaderboard-subtext">
                    {identity.secondary} · {user.resolved_market_count} resolved markets · {user.winning_market_count} wins
                  </div>
                </div>
                <div className="leaderboard-score">{formatCurrency(user.realized_pnl)}</div>
              </div>
              <div className="leaderboard-meta">
                <span className="meta-pill">ROI {formatPercent(user.realized_roi)}</span>
                <span className="meta-pill">Win {formatPercent(user.win_rate)}</span>
                <span className="meta-pill">Trust {user.trust_score.toFixed(3)}</span>
                <span className="meta-pill">Profit {user.profitability_score.toFixed(3)}</span>
                <span className="meta-pill">
                  Avg P&amp;L {avgResolvedPnl === null ? "--" : formatCurrency(avgResolvedPnl)}
                </span>
                <span className="meta-pill">Trades {user.sample_trade_count}</span>
                <span className="meta-pill">Last active {formatDateTime(user.latest_trade_time)}</span>
                <span className={`meta-pill ${deriveWhaleTierPillClass(user)}`}>
                  {deriveWhaleTierLabel(user)}
                </span>
              </div>
            </div>
            <div className="leaderboard-row-action">
              <FollowButton
                compact
                isFollowing={isUserFollowed(user.user_id)}
                onToggle={() => onToggleUser(user.user_id)}
              />
            </div>
          </article>
        );
      })}
      <PlaceholderRows count={Math.max(0, RESEARCH_CARD_LIMIT - items.length)} />
    </div>
  );
}

function ResearchMarketRows({
  items,
  isMarketFollowed,
  onToggleMarket,
}: {
  items: MarketConcentrationRow[];
  isMarketFollowed: (marketSlug: string) => boolean;
  onToggleMarket: (marketSlug: string) => void;
}) {
  return (
    <div className="leaderboard-list">
      {items.map((market, index) => (
        <article key={`${market.market_id}-${market.market_slug}`} className="leaderboard-row leaderboard-row-shell">
          <div className="leaderboard-rank">{index + 1}</div>
          <div className="leaderboard-main">
            <div className="leaderboard-main-top">
              <div>
                <Link to={`/markets/${market.market_slug}`} className="leaderboard-name">
                  {market.question}
                </Link>
                <div className="leaderboard-subtext">{market.platform_name} · {market.market_slug}</div>
              </div>
              <div className="leaderboard-price">{formatPercent(market.price)}</div>
            </div>
            <div className="leaderboard-meta">
              <span className="meta-pill">Whale Traders {market.whale_count}</span>
              <span className="meta-pill">Trusted Whales {market.trusted_whale_count}</span>
              <span className="meta-pill">Volume {formatNumber(market.volume)}</span>
              <span className="meta-pill">Depth {formatNumber(market.orderbook_depth)}</span>
              <span className="meta-pill">Last entry {formatDateTime(market.last_entry_time ?? market.read_time)}</span>
              <span className="meta-pill">Updated {formatDateTime(market.read_time)}</span>
              <span className={`meta-pill ${leanClass(market.whale_bias_label)}`}>{market.whale_bias_label}</span>
              <span className={`meta-pill ${marketStatusClass(market.market_status_label)}`}>{market.market_status_label}</span>
            </div>
          </div>
          <div className="leaderboard-row-action">
            <FollowButton
              compact
              isFollowing={isMarketFollowed(market.market_slug)}
              onToggle={() => onToggleMarket(market.market_slug)}
            />
          </div>
        </article>
      ))}
      <PlaceholderRows count={Math.max(0, RESEARCH_CARD_LIMIT - items.length)} />
    </div>
  );
}

function ResearchRecentEntryRows({
  items,
  isMarketFollowed,
  onToggleMarket,
}: {
  items: RecentWhaleEntryRow[];
  isMarketFollowed: (marketSlug: string) => boolean;
  onToggleMarket: (marketSlug: string) => void;
}) {
  return (
    <div className="leaderboard-list">
      {items.map((market, index) => (
        <article key={`${market.market_id}-${market.market_slug}`} className="leaderboard-row leaderboard-row-shell">
          <div className="leaderboard-rank">{index + 1}</div>
          <div className="leaderboard-main">
            <div className="leaderboard-main-top">
              <div>
                <Link to={`/markets/${market.market_slug}`} className="leaderboard-name">
                  {market.question}
                </Link>
                <div className="leaderboard-subtext">{market.platform_name} · {market.market_slug}</div>
              </div>
              <div className="leaderboard-price">{formatPercent(market.price)}</div>
            </div>
            <div className="leaderboard-meta">
              <span className="meta-pill">Whale Traders {market.whale_count}</span>
              <span className="meta-pill">Trusted Whales {market.trusted_whale_count}</span>
              <span className="meta-pill">Entries {market.entry_trade_count}</span>
              <span className="meta-pill">Notional {formatCurrency(market.total_entry_notional)}</span>
              <span className="meta-pill">Latest {formatDateTime(market.latest_entry_time)}</span>
              <span className={`meta-pill ${leanClass(market.whale_bias_label)}`}>{market.whale_bias_label}</span>
              <span className={`meta-pill ${marketStatusClass(market.market_status_label)}`}>{market.market_status_label}</span>
            </div>
          </div>
          <div className="leaderboard-row-action">
            <FollowButton
              compact
              isFollowing={isMarketFollowed(market.market_slug)}
              onToggle={() => onToggleMarket(market.market_slug)}
            />
          </div>
        </article>
      ))}
      <PlaceholderRows count={Math.max(0, RESEARCH_CARD_LIMIT - items.length)} />
    </div>
  );
}

function ResearchEntryRows({
  items,
  isUserFollowed,
  onToggleUser,
}: {
  items: WhaleEntryBehaviorRow[];
  isUserFollowed: (userId: number) => boolean;
  onToggleUser: (userId: number) => void;
}) {
  return (
    <div className="leaderboard-list">
      {items.map((user, index) => {
        const identity = deriveUserIdentity(user);
        const entryBiasLabel = userEntryBiasLabel(user);
        const entryEdgeClass =
          user.entry_edge === null ? "signal-neutral" : user.entry_edge >= 0 ? "signal-positive" : "signal-negative";
        return (
          <article key={`${user.user_id}-${user.external_user_ref}`} className="leaderboard-row leaderboard-row-shell">
            <div className="leaderboard-rank">{index + 1}</div>
            <div className="leaderboard-main">
              <div className="leaderboard-main-top">
                <div>
                  <Link to={`/users/${user.user_id}`} className="leaderboard-name">
                    {identity.primary}
                  </Link>
                  <div className="leaderboard-subtext">
                    {identity.secondary} · {user.entry_trade_count} buy entries · {user.distinct_markets} markets
                  </div>
                </div>
                <div className="leaderboard-score">{formatPercent(user.weighted_avg_entry_price)}</div>
              </div>
              <div className="leaderboard-meta">
                <span className="meta-pill">Now {formatPercent(user.weighted_current_price)}</span>
                <span className={`meta-pill ${entryEdgeClass}`}>
                  Price vs Entry {formatPointDelta(user.entry_edge)}
                </span>
                <span className={`meta-pill ${leanClass(entryBiasLabel)}`}>{entryBiasLabel}</span>
                <span className="meta-pill">Yes Buys {user.yes_entry_trade_count}</span>
                <span className="meta-pill">No Buys {user.no_entry_trade_count}</span>
                <span className="meta-pill">Latest Buy {formatDateTime(user.last_entry_time)}</span>
                <span className="meta-pill">Avg shares {user.avg_entry_shares.toFixed(1)}</span>
                <span className={`meta-pill ${deriveWhaleTierPillClass(user)}`}>
                  {deriveWhaleTierLabel(user)}
                </span>
              </div>
            </div>
            <div className="leaderboard-row-action">
              <FollowButton
                compact
                isFollowing={isUserFollowed(user.user_id)}
                onToggle={() => onToggleUser(user.user_id)}
              />
            </div>
          </article>
        );
      })}
      <PlaceholderRows count={Math.max(0, RESEARCH_CARD_LIMIT - items.length)} />
    </div>
  );
}

type ResearchAnalyticsSectionProps = {
  showExportControls?: boolean;
  persistTimeframePreference?: boolean;
};

export default function ResearchAnalyticsSection({
  showExportControls = false,
  persistTimeframePreference = false,
}: ResearchAnalyticsSectionProps) {
  const { isAuthenticated, preferences, updatePreferences } = useAuth();
  const [publicTimeframe, setPublicTimeframe] = useState<AnalyticsTimeframe>("all");
  const [userSearch, setUserSearch] = useState("");
  const { isUserFollowed, toggleUser, isMarketFollowed, toggleMarket } = useWatchlist();
  const timeframe =
    persistTimeframePreference && isAuthenticated ? preferences.homepage.research_timeframe : publicTimeframe;
  const handleTimeframeChange = useCallback(
    (value: AnalyticsTimeframe) => {
      if (persistTimeframePreference && isAuthenticated) {
        if (preferences.homepage.research_timeframe === value) return;
        void updatePreferences({
          homepage: {
            research_timeframe: value,
          },
        });
        return;
      }
      setPublicTimeframe(value);
    },
    [
      isAuthenticated,
      persistTimeframePreference,
      preferences.homepage.research_timeframe,
      updatePreferences,
    ],
  );

  const loadAnalytics = useCallback(
    async () => {
      const [topUsers, recentEntries, topMarkets, entryBehavior] = await Promise.all([
        fetchTopProfitableUsers(RESEARCH_CARD_LIMIT, timeframe),
        fetchRecentWhaleEntries(RESEARCH_CARD_LIMIT, timeframe),
        fetchMarketWhaleConcentration(RESEARCH_CARD_LIMIT, timeframe),
        fetchWhaleEntryBehavior(RESEARCH_CARD_LIMIT, timeframe),
      ]);
      return { topUsers, recentEntries, topMarkets, entryBehavior };
    },
    [timeframe],
  );
  const { data, loading, refreshing, error } = useApiData(loadAnalytics, { keepPreviousData: true });
  const filteredTopUsers = useMemo(() => {
    if (!data) return [];
    return data.topUsers.filter((user) => matchesUserIdentityQuery(user, userSearch));
  }, [data, userSearch]);
  const visibleTopUsers = useMemo(
    () => filteredTopUsers.slice(0, RESEARCH_CARD_LIMIT),
    [filteredTopUsers],
  );
  const visibleRecentEntries = useMemo(
    () => (data ? data.recentEntries.slice(0, RESEARCH_CARD_LIMIT) : []),
    [data],
  );
  const visibleTopMarkets = useMemo(
    () => (data ? data.topMarkets.slice(0, RESEARCH_CARD_LIMIT) : []),
    [data],
  );
  const filteredEntryBehavior = useMemo(() => {
    if (!data) return [];
    return data.entryBehavior.filter((user) => matchesUserIdentityQuery(user, userSearch));
  }, [data, userSearch]);
  const visibleEntryBehavior = useMemo(
    () => filteredEntryBehavior.slice(0, RESEARCH_CARD_LIMIT),
    [filteredEntryBehavior],
  );

  return (
    <section className="analytics-section">
      <div className="summary-section-header">
        <p className="leaderboard-kicker">Research Views</p>
        <h2>Whale Behavior Signals</h2>
        <p className="summary-card-subtext">
          Conservative resolved-user profitability and latest market concentration, ready for report/dashboard use.
        </p>
        {refreshing && <p className="summary-card-subtext">Updating research view...</p>}
        <div className="analytics-toolbar">
          <label className="filter-field filter-field-search">
            <span>Search trader</span>
            <input
              value={userSearch}
              onChange={(event) => setUserSearch(event.target.value)}
              placeholder="username or wallet"
              type="search"
            />
          </label>
          <label className="filter-field analytics-timeframe-field">
            <span>Timeframe</span>
            <select value={timeframe} onChange={(event) => handleTimeframeChange(event.target.value as AnalyticsTimeframe)}>
              <option value="7d">Last 7 days</option>
              <option value="30d">Last 30 days</option>
              <option value="90d">Last 90 days</option>
              <option value="all">All time</option>
            </select>
          </label>
        </div>
      </div>

      {loading && <div className="status-panel">Loading research analytics...</div>}
      {error && <div className="status-panel error-panel">{error}</div>}

      {!loading && !error && data && (
        <div className="analytics-grid research-analytics-grid">
          <section className="leaderboard-card">
            <div className="leaderboard-top">
              <p className="leaderboard-kicker">Polymarket</p>
              <h2>Top Profitable Resolved Users</h2>
              <p className="leaderboard-count">Ranked by conservative realized P&amp;L on resolved markets for {timeframe}.</p>
              <p className="leaderboard-count">
                Showing {visibleTopUsers.length} of top {RESEARCH_CARD_LIMIT} traders
              </p>
              {showExportControls && visibleTopUsers.length > 0 && (
                <div className="analytics-export-row">
                  <button
                    type="button"
                    className="analytics-export-btn"
                    onClick={() => downloadRowsAsCsv(`top_profitable_resolved_users_${timeframe}.csv`, visibleTopUsers)}
                  >
                    Export CSV
                  </button>
                  <button
                    type="button"
                    className="analytics-export-btn"
                    onClick={() => downloadRowsAsJson(`top_profitable_resolved_users_${timeframe}.json`, visibleTopUsers)}
                  >
                    Export JSON
                  </button>
                </div>
              )}
            </div>
            {visibleTopUsers.length > 0 ? (
              <ResearchUserRows items={visibleTopUsers} isUserFollowed={isUserFollowed} onToggleUser={toggleUser} />
            ) : (
              <div className="status-panel">
                {userSearch.trim().length > 0
                  ? "No profitable traders match that username or wallet."
                  : "No resolved-user profitability rows are available yet."}
              </div>
            )}
          </section>

          <section className="leaderboard-card">
            <div className="leaderboard-top">
              <p className="leaderboard-kicker">Live Flow</p>
              <h2>Recent Whale Entries</h2>
              <p className="leaderboard-count">Top {RESEARCH_CARD_LIMIT} markets by latest whale buy entry for {timeframe}.</p>
              <p className="leaderboard-count">
                Showing {visibleRecentEntries.length} of top {RESEARCH_CARD_LIMIT} markets
              </p>
              {showExportControls && visibleRecentEntries.length > 0 && (
                <div className="analytics-export-row">
                  <button
                    type="button"
                    className="analytics-export-btn"
                    onClick={() => downloadRowsAsCsv(`recent_whale_entries_${timeframe}.csv`, visibleRecentEntries)}
                  >
                    Export CSV
                  </button>
                  <button
                    type="button"
                    className="analytics-export-btn"
                    onClick={() => downloadRowsAsJson(`recent_whale_entries_${timeframe}.json`, visibleRecentEntries)}
                  >
                    Export JSON
                  </button>
                </div>
              )}
            </div>
            {visibleRecentEntries.length > 0 ? (
              <ResearchRecentEntryRows
                items={visibleRecentEntries}
                isMarketFollowed={isMarketFollowed}
                onToggleMarket={toggleMarket}
              />
            ) : (
              <div className="status-panel">No recent whale entry markets are available for that timeframe.</div>
            )}
          </section>

          <section className="leaderboard-card">
            <div className="leaderboard-top">
              <p className="leaderboard-kicker">Cross-Platform Markets</p>
              <h2>Whale Concentration by Market</h2>
              <p className="leaderboard-count">Ranked by trusted-whale count, then whale count, then market volume for {timeframe}.</p>
              <p className="leaderboard-count">
                Showing {visibleTopMarkets.length} of top {RESEARCH_CARD_LIMIT} markets
              </p>
              {showExportControls && visibleTopMarkets.length > 0 && (
                <div className="analytics-export-row">
                  <button
                    type="button"
                    className="analytics-export-btn"
                    onClick={() => downloadRowsAsCsv(`market_whale_concentration_${timeframe}.csv`, visibleTopMarkets)}
                  >
                    Export CSV
                  </button>
                  <button
                    type="button"
                    className="analytics-export-btn"
                    onClick={() => downloadRowsAsJson(`market_whale_concentration_${timeframe}.json`, visibleTopMarkets)}
                  >
                    Export JSON
                  </button>
                </div>
              )}
            </div>
            {visibleTopMarkets.length > 0 ? (
              <ResearchMarketRows
                items={visibleTopMarkets}
                isMarketFollowed={isMarketFollowed}
                onToggleMarket={toggleMarket}
              />
            ) : (
              <div className="status-panel">No market concentration rows are available yet.</div>
            )}
          </section>

          <section className="leaderboard-card">
            <div className="leaderboard-top">
              <p className="leaderboard-kicker">Polymarket Whales</p>
              <h2>Whale Entry Behavior</h2>
              <p className="leaderboard-count">Weighted average buy-entry price and entry size for {timeframe}.</p>
              <p className="leaderboard-count">
                Showing {visibleEntryBehavior.length} of top {RESEARCH_CARD_LIMIT} traders
              </p>
              {showExportControls && visibleEntryBehavior.length > 0 && (
                <div className="analytics-export-row">
                  <button
                    type="button"
                    className="analytics-export-btn"
                    onClick={() => downloadRowsAsCsv(`whale_entry_behavior_${timeframe}.csv`, visibleEntryBehavior)}
                  >
                    Export CSV
                  </button>
                  <button
                    type="button"
                    onClick={() => downloadRowsAsJson(`whale_entry_behavior_${timeframe}.json`, visibleEntryBehavior)}
                    className="analytics-export-btn"
                  >
                    Export JSON
                  </button>
                </div>
              )}
            </div>
            {visibleEntryBehavior.length > 0 ? (
              <ResearchEntryRows
                items={visibleEntryBehavior}
                isUserFollowed={isUserFollowed}
                onToggleUser={toggleUser}
              />
            ) : (
              <div className="status-panel">
                {userSearch.trim().length > 0
                  ? "No whale entry rows match that username or wallet."
                  : "No whale entry rows are available yet."}
              </div>
            )}
          </section>
        </div>
      )}
    </section>
  );
}
