import { useCallback, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { useApiData } from "../hooks/useApiData";
import { useWatchlist } from "../hooks/useWatchlist";
import {
  type FollowedTraderFocusRow,
  type FollowingClosedMarketRow,
  type FollowingDashboard,
  type FollowingMarketCard,
  type FollowingInflowRow,
  type FollowingMarketFocusRecentRow,
  type FollowingUserCard,
  fetchFollowingDashboard,
} from "../lib/api";
import { deriveUserIdentity } from "../lib/userIdentity";
import TopNavbar from "../homepage/TopNavbar";

const FOLLOWING_DONUT_COLORS = [
  "#ef4444",
  "#f97316",
  "#eab308",
  "#84cc16",
  "#22c55e",
  "#14b8a6",
  "#06b6d4",
  "#3b82f6",
  "#8b5cf6",
  "#d946ef",
];

function focusMarketColor(marketSlug: string) {
  let hash = 0;
  for (let index = 0; index < marketSlug.length; index += 1) {
    hash = (hash * 31 + marketSlug.charCodeAt(index)) >>> 0;
  }
  return FOLLOWING_DONUT_COLORS[hash % FOLLOWING_DONUT_COLORS.length];
}

function formatCurrency(value: number | null | undefined) {
  if (value === null || value === undefined) return "--";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
}

function formatPercent(value: number | null | undefined) {
  if (value === null || value === undefined) return "--";
  return `${Math.round(value * 100)}%`;
}

function formatDateTime(value: string | null | undefined) {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en-US", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

function sourceLabel(value: FollowedTraderFocusRow["focus_source"]) {
  if (value === "position") return "Current holding";
  if (value === "recent_flow") return "Recent buying (30d)";
  return "Lifetime buying";
}

function compactMarketLabel(value: string | null | undefined, maxLength = 38) {
  if (!value) return "No main market";
  if (value.length <= maxLength) return value;
  return `${value.slice(0, maxLength - 1)}…`;
}

function statusPillClass(status: "Open" | "Closed") {
  return status === "Closed" ? "is-closed" : "is-open";
}

function buildFallbackFollowingUser(userId: number): FollowingUserCard {
  return {
    user_id: userId,
    external_user_ref: `User ${userId}`,
    wallet_address: null,
    preferred_username: null,
    display_label: null,
    is_likely_insider: false,
    latest_whale_score: null,
  };
}

function buildFallbackFollowingMarket(marketSlug: string): FollowingMarketCard {
  return {
    market_slug: marketSlug,
    question: marketSlug,
    price: null,
    whale_count: 0,
    trusted_whale_count: 0,
    market_status_label: "Open",
  };
}

function emptyFollowingDashboard(): FollowingDashboard {
  return {
    overview: {
      summary: {
        followed_trader_count: 0,
        followed_market_count: 0,
        active_followed_traders_24h: 0,
        markets_entered_24h: 0,
        recent_closed_followed_market_count: 0,
      },
      inflow_24h: [],
      market_focus_recent: [],
      recent_closed_markets: [],
      trader_focus: [],
    },
    users: [],
    markets: [],
  };
}

type TraderFocusMarketSlice = {
  market_slug: string;
  question: string;
  market_status_label: "Open" | "Closed";
  latest_activity_time: string | null;
  share_percentage: number;
  trader_count: number;
};

function buildTraderFocusMarketSlices(items: FollowedTraderFocusRow[]): TraderFocusMarketSlice[] {
  const activeItems = items.filter((item) => item.share_percentage > 0);
  const byMarket = new Map<string, TraderFocusMarketSlice>();

  activeItems.forEach((item) => {
    const current = byMarket.get(item.main_market_slug);
    if (!current) {
      byMarket.set(item.main_market_slug, {
        market_slug: item.main_market_slug,
        question: item.main_market_question,
        market_status_label: item.market_status_label,
        latest_activity_time: item.latest_activity_time,
        share_percentage: item.share_percentage,
        trader_count: 1,
      });
      return;
    }

    const nextLatestTime =
      current.latest_activity_time && item.latest_activity_time
        ? current.latest_activity_time > item.latest_activity_time
          ? current.latest_activity_time
          : item.latest_activity_time
        : current.latest_activity_time ?? item.latest_activity_time;

    byMarket.set(item.main_market_slug, {
      ...current,
      share_percentage: current.share_percentage + item.share_percentage,
      trader_count: current.trader_count + 1,
      latest_activity_time: nextLatestTime,
    });
  });

  return [...byMarket.values()].sort((left, right) => right.share_percentage - left.share_percentage);
}

function buildRecentMarketFocusRows(items: FollowedTraderFocusRow[]): FollowingMarketFocusRecentRow[] {
  const byMarket = new Map<string, FollowingMarketFocusRecentRow>();

  items.forEach((item) => {
    const current = byMarket.get(item.main_market_slug);
    if (!current) {
      byMarket.set(item.main_market_slug, {
        market_slug: item.main_market_slug,
        question: item.main_market_question,
        trader_count: 1,
        total_focus_value: item.focus_value,
        latest_activity_time: item.latest_activity_time,
        market_status_label: item.market_status_label,
      });
      return;
    }

    const nextLatestTime =
      current.latest_activity_time && item.latest_activity_time
        ? current.latest_activity_time > item.latest_activity_time
          ? current.latest_activity_time
          : item.latest_activity_time
        : current.latest_activity_time ?? item.latest_activity_time;

    byMarket.set(item.main_market_slug, {
      ...current,
      trader_count: current.trader_count + 1,
      total_focus_value: current.total_focus_value + item.focus_value,
      latest_activity_time: nextLatestTime,
      market_status_label:
        current.market_status_label === "Open" || item.market_status_label === "Open"
          ? "Open"
          : "Closed",
    });
  });

  return [...byMarket.values()]
    .sort((left, right) => {
      const leftTime = left.latest_activity_time ?? "";
      const rightTime = right.latest_activity_time ?? "";
      if (rightTime !== leftTime) {
        return rightTime.localeCompare(leftTime);
      }
      if (right.total_focus_value !== left.total_focus_value) {
        return right.total_focus_value - left.total_focus_value;
      }
      return right.trader_count - left.trader_count;
    })
    .slice(0, 5);
}

function OverviewMetricCard({
  label,
  value,
  accent,
}: {
  label: string;
  value: string | number;
  accent?: boolean;
}) {
  return (
    <article className={`following-metric-card ${accent ? "accent" : ""}`}>
      <p>{label}</p>
      <strong>{value}</strong>
    </article>
  );
}

function TraderFocusDonut({ items }: { items: FollowedTraderFocusRow[] }) {
  const marketSlices = buildTraderFocusMarketSlices(items);
  if (marketSlices.length === 0) {
    return <div className="empty-chart-state">No current positions or buy-flow history for followed traders.</div>;
  }

  const segments = marketSlices.reduce<Array<TraderFocusMarketSlice & { strokeDashoffset: number }>>((rows, item) => {
    const currentOffset = rows.reduce((sum, row) => sum + row.share_percentage * 100, 0);
    rows.push({
      ...item,
      strokeDashoffset: -currentOffset,
    });
    return rows;
  }, []);

  return (
    <div className="donut-layout following-donut-layout">
      <div className="donut-shell">
        <svg viewBox="0 0 36 36" className="donut-chart" aria-label="Followed trader main markets donut chart">
          <circle cx="18" cy="18" r="15.915" className="donut-track" />
          <g transform="rotate(-90 18 18)">
            {segments.map((item) => {
              const dash = item.share_percentage * 100;
              return (
                <circle
                  key={item.market_slug}
                  cx="18"
                  cy="18"
                  r="15.915"
                  className="donut-segment"
                  stroke={focusMarketColor(item.market_slug)}
                  strokeDasharray={`${dash} ${100 - dash}`}
                  strokeDashoffset={item.strokeDashoffset}
                />
              );
            })}
          </g>
        </svg>
        <div className="donut-center following-donut-center">
          <div className="following-donut-center-label">
            <strong>{marketSlices.length}</strong>
            <span>Main Markets</span>
          </div>
        </div>
      </div>

      <div className="watchlist-list following-overview-card-list">
        {marketSlices.map((item) => {
          return (
            <article key={item.market_slug} className="watchlist-card overview-watchlist-card following-focus-card">
              <span
                className="chart-swatch overview-card-swatch"
                style={{ backgroundColor: focusMarketColor(item.market_slug) }}
              />
              <div className="watchlist-card-main following-focus-main">
                <p className="watchlist-card-kicker">Main Market</p>
                <Link to={`/markets/${item.market_slug}`} className="watchlist-card-title following-legend-link following-row-title">
                  {item.question}
                </Link>
                <p className="watchlist-card-subtitle">{item.market_slug}</p>
                <div className="leaderboard-meta">
                  <span className="meta-pill">{item.trader_count} traders</span>
                  <span className="meta-pill">{formatPercent(item.share_percentage)}</span>
                  <span className="meta-pill">{formatDateTime(item.latest_activity_time)}</span>
                </div>
                <div className="watchlist-card-tags">
                  <span className={`following-pill following-status-pill ${statusPillClass(item.market_status_label)}`}>
                    {item.market_status_label}
                  </span>
                </div>
              </div>
            </article>
          );
        })}
      </div>
    </div>
  );
}

function InflowRows({ items }: { items: FollowingInflowRow[] }) {
  if (items.length === 0) {
    return <div className="status-panel">No followed-trader buy entries landed in the last 24 hours.</div>;
  }

  return (
    <div className="following-row-list">
      {items.map((item, index) => (
        <article key={item.market_slug} className="following-data-row">
          <div className="following-rank">{index + 1}</div>
          <div className="following-row-main">
            <Link to={`/markets/${item.market_slug}`} className="watchlist-card-title following-row-title">
              {item.question}
            </Link>
            <p className="watchlist-card-subtitle following-row-subtitle">
              {item.distinct_trader_count} traders · {formatCurrency(item.total_notional)} · {formatDateTime(item.latest_trade_time)}
            </p>
          </div>
          <div className="following-pill-row">
            <span className={`following-pill following-status-pill ${statusPillClass(item.market_status_label)}`}>
              {item.market_status_label}
            </span>
          </div>
        </article>
      ))}
    </div>
  );
}

function MarketFocusRows({ items }: { items: FollowingMarketFocusRecentRow[] }) {
  if (items.length === 0) {
    return <div className="status-panel">Follow traders to see their main markets right now.</div>;
  }

  return (
    <div className="watchlist-list following-overview-card-list">
      {items.map((item) => (
        <article key={item.market_slug} className="watchlist-card overview-watchlist-card">
          <div className="watchlist-card-main">
            <p className="watchlist-card-kicker">Main Market</p>
            <Link to={`/markets/${item.market_slug}`} className="watchlist-card-title following-row-title">
              {item.question}
            </Link>
            <p className="watchlist-card-subtitle">{item.market_slug}</p>
            <div className="leaderboard-meta">
              <span className="meta-pill">{item.trader_count} traders</span>
              <span className="meta-pill">{formatCurrency(item.total_focus_value)}</span>
              <span className="meta-pill">{formatDateTime(item.latest_activity_time)}</span>
            </div>
            <div className="watchlist-card-tags">
              <span className={`following-pill following-status-pill ${statusPillClass(item.market_status_label)}`}>
                {item.market_status_label}
              </span>
            </div>
          </div>
        </article>
      ))}
    </div>
  );
}

function ClosedMarketRows({ items }: { items: FollowingClosedMarketRow[] }) {
  if (items.length === 0) {
    return <div className="status-panel">No followed markets have closed recently.</div>;
  }

  return (
    <div className="watchlist-list following-overview-card-list">
      {items.map((item) => (
        <article key={item.market_slug} className="watchlist-card overview-watchlist-card">
          <div className="watchlist-card-main">
            <p className="watchlist-card-kicker">Closed Market</p>
            <Link to={`/markets/${item.market_slug}`} className="watchlist-card-title following-row-title">
              {item.question}
            </Link>
            <p className="watchlist-card-subtitle">{item.market_slug}</p>
            <div className="leaderboard-meta">
              <span className="meta-pill">{formatDateTime(item.closed_time)}</span>
            </div>
            <div className="watchlist-card-tags">
              <span className="following-pill following-result-pill">{item.result_label}</span>
              <span className="following-pill following-status-pill is-closed">{item.market_status_label}</span>
            </div>
          </div>
        </article>
      ))}
    </div>
  );
}

function UserWatchlistRows({
  items,
  traderFocusByUser,
  onRemoveUser,
}: {
  items: FollowingUserCard[];
  traderFocusByUser: Map<number, FollowedTraderFocusRow>;
  onRemoveUser: (userId: number) => void;
}) {
  if (items.length === 0) {
    return <div className="status-panel">No followed users yet.</div>;
  }

  return (
    <div className="watchlist-list">
      {items.map((user) => {
        const { primary: title, secondary: subtitle } = deriveUserIdentity(user);
        const focus = traderFocusByUser.get(user.user_id);
        return (
          <article key={user.user_id} className="watchlist-card">
            <div className="watchlist-card-main">
              <p className="watchlist-card-kicker">Trader</p>
              <Link to={`/users/${user.user_id}`} className="watchlist-card-title">
                {title}
              </Link>
              <p className="watchlist-card-subtitle">{subtitle}</p>
              <div className="leaderboard-meta">
                <span className="meta-pill">Trust {user.latest_whale_score?.trust_score?.toFixed(3) ?? "--"}</span>
                <span className="meta-pill">Trades {user.latest_whale_score?.sample_trade_count ?? 0}</span>
                {focus && <span className="meta-pill">{sourceLabel(focus.focus_source)}</span>}
              </div>
              <div className="watchlist-card-tags">
                {focus ? (
                  <>
                    <Link
                      to={`/markets/${focus.main_market_slug}`}
                      className="following-pill following-market-pill"
                      title={focus.main_market_question}
                    >
                      {compactMarketLabel(focus.main_market_question)}
                    </Link>
                    <span className={`following-pill following-status-pill ${statusPillClass(focus.market_status_label)}`}>
                      {focus.market_status_label}
                    </span>
                  </>
                ) : (
                  <span className="following-pill following-market-pill muted">No main market yet</span>
                )}
              </div>
            </div>
            <button type="button" className="watchlist-remove" onClick={() => onRemoveUser(user.user_id)}>
              Unfollow
            </button>
          </article>
        );
      })}
    </div>
  );
}

function MarketWatchlistRows({
  items,
  closedMarketBySlug,
  onRemoveMarket,
}: {
  items: FollowingMarketCard[];
  closedMarketBySlug: Map<string, FollowingClosedMarketRow>;
  onRemoveMarket: (marketSlug: string) => void;
}) {
  if (items.length === 0) {
    return <div className="status-panel">No followed markets yet.</div>;
  }

  return (
    <div className="watchlist-list">
      {items.map((market) => {
        const closedMarket = closedMarketBySlug.get(market.market_slug);
        return (
          <article key={market.market_slug} className="watchlist-card">
            <div className="watchlist-card-main">
              <p className="watchlist-card-kicker">Market</p>
              <Link to={`/markets/${market.market_slug}`} className="watchlist-card-title">
                {market.question}
              </Link>
              <p className="watchlist-card-subtitle">{market.market_slug}</p>
              <div className="leaderboard-meta">
                <span className="meta-pill">Price {market.price === null ? "--" : formatPercent(market.price)}</span>
                <span className="meta-pill">Whale Traders {market.whale_count}</span>
                <span className="meta-pill">Trusted Whales {market.trusted_whale_count}</span>
              </div>
              <div className="watchlist-card-tags">
                <span className={`following-pill following-status-pill ${statusPillClass(market.market_status_label)}`}>
                  {market.market_status_label}
                </span>
                {closedMarket && (
                  <span className="following-pill following-result-pill">{closedMarket.result_label}</span>
                )}
              </div>
            </div>
            <button type="button" className="watchlist-remove" onClick={() => onRemoveMarket(market.market_slug)}>
              Unfollow
            </button>
          </article>
        );
      })}
    </div>
  );
}

type FollowingDetailTab = "positioning" | "results" | "users" | "markets";

export default function FollowingPage() {
  const [activeDetailTab, setActiveDetailTab] = useState<FollowingDetailTab>("positioning");
  const { watchlist, removeUser, removeMarket } = useWatchlist();

  const loadFollowing = useCallback(async () => {
    if (watchlist.users.length === 0 && watchlist.markets.length === 0) {
      return emptyFollowingDashboard();
    }

    const dashboard = await fetchFollowingDashboard({
      user_ids: watchlist.users,
      market_slugs: watchlist.markets,
    });
    const usersById = new Map(dashboard.users.map((user) => [user.user_id, user]));
    const marketsBySlug = new Map(dashboard.markets.map((market) => [market.market_slug.toLowerCase(), market]));

    return {
      overview: dashboard.overview,
      users: watchlist.users.map((userId) => usersById.get(userId) ?? buildFallbackFollowingUser(userId)),
      markets: watchlist.markets.map(
        (marketSlug) => marketsBySlug.get(marketSlug.toLowerCase()) ?? buildFallbackFollowingMarket(marketSlug),
      ),
    } satisfies FollowingDashboard;
  }, [watchlist.markets, watchlist.users]);

  const { data, loading, error } = useApiData(loadFollowing, { keepPreviousData: true });
  const visibleTraderFocus = useMemo(
    () => (data?.overview.trader_focus ?? []).filter((item) => watchlist.users.includes(item.user_id)),
    [data?.overview.trader_focus, watchlist.users],
  );
  const visibleRecentClosedMarkets = useMemo(
    () => (data?.overview.recent_closed_markets ?? []).filter((item) => watchlist.markets.includes(item.market_slug)),
    [data?.overview.recent_closed_markets, watchlist.markets],
  );
  const visibleUsers = useMemo(
    () => (data?.users ?? []).filter((user) => watchlist.users.includes(user.user_id)),
    [data?.users, watchlist.users],
  );
  const visibleMarkets = useMemo(
    () => (data?.markets ?? []).filter((market) => watchlist.markets.includes(market.market_slug)),
    [data?.markets, watchlist.markets],
  );
  const visibleMarketFocusRecent = useMemo(
    () => buildRecentMarketFocusRows(visibleTraderFocus),
    [visibleTraderFocus],
  );
  const traderFocusByUser = useMemo(
    () => new Map(visibleTraderFocus.map((item) => [item.user_id, item])),
    [visibleTraderFocus],
  );
  const closedMarketBySlug = useMemo(
    () => new Map(visibleRecentClosedMarkets.map((item) => [item.market_slug, item])),
    [visibleRecentClosedMarkets],
  );
  const traderFocusMarketCount = useMemo(
    () => buildTraderFocusMarketSlices(visibleTraderFocus).length,
    [visibleTraderFocus],
  );
  const activeDetailMeta = useMemo(() => {
    switch (activeDetailTab) {
      case "results":
        return {
          kicker: "Results",
          title: "Recent Closed Market Results",
          description: `Latest ${Math.min(5, visibleRecentClosedMarkets.length)} followed markets with close time and resolved outcome.`,
        };
      case "users":
        return {
          kicker: "Watchlist",
          title: "Followed Traders",
          description: `${visibleUsers.length} followed traders with trust, activity, and main-market context.`,
        };
      case "markets":
        return {
          kicker: "Watchlist",
          title: "Followed Markets",
          description: `${visibleMarkets.length} followed markets with price, whale counts, and status tags.`,
        };
      default:
        return {
          kicker: "Trader Positioning",
          title: "Main Markets for Your Followed Traders",
          description: `${traderFocusMarketCount} main markets across your followed traders, sized by combined main-market share.`,
        };
    }
  }, [
    activeDetailTab,
    traderFocusMarketCount,
    visibleMarkets.length,
    visibleRecentClosedMarkets.length,
    visibleUsers.length,
  ]);

  return (
    <div className="page">
      <TopNavbar />

      <section className="hero">
        <p className="eyebrow">Orca Polymarkets</p>
        <h1>Following</h1>
        <p className="hero-text">
          Watchlist intelligence for the traders and markets you want to keep close.
        </p>
      </section>

      {loading && !data && <section className="analytics-section"><div className="status-panel">Loading watchlist...</div></section>}
      {error && !data && <section className="analytics-section"><div className="status-panel error-panel">{error}</div></section>}

      {data && (
        <>
          <section className="analytics-section following-overview-section">
            <div className="summary-section-header">
              <p className="leaderboard-kicker">Info on Your Watchlist</p>
              <h2>Overview</h2>
              <p className="summary-card-subtext">
                Followed traders drive the flow and main-market views. Followed markets drive the recent-closed result tracker.
              </p>
            </div>

            <div className="following-metric-grid">
              <OverviewMetricCard label="Followed Traders" value={data.overview.summary.followed_trader_count} accent />
              <OverviewMetricCard label="Followed Markets" value={data.overview.summary.followed_market_count} />
              <OverviewMetricCard label="Active Traders (24h)" value={data.overview.summary.active_followed_traders_24h} />
              <OverviewMetricCard label="Markets Entered (24h)" value={data.overview.summary.markets_entered_24h} />
              <OverviewMetricCard label="Closed Followed Markets" value={data.overview.summary.recent_closed_followed_market_count} />
            </div>

            <div className="following-panel-grid">
              <section className="leaderboard-card following-panel">
                <div className="leaderboard-top">
                  <p className="leaderboard-kicker">Flow</p>
                  <h2>Whales Entering Markets in the Last 24 Hours</h2>
                  <p className="leaderboard-count">Top 5 markets by buy-side notional from your followed traders.</p>
                </div>
                <InflowRows items={data.overview.inflow_24h} />
              </section>

              <section className="leaderboard-card following-panel following-tight-panel">
                <div className="leaderboard-top">
                  <p className="leaderboard-kicker">Main Markets</p>
                  <h2>Most Recent Main Markets</h2>
                  <p className="leaderboard-count">Top 5 markets ranked by the latest followed-trader main-market activity.</p>
                </div>
                <MarketFocusRows items={visibleMarketFocusRecent} />
              </section>
            </div>
            <section className="leaderboard-card following-panel following-tabbed-card">
              <div className="leaderboard-top following-tabbed-top">
                <div className="following-tabbed-summary">
                  <p className="leaderboard-kicker">{activeDetailMeta.kicker}</p>
                  <h2>{activeDetailMeta.title}</h2>
                  <p className="leaderboard-count">{activeDetailMeta.description}</p>
                </div>

                <div className="leaderboard-toggle following-tab-toggle" role="tablist" aria-label="Following detail views">
                  <button
                    type="button"
                    role="tab"
                    aria-selected={activeDetailTab === "positioning"}
                    className={`leaderboard-toggle-btn ${activeDetailTab === "positioning" ? "active" : ""}`}
                    onClick={() => setActiveDetailTab("positioning")}
                  >
                    Trader Positioning
                  </button>
                  <button
                    type="button"
                    role="tab"
                    aria-selected={activeDetailTab === "results"}
                    className={`leaderboard-toggle-btn ${activeDetailTab === "results" ? "active" : ""}`}
                    onClick={() => setActiveDetailTab("results")}
                  >
                    Results
                  </button>
                  <button
                    type="button"
                    role="tab"
                    aria-selected={activeDetailTab === "users"}
                    className={`leaderboard-toggle-btn ${activeDetailTab === "users" ? "active" : ""}`}
                    onClick={() => setActiveDetailTab("users")}
                  >
                    Users
                  </button>
                  <button
                    type="button"
                    role="tab"
                    aria-selected={activeDetailTab === "markets"}
                    className={`leaderboard-toggle-btn ${activeDetailTab === "markets" ? "active" : ""}`}
                    onClick={() => setActiveDetailTab("markets")}
                  >
                    Markets
                  </button>
                </div>
              </div>

              <div className="following-tab-panel">
                {activeDetailTab === "positioning" && <TraderFocusDonut items={visibleTraderFocus} />}
                {activeDetailTab === "results" && <ClosedMarketRows items={visibleRecentClosedMarkets} />}
                {activeDetailTab === "users" && (
                  <UserWatchlistRows
                    items={visibleUsers}
                    traderFocusByUser={traderFocusByUser}
                    onRemoveUser={removeUser}
                  />
                )}
                {activeDetailTab === "markets" && (
                  <MarketWatchlistRows
                    items={visibleMarkets}
                    closedMarketBySlug={closedMarketBySlug}
                    onRemoveMarket={removeMarket}
                  />
                )}
              </div>
            </section>
          </section>
        </>
      )}
    </div>
  );
}
