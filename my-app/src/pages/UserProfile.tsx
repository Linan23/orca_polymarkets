import { useCallback, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";
import FollowButton from "../components/FollowButton";
import { useApiData } from "../hooks/useApiData";
import { useWatchlist } from "../hooks/useWatchlist";
import {
  type AnalyticsTimeframe,
  type CurrentPositionRow,
  type RecentTradeRow,
  type UserActivityInsights,
  type WhaleProfile,
  fetchUserActivityInsights,
  fetchUserWhaleProfile,
} from "../lib/api";
import { formatContractPrice, formatProbabilityPercent } from "../lib/marketFormatting";
import { formatProfitabilityScorePercent, formatTrustScorePercent } from "../lib/scoreFormatting";
import { deriveUserIdentity, deriveWhaleTierLabel } from "../lib/userIdentity";
import {
  HourlyActivityChart,
  OutcomeBiasBar,
  TagExposureDonut,
} from "../profile/UserActivityVisuals";

type ProfileTab = "overview" | "analytics" | "activity";

function formatCurrency(value: number | null | undefined) {
  if (value === null || value === undefined) return "--";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
}

function formatSignedCurrency(value: number | null | undefined) {
  if (value === null || value === undefined) return "--";
  const formatted = formatCurrency(Math.abs(value));
  if (formatted === "--") return formatted;
  if (value > 0) return `+${formatted}`;
  if (value < 0) return `-${formatted}`;
  return formatted;
}

function formatPercent(value: number | null | undefined) {
  return formatProbabilityPercent(value);
}

function formatCompactNumber(value: number | null | undefined) {
  if (value === null || value === undefined) return "--";
  return new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
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

function pnlClass(value: number | null | undefined) {
  if (!value) return "neutral";
  return value > 0 ? "profit" : "loss";
}

function TimeframeField({
  timeframe,
  onChange,
}: {
  timeframe: AnalyticsTimeframe;
  onChange: (value: AnalyticsTimeframe) => void;
}) {
  return (
    <div className="insight-toolbar">
      <label className="filter-field profile-timeframe-field">
        <span>Timeframe</span>
        <select value={timeframe} onChange={(event) => onChange(event.target.value as AnalyticsTimeframe)}>
          <option value="7d">Last 7 days</option>
          <option value="30d">Last 30 days</option>
          <option value="90d">Last 90 days</option>
          <option value="all">All time</option>
        </select>
      </label>
    </div>
  );
}

function ActivitySummaryStrip({ summary }: { summary: UserActivityInsights["summary"] }) {
  return (
    <div className="insight-strip">
      <div className="insight-metric">
        <span>Trades</span>
        <strong>{summary.trade_count}</strong>
      </div>
      <div className="insight-metric">
        <span>Active Days</span>
        <strong>{summary.active_days}</strong>
      </div>
      <div className="insight-metric">
        <span>Markets</span>
        <strong>{summary.distinct_markets}</strong>
      </div>
      <div className="insight-metric">
        <span>Total Notional</span>
        <strong>{formatCurrency(summary.total_notional)}</strong>
      </div>
    </div>
  );
}

function OverviewTab({
  profile,
  insights,
}: {
  profile: WhaleProfile;
  insights: UserActivityInsights | null;
}) {
  const score = profile.latest_whale_score;
  const resolved = profile.resolved_performance;
  const dashboard = profile.dashboard_profile;
  const { primary, secondary } = deriveUserIdentity(profile);
  const traderTierSource = {
    is_likely_insider: profile.is_likely_insider,
    ...(score ?? {}),
  };

  return (
    <div className="tab-panel">
      <div className="tab-grid two-col">
        <article className="tab-card">
          <h3>Identity</h3>
          <p className="tab-copy">Primary wallet identity and the current account label used throughout the dashboard.</p>
          <div className="info-list">
            <div>
              <span>Display Name</span>
              <strong>{primary}</strong>
            </div>
            <div>
              <span>Wallet</span>
              <strong>{secondary}</strong>
            </div>
            <div>
              <span>Trader Tier</span>
              <strong>
                {deriveWhaleTierLabel(traderTierSource)}
              </strong>
            </div>
            <div>
              <span>Insider Flag</span>
              <strong>{profile.is_likely_insider ? "Flagged" : "Not flagged"}</strong>
            </div>
          </div>
        </article>

        <article className="tab-card">
          <h3>Whale Score</h3>
          <p className="tab-copy">Latest scoring batch, trust posture, and sample-trade coverage.</p>
          <div className="info-list">
            <div>
              <span>Trust Score</span>
              <strong>{formatTrustScorePercent(score?.trust_score)}</strong>
            </div>
            <div>
              <span>Profitability Score</span>
              <strong>{formatProfitabilityScorePercent(score?.profitability_score)}</strong>
            </div>
            <div>
              <span>Sample Trades</span>
              <strong>{score?.sample_trade_count ?? 0}</strong>
            </div>
            <div>
              <span>Scoring Version</span>
              <strong>{score?.scoring_version ?? "--"}</strong>
            </div>
          </div>
        </article>

        <article className="tab-card">
          <h3>Resolved Performance</h3>
          <p className="tab-copy">How the trader has performed on resolved markets observed by the whale-scoring pipeline.</p>
          <div className="info-list">
            <div>
              <span>Resolved Markets</span>
              <strong>{resolved?.resolved_market_count ?? 0}</strong>
            </div>
            <div>
              <span>Wins</span>
              <strong>{resolved?.winning_market_count ?? 0}</strong>
            </div>
            <div>
              <span>Win Rate</span>
              <strong>{formatPercent(resolved?.win_rate)}</strong>
            </div>
            <div>
              <span>Realized P&amp;L</span>
              <strong>{formatCurrency(resolved?.realized_pnl)}</strong>
            </div>
          </div>
        </article>

        <article className="tab-card">
          <h3>Dashboard Footprint</h3>
          <p className="tab-copy">Latest dashboard snapshot totals plus activity depth for the selected trader.</p>
          <div className="info-list">
            <div>
              <span>Total Volume</span>
              <strong>{formatCurrency(dashboard?.total_volume)}</strong>
            </div>
            <div>
              <span>Total Shares</span>
              <strong>{formatCompactNumber(dashboard?.total_shares)}</strong>
            </div>
            <div>
              <span>Recent Trade Window</span>
              <strong>{insights?.timeframe ?? "30d"}</strong>
            </div>
            <div>
              <span>Latest Trade</span>
              <strong>{formatDateTime(insights?.summary.latest_trade_time)}</strong>
            </div>
          </div>
        </article>
      </div>
    </div>
  );
}

function RecentTradesTable({ trades }: { trades: RecentTradeRow[] }) {
  if (trades.length === 0) {
    return <div className="status-panel">No recent trades in this timeframe.</div>;
  }

  return (
    <div className="table-wrap">
      <table className="data-table">
        <thead>
          <tr>
            <th>Time</th>
            <th>Market</th>
            <th>Outcome</th>
            <th>Price Paid</th>
            <th>Shares</th>
            <th>Notional</th>
          </tr>
        </thead>
        <tbody>
          {trades.map((trade) => (
            <tr key={trade.transaction_id}>
              <td>{formatDateTime(trade.transaction_time)}</td>
              <td>
                <Link to={`/markets/${trade.market_slug}`} className="table-link">
                  {trade.question}
                </Link>
              </td>
              <td>{trade.outcome_label ?? "--"}</td>
              <td>{formatContractPrice(trade.price)}</td>
              <td>{formatCompactNumber(trade.shares)}</td>
              <td>{formatCurrency(trade.notional_value)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function CurrentPositionsTable({ positions }: { positions: CurrentPositionRow[] }) {
  if (positions.length === 0) {
    return <div className="status-panel">No current positions are available for this trader.</div>;
  }

  return (
    <div className="table-wrap">
      <table className="data-table">
        <thead>
          <tr>
            <th>Market</th>
            <th>Snapshot</th>
            <th>Size</th>
            <th>Avg Entry Price</th>
            <th>Current Price</th>
            <th>Value</th>
            <th>Unrealized P&amp;L</th>
          </tr>
        </thead>
        <tbody>
          {positions.map((position) => (
            <tr key={position.position_snapshot_id}>
              <td>
                <Link to={`/markets/${position.market_slug}`} className="table-link">
                  {position.question}
                </Link>
              </td>
              <td>{formatDateTime(position.snapshot_time)}</td>
              <td>{formatCompactNumber(position.position_size)}</td>
              <td>{formatContractPrice(position.avg_entry_price)}</td>
              <td>{formatContractPrice(position.current_mark_price)}</td>
              <td>{formatCurrency(position.market_value)}</td>
              <td>{formatSignedCurrency(position.unrealized_pnl)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function UserProfile() {
  const { userId } = useParams();
  const { isAuthenticated, preferences, updatePreferences } = useAuth();
  const parsedUserId = Number(userId);
  const invalidUser = Number.isNaN(parsedUserId);
  const [activeTab, setActiveTab] = useState<ProfileTab>("overview");
  const [publicTimeframe, setPublicTimeframe] = useState<AnalyticsTimeframe>("30d");
  const { isUserFollowed, toggleUser } = useWatchlist();
  const timeframe = isAuthenticated ? preferences.user_profile.analytics_timeframe : publicTimeframe;
  const handleTimeframeChange = useCallback(
    (value: AnalyticsTimeframe) => {
      if (isAuthenticated) {
        if (preferences.user_profile.analytics_timeframe === value) return;
        void updatePreferences({
          user_profile: {
            analytics_timeframe: value,
          },
        });
        return;
      }
      setPublicTimeframe(value);
    },
    [isAuthenticated, preferences.user_profile.analytics_timeframe, updatePreferences],
  );

  const loadProfile = useCallback(async () => {
    if (invalidUser) {
      throw new Error("Invalid user id");
    }
    return fetchUserWhaleProfile(parsedUserId);
  }, [invalidUser, parsedUserId]);
  const loadInsights = useCallback(async () => {
    if (invalidUser) {
      throw new Error("Invalid user id");
    }
    return fetchUserActivityInsights(parsedUserId, timeframe);
  }, [invalidUser, parsedUserId, timeframe]);

  const { data, loading, error } = useApiData(loadProfile);
  const {
    data: insights,
    loading: insightsLoading,
    refreshing: insightsRefreshing,
    error: insightsError,
  } = useApiData(loadInsights, { keepPreviousData: true });

  if (invalidUser) {
    return (
      <div className="page">
        <section className="hero market-hero user-hero">
          <p className="eyebrow">Trader Profile</p>
          <h1 className="market-title">Invalid user id</h1>
          <p className="hero-text">User profile routes require a numeric `user_id`.</p>
        </section>
      </div>
    );
  }

  const score = data?.latest_whale_score;
  const resolved = data?.resolved_performance;
  const dashboard = data?.dashboard_profile;
  const { primary, secondary } = deriveUserIdentity(data ?? {});
  const traderTierSource = {
    is_likely_insider: data?.is_likely_insider,
    ...(score ?? {}),
  };

  return (
    <div className="page user-profile-page">
      <header className="hero market-hero user-hero">
        <div className="hero-top-row">
          <div className="profile-title-stack">
            <p className="eyebrow">Trader Profile</p>
            <h1 className="market-title">{primary}</h1>
            <p className="profile-secondary-line">{secondary}</p>
            <p className="hero-text">
              Trader intelligence view with whale scoring, market-preference analytics, and recent position history.
            </p>
          </div>

          <div className="hero-action-stack">
            <FollowButton
              isFollowing={isUserFollowed(parsedUserId)}
              onToggle={() => toggleUser(parsedUserId)}
            />
          </div>
        </div>

        <div className="hero-actions">
          <Link to="/leaderboard" className="table-link back-link">
            ← Back to leaderboard
          </Link>

          {data && (
            <div className="hero-pills">
              <span className="hero-pill">User #{data.user_id}</span>
              <span className="hero-pill">{deriveWhaleTierLabel(traderTierSource)}</span>
              <span className="hero-pill">Trades {score?.sample_trade_count ?? 0}</span>
            </div>
          )}
        </div>
      </header>

      {loading && <section className="status-panel">Loading trader intelligence...</section>}
      {error && <section className="status-panel error-panel">{error}</section>}

      {!loading && !error && data && (
        <>
          <section className="trader-overview-box">
            <div className="trader-overview-main">
              <div className="trader-pnl-row">
                <p className={`trader-pnl ${pnlClass(resolved?.realized_pnl)}`}>
                  {formatSignedCurrency(resolved?.realized_pnl)}
                </p>
                <div className="hero-pills">
                  <span className="hero-pill">ROI {formatPercent(resolved?.realized_roi)}</span>
                  <span className="hero-pill">Win Rate {formatPercent(resolved?.win_rate)}</span>
                  <span className="hero-pill">Resolved {resolved?.resolved_market_count ?? 0}</span>
                </div>
              </div>

              <div className="overview-inline-metrics">
                <div className="inline-metric">
                  <span>Trust Score</span>
                  <strong>{formatTrustScorePercent(score?.trust_score)}</strong>
                </div>
                <div className="inline-metric">
                  <span>Profitability</span>
                  <strong>{formatProfitabilityScorePercent(score?.profitability_score)}</strong>
                </div>
                <div className="inline-metric">
                  <span>Resolved Markets</span>
                  <strong>{resolved?.resolved_market_count ?? 0}</strong>
                </div>
                <div className="inline-metric">
                  <span>Win Rate</span>
                  <strong>{formatPercent(resolved?.win_rate)}</strong>
                </div>
              </div>

              <div className="overview-bottom-grid">
                <div className="overview-info-block">
                  <span className="overview-label">Wallet Identity</span>
                  <strong className="wallet-identity-text">{secondary}</strong>
                  <small>{primary}</small>
                </div>
                <div className="overview-info-block">
                  <span className="overview-label">Dashboard Volume</span>
                  <strong>{formatCurrency(dashboard?.total_volume)}</strong>
                  <small>Shares {formatCompactNumber(dashboard?.total_shares)}</small>
                </div>
                <div className="overview-info-block">
                  <span className="overview-label">Insider Flag</span>
                  <strong>{data.is_likely_insider ? "Flagged" : "Not flagged"}</strong>
                  <small>Excluded markets {resolved?.excluded_market_count ?? 0}</small>
                </div>
                <div className="overview-info-block">
                  <span className="overview-label">Latest Activity</span>
                  <strong>{formatDateTime(insights?.summary.latest_trade_time)}</strong>
                  <small>{insights?.summary.trade_count ?? 0} trades in the selected timeframe</small>
                </div>
              </div>
            </div>

            <div className="trader-overview-side">
              <div className="side-metric-card accent">
                <span>Trader Tier</span>
                <strong>{deriveWhaleTierLabel(traderTierSource)}</strong>
              </div>
              <div className="side-metric-card surface">
                <span>Active Markets</span>
                <strong>{insights?.summary.distinct_markets ?? 0}</strong>
              </div>
            </div>
          </section>

          <section className="trader-tabs-box">
            <div className="user-tabs">
              <button
                type="button"
                className={`user-tab ${activeTab === "overview" ? "active" : ""}`}
                onClick={() => setActiveTab("overview")}
              >
                Overview
              </button>
              <button
                type="button"
                className={`user-tab ${activeTab === "analytics" ? "active" : ""}`}
                onClick={() => setActiveTab("analytics")}
              >
                Analytics
              </button>
              <button
                type="button"
                className={`user-tab ${activeTab === "activity" ? "active" : ""}`}
                onClick={() => setActiveTab("activity")}
              >
                Past Activity
              </button>
            </div>

            {activeTab === "overview" && <OverviewTab profile={data} insights={insights} />}

            {activeTab === "analytics" && (
              <div className="tab-panel">
                <TimeframeField timeframe={timeframe} onChange={handleTimeframeChange} />
                {insightsRefreshing && <p className="tab-copy">Updating trader analytics...</p>}
                {insightsLoading && <div className="status-panel">Loading trader analytics...</div>}
                {insightsError && <div className="status-panel error-panel">{insightsError}</div>}
                {!insightsLoading && !insightsError && insights && (
                  <>
                    <ActivitySummaryStrip summary={insights.summary} />
                    <div className="tab-grid two-col">
                      <article className="tab-card chart-card">
                        <h3>Market Mix</h3>
                        <p className="tab-copy">Activity split by market-tag exposure, weighted by traded notional.</p>
                        <TagExposureDonut slices={insights.tag_exposure} />
                      </article>

                      <article className="tab-card chart-card">
                        <h3>Outcome Bias</h3>
                        <p className="tab-copy">YES/NO/Other bias based on trade count across the selected timeframe.</p>
                        <OutcomeBiasBar items={insights.outcome_bias} />
                      </article>

                      <article className="tab-card chart-card chart-card-full">
                        <h3>Trading Hours</h3>
                        <p className="tab-copy">When this trader is most active, bucketed by hour of day in UTC.</p>
                        <HourlyActivityChart buckets={insights.hourly_activity_utc} />
                      </article>
                    </div>
                  </>
                )}
              </div>
            )}

            {activeTab === "activity" && (
              <div className="tab-panel">
                <TimeframeField timeframe={timeframe} onChange={handleTimeframeChange} />
                {insightsRefreshing && <p className="tab-copy">Updating past activity...</p>}
                {insightsLoading && <div className="status-panel">Loading past activity...</div>}
                {insightsError && <div className="status-panel error-panel">{insightsError}</div>}
                {!insightsLoading && !insightsError && insights && (
                  <>
                    <ActivitySummaryStrip summary={insights.summary} />
                    <div className="tab-grid">
                      <article className="tab-card table-card">
                        <h3>Recent Trades</h3>
                        <p className="tab-copy">Newest trades first, showing the outcome label, pricing, and trade size.</p>
                        <RecentTradesTable trades={insights.recent_trades} />
                      </article>

                      <article className="tab-card table-card">
                        <h3>Current Positions</h3>
                        <p className="tab-copy">Latest known position snapshot per market, independent of the recent-trade timeframe.</p>
                        <CurrentPositionsTable positions={insights.current_positions} />
                      </article>
                    </div>
                  </>
                )}
              </div>
            )}
          </section>
        </>
      )}
    </div>
  );
}
