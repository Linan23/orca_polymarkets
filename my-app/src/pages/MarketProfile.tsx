import { useCallback, useState } from "react";
import { Link, useParams } from "react-router-dom";
import FollowButton from "../components/FollowButton";
import { useWatchlist } from "../hooks/useWatchlist";
import {
  fetchMarketProfile,
  type MarketProfileMlPredictionCase,
  type MarketProfileMlPredictionTrend,
  type MarketProfileTopWhale,
  type MarketProfileTopWhales,
} from "../lib/api";
import { useApiData } from "../hooks/useApiData";
import { formatTrustScorePercent } from "../lib/scoreFormatting";
import { deriveUserIdentity, deriveWhaleTierLabel } from "../lib/userIdentity";

const PREDICTION_WINDOWS = ["12h", "24h"] as const;

function formatPercent(value: number | null) {
  if (value === null) return "--";
  return `${Math.round(value * 100)}%`;
}

function formatOddsPercent(value: number | null | undefined, digits = 1) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return `${value.toFixed(digits)}%`;
}

function formatSignedPoints(value: number | null | undefined, digits = 1) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(digits)} pts`;
}

function formatCompactNumber(value: number | string | null | undefined, digits = 1) {
  if (value === null || typeof value === "undefined" || value === "") return "--";
  const numeric = typeof value === "number" ? value : Number(value ?? 0);
  if (!Number.isFinite(numeric)) return "--";
  return numeric.toFixed(digits);
}

function formatDateTime(value: string | null | undefined) {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function formatLabel(value: string | null | undefined) {
  if (!value) return "Unknown";
  return value
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function formatOpposingPercent(value: number | null) {
  if (value === null) return "--";
  const normalized = Math.min(Math.max(value, 0), 1);
  return `${Math.round((1 - normalized) * 100)}%`;
}

function formatCurrency(value: number | null) {
  if (value === null) return "--";
  return `$${value.toLocaleString()}`;
}

function profileTrendCases(trend: MarketProfileMlPredictionTrend | undefined) {
  if (!trend?.available || !trend.windows) return [];
  return PREDICTION_WINDOWS.flatMap((windowName) =>
    (trend.windows?.[windowName] ?? []).map((item) => ({
      ...item,
      window: windowName,
    })),
  );
}

function windowHours(windowName: string) {
  return windowName === "12h" ? 12 : 24;
}

function predictionRank(item: MarketProfileMlPredictionCase) {
  const tierRank: Record<string, number> = { show: 0, review: 1, hidden: 2 };
  const signalRank: Record<string, number> = { strong: 0, watch: 1, abstain: 2 };
  return (
    (tierRank[item.display_tier] ?? 3) * 1000 +
    (signalRank[item.direction_signal_tier] ?? 3) * 100 -
    Math.abs(item.predicted_delta_pts ?? 0)
  );
}

function primaryTrendCases(cases: MarketProfileMlPredictionCase[]) {
  const primary = [...cases].sort((left, right) => predictionRank(left) - predictionRank(right))[0];
  if (!primary) return [];
  return cases
    .filter((item) => item.side_label === primary.side_label)
    .sort((left, right) => windowHours(left.window) - windowHours(right.window));
}

function tierClass(value: string | undefined) {
  if (value === "show" || value === "strong") return "market-ml-good";
  if (value === "review" || value === "watch") return "market-ml-watch";
  return "market-ml-muted";
}

function forecastSourceTag(item: MarketProfileMlPredictionCase) {
  const source = item.prediction_source ?? "";
  if (source === "whale_anchored_report" || source === "local_whale_anchored_report") {
    return "Trained trend model";
  }
  if (source === "live_whale_signal_model") {
    return "Live whale model";
  }
  if (source === "current_odds_baseline") {
    return "Current odds baseline";
  }
  return "ML forecast";
}

function signalTag(item: MarketProfileMlPredictionCase) {
  if (item.direction_signal_tier === "strong") return "Strong signal";
  if (item.direction_signal_tier === "watch") return "Watch signal";
  if (item.reliability_warnings?.includes("no_recent_whale_signal_for_side")) return "No whale signal";
  return "No clear signal";
}

function forecastDirectionTag(item: MarketProfileMlPredictionCase) {
  if (item.predicted_direction === "up") return "Up forecast";
  if (item.predicted_direction === "down") return "Down forecast";
  return "Flat forecast";
}

function confidenceTag(item: MarketProfileMlPredictionCase) {
  if (item.direction_signal_tier === "strong") return "High confidence";
  if (item.direction_signal_tier === "watch") return "Watch closely";
  if (item.reliability_warnings?.includes("no_recent_whale_signal_for_side")) return "Current odds baseline";
  if (item.review_reasons?.includes("insufficient_watch_confidence")) return "Low confidence";
  if (item.display_tier === "show") return "Forecast ready";
  return "Review only";
}

function whaleAnchorValue(item: MarketProfileMlPredictionCase, key: string) {
  return item.whale_anchor?.[key];
}

function LiveWhaleEntrySummary({ trend }: { trend: MarketProfileMlPredictionTrend | undefined }) {
  const anchor = trend?.prediction_anchor;
  const sequence = trend?.live_whale_sequence;
  if (!anchor?.available && !sequence?.available) return null;

  return (
    <div className="market-ml-live-entry">
      <div>
        <p className="market-ml-side-label">Whale entry anchor</p>
        <h3>{anchor?.available ? formatLabel(anchor.side_label) : "Recent whale activity"}</h3>
        <div className="market-ml-summary-row">
          <span>Entry {formatDateTime(anchor?.event_time)}</span>
          <span>Entry odds {formatOddsPercent(anchor?.odds_pct)}</span>
          <span>Notional {formatCurrency(anchor?.notional_value ?? null)}</span>
          <span>{anchor?.is_trusted_whale ? "Trusted whale" : "Whale"}</span>
        </div>
      </div>
      <div className="market-ml-live-stats">
        <span>{formatCompactNumber(sequence?.sequence_count, 0)} market-side sequences</span>
        <span>{formatCompactNumber(sequence?.queried_event_rows, 0)} whale events</span>
        <span>as of {formatDateTime(sequence?.as_of)}</span>
      </div>
    </div>
  );
}

function MarketPredictionTrendChart({ cases }: { cases: MarketProfileMlPredictionCase[] }) {
  if (cases.length === 0) return null;

  const base = cases[0];
  const entryOdds = base.whale_entry_odds_pct ?? base.current_odds_pct ?? 50;
  const points = [
    { hour: 0, odds: entryOdds, label: "entry" },
    ...cases
      .filter((item) => typeof item.predicted_future_odds_pct === "number")
      .map((item) => ({
        hour: item.prediction_window_hours ?? windowHours(item.window),
        odds: item.predicted_future_odds_pct ?? entryOdds,
        label: item.window,
      })),
  ];
  const intervalValues = cases.flatMap((item) => [
    item.interval_low_future_odds_pct,
    item.interval_high_future_odds_pct,
  ]);
  const values = [...points.map((point) => point.odds), ...intervalValues].filter(
    (value): value is number => typeof value === "number" && Number.isFinite(value),
  );
  const rawMin = Math.min(...values);
  const rawMax = Math.max(...values);
  const paddedMin = Math.max(0, Math.floor(rawMin - 8));
  const paddedMax = Math.min(100, Math.ceil(rawMax + 8));
  const minOdds = paddedMax - paddedMin < 20 ? Math.max(0, paddedMin - 10) : paddedMin;
  const maxOdds = paddedMax - paddedMin < 20 ? Math.min(100, paddedMax + 10) : paddedMax;
  const width = 520;
  const height = 230;
  const left = 46;
  const right = 32;
  const top = 24;
  const bottom = 38;
  const plotWidth = width - left - right;
  const plotHeight = height - top - bottom;
  const yFor = (odds: number) => top + ((maxOdds - odds) / Math.max(maxOdds - minOdds, 1)) * plotHeight;
  const xFor = (hour: number) => left + (Math.min(Math.max(hour, 0), 24) / 24) * plotWidth;
  const linePoints = points.map((point) => `${xFor(point.hour)},${yFor(point.odds)}`).join(" ");

  return (
    <div className="market-ml-chart-shell">
      <svg className="market-ml-chart" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="ML prediction trend">
        {[minOdds, Math.round((minOdds + maxOdds) / 2), maxOdds].map((odds) => (
          <g key={`grid-${odds}`}>
            <line x1={left} x2={width - right} y1={yFor(odds)} y2={yFor(odds)} />
            <text x={8} y={yFor(odds) + 4}>{odds}%</text>
          </g>
        ))}
        {[0, 12, 24].map((hour) => (
          <text className="market-ml-axis-label" key={`axis-${hour}`} x={xFor(hour)} y={height - 10} textAnchor="middle">
            {hour === 0 ? "entry" : `+${hour}h`}
          </text>
        ))}
        <polyline className="market-ml-chart-line" points={linePoints} />
        {points.map((point) => (
          <g key={`${point.label}-${point.hour}`}>
            <circle cx={xFor(point.hour)} cy={yFor(point.odds)} r={point.hour === 0 ? 4 : 6} />
            <text x={xFor(point.hour) + 8} y={yFor(point.odds) - 8}>
              {formatOddsPercent(point.odds)}
            </text>
          </g>
        ))}
      </svg>
    </div>
  );
}

function TopMarketWhaleRow({ whale, rank }: { whale: MarketProfileTopWhale; rank: number }) {
  const identity = deriveUserIdentity(whale);
  const latestAction = [whale.latest_side, whale.latest_outcome_label].filter(Boolean).map(formatLabel).join(" ");

  return (
    <article className="market-ml-whale-row">
      <div className="market-ml-whale-rank">#{rank}</div>
      <div className="market-ml-whale-main">
        <div className="market-ml-whale-title">
          <Link to={`/users/${whale.user_id}`}>{identity.primary}</Link>
          <span>{deriveWhaleTierLabel(whale)}</span>
        </div>
        <p>{identity.secondary}</p>
        <div className="market-ml-summary-row">
          <span>Trust {formatTrustScorePercent(whale.trust_score)}</span>
          <span>Market volume {formatCurrency(whale.total_notional)}</span>
          <span>Trades {formatCompactNumber(whale.trade_count, 0)}</span>
          <span>
            Buys {formatCompactNumber(whale.buy_trade_count, 0)} | Sells {formatCompactNumber(whale.sell_trade_count, 0)}
          </span>
        </div>
      </div>
      <div className="market-ml-whale-meta">
        <span>Latest {latestAction || "Trade"}</span>
        <span>{formatDateTime(whale.latest_trade_time)}</span>
        <span>Avg price {formatPercent(whale.avg_trade_price)}</span>
      </div>
    </article>
  );
}

function TopMarketWhalesPanel({ topWhales }: { topWhales: MarketProfileTopWhales | undefined }) {
  const whales = topWhales?.items ?? [];

  if (whales.length === 0) {
    return (
      <div className="market-ml-empty">
        <strong>No ranked whales found for this market yet.</strong>
        <span>Top-whale ranking appears after scored whale wallets have trades in this market.</span>
      </div>
    );
  }

  return (
    <div className="market-ml-whales-panel">
      <div className="market-ml-whales-summary">
        <div>
          <p className="market-ml-side-label">Market whale ranking</p>
          <h3>Top 5 whales by trust score</h3>
        </div>
        <div className="market-ml-live-stats">
          <span>{formatCompactNumber(topWhales?.count, 0)} ranked whales</span>
          <span>Scores {formatDateTime(topWhales?.snapshot_time)}</span>
        </div>
      </div>
      <div className="market-ml-whale-list">
        {whales.map((whale, index) => (
          <TopMarketWhaleRow key={whale.user_id} whale={whale} rank={index + 1} />
        ))}
      </div>
    </div>
  );
}

function MarketMlPredictionTrendPanel({
  trend,
  topWhales,
}: {
  trend: MarketProfileMlPredictionTrend | undefined;
  topWhales: MarketProfileTopWhales | undefined;
}) {
  const cases = profileTrendCases(trend);
  const primaryCases = primaryTrendCases(cases);
  const anchor = trend?.prediction_anchor;
  const [activeTab, setActiveTab] = useState<"trend" | "whales">("trend");

  return (
    <section className="card profile-card market-ml-trend-card">
      <div className="card-header market-ml-header">
        <div>
          <p className="card-label">ML Trend</p>
          <h2>{activeTab === "whales" ? "Top Market Whales" : "Prediction Trend"}</h2>
          <p className="card-subtext">
            {activeTab === "whales"
              ? "Top 5 whales active in this market, ranked by latest trust score."
              : "Whale entry time followed by server-backed 12h and 24h trend predictions."}
          </p>
        </div>
        <div className="market-ml-tabs" role="tablist" aria-label="Market ML profile views">
          <button
            type="button"
            className={activeTab === "trend" ? "active" : ""}
            onClick={() => setActiveTab("trend")}
            role="tab"
            aria-selected={activeTab === "trend"}
          >
            Prediction Trend
          </button>
          <button
            type="button"
            className={activeTab === "whales" ? "active" : ""}
            onClick={() => setActiveTab("whales")}
            role="tab"
            aria-selected={activeTab === "whales"}
          >
            Top Whales
          </button>
        </div>
      </div>

      {activeTab === "whales" ? (
        <TopMarketWhalesPanel topWhales={topWhales} />
      ) : cases.length === 0 ? (
        <>
          <LiveWhaleEntrySummary trend={trend} />
          <div className="market-ml-empty">
            <strong>No 12h/24h ML prediction snapshot for this market yet.</strong>
            <span>{formatLabel(trend?.prediction_status ?? trend?.reason ?? "market_not_in_ml_prediction_snapshot")}</span>
          </div>
        </>
      ) : (
        <>
          <LiveWhaleEntrySummary trend={trend} />
          <div className="market-ml-chart-layout">
            <div>
              <p className="market-ml-side-label">Prediction anchor</p>
              <h3>{formatLabel(primaryCases[0]?.side_label)}</h3>
              <div className="market-ml-summary-row">
                <span>Entry {formatDateTime(primaryCases[0]?.whale_entry_time ?? anchor?.event_time)}</span>
                <span>{formatLabel(primaryCases[0]?.focused_fit_category)}</span>
                <span>{formatLabel(primaryCases[0]?.display_tier)}</span>
              </div>
            </div>
            <MarketPredictionTrendChart cases={primaryCases} />
          </div>

          <div className="market-ml-prediction-grid">
            {cases.map((item) => {
              const reason = [...(item.display_reasons ?? []), ...(item.review_reasons ?? [])]
                .slice(0, 2)
                .map(formatLabel)
                .join(", ");
              const confidence = confidenceTag(item);
              return (
                <article className="market-ml-prediction-row" key={`${item.window}-${item.side_label}`}>
                  <div>
                    <div className="market-ml-row-title">
                      <strong>{item.window}</strong>
                      <span>{formatLabel(item.side_label)}</span>
                    </div>
                    <p>
                      {formatOddsPercent(item.current_odds_pct)} to {formatOddsPercent(item.predicted_future_odds_pct)}
                      <span className={(item.predicted_delta_pts ?? 0) >= 0 ? "market-ml-up" : "market-ml-down"}>
                        {formatSignedPoints(item.predicted_delta_pts)}
                      </span>
                    </p>
                  </div>
                  <div className="market-ml-row-metrics">
                    <span className={tierClass(item.display_tier)}>{forecastSourceTag(item)}</span>
                    <span className={tierClass(item.direction_signal_tier)}>{signalTag(item)}</span>
                    <span>{forecastDirectionTag(item)}</span>
                  </div>
                  <div className="market-ml-row-detail">
                    <span>
                      Entry {formatDateTime(item.whale_entry_time)} to target {formatDateTime(item.prediction_target_time)}
                    </span>
                    <span>
                      Whale entries 12h {formatCompactNumber(whaleAnchorValue(item, "recent_entry_count_12h"), 0)} |
                      exits {formatCompactNumber(whaleAnchorValue(item, "recent_exit_count_12h"), 0)}
                    </span>
                    <span>Net pressure {formatCompactNumber(whaleAnchorValue(item, "recent_weighted_net_pressure_12h"), 2)}</span>
                    <span>{confidence || reason || formatLabel(item.prediction_status ?? item.prediction_source)}</span>
                  </div>
                </article>
              );
            })}
          </div>
        </>
      )}
    </section>
  );
}

export default function MarketProfile() {
  const { marketId } = useParams();
  const marketSlug = marketId ?? "";
  const { isMarketFollowed, toggleMarket } = useWatchlist();
  const loadMarket = useCallback(() => fetchMarketProfile(marketSlug), [marketSlug]);
  const { data, loading, error } = useApiData(loadMarket);

  return (
    <div className="page market-profile-page">
      <header className="hero market-hero">
        <div className="hero-top-row">
          <div>
            <p className="eyebrow">Market Profile</p>
            <h1 className="market-title">{data?.question ?? marketSlug}</h1>
            <p className="hero-text">Latest dashboard-backed market snapshot and whale concentration details.</p>
          </div>

          <div className="hero-action-stack">
            <FollowButton
              isFollowing={isMarketFollowed(marketSlug)}
              onToggle={() => toggleMarket(marketSlug)}
            />
          </div>
        </div>

        <div className="hero-actions">
          <Link to="/leaderboard" className="table-link back-link">
            ← Back to leaderboard
          </Link>

          {data && (
            <div className="hero-pills">
              <span className="hero-pill">{data.market_slug}</span>
              <span className="hero-pill">Whale Traders {data.whale_count}</span>
              <span className="hero-pill">Trusted Whales {data.trusted_whale_count}</span>
            </div>
          )}
        </div>
      </header>

      {loading && <section className="status-panel">Loading market profile...</section>}
      {error && <section className="status-panel error-panel">{error}</section>}

      {!loading && !error && data && (
        <>
          <section className="market-summary-card">
            <div className="market-summary-left">
              <p className="summary-label">Current Yes Probability</p>
        

              <div className="summary-stats">
                <div className="stat-chip">
                  <span className="stat-chip-label">Volume</span>
                  <strong>{formatCurrency(data.volume)}</strong>
                </div>
                <div className="stat-chip">
                  <span className="stat-chip-label">Depth</span>
                  <strong>{data.orderbook_depth?.toLocaleString() ?? "--"}</strong>
                </div>
                <div className="stat-chip">
                  <span className="stat-chip-label">Whale Traders</span>
                  <strong>{data.whale_count}</strong>
                </div>
                <div className="stat-chip">
                  <span className="stat-chip-label">Trusted Whales</span>
                  <strong>{data.trusted_whale_count}</strong>
                </div>
                <div className="stat-chip stat-chip-wide">
                  <span className="stat-chip-label">Read Time</span>
                  <strong>{formatDateTime(data.read_time)}</strong>
                </div>
              </div>
            </div>

<div className="market-summary-right">
  <button className="trade-btn trade-btn-yes" type="button">
    <span className="trade-label">Yes Probability</span>
    <strong>{formatPercent(data.price)}</strong>
  </button>

  <button className="trade-btn trade-btn-no" type="button">
    <span className="trade-label">No Probability</span>
    <strong>{formatOpposingPercent(data.odds ?? data.price)}</strong>
  </button>
</div>
          </section>

          <MarketMlPredictionTrendPanel trend={data.ml_prediction_trend} topWhales={data.top_whales} />
        </>
      )}
    </div>
  );
}
