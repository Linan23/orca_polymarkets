import { useCallback, useState } from "react";
import { Link } from "react-router-dom";
import { useApiData } from "../hooks/useApiData";
import {
  type AnalyticsTimeframe,
  fetchMarketWhaleConcentration,
  fetchTopProfitableUsers,
  fetchWhaleEntryBehavior,
  type MarketConcentrationRow,
  type TopProfitableUserRow,
  type WhaleEntryBehaviorRow,
} from "../lib/api";

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

function ResearchUserRows({ items }: { items: TopProfitableUserRow[] }) {
  return (
    <div className="leaderboard-list">
      {items.map((user, index) => (
        <Link key={`${user.user_id}-${user.external_user_ref}`} to={`/users/${user.user_id}`} className="leaderboard-row">
          <div className="leaderboard-rank">{index + 1}</div>
          <div className="leaderboard-main">
            <div className="leaderboard-main-top">
              <div>
                <div className="leaderboard-name">{user.external_user_ref}</div>
                <div className="leaderboard-subtext">
                  {user.resolved_market_count} resolved markets · {user.winning_market_count} wins
                </div>
              </div>
              <div className="leaderboard-score">{formatCurrency(user.realized_pnl)}</div>
            </div>
            <div className="leaderboard-meta">
              <span className="meta-pill">ROI {formatPercent(user.realized_roi)}</span>
              <span className="meta-pill">Win {formatPercent(user.win_rate)}</span>
              <span className="meta-pill">Trust {user.trust_score.toFixed(3)}</span>
            </div>
          </div>
        </Link>
      ))}
    </div>
  );
}

function ResearchMarketRows({ items }: { items: MarketConcentrationRow[] }) {
  return (
    <div className="leaderboard-list">
      {items.map((market, index) => (
        <Link key={`${market.market_id}-${market.market_slug}`} to={`/markets/${market.market_slug}`} className="leaderboard-row">
          <div className="leaderboard-rank">{index + 1}</div>
          <div className="leaderboard-main">
            <div className="leaderboard-main-top">
              <div>
                <div className="leaderboard-name">{market.question}</div>
                <div className="leaderboard-subtext">{market.platform_name} · {market.market_slug}</div>
              </div>
              <div className="leaderboard-price">{formatPercent(market.price)}</div>
            </div>
            <div className="leaderboard-meta">
              <span className="meta-pill">Whales {market.whale_count}</span>
              <span className="meta-pill">Trusted {market.trusted_whale_count}</span>
              <span className="meta-pill">Volume {market.volume?.toLocaleString() ?? "--"}</span>
            </div>
          </div>
        </Link>
      ))}
    </div>
  );
}

function ResearchEntryRows({ items }: { items: WhaleEntryBehaviorRow[] }) {
  return (
    <div className="leaderboard-list">
      {items.map((user, index) => (
        <Link key={`${user.user_id}-${user.external_user_ref}`} to={`/users/${user.user_id}`} className="leaderboard-row">
          <div className="leaderboard-rank">{index + 1}</div>
          <div className="leaderboard-main">
            <div className="leaderboard-main-top">
              <div>
                <div className="leaderboard-name">{user.external_user_ref}</div>
                <div className="leaderboard-subtext">
                  {user.entry_trade_count} buy entries · {user.distinct_markets} markets
                </div>
              </div>
              <div className="leaderboard-score">{formatPercent(user.weighted_avg_entry_price)}</div>
            </div>
            <div className="leaderboard-meta">
              <span className="meta-pill">Avg shares {user.avg_entry_shares.toFixed(1)}</span>
              <span className="meta-pill">Min {formatPercent(user.min_entry_price)}</span>
              <span className="meta-pill">Max {formatPercent(user.max_entry_price)}</span>
              <span className={`meta-pill ${user.is_trusted_whale ? "internal-pill" : "public-pill"}`}>
                {user.is_trusted_whale ? "trusted" : user.is_whale ? "whale" : "candidate"}
              </span>
            </div>
          </div>
        </Link>
      ))}
    </div>
  );
}

type ResearchAnalyticsSectionProps = {
  showExportControls?: boolean;
};

export default function ResearchAnalyticsSection({
  showExportControls = false,
}: ResearchAnalyticsSectionProps) {
  const [timeframe, setTimeframe] = useState<AnalyticsTimeframe>("all");
  const loadAnalytics = useCallback(
    async () => {
      const [topUsers, topMarkets, entryBehavior] = await Promise.all([
        fetchTopProfitableUsers(5, timeframe),
        fetchMarketWhaleConcentration(5, timeframe),
        fetchWhaleEntryBehavior(5, timeframe),
      ]);
      return { topUsers, topMarkets, entryBehavior };
    },
    [timeframe],
  );
  const { data, loading, error } = useApiData(loadAnalytics);

  return (
    <section className="analytics-section">
      <div className="summary-section-header">
        <p className="leaderboard-kicker">Research Views</p>
        <h2>Whale Behavior Signals</h2>
        <p className="summary-card-subtext">
          Conservative resolved-user profitability and latest market concentration, ready for report/dashboard use.
        </p>
        <div className="analytics-toolbar">
          <label className="filter-field analytics-timeframe-field">
            <span>Timeframe</span>
            <select value={timeframe} onChange={(event) => setTimeframe(event.target.value as AnalyticsTimeframe)}>
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
        <div className="analytics-grid">
          <section className="leaderboard-card">
            <div className="leaderboard-top">
              <p className="leaderboard-kicker">Polymarket</p>
              <h2>Top Profitable Resolved Users</h2>
              <p className="leaderboard-count">Ranked by conservative realized P&amp;L on resolved markets for {timeframe}.</p>
              {showExportControls && data.topUsers.length > 0 && (
                <div className="analytics-export-row">
                  <button
                    type="button"
                    className="analytics-export-btn"
                    onClick={() => downloadRowsAsCsv(`top_profitable_resolved_users_${timeframe}.csv`, data.topUsers)}
                  >
                    Export CSV
                  </button>
                  <button
                    type="button"
                    className="analytics-export-btn"
                    onClick={() => downloadRowsAsJson(`top_profitable_resolved_users_${timeframe}.json`, data.topUsers)}
                  >
                    Export JSON
                  </button>
                </div>
              )}
            </div>
            {data.topUsers.length > 0 ? (
              <ResearchUserRows items={data.topUsers} />
            ) : (
              <div className="status-panel">No resolved-user profitability rows are available yet.</div>
            )}
          </section>

          <section className="leaderboard-card">
            <div className="leaderboard-top">
              <p className="leaderboard-kicker">Cross-Platform Markets</p>
              <h2>Whale Concentration by Market</h2>
              <p className="leaderboard-count">Ranked by trusted-whale count, then whale count, then market volume for {timeframe}.</p>
              {showExportControls && data.topMarkets.length > 0 && (
                <div className="analytics-export-row">
                  <button
                    type="button"
                    className="analytics-export-btn"
                    onClick={() => downloadRowsAsCsv(`market_whale_concentration_${timeframe}.csv`, data.topMarkets)}
                  >
                    Export CSV
                  </button>
                  <button
                    type="button"
                    className="analytics-export-btn"
                    onClick={() => downloadRowsAsJson(`market_whale_concentration_${timeframe}.json`, data.topMarkets)}
                  >
                    Export JSON
                  </button>
                </div>
              )}
            </div>
            {data.topMarkets.length > 0 ? (
              <ResearchMarketRows items={data.topMarkets} />
            ) : (
              <div className="status-panel">No market concentration rows are available yet.</div>
            )}
          </section>

          <section className="leaderboard-card">
            <div className="leaderboard-top">
              <p className="leaderboard-kicker">Polymarket Whales</p>
              <h2>Whale Entry Behavior</h2>
              <p className="leaderboard-count">Weighted average buy-entry price and entry size for {timeframe}.</p>
              {showExportControls && data.entryBehavior.length > 0 && (
                <div className="analytics-export-row">
                  <button
                    type="button"
                    className="analytics-export-btn"
                    onClick={() => downloadRowsAsCsv(`whale_entry_behavior_${timeframe}.csv`, data.entryBehavior)}
                  >
                    Export CSV
                  </button>
                  <button
                    type="button"
                    className="analytics-export-btn"
                    onClick={() => downloadRowsAsJson(`whale_entry_behavior_${timeframe}.json`, data.entryBehavior)}
                  >
                    Export JSON
                  </button>
                </div>
              )}
            </div>
            {data.entryBehavior.length > 0 ? (
              <ResearchEntryRows items={data.entryBehavior} />
            ) : (
              <div className="status-panel">No whale entry rows are available yet.</div>
            )}
          </section>
        </div>
      )}
    </section>
  );
}
