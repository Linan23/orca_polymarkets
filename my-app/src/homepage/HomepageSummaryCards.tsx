import { useCallback } from "react";
import { Link } from "react-router-dom";
import { useApiData } from "../hooks/useApiData";
import { fetchHomeSummary } from "../lib/api";
import { deriveUserIdentity } from "../lib/userIdentity";

function formatPercent(value: number | null) {
  if (value === null) return "--";
  return `${Math.round(value * 100)}%`;
}

function formatCompact(value: number) {
  return new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

export default function HomepageSummaryCards() {
  const loadSummary = useCallback(() => fetchHomeSummary(), []);
  const { data, loading, error } = useApiData(loadSummary);

  return (
    <section className="summary-section">
      <div className="summary-section-header">
        <p className="leaderboard-kicker">Live System State</p>
        <h2>Research Summary</h2>
        <p className="summary-card-subtext">
          Whale identity metrics currently derive from Polymarket user history. Kalshi remains market-level only.
        </p>
      </div>

      {loading && <div className="status-panel">Loading homepage summary...</div>}
      {error && <div className="status-panel error-panel">{error}</div>}

      {!loading && !error && data && (
        <div className="summary-grid">
          <article className="summary-card summary-card-feature">
            <p className="summary-card-label">Polymarket Whale Coverage</p>
            <div className="summary-card-value">{data.whales_detected}</div>
            <p className="summary-card-subtext">
              {data.trusted_whales} trusted whale{data.trusted_whales === 1 ? "" : "s"} in
              scoring batch {data.scoring_version ?? "--"}.
            </p>
            <div className="summary-chip-row">
              <span className="meta-pill">Resolved {data.resolved_markets_observed}/{data.resolved_markets_available}</span>
              <span className="meta-pill">Profit users {data.profitability_users}</span>
            </div>
          </article>

          <article className="summary-card">
            <p className="summary-card-label">Top Trusted Whale (Polymarket)</p>
            {data.top_trusted_whale ? (
              (() => {
                const identity = deriveUserIdentity(data.top_trusted_whale);
                return (
                  <>
                <Link to={`/users/${data.top_trusted_whale.user_id}`} className="summary-card-link">
                  {identity.primary}
                </Link>
                <p className="summary-card-subtext">{identity.secondary}</p>
                <div className="summary-stat-list">
                  <div>
                    <span>Trust</span>
                    <strong>{data.top_trusted_whale.trust_score.toFixed(3)}</strong>
                  </div>
                  <div>
                    <span>Profit</span>
                    <strong>{data.top_trusted_whale.profitability_score.toFixed(3)}</strong>
                  </div>
                  <div>
                    <span>Trades</span>
                    <strong>{data.top_trusted_whale.sample_trade_count}</strong>
                  </div>
                </div>
              </>
                );
              })()
            ) : (
              <p className="summary-card-subtext">No trusted whale qualifies in the latest batch yet.</p>
            )}
          </article>

          <article className="summary-card">
            <p className="summary-card-label">Most Whale-Concentrated Market</p>
            {data.most_whale_concentrated_market ? (
              <>
                <Link
                  to={`/markets/${data.most_whale_concentrated_market.market_slug}`}
                  className="summary-card-link"
                >
                  {data.most_whale_concentrated_market.question}
                </Link>
                <div className="summary-stat-list">
                  <div>
                    <span>Price</span>
                    <strong>{formatPercent(data.most_whale_concentrated_market.price)}</strong>
                  </div>
                  <div>
                    <span>Whale Traders</span>
                    <strong>{data.most_whale_concentrated_market.whale_count}</strong>
                  </div>
                  <div>
                    <span>Trusted Whales</span>
                    <strong>{data.most_whale_concentrated_market.trusted_whale_count}</strong>
                  </div>
                </div>
              </>
            ) : (
              <p className="summary-card-subtext">No dashboard market snapshot is available yet.</p>
            )}
          </article>

          <article className="summary-card">
            <p className="summary-card-label">Latest Ingestion</p>
            {data.latest_ingestion ? (
              <>
                <div className="summary-card-value summary-card-value-small">{data.latest_ingestion.status}</div>
                <p className="summary-card-subtext">
                  {data.latest_ingestion.job_name} · {data.latest_ingestion.endpoint_name}
                </p>
                <div className="summary-stat-list">
                  <div>
                    <span>Records</span>
                    <strong>{formatCompact(data.latest_ingestion.records_written)}</strong>
                  </div>
                  <div>
                    <span>Errors</span>
                    <strong>{data.latest_ingestion.error_count}</strong>
                  </div>
                </div>
              </>
            ) : (
              <p className="summary-card-subtext">No scrape run has been recorded yet.</p>
            )}
          </article>

          <article className="summary-card summary-card-wide">
            <p className="summary-card-label">Platform Coverage</p>
            <div className="summary-platform-list">
              {data.platform_coverage.map((platform) => (
                <div key={platform.platform_name} className="summary-platform-row">
                  <div className="summary-platform-name">{platform.platform_name}</div>
                  <div className="summary-platform-metrics">
                    <span>Users {formatCompact(platform.user_count)}</span>
                    <span>Markets {formatCompact(platform.market_count)}</span>
                    <span>Trades {formatCompact(platform.transaction_count)}</span>
                    <span>Books {formatCompact(platform.orderbook_snapshot_count)}</span>
                  </div>
                </div>
              ))}
            </div>
          </article>
        </div>
      )}
    </section>
  );
}
