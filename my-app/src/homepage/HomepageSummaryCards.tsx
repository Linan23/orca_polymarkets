import { useCallback } from "react";
import { Link } from "react-router-dom";
import { useApiData } from "../hooks/useApiData";
import { fetchHomeSummary } from "../lib/api";
import { formatContractPrice } from "../lib/marketFormatting";
import { formatProfitabilityScorePercent, formatTrustScorePercent } from "../lib/scoreFormatting";
import { deriveUserIdentity } from "../lib/userIdentity";

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
                    <strong>{formatTrustScorePercent(data.top_trusted_whale.trust_score)}</strong>
                  </div>
                  <div>
                    <span>Profit</span>
                    <strong>{formatProfitabilityScorePercent(data.top_trusted_whale.profitability_score)}</strong>
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
                    <span>Yes Price</span>
                    <strong>{formatContractPrice(data.most_whale_concentrated_market.price)}</strong>
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


<article className="summary-card summary-card-wide">
  <p className="summary-card-label">Platform Coverage</p>

  <div className="coverage-pie-grid">
    {[
      {
        title: "Users",
        total: data.platform_coverage.reduce(
          (sum, platform) => sum + platform.user_count,
          0
        ),
        getValue: (platform: (typeof data.platform_coverage)[number]) =>
          platform.user_count,
      },
      {
        title: "Markets",
        total: data.platform_coverage.reduce(
          (sum, platform) => sum + platform.market_count,
          0
        ),
        getValue: (platform: (typeof data.platform_coverage)[number]) =>
          platform.market_count,
      },
    ].map((chart) => {
      let offset = 0;

      return (
        <div className="coverage-pie-card" key={chart.title}>
          <div className="coverage-pie-header">
            <h3>{chart.title}</h3>
            <strong>{formatCompact(chart.total)}</strong>
          </div>

          <div className="coverage-pie-content">
            <div
              className="coverage-donut"
              style={{
                background: `conic-gradient(${data.platform_coverage
                  .map((platform, index) => {
                    const value = chart.getValue(platform);
                    const start = offset;
                    const end = offset + (value / chart.total) * 100;
                    offset = end;

                    const color = index === 0 ? "#6f7cff" : "#42d3ff";

                    return `${color} ${start}% ${end}%`;
                  })
                  .join(", ")})`,
              }}
            >
              <div className="coverage-donut-hole">
                <span>Total</span>
                <strong>{formatCompact(chart.total)}</strong>
              </div>
            </div>

            <div className="coverage-pie-legend">
              {data.platform_coverage.map((platform, index) => {
                const value = chart.getValue(platform);
                const percent =
                  chart.total > 0 ? Math.round((value / chart.total) * 100) : 0;

                return (
                  <div className="coverage-legend-row" key={platform.platform_name}>
                    <span
                      className="coverage-legend-dot"
                      style={{
                        background: index === 0 ? "#6f7cff" : "#42d3ff",
                      }}
                    />

                    <div>
                      <strong>{platform.platform_name}</strong>
                      <p>
                        {formatCompact(value)} · {percent}%
                      </p>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      );
    })}
  </div>
</article>
        </div>
      )}
    </section>
  );
}
