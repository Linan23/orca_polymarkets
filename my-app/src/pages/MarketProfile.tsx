import { useCallback } from "react";
import { Link, useParams } from "react-router-dom";
import FollowButton from "../components/FollowButton";
import { useWatchlist } from "../hooks/useWatchlist";
import { fetchMarketProfile } from "../lib/api";
import { useApiData } from "../hooks/useApiData";

function formatPercent(value: number | null) {
  if (value === null) return "--";
  return `${Math.round(value * 100)}%`;
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

function renderJson(value: unknown) {
  return JSON.stringify(value, null, 2);
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
              <div className="summary-main">
                <h2>{formatPercent(data.price)}</h2>
                <span className="summary-trend">No {formatOpposingPercent(data.odds ?? data.price)}</span>
              </div>

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
              </div>
            </div>

            <div className="market-summary-right">
              <button className="trade-btn trade-btn-yes" type="button">
                <span className="trade-label">Top Whale Traders</span>
                <strong>{data.whale_market_focus ?? "Broad"}</strong>
              </button>
              <button className="trade-btn trade-btn-no" type="button">
                <span className="trade-label">Snapshot</span>
                <strong>{data.snapshot_time ?? "--"}</strong>
              </button>
            </div>
          </section>

          <section className="card profile-card">
            <div className="card-header">
              <p className="card-label">Profile Snapshot</p>
              <h2>Market Details</h2>
              <p className="card-subtext">Realtime payload plus dashboard market metrics from the backend.</p>
            </div>

            <div className="profile-grid polished-grid">
              <div className="profile-item">
                <span className="profile-key">market_contract_id</span>
                <span className="profile-value">{data.market_contract_id}</span>
              </div>
              <div className="profile-item">
                <span className="profile-key">dashboard_id</span>
                <span className="profile-value">{data.dashboard_id}</span>
              </div>
              <div className="profile-item">
                <span className="profile-key">realtime_source</span>
                <span className="profile-value">{data.realtime_source}</span>
              </div>
              <div className="profile-item">
                <span className="profile-key">read_time</span>
                <span className="profile-value">{data.read_time ?? "--"}</span>
              </div>
              <div className="profile-item profile-item-full">
                <span className="profile-key">realtime_payload</span>
                <pre className="payload-box">{renderJson(data.realtime_payload)}</pre>
              </div>
            </div>
          </section>
        </>
      )}
    </div>
  );
}
