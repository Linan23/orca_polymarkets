import { useCallback } from "react";
import { Link, useParams } from "react-router-dom";
import { fetchUserWhaleProfile } from "../lib/api";
import { useApiData } from "../hooks/useApiData";

function formatCurrency(value: number) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
}

function formatPercent(value: number | null) {
  if (value === null) return "--";
  return `${(value * 100).toFixed(1)}%`;
}

function renderJson(value: unknown) {
  return JSON.stringify(value, null, 2);
}

export default function UserProfile() {
  const { userId } = useParams();
  const parsedUserId = Number(userId);
  const invalidUser = Number.isNaN(parsedUserId);
  const loadProfile = useCallback(() => fetchUserWhaleProfile(parsedUserId), [parsedUserId]);
  const { data, loading, error } = useApiData(loadProfile);

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

  return (
    <div className="page user-profile-page">
      <header className="hero market-hero user-hero">
        <div className="hero-top-row">
          <div>
            <p className="eyebrow">Trader Profile</p>
            <h1 className="market-title">{data?.external_user_ref ?? `User ${userId}`}</h1>
            <p className="hero-text">
              Polymarket whale score, resolved-performance summary, and latest dashboard profile data.
            </p>
          </div>
        </div>

        <div className="hero-actions">
          <Link to="/leaderboard" className="table-link back-link">
            ← Back to leaderboard
          </Link>

          {data && (
            <div className="hero-pills">
              <span className="hero-pill">User #{data.user_id}</span>
              <span className="hero-pill">
                {score?.is_trusted_whale ? "Trusted Whale" : score?.is_whale ? "Whale" : "Candidate"}
              </span>
              <span className="hero-pill">Trades {score?.sample_trade_count ?? 0}</span>
            </div>
          )}
        </div>
      </header>

      {loading && <section className="status-panel">Loading user profile...</section>}
      {error && <section className="status-panel error-panel">{error}</section>}

      {!loading && !error && data && (
        <>
          <section className="trader-overview-box">
            <div className="trader-overview-main">
              <div className="overview-inline-metrics">
                <div className="inline-metric">
                  <span>Trust Score</span>
                  <strong>{score ? score.trust_score.toFixed(3) : "--"}</strong>
                </div>
                <div className="inline-metric">
                  <span>Profitability</span>
                  <strong>{score ? score.profitability_score.toFixed(3) : "--"}</strong>
                </div>
                <div className="inline-metric">
                  <span>Resolved Markets</span>
                  <strong>{resolved?.resolved_market_count ?? 0}</strong>
                </div>
                <div className="inline-metric">
                  <span>Win Rate</span>
                  <strong>{formatPercent(resolved?.win_rate ?? null)}</strong>
                </div>
              </div>

              <div className="overview-bottom-grid">
                <div className="overview-info-block">
                  <span className="overview-label">Wallet</span>
                  <strong>{data.wallet_address ?? "Not available"}</strong>
                  <small>{data.display_label ?? data.external_user_ref}</small>
                </div>
                <div className="overview-info-block">
                  <span className="overview-label">Realized P&amp;L</span>
                  <strong>{formatCurrency(resolved?.realized_pnl ?? 0)}</strong>
                  <small>ROI {formatPercent(resolved?.realized_roi ?? null)}</small>
                </div>
                <div className="overview-info-block">
                  <span className="overview-label">Dashboard Volume</span>
                  <strong>{formatCurrency(dashboard?.total_volume ?? 0)}</strong>
                  <small>Shares {dashboard?.total_shares ?? 0}</small>
                </div>
                <div className="overview-info-block">
                  <span className="overview-label">Insider Flag</span>
                  <strong>{data.is_likely_insider ? "Flagged" : "Not flagged"}</strong>
                  <small>Excluded markets {resolved?.excluded_market_count ?? 0}</small>
                </div>
              </div>
            </div>
          </section>

          <section className="card profile-card">
            <div className="card-header">
              <p className="card-label">Resolved Performance</p>
              <h2>Whale Metrics</h2>
              <p className="card-subtext">Latest score batch plus resolved-market performance summary.</p>
            </div>
            <div className="profile-grid polished-grid">
              <div className="profile-item">
                <span className="profile-key">scoring_version</span>
                <span className="profile-value">{score?.scoring_version ?? "--"}</span>
              </div>
              <div className="profile-item">
                <span className="profile-key">snapshot_time</span>
                <span className="profile-value">{score?.snapshot_time ?? "--"}</span>
              </div>
              <div className="profile-item">
                <span className="profile-key">is_whale</span>
                <span className="profile-value">{score?.is_whale ? "true" : "false"}</span>
              </div>
              <div className="profile-item">
                <span className="profile-key">is_trusted_whale</span>
                <span className="profile-value">{score?.is_trusted_whale ? "true" : "false"}</span>
              </div>
              <div className="profile-item">
                <span className="profile-key">winning_market_count</span>
                <span className="profile-value">{resolved?.winning_market_count ?? 0}</span>
              </div>
              <div className="profile-item">
                <span className="profile-key">resolved_market_count</span>
                <span className="profile-value">{resolved?.resolved_market_count ?? 0}</span>
              </div>
              <div className="profile-item profile-item-full">
                <span className="profile-key">historical_actions_summary</span>
                <pre className="payload-box">{renderJson(dashboard?.historical_actions_summary ?? {})}</pre>
              </div>
              <div className="profile-item profile-item-full">
                <span className="profile-key">trusted_traders_summary</span>
                <pre className="payload-box">{renderJson(dashboard?.trusted_traders_summary ?? {})}</pre>
              </div>
              <div className="profile-item profile-item-full">
                <span className="profile-key">insider_stats</span>
                <pre className="payload-box">{renderJson(dashboard?.insider_stats ?? {})}</pre>
              </div>
            </div>
          </section>
        </>
      )}
    </div>
  );
}
