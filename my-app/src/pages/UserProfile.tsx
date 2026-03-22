import { Link, useParams } from "react-router-dom";
import { useMemo, useState } from "react";

type UserProfileRow = {
  user_profile_section_id: string;
  dashboard_id: string;
  user_id: string;
  market_ref: string;
  historical_actions: string;
  insider_stats: string;
  profit_loss: number;
  wallet_balance: number;
  wallet_transactions: string;
  market_invested: number;
  trusted_traders: string[];
  preference_probabilities: string;
  total_volume: number;
  total_shares: number;
  win_rate: number;
  leaderboard_rank: number;
  wallet_address: string;
  total_trades: number;
  active_trades: number;
  pending_trades: number;
  resolved_trades: number;
  realized_pnl: number;
  unrealized_pnl: number;
  portfolio_value: number;
};

type TabKey =
  | "historical"
  | "wallet"
  | "trusted"
  | "performance"
  | "patterns"
  | "categories"
  | "insider";

const userProfiles: UserProfileRow[] = [
  {
    user_profile_section_id: "up-001",
    dashboard_id: "dashboard-001",
    user_id: "FedWillWin",
    market_ref: "market-1",
    historical_actions:
      "Bought YES, sold partial, re-entered position around key macro headlines and rotation windows.",
    insider_stats: "Unlikely Insider",
    profit_loss: 1566.52,
    wallet_balance: 3128336.17,
    wallet_transactions: "12 deposits, 33 trades, 5 withdrawals",
    market_invested: 2500,
    trusted_traders: ["Naive Whale", "Moderate Risk Trader"],
    preference_probabilities: "YES-heavy bias in political markets",
    total_volume: 2399119.74,
    total_shares: 3250,
    win_rate: 64.2,
    leaderboard_rank: 1012,
    wallet_address: "0xfdc07e182e6f959256295567e450a8727272fa79",
    total_trades: 45,
    active_trades: 3,
    pending_trades: 2,
    resolved_trades: 20,
    realized_pnl: 413412.8,
    unrealized_pnl: -373808.53,
    portfolio_value: 3128336.17,
  },
];

function formatCurrency(value: number) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
}

function formatPercent(value: number) {
  return `${value.toFixed(1)}%`;
}

function shortenWallet(address: string) {
  if (address.length < 14) return address;
  return `${address.slice(0, 6)}...${address.slice(-4)}`;
}

function getPnlClass(value: number) {
  if (value > 0) return "profit";
  if (value < 0) return "loss";
  return "neutral";
}

export default function UserProfile() {
  const { userId } = useParams();
  const [isFollowing, setIsFollowing] = useState(false);
  const [activeTab, setActiveTab] = useState<TabKey>("historical");

  const profile =
    userProfiles.find((item) => item.user_id === userId) ??
    ({
      user_profile_section_id: "up-placeholder",
      dashboard_id: "dashboard-001",
      user_id: userId ?? "UnknownTrader",
      market_ref: "market-1",
      historical_actions: "No historical actions found yet.",
      insider_stats: "No insider metrics found yet.",
      profit_loss: 0,
      wallet_balance: 0,
      wallet_transactions: "No wallet transactions found.",
      market_invested: 0,
      trusted_traders: ["Naive Whale"],
      preference_probabilities: "No preference model available.",
      total_volume: 0,
      total_shares: 0,
      win_rate: 0,
      leaderboard_rank: 0,
      wallet_address: "0x0000000000000000000000000000000000000000",
      total_trades: 0,
      active_trades: 0,
      pending_trades: 0,
      resolved_trades: 0,
      realized_pnl: 0,
      unrealized_pnl: 0,
      portfolio_value: 0,
    } as UserProfileRow);

  const pnlClass = getPnlClass(profile.profit_loss);

  const derived = useMemo(() => {
    const avgTradeSize =
      profile.total_trades > 0 ? profile.total_volume / profile.total_trades : 0;
    const exposureRatio =
      profile.portfolio_value > 0
        ? (profile.market_invested / profile.portfolio_value) * 100
        : 0;
    const resolutionRate =
      profile.total_trades > 0
        ? (profile.resolved_trades / profile.total_trades) * 100
        : 0;
    const activityRate =
      profile.total_trades > 0
        ? (profile.active_trades / profile.total_trades) * 100
        : 0;

    return {
      avgTradeSize,
      exposureRatio,
      resolutionRate,
      activityRate,
      categoryMix: [
        { label: "Politics", value: 46 },
        { label: "Macro", value: 28 },
        { label: "Crypto", value: 16 },
        { label: "Sports", value: 10 },
      ],
      behaviorTags: [
        "Event-driven entries",
        "Momentum chasing",
        "Headline reactive",
        "Medium holding periods",
      ],
      insiderSignals: [
        "Timing score: Low concern",
        "Concentration score: Moderate",
        "Behavior anomaly: None detected",
        "Copy-trader overlap: Limited",
      ],
      historicalTimeline: [
        "Opened initial position in linked market",
        "Trimmed after favorable move",
        "Re-entered after volatility reset",
        "Currently holding residual exposure",
      ],
      walletEvents: [
        "Primary wallet funded",
        "Capital rotated into active markets",
        "No unusual withdrawal clustering",
      ],
    };
  }, [profile]);

  const tabs: { key: TabKey; label: string }[] = [
    { key: "historical", label: "Historical" },
    { key: "wallet", label: "Wallet" },
    { key: "trusted", label: "Trusted" },
    { key: "performance", label: "Performance" },
    { key: "patterns", label: "Patterns" },
    { key: "categories", label: "Categories" },
    { key: "insider", label: "Insider Risk" },
  ];

  return (
    <div className="page user-profile-page">
      <header className="hero market-hero user-hero">
        <div className="hero-top-row">
          <div>
            <p className="eyebrow">Trader Profile</p>
            <h1 className="market-title">{profile.user_id}</h1>
            <p className="hero-text">
              Detailed performance metrics, trading patterns, category breakdowns,
              and insider risk analysis.
            </p>
          </div>

          <button
            type="button"
            className={`follow-btn ${isFollowing ? "active" : ""}`}
            onClick={() => setIsFollowing((prev) => !prev)}
            aria-pressed={isFollowing}
          >
            <span className="follow-icon">
              {isFollowing ? (
                <svg viewBox="0 0 24 24" fill="currentColor">
                  <path d="M12 17.3l6.18 3.73-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.76-1.64 7.03z" />
                </svg>
              ) : (
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                  <path d="M12 17.3l6.18 3.73-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.76-1.64 7.03z" />
                </svg>
              )}
            </span>
            <span>{isFollowing ? "Following" : "Follow"}</span>
          </button>
        </div>

        <div className="hero-actions">
          <Link to="/" className="table-link back-link">
            ← Back to dashboard
          </Link>

          <div className="hero-pills">
            <span className="hero-pill">Rank #{profile.leaderboard_rank}</span>
            <span className="hero-pill">Market {profile.market_ref}</span>
            <span className="hero-pill">Wallet {shortenWallet(profile.wallet_address)}</span>
          </div>
        </div>
      </header>

      <section className="trader-overview-box">
        <div className="trader-overview-main">
          <div className="overview-top">
            <div>
              <p className="summary-label">Total P&amp;L</p>
              <div className="trader-pnl-row">
                <h2 className={`trader-pnl ${pnlClass}`}>
                  {profile.profit_loss >= 0 ? "+" : "-"}
                  {formatCurrency(Math.abs(profile.profit_loss))}
                </h2>
                <span className="summary-trend">
                  {profile.resolved_trades} closed • {profile.active_trades} active
                </span>
              </div>
            </div>
          </div>

          <div className="overview-inline-metrics">
            <div className="inline-metric">
              <span>Win Rate</span>
              <strong>{formatPercent(profile.win_rate)}</strong>
            </div>
            <div className="inline-metric">
              <span>Volume</span>
              <strong>{formatCurrency(profile.total_volume)}</strong>
            </div>
            <div className="inline-metric">
              <span>Portfolio</span>
              <strong>{formatCurrency(profile.portfolio_value)}</strong>
            </div>
            <div className="inline-metric">
              <span>Invested</span>
              <strong>{formatCurrency(profile.market_invested)}</strong>
            </div>
          </div>

          <div className="overview-bottom-grid">
            <div className="overview-info-block">
              <span className="overview-label">Total Trades</span>
              <strong>{profile.total_trades}</strong>
              <small>
                {profile.active_trades} active · {profile.pending_trades} pending ·{" "}
                {profile.resolved_trades} resolved
              </small>
            </div>

            <div className="overview-info-block">
              <span className="overview-label">Wallet Balance</span>
              <strong>{formatCurrency(profile.wallet_balance)}</strong>
              <small>{shortenWallet(profile.wallet_address)}</small>
            </div>

            <div className="overview-info-block">
              <span className="overview-label">Trusted Traders</span>
              <strong>{profile.trusted_traders.length}</strong>
              <small>{profile.trusted_traders.join(" · ")}</small>
            </div>

            <div className="overview-info-block">
              <span className="overview-label">Insider Signal</span>
              <strong>{profile.insider_stats}</strong>
              <small>{profile.preference_probabilities}</small>
            </div>
          </div>
        </div>

        <div className="trader-overview-side">
          <div className="side-metric-card positive">
            <span>Realized P&amp;L</span>
            <strong>
              {profile.realized_pnl >= 0 ? "+" : "-"}
              {formatCurrency(Math.abs(profile.realized_pnl))}
            </strong>
          </div>

          <div className="side-metric-card negative">
            <span>Unrealized P&amp;L</span>
            <strong>
              {profile.unrealized_pnl >= 0 ? "+" : "-"}
              {formatCurrency(Math.abs(profile.unrealized_pnl))}
            </strong>
          </div>
        </div>
      </section>

      <section className="trader-tabs-box">
        <div className="user-tabs">
          {tabs.map((tab) => (
            <button
              key={tab.key}
              type="button"
              className={`user-tab ${activeTab === tab.key ? "active" : ""}`}
              onClick={() => setActiveTab(tab.key)}
            >
              {tab.label}
            </button>
          ))}
        </div>

        <div className="tab-panel">
          {activeTab === "historical" && (
            <article className="tab-card">
              <p className="card-label">Historical Actions</p>
              <h3>Trading Timeline</h3>
              <p className="tab-copy">{profile.historical_actions}</p>
              <div className="timeline-list">
                {derived.historicalTimeline.map((item) => (
                  <div key={item} className="timeline-item">{item}</div>
                ))}
              </div>
            </article>
          )}

          {activeTab === "wallet" && (
            <div className="tab-grid two-col">
              <article className="tab-card">
                <p className="card-label">Wallet</p>
                <h3>Wallet Overview</h3>
                <div className="info-list">
                  <div><span>Address</span><strong>{profile.wallet_address}</strong></div>
                  <div><span>Balance</span><strong>{formatCurrency(profile.wallet_balance)}</strong></div>
                  <div><span>Transactions</span><strong>{profile.wallet_transactions}</strong></div>
                </div>
              </article>

              <article className="tab-card">
                <p className="card-label">Wallet Activity</p>
                <h3>Transaction Highlights</h3>
                <div className="timeline-list">
                  {derived.walletEvents.map((item) => (
                    <div key={item} className="timeline-item">{item}</div>
                  ))}
                </div>
              </article>
            </div>
          )}

          {activeTab === "trusted" && (
            <article className="tab-card">
              <p className="card-label">Trusted Network</p>
              <h3>Related Traders</h3>
              <div className="pill-cloud">
                {profile.trusted_traders.map((trader) => (
                  <span key={trader} className="network-pill">{trader}</span>
                ))}
              </div>
            </article>
          )}

          {activeTab === "performance" && (
            <div className="tab-grid two-col">
              <article className="tab-card">
                <p className="card-label">Performance</p>
                <h3>Core Metrics</h3>
                <div className="info-list">
                  <div><span>Total P&amp;L</span><strong>{formatCurrency(profile.profit_loss)}</strong></div>
                  <div><span>Realized P&amp;L</span><strong>{formatCurrency(profile.realized_pnl)}</strong></div>
                  <div><span>Unrealized P&amp;L</span><strong>{formatCurrency(profile.unrealized_pnl)}</strong></div>
                  <div><span>Win Rate</span><strong>{formatPercent(profile.win_rate)}</strong></div>
                  <div><span>Avg Trade Size</span><strong>{formatCurrency(derived.avgTradeSize)}</strong></div>
                </div>
              </article>

              <article className="tab-card">
                <p className="card-label">Execution</p>
                <h3>Trade Resolution</h3>
                <div className="info-list">
                  <div><span>Total Trades</span><strong>{profile.total_trades}</strong></div>
                  <div><span>Resolved Rate</span><strong>{formatPercent(derived.resolutionRate)}</strong></div>
                  <div><span>Active Rate</span><strong>{formatPercent(derived.activityRate)}</strong></div>
                  <div><span>Exposure Ratio</span><strong>{formatPercent(derived.exposureRatio)}</strong></div>
                </div>
              </article>
            </div>
          )}

          {activeTab === "patterns" && (
            <article className="tab-card">
              <p className="card-label">Trading Patterns</p>
              <h3>Behavioral Read</h3>
              <div className="pill-cloud">
                {derived.behaviorTags.map((tag) => (
                  <span key={tag} className="network-pill subtle">{tag}</span>
                ))}
              </div>
            </article>
          )}

          {activeTab === "categories" && (
            <article className="tab-card">
              <p className="card-label">Category Breakdown</p>
              <h3>Market Focus</h3>
              <div className="breakdown-list">
                {derived.categoryMix.map((item) => (
                  <div key={item.label} className="breakdown-row">
                    <div className="breakdown-row-top">
                      <span>{item.label}</span>
                      <strong>{item.value}%</strong>
                    </div>
                    <div className="breakdown-bar">
                      <div
                        className="breakdown-bar-fill"
                        style={{ width: `${item.value}%` }}
                      />
                    </div>
                  </div>
                ))}
              </div>
            </article>
          )}

          {activeTab === "insider" && (
            <div className="tab-grid two-col">
              <article className="tab-card">
                <p className="card-label">Insider Risk</p>
                <h3>Risk Classification</h3>
                <div className="info-list">
                  <div><span>Profile</span><strong>{profile.insider_stats}</strong></div>
                  <div><span>Preference Model</span><strong>{profile.preference_probabilities}</strong></div>
                </div>
              </article>

              <article className="tab-card">
                <p className="card-label">Signals</p>
                <h3>Diagnostic Flags</h3>
                <div className="timeline-list">
                  {derived.insiderSignals.map((signal) => (
                    <div key={signal} className="timeline-item">{signal}</div>
                  ))}
                </div>
              </article>
            </div>
          )}
        </div>
      </section>
    </div>
  );
}