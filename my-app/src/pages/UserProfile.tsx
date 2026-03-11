import { Link, useParams } from "react-router-dom";

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

const userProfiles: UserProfileRow[] = [
  {
    user_profile_section_id: "up-001",
    dashboard_id: "dashboard-001",
    user_id: "FedWillWin",
    market_ref: "market-1",
    historical_actions: "Bought YES, sold partial, re-entered position",
    insider_stats: "Unlikely Insider",
    profit_loss: 1566.52,
    wallet_balance: 3128336.17,
    wallet_transactions: "12 deposits, 33 trades, 5 withdrawals",
    market_invested: 2500,
    trusted_traders: ["Naive Whale", "Moderate Risk Trader"],
    preference_probabilities: "YES-heavy bias in political markets",
    total_volume: 2399119.74,
    total_shares: 3250,
    win_rate: 0,
    leaderboard_rank: 1012,
    wallet_address: "0xfdc07e182e6f959256295567e450a8727272fa79",
    total_trades: 1,
    active_trades: 1,
    pending_trades: 0,
    resolved_trades: 0,
    realized_pnl: 0,
    unrealized_pnl: 1566.52,
    portfolio_value: 3128336.17,
  },
  {
    user_profile_section_id: "up-002",
    dashboard_id: "dashboard-001",
    user_id: "Trader_1001",
    market_ref: "market-2",
    historical_actions: "Scaled into NO positions across 3 markets",
    insider_stats: "Moderate Risk Trader",
    profit_loss: -320.12,
    wallet_balance: 8920.42,
    wallet_transactions: "7 deposits, 21 trades, 2 withdrawals",
    market_invested: 1100,
    trusted_traders: ["Trader_1000", "Trader_1009"],
    preference_probabilities: "Balanced but event-volatility seeking",
    total_volume: 18700,
    total_shares: 1480,
    win_rate: 54,
    leaderboard_rank: 221,
    wallet_address: "0x12ab34cd56ef78ab90cd12ef34ab56cd78ef90ab",
    total_trades: 18,
    active_trades: 3,
    pending_trades: 2,
    resolved_trades: 13,
    realized_pnl: -140.4,
    unrealized_pnl: -179.72,
    portfolio_value: 8920.42,
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

export default function UserProfile() {
  const { userId } = useParams();

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

  const pnlPositive = profile.profit_loss >= 0;

  return (
    <div className="page profile-page">
      <div className="profile-topbar">
        <Link to="/" className="back-link">
          ← Back to dashboard
        </Link>
        <button className="watchlist-button">👁 Add to Watchlist</button>
      </div>

      <section className="profile-hero-card">
        <div className="avatar-circle">◡</div>

        <div className="profile-hero-content">
          <div className="profile-title-row">
            <h1 className="profile-name">{profile.user_id}</h1>
            <span className="leaderboard-rank">
              Leaderboard Rank <strong>#{profile.leaderboard_rank}</strong>
            </span>
          </div>

          <div className="wallet-line">{profile.wallet_address}</div>

          <div className="tag-row">
            <span className="profile-tag muted">
              {profile.trusted_traders[0] ?? "Naive Whale"}
            </span>
            <span className="profile-tag blue">{profile.insider_stats}</span>
            <span className="profile-tag gold">
              {profile.trusted_traders[1] ?? "Moderate Risk Trader"}
            </span>
          </div>
        </div>
      </section>

      <section className="stats-grid">
        <article className="stat-card">
          <div className="stat-label">Total Trades</div>
          <div className="stat-value">{profile.total_trades}</div>
          <div className="stat-subtle">{profile.total_trades} unique trades</div>
          <div className="trade-breakdown">
            <span className="green">{profile.active_trades} Active</span>
            <span className="orange">{profile.pending_trades} Pending</span>
            <span className="blue-text">{profile.resolved_trades} Resolved</span>
          </div>
        </article>

        <article className="stat-card">
          <div className="stat-label">Win Rate</div>
          <div className="stat-value">{formatPercent(profile.win_rate)}</div>
          <div className="stat-subtle">
            {profile.resolved_trades} resolved trades
          </div>
        </article>

        <article className="stat-card">
          <div className="stat-label">Total Volume</div>
          <div className="stat-value">{formatCurrency(profile.total_volume)}</div>
          <div className="stat-subtle">
            Avg: {formatCurrency(profile.total_volume || 0)}
          </div>
        </article>

        <article className="stat-card">
          <div className="stat-label">Total P&amp;L</div>
          <div className={`stat-value ${pnlPositive ? "profit" : "loss"}`}>
            {pnlPositive ? "+" : "-"}
            {formatCurrency(Math.abs(profile.profit_loss))}
          </div>
          <div className="stat-subtle">
            From {profile.resolved_trades} closed + {profile.active_trades} active
            positions
          </div>

          <div className="pnl-breakdown">
            <div>
              <span>Realized:</span>
              <strong className={profile.realized_pnl >= 0 ? "profit" : "loss"}>
                {profile.realized_pnl >= 0 ? "+" : "-"}
                {formatCurrency(Math.abs(profile.realized_pnl))}
              </strong>
            </div>
            <div>
              <span>Unrealized:</span>
              <strong className={profile.unrealized_pnl >= 0 ? "profit" : "loss"}>
                {profile.unrealized_pnl >= 0 ? "+" : "-"}
                {formatCurrency(Math.abs(profile.unrealized_pnl))}
              </strong>
            </div>
          </div>
        </article>

        <article className="stat-card">
          <div className="stat-label">Portfolio Value</div>
          <div className="stat-value blue-number">
            {formatCurrency(profile.portfolio_value)}
          </div>
          <div className="stat-subtle">
            Wallet balance at snapshot: {formatCurrency(profile.wallet_balance)}
          </div>
        </article>

        <article className="stat-card">
          <div className="stat-label">Market Invested</div>
          <div className="stat-value">{formatCurrency(profile.market_invested)}</div>
          <div className="stat-subtle">Linked market: {profile.market_ref}</div>
        </article>
      </section>

      <section className="detail-grid">
        <article className="detail-card">
          <h3>Historical Actions</h3>
          <p>{profile.historical_actions}</p>
        </article>

        <article className="detail-card">
          <h3>Wallet Transactions</h3>
          <p>{profile.wallet_transactions}</p>
        </article>

        <article className="detail-card">
          <h3>Trusted Traders</h3>
          <p>{profile.trusted_traders.join(", ")}</p>
        </article>

        <article className="detail-card">
          <h3>Preference Probabilities</h3>
          <p>{profile.preference_probabilities}</p>
        </article>
      </section>
    </div>
  );
}