import { useCallback } from "react";
import { Link } from "react-router-dom";
import { useApiData } from "../hooks/useApiData";
import { useWatchlist } from "../hooks/useWatchlist";
import { fetchMarketProfile, fetchUserWhaleProfile } from "../lib/api";
import { deriveUserIdentity } from "../lib/userIdentity";
import TopNavbar from "../homepage/TopNavbar";

export default function FollowingPage() {
  const { watchlist, removeUser, removeMarket } = useWatchlist();

  const loadFollowing = useCallback(async () => {
    const [users, markets] = await Promise.all([
      Promise.all(
        watchlist.users.map(async (userId) => {
          try {
            return await fetchUserWhaleProfile(userId);
          } catch {
            return {
              user_id: userId,
              external_user_ref: `User ${userId}`,
              wallet_address: null,
              preferred_username: null,
              display_label: null,
              is_likely_insider: false,
              latest_whale_score: null,
              resolved_performance: {
                resolved_market_count: 0,
                winning_market_count: 0,
                realized_pnl: 0,
                realized_roi: 0,
                excluded_market_count: 0,
                win_rate: null,
              },
              dashboard_profile: null,
            };
          }
        }),
      ),
      Promise.all(
        watchlist.markets.map(async (marketSlug) => {
          try {
            return await fetchMarketProfile(marketSlug);
          } catch {
            return {
              dashboard_id: 0,
              market_id: 0,
              market_contract_id: 0,
              market_slug: marketSlug,
              market_url: null,
              question: marketSlug,
              price: null,
              volume: null,
              odds: null,
              orderbook_depth: null,
              whale_count: 0,
              trusted_whale_count: 0,
              whale_market_focus: null,
              read_time: null,
              realtime_source: "unavailable",
              snapshot_time: null,
              realtime_payload: {},
            };
          }
        }),
      ),
    ]);

    return { users, markets };
  }, [watchlist.markets, watchlist.users]);

  const { data, loading, error } = useApiData(loadFollowing);

  return (
    <div className="page">
      <TopNavbar />

      <section className="hero">
        <p className="eyebrow">Orca Polymarkets</p>
        <h1>Following</h1>
        <p className="hero-text">
          Local watchlist for the traders and markets you want to keep close.
        </p>
      </section>

      {loading && <section className="analytics-section"><div className="status-panel">Loading watchlist...</div></section>}
      {error && <section className="analytics-section"><div className="status-panel error-panel">{error}</div></section>}

      {!loading && !error && (
        <section className="analytics-section">
          <div className="analytics-grid following-grid">
            <section className="leaderboard-card">
              <div className="leaderboard-top">
                <p className="leaderboard-kicker">Watchlist</p>
                <h2>Users</h2>
                <p className="leaderboard-count">{watchlist.users.length} followed traders</p>
              </div>
              <div className="watchlist-list">
                {watchlist.users.length === 0 && <div className="status-panel">No followed users yet.</div>}
                {data?.users.map((user) => {
                  const { primary: title, secondary: subtitle } = deriveUserIdentity(user);
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
                        </div>
                      </div>
                      <button type="button" className="watchlist-remove" onClick={() => removeUser(user.user_id)}>
                        Unfollow
                      </button>
                    </article>
                  );
                })}
              </div>
            </section>

            <section className="leaderboard-card">
              <div className="leaderboard-top">
                <p className="leaderboard-kicker">Watchlist</p>
                <h2>Markets</h2>
                <p className="leaderboard-count">{watchlist.markets.length} followed markets</p>
              </div>
              <div className="watchlist-list">
                {watchlist.markets.length === 0 && <div className="status-panel">No followed markets yet.</div>}
                {data?.markets.map((market) => (
                  <article key={market.market_slug} className="watchlist-card">
                    <div className="watchlist-card-main">
                      <p className="watchlist-card-kicker">Market</p>
                      <Link to={`/markets/${market.market_slug}`} className="watchlist-card-title">
                        {market.question}
                      </Link>
                      <p className="watchlist-card-subtitle">{market.market_slug}</p>
                      <div className="leaderboard-meta">
                        <span className="meta-pill">Price {market.price === null ? "--" : `${Math.round(market.price * 100)}%`}</span>
                        <span className="meta-pill">Whales {market.whale_count}</span>
                        <span className="meta-pill">Trusted {market.trusted_whale_count}</span>
                      </div>
                    </div>
                    <button type="button" className="watchlist-remove" onClick={() => removeMarket(market.market_slug)}>
                      Unfollow
                    </button>
                  </article>
                ))}
              </div>
            </section>
          </div>
        </section>
      )}
    </div>
  );
}
