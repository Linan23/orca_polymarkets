import { Link } from "react-router-dom";

type MarketRow = {
  market_section_id: string;
  market_slug: string;
  market_url: string;
  price: number;
  volume: number;
  odds: number;
  whale_count: number;
  trusted_whale_count: number;
  Orderbook_depth: number;
  whale_market_focus: string;
};

const marketData: MarketRow[] = Array.from({ length: 10 }, (_, i) => ({
  market_section_id: `market-${i + 1}`,
  market_slug: `market-${i + 1}`,
  market_url: `/markets/market-${i + 1}`,
  price: Number((0.45 + i * 0.02).toFixed(2)),
  volume: 50000 + i * 10000,
  odds: 50 + i * 2,
  whale_count: 3 + i,
  trusted_whale_count: 1 + Math.floor(i / 2),
  Orderbook_depth: 12000 + i * 2000,
  whale_market_focus: `Focus tag ${i + 1}`,
}));

function getRankClass(rank: number) {
  if (rank === 1) return "gold";
  if (rank === 2) return "silver";
  if (rank === 3) return "bronze";
  return "default";
}

export default function MarketLeaderboard() {
  return (
    <section className="leaderboard-card">
      <div className="leaderboard-top">
        <p className="leaderboard-kicker">Top 10</p>
        <h2>Market Leaderboard</h2>
      </div>

      <div className="leaderboard-list">
        {marketData.map((market, index) => {
          const rank = index + 1;

          return (
            <Link
              key={market.market_section_id}
              to={market.market_url}
              className="leaderboard-row"
            >
              <div className={`leaderboard-rank ${getRankClass(rank)}`}>
                {rank}
              </div>

              <div className="leaderboard-main">
                <div className="leaderboard-main-top">
                  <div>
                    <div className="leaderboard-name">{market.market_slug}</div>
                    <div className="leaderboard-subtext">
                      {market.whale_market_focus}
                    </div>
                  </div>

                  <div className="leaderboard-price">${market.price}</div>
                </div>

                <div className="leaderboard-meta">
                  <span className="meta-pill">
                    Vol {market.volume.toLocaleString()}
                  </span>
                  <span className="meta-pill">Odds {market.odds}%</span>
                  <span className="meta-pill">Whales {market.whale_count}</span>
                  <span className="meta-pill">
                    Trusted {market.trusted_whale_count}
                  </span>
                  <span className="meta-pill">
                    Depth {market.Orderbook_depth.toLocaleString()}
                  </span>
                </div>
              </div>
            </Link>
          );
        })}
      </div>
    </section>
  );
}