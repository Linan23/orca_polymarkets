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

export default function MarketLeaderboard() {
  return (
    <section className="card">
      <div className="card-header">
        <p className="card-label">Top 10</p>
        <h2>Market Leaderboard</h2>
      </div>

      <div className="scroll-area">
        <table className="leaderboard-table">
          <thead>
            <tr>
              <th>#</th>
              <th>Market</th>
              <th>Price</th>
              <th>Volume</th>
              <th>Odds</th>
              <th>Whales</th>
              <th>Trusted</th>
              <th>Depth</th>
            </tr>
          </thead>
          <tbody>
            {marketData.map((market, index) => (
              <tr key={market.market_section_id}>
                <td>
                  <span className="rank-badge">{index + 1}</span>
                </td>
                <td>
                  <div className="primary-cell">
                    <Link to={market.market_url} className="table-link">
                      {market.market_slug}
                    </Link>
                    <span className="subtext">{market.whale_market_focus}</span>
                  </div>
                </td>
                <td>${market.price}</td>
                <td>{market.volume.toLocaleString()}</td>
                <td>{market.odds}%</td>
                <td>{market.whale_count}</td>
                <td>{market.trusted_whale_count}</td>
                <td>{market.Orderbook_depth.toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}