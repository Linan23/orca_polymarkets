import { useCallback, useMemo } from "react";
import { Link } from "react-router-dom";
import { fetchDashboardMarkets, type DashboardMarketRow } from "../lib/api";
import { useApiData } from "../hooks/useApiData";
import { formatContractPrice, formatOpposingContractPrice } from "../lib/marketFormatting";

function getRankClass(rank: number) {
  if (rank === 1) return "gold";
  if (rank === 2) return "silver";
  if (rank === 3) return "bronze";
  return "default";
}

function formatCurrency(value: number | null) {
  if (value === null) return "--";
  return `$${value.toLocaleString()}`;
}

function formatDepth(value: number | null) {
  if (value === null) return "--";
  return value.toLocaleString();
}

type MarketLeaderboardProps = {
  search: string;
  minWhaleCount: number;
  sortBy: "trusted" | "whales" | "volume";
};

function MarketRows({ items }: { items: DashboardMarketRow[] }) {
  return (
    <div className="leaderboard-list">
      {items.map((market, index) => {
        const rank = index + 1;

        return (
          <Link
            key={`${market.market_id}-${market.market_slug}`}
            to={`/markets/${market.market_slug}`}
            className="leaderboard-row"
          >
            <div className={`leaderboard-rank ${getRankClass(rank)}`}>{rank}</div>

            <div className="leaderboard-main">
              <div className="leaderboard-main-top">
                <div>
                  <div className="leaderboard-name">{market.question}</div>
                  <div className="leaderboard-subtext">
                    {market.whale_market_focus || market.market_slug}
                  </div>
                </div>

                <div className="leaderboard-price">Yes {formatContractPrice(market.price)}</div>
              </div>

              <div className="leaderboard-meta">
                <span className="meta-pill">Vol {formatCurrency(market.volume)}</span>
                <span className="meta-pill">No {formatOpposingContractPrice(market.odds ?? market.price)}</span>
                <span className="meta-pill">Whale Traders {market.whale_count}</span>
                <span className="meta-pill">Trusted Whales {market.trusted_whale_count}</span>
                <span className="meta-pill">Depth {formatDepth(market.orderbook_depth)}</span>
              </div>
            </div>
          </Link>
        );
      })}
    </div>
  );
}

export default function MarketLeaderboard({ search, minWhaleCount, sortBy }: MarketLeaderboardProps) {
  const loadMarkets = useCallback(() => fetchDashboardMarkets(100), []);
  const { data, loading, error } = useApiData(loadMarkets);
  const filtered = useMemo(() => {
    if (!data) return [];
    const normalizedSearch = search.trim().toLowerCase();
    const items = data.filter((market) => {
      const haystack = `${market.question} ${market.market_slug} ${market.whale_market_focus ?? ""}`.toLowerCase();
      const matchesSearch = normalizedSearch.length === 0 || haystack.includes(normalizedSearch);
      const matchesWhales = market.whale_count >= minWhaleCount;
      return matchesSearch && matchesWhales;
    });
    items.sort((left, right) => {
      if (sortBy === "volume") {
        return (right.volume ?? 0) - (left.volume ?? 0);
      }
      if (sortBy === "whales") {
        return right.whale_count - left.whale_count || right.trusted_whale_count - left.trusted_whale_count;
      }
      return right.trusted_whale_count - left.trusted_whale_count || right.whale_count - left.whale_count;
    });
    return items;
  }, [data, minWhaleCount, search, sortBy]);

  return (
    <section className="leaderboard-card">
      <div className="leaderboard-top">
        <p className="leaderboard-kicker">Live Dashboard</p>
        <h2>Market Leaderboard</h2>
        {!loading && !error && <p className="leaderboard-count">{filtered.length} matching markets</p>}
      </div>

      {loading && <div className="status-panel">Loading market leaderboard...</div>}
      {error && <div className="status-panel error-panel">{error}</div>}
      {!loading && !error && filtered.length === 0 && (
        <div className="status-panel">No dashboard markets are available yet.</div>
      )}
      {!loading && !error && filtered.length > 0 && <MarketRows items={filtered} />}
    </section>
  );
}
