import { useCallback, useMemo } from "react";
import { Link } from "react-router-dom";
import {
  fetchLatestWhales,
  type LeaderboardUserBoardFilter,
  type WhaleScoreRow,
} from "../lib/api";
import {
  deriveUserIdentity,
  deriveWhaleTier,
  deriveWhaleTierLabel,
  deriveWhaleTierPillClass,
  matchesUserIdentityQuery,
} from "../lib/userIdentity";
import { useApiData } from "../hooks/useApiData";
import { formatProfitabilityScorePercent, formatTrustScorePercent } from "../lib/scoreFormatting";

function getRankClass(rank: number) {
  if (rank === 1) return "gold";
  if (rank === 2) return "silver";
  if (rank === 3) return "bronze";
  return "default";
}

type UserLeaderboardProps = {
  search: string;
  boardFilter: LeaderboardUserBoardFilter;
  platformFilter: "all" | "polymarket";
  minTradeCount: number;
  sortBy: "trust" | "profitability" | "trades";
};

function UserRows({ items }: { items: WhaleScoreRow[] }) {
  return (
    <div className="leaderboard-list">
      {items.map((user, index) => {
        const rank = index + 1;
        const tier = deriveWhaleTier(user);
        const identity = deriveUserIdentity(user);

        return (
            <div key={`${user.user_id}-${user.external_user_ref}`} className="leaderboard-row">
              <div className={`leaderboard-rank ${getRankClass(rank)}`}>{rank}</div>
              <div className="leaderboard-avatar">
                {tier === "trusted" ? "★" : tier === "whale" ? "◉" : tier === "potential" ? "◎" : "·"}
              </div>

            <div className="leaderboard-main">
              <div className="leaderboard-main-top">
                <div>
                  <Link to={`/users/${user.user_id}`} className="leaderboard-name">
                    {identity.primary}
                  </Link>
                  <div className="leaderboard-subtext">
                    {identity.secondary} · {user.platform_name} · {user.sample_trade_count} scored trades
                  </div>
                </div>

                <div className="leaderboard-score">{formatTrustScorePercent(user.trust_score)}</div>
              </div>

              <div className="leaderboard-meta">
                <span className="meta-pill">Profit {formatProfitabilityScorePercent(user.profitability_score)}</span>
                <span className={`meta-pill ${deriveWhaleTierPillClass(tier)}`}>
                  {deriveWhaleTierLabel(tier)}
                </span>
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

export default function UserLeaderboard({
  search,
  boardFilter,
  platformFilter,
  minTradeCount,
  sortBy,
}: UserLeaderboardProps) {
  const loadWhales = useCallback(
    () => fetchLatestWhales({ limit: 250, tier: boardFilter }),
    [boardFilter],
  );
  const { data, loading, refreshing, error } = useApiData(loadWhales, { keepPreviousData: true });
  const filtered = useMemo(() => {
    if (!data) return [];
    const items = data.filter((user) => {
      const matchesSearch = matchesUserIdentityQuery(user, search);
      const matchesBoard = boardFilter === "all" || boardFilter === deriveWhaleTier(user);
      const matchesPlatform = platformFilter === "all" || user.platform_name === platformFilter;
      const matchesTrades = user.sample_trade_count >= minTradeCount;
      return matchesSearch && matchesBoard && matchesPlatform && matchesTrades;
    });
    items.sort((left, right) => {
      if (sortBy === "profitability") {
        return right.profitability_score - left.profitability_score;
      }
      if (sortBy === "trades") {
        return right.sample_trade_count - left.sample_trade_count;
      }
      return right.trust_score - left.trust_score;
    });
    return items;
  }, [boardFilter, data, minTradeCount, platformFilter, search, sortBy]);

  return (
    <section className="leaderboard-card">
      <div className="leaderboard-top">
        <p className="leaderboard-kicker">Polymarket Whale Scores</p>
        <h2>User Leaderboard</h2>
        <p className="leaderboard-count">User-level whale ranking is currently Polymarket-only.</p>
        {!loading && !error && <p className="leaderboard-count">{filtered.length} matching traders</p>}
        {refreshing && <p className="leaderboard-count">Updating traders...</p>}
      </div>

      {loading && <div className="status-panel">Loading whale leaderboard...</div>}
      {error && <div className="status-panel error-panel">{error}</div>}
      {!loading && !error && filtered.length === 0 && (
        <div className="status-panel">
          {search.trim().length > 0
            ? "No traders match that username or wallet."
            : "No whale scores are available yet."}
        </div>
      )}
      {!loading && !error && filtered.length > 0 && <UserRows items={filtered} />}
    </section>
  );
}
