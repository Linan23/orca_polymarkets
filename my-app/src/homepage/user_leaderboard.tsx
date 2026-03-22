import { Link } from "react-router-dom";

type UserRow = {
  leaderboard_section_id: string;
  user_identity_ref: string;
  market_ref: string;
  rank: number;
  score_metric: string;
  timeframe: string;
  board_type: "public" | "internal";
};

const userData: UserRow[] = Array.from({ length: 10 }, (_, i) => ({
  leaderboard_section_id: `user-${i + 1}`,
  user_identity_ref: `Trader_${1000 + i}`,
  market_ref: `market-${i + 1}`,
  rank: i + 1,
  score_metric: `${1500 - i * 90} pts`,
  timeframe: "24h",
  board_type: i % 2 === 0 ? "public" : "internal",
}));

function getRankClass(rank: number) {
  if (rank === 1) return "gold";
  if (rank === 2) return "silver";
  if (rank === 3) return "bronze";
  return "default";
}

export default function UserLeaderboard() {
  return (
    <section className="leaderboard-card">
      <div className="leaderboard-top">
        <p className="leaderboard-kicker">Top 10</p>
        <h2>User Leaderboard</h2>
      </div>

      <div className="leaderboard-list">
        {userData.map((user) => (
          <div key={user.leaderboard_section_id} className="leaderboard-row">
            <div className={`leaderboard-rank ${getRankClass(user.rank)}`}>
              {user.rank}
            </div>

            <div className="leaderboard-avatar">👤</div>

            <div className="leaderboard-main">
              <div className="leaderboard-main-top">
                <div>
                  <Link
                    to={`/users/${user.user_identity_ref}`}
                    className="leaderboard-name"
                  >
                    {user.user_identity_ref}
                  </Link>

                  <div className="leaderboard-subtext">
                    Market{" "}
                    <Link
                      to={`/markets/${user.market_ref}`}
                      className="leaderboard-inline-link"
                    >
                      {user.market_ref}
                    </Link>
                  </div>
                </div>

                <div className="leaderboard-score">{user.score_metric}</div>
              </div>

              <div className="leaderboard-meta">
                <span className="meta-pill">{user.timeframe}</span>

                <span
                  className={
                    user.board_type === "public"
                      ? "meta-pill public-pill"
                      : "meta-pill internal-pill"
                  }
                >
                  {user.board_type}
                </span>
              </div>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}