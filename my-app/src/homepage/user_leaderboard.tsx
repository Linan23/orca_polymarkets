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

export default function UserLeaderboard() {
  return (
    <section className="card">
      <div className="card-header">
        <p className="card-label">Top 10</p>
        <h2>User Leaderboard</h2>
      </div>

      <div className="scroll-area">
        <table className="leaderboard-table">
          <thead>
            <tr>
              <th>Rank</th>
              <th>User</th>
              <th>Market</th>
              <th>Score</th>
              <th>Timeframe</th>
              <th>Board Type</th>
            </tr>
          </thead>
          <tbody>
            {userData.map((user) => (
              <tr key={user.leaderboard_section_id}>
                <td>
                  <span className="rank-badge">{user.rank}</span>
                </td>
                <td>
                  <Link to={`/users/${user.user_identity_ref}`} className="table-link">
                    {user.user_identity_ref}
                  </Link>
                </td>
                <td>
                  <Link to={`/markets/${user.market_ref}`} className="table-link">
                    {user.market_ref}
                  </Link>
                </td>
                <td>{user.score_metric}</td>
                <td>{user.timeframe}</td>
                <td>
                  <span
                    className={
                      user.board_type === "public"
                        ? "status-pill public"
                        : "status-pill internal"
                    }
                  >
                    {user.board_type}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}