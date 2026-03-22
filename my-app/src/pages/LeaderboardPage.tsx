import { useState } from "react";
import MarketLeaderboard from "../homepage/market_leaderboard";
import UserLeaderboard from "../homepage/user_leaderboard";
import TopNavbar from "../homepage/TopNavbar";

export default function LeaderboardPage() {
  const [activeBoard, setActiveBoard] = useState<"market" | "user">("market");

  return (
    <div className="page">
      <TopNavbar />

      <section className="hero">
        <p className="eyebrow">Orca Polymarkets</p>
        <h1>Leaderboard</h1>
        <p className="hero-text">
          Browse the top markets and top users in one place.
        </p>

        <div className="leaderboard-toggle">
          <button
            type="button"
            className={`leaderboard-toggle-btn ${
              activeBoard === "market" ? "active" : ""
            }`}
            onClick={() => setActiveBoard("market")}
          >
            Market Leaderboard
          </button>

          <button
            type="button"
            className={`leaderboard-toggle-btn ${
              activeBoard === "user" ? "active" : ""
            }`}
            onClick={() => setActiveBoard("user")}
          >
            User Leaderboard
          </button>
        </div>
      </section>

      <main className="single-leaderboard-wrap">
        {activeBoard === "market" && <MarketLeaderboard />}
        {activeBoard === "user" && <UserLeaderboard />}
      </main>
    </div>
  );
}