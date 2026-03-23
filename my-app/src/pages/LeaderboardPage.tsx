import { useState } from "react";
import MarketLeaderboard from "../homepage/market_leaderboard";
import UserLeaderboard from "../homepage/user_leaderboard";
import TopNavbar from "../homepage/TopNavbar";

export default function LeaderboardPage() {
  const [activeBoard, setActiveBoard] = useState<"market" | "user">("market");
  const [userSearch, setUserSearch] = useState("");
  const [userBoardFilter, setUserBoardFilter] = useState<"all" | "whale" | "trusted">("all");
  const [userPlatformFilter, setUserPlatformFilter] = useState<"all" | "polymarket">("all");
  const [userMinTrades, setUserMinTrades] = useState(0);
  const [userSortBy, setUserSortBy] = useState<"trust" | "profitability" | "trades">("trust");
  const [marketSearch, setMarketSearch] = useState("");
  const [marketMinWhales, setMarketMinWhales] = useState(0);
  const [marketSortBy, setMarketSortBy] = useState<"trusted" | "whales" | "volume">("trusted");

  return (
    <div className="page">
      <TopNavbar />

      <section className="hero">
        <p className="eyebrow">Orca Polymarkets</p>
        <h1>Leaderboard</h1>
        <p className="hero-text">
          Browse market concentration across platforms and user-level whale rankings from Polymarket.
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

        {activeBoard === "user" && (
          <div className="leaderboard-filters">
            <label className="filter-field filter-field-search">
              <span>Search wallet</span>
              <input
                value={userSearch}
                onChange={(event) => setUserSearch(event.target.value)}
                placeholder="0x..."
                type="search"
              />
            </label>

            <label className="filter-field">
              <span>Board</span>
              <select value={userBoardFilter} onChange={(event) => setUserBoardFilter(event.target.value as "all" | "whale" | "trusted")}>
                <option value="all">All candidates</option>
                <option value="whale">Whales only</option>
                <option value="trusted">Trusted only</option>
              </select>
            </label>

            <label className="filter-field">
              <span>Platform</span>
              <select
                value={userPlatformFilter}
                onChange={(event) => setUserPlatformFilter(event.target.value as "all" | "polymarket")}
              >
                <option value="all">All scored users</option>
                <option value="polymarket">Polymarket</option>
              </select>
            </label>

            <label className="filter-field">
              <span>Min trades</span>
              <input
                min={0}
                step={1}
                type="number"
                value={userMinTrades}
                onChange={(event) => setUserMinTrades(Number(event.target.value || 0))}
              />
            </label>

            <label className="filter-field">
              <span>Sort by</span>
              <select
                value={userSortBy}
                onChange={(event) => setUserSortBy(event.target.value as "trust" | "profitability" | "trades")}
              >
                <option value="trust">Trust score</option>
                <option value="profitability">Profitability</option>
                <option value="trades">Trade count</option>
              </select>
            </label>
          </div>
        )}

        {activeBoard === "market" && (
          <div className="leaderboard-filters">
            <label className="filter-field filter-field-search">
              <span>Search market</span>
              <input
                value={marketSearch}
                onChange={(event) => setMarketSearch(event.target.value)}
                placeholder="title or slug"
                type="search"
              />
            </label>

            <label className="filter-field">
              <span>Min whales</span>
              <input
                min={0}
                step={1}
                type="number"
                value={marketMinWhales}
                onChange={(event) => setMarketMinWhales(Number(event.target.value || 0))}
              />
            </label>

            <label className="filter-field">
              <span>Sort by</span>
              <select
                value={marketSortBy}
                onChange={(event) => setMarketSortBy(event.target.value as "trusted" | "whales" | "volume")}
              >
                <option value="trusted">Trusted whales</option>
                <option value="whales">Total whales</option>
                <option value="volume">Volume</option>
              </select>
            </label>
          </div>
        )}
      </section>

      <main className="single-leaderboard-wrap">
        {activeBoard === "market" && (
          <MarketLeaderboard
            search={marketSearch}
            minWhaleCount={marketMinWhales}
            sortBy={marketSortBy}
          />
        )}
        {activeBoard === "user" && (
          <UserLeaderboard
            search={userSearch}
            boardFilter={userBoardFilter}
            platformFilter={userPlatformFilter}
            minTradeCount={userMinTrades}
            sortBy={userSortBy}
          />
        )}
      </main>
    </div>
  );
}
