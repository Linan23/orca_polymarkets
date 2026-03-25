import { useState } from "react";
import { useAuth } from "../auth/AuthContext";
import MarketLeaderboard from "../homepage/market_leaderboard";
import UserLeaderboard from "../homepage/user_leaderboard";
import type { LeaderboardUserBoardFilter } from "../lib/api";
import TopNavbar from "../homepage/TopNavbar";

export default function LeaderboardPage() {
  const { isAuthenticated, preferences, updatePreferences } = useAuth();
  const [localActiveBoard, setLocalActiveBoard] = useState<"market" | "user">("market");
  const [userSearch, setUserSearch] = useState("");
  const [localUserBoardFilter, setLocalUserBoardFilter] = useState<LeaderboardUserBoardFilter>("all");
  const [localUserPlatformFilter, setLocalUserPlatformFilter] = useState<"all" | "polymarket">("all");
  const [localUserMinTrades, setLocalUserMinTrades] = useState(0);
  const [localUserSortBy, setLocalUserSortBy] = useState<"trust" | "profitability" | "trades">("trust");
  const [marketSearch, setMarketSearch] = useState("");
  const [localMarketMinWhales, setLocalMarketMinWhales] = useState(0);
  const [localMarketSortBy, setLocalMarketSortBy] = useState<"trusted" | "whales" | "volume">("trusted");
  const activeBoard = isAuthenticated ? preferences.leaderboard.active_board : localActiveBoard;
  const userBoardFilter = isAuthenticated ? preferences.leaderboard.user_filters.board : localUserBoardFilter;
  const userPlatformFilter = isAuthenticated ? preferences.leaderboard.user_filters.platform : localUserPlatformFilter;
  const userMinTrades = isAuthenticated ? preferences.leaderboard.user_filters.min_trades : localUserMinTrades;
  const userSortBy = isAuthenticated ? preferences.leaderboard.user_filters.sort : localUserSortBy;
  const marketMinWhales = isAuthenticated ? preferences.leaderboard.market_filters.min_whales : localMarketMinWhales;
  const marketSortBy = isAuthenticated ? preferences.leaderboard.market_filters.sort : localMarketSortBy;

  function patchLeaderboardPreferences(
    patch: Parameters<typeof updatePreferences>[0]["leaderboard"],
  ) {
    if (!isAuthenticated) return;
    void updatePreferences({ leaderboard: patch });
  }

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
            onClick={() => {
              if (isAuthenticated) {
                patchLeaderboardPreferences({ active_board: "market" });
                return;
              }
              setLocalActiveBoard("market");
            }}
          >
            Market Leaderboard
          </button>

          <button
            type="button"
            className={`leaderboard-toggle-btn ${
              activeBoard === "user" ? "active" : ""
            }`}
            onClick={() => {
              if (isAuthenticated) {
                patchLeaderboardPreferences({ active_board: "user" });
                return;
              }
              setLocalActiveBoard("user");
            }}
          >
            User Leaderboard
          </button>
        </div>

        {activeBoard === "user" && (
          <div className="leaderboard-filters">
            <label className="filter-field filter-field-search">
              <span>Search trader</span>
              <input
                value={userSearch}
                onChange={(event) => setUserSearch(event.target.value)}
                placeholder="username or wallet"
                type="search"
              />
            </label>

            <label className="filter-field">
              <span>Trader Tier</span>
              <select
                value={userBoardFilter}
                onChange={(event) => {
                  const value = event.target.value as LeaderboardUserBoardFilter;
                  if (isAuthenticated) {
                    patchLeaderboardPreferences({ user_filters: { board: value } });
                    return;
                  }
                  setLocalUserBoardFilter(value);
                }}
              >
                <option value="all">All scored traders</option>
                <option value="trusted">Trusted whales</option>
                <option value="whale">Whales</option>
                <option value="potential">Potential whales</option>
                <option value="standard">Standard traders</option>
              </select>
            </label>

            <label className="filter-field">
              <span>Platform</span>
              <select
                value={userPlatformFilter}
                onChange={(event) => {
                  const value = event.target.value as "all" | "polymarket";
                  if (isAuthenticated) {
                    patchLeaderboardPreferences({ user_filters: { platform: value } });
                    return;
                  }
                  setLocalUserPlatformFilter(value);
                }}
              >
                <option value="all">All scored traders</option>
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
                onChange={(event) => {
                  const value = Number(event.target.value || 0);
                  if (isAuthenticated) {
                    patchLeaderboardPreferences({ user_filters: { min_trades: value } });
                    return;
                  }
                  setLocalUserMinTrades(value);
                }}
              />
            </label>

            <label className="filter-field">
              <span>Sort by</span>
              <select
                value={userSortBy}
                onChange={(event) => {
                  const value = event.target.value as "trust" | "profitability" | "trades";
                  if (isAuthenticated) {
                    patchLeaderboardPreferences({ user_filters: { sort: value } });
                    return;
                  }
                  setLocalUserSortBy(value);
                }}
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
              <span>Min whale traders</span>
              <input
                min={0}
                step={1}
                type="number"
                value={marketMinWhales}
                onChange={(event) => {
                  const value = Number(event.target.value || 0);
                  if (isAuthenticated) {
                    patchLeaderboardPreferences({ market_filters: { min_whales: value } });
                    return;
                  }
                  setLocalMarketMinWhales(value);
                }}
              />
            </label>

            <label className="filter-field">
              <span>Sort by</span>
              <select
                value={marketSortBy}
                onChange={(event) => {
                  const value = event.target.value as "trusted" | "whales" | "volume";
                  if (isAuthenticated) {
                    patchLeaderboardPreferences({ market_filters: { sort: value } });
                    return;
                  }
                  setLocalMarketSortBy(value);
                }}
              >
                <option value="trusted">Trusted whales</option>
                <option value="whales">Whale traders</option>
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
