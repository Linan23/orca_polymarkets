import "./App.css";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import MarketLeaderboard from "./homepage/market_leaderboard";
import UserLeaderboard from "./homepage/user_leaderboard";
import MarketProfile from "./pages/MarketProfile";
import UserProfile from "./pages/UserProfile";

function DashboardHome() {
  return (
    <div className="page">
      <header className="hero">
        <p className="eyebrow">Orca Polymarkets</p>
        <h1>Orca Dashboard</h1>
        <p className="hero-text">
          Front-page view of market and user leaderboards.
        </p>
      </header>

      <main className="leaderboard-grid">
        <MarketLeaderboard />
        <UserLeaderboard />
      </main>
    </div>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<DashboardHome />} />
        <Route path="/markets/:marketId" element={<MarketProfile />} />
        <Route path="/users/:userId" element={<UserProfile />} />
      </Routes>
    </BrowserRouter>
  );
}