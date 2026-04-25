import { Routes, Route, Navigate } from "react-router-dom";
import RequireAuth from "./auth/RequireAuth";
import HomePage from "./pages/HomePage";
import LoginPage from "./pages/LoginPage";
import MarketProfile from "./pages/MarketProfile";
import UserProfile from "./pages/UserProfile";
import FollowingPage from "./pages/FollowingPage";
import LeaderboardPage from "./pages/LeaderboardPage";
import DefinitionsPage from "./pages/DefinitionsPage";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<HomePage />} />
      <Route path="/analytics" element={<Navigate to="/" replace />} />

      <Route
        path="/following"
        element={
          <RequireAuth>
            <FollowingPage />
          </RequireAuth>
        }
      />

      <Route path="/leaderboard" element={<LeaderboardPage />} />
      <Route path="/definitions" element={<DefinitionsPage />} />
      <Route path="/login" element={<LoginPage />} />
      <Route path="/markets/:marketId" element={<MarketProfile />} />
      <Route path="/users/:userId" element={<UserProfile />} />
    </Routes>
  );
}
