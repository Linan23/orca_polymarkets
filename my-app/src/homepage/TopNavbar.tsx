import { NavLink, Link } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";

export default function TopNavbar() {
  const { account, isAuthenticated, logout } = useAuth();

  return (
    <header className="topbar">
      <div className="topbar-inner">
        <div className="topbar-left">
          <Link to="/" className="topbar-brand">
            <span className="brand-dot" />
            <span>Orca</span>
          </Link>

          <nav className="topbar-nav">
            <NavLink
              to="/"
              end
              className={({ isActive }) =>
                isActive ? "topbar-link active" : "topbar-link"
              }
            >
              Homepage
            </NavLink>

            <NavLink
              to="/following"
              className={({ isActive }) =>
                isActive ? "topbar-link active" : "topbar-link"
              }
            >
              Following
            </NavLink>

            <NavLink
              to="/leaderboard"
              className={({ isActive }) =>
                isActive ? "topbar-link active" : "topbar-link"
              }
            >
              Leaderboard
            </NavLink>
          </nav>
        </div>

        {isAuthenticated && account ? (
          <div className="topbar-account">
            <div className="topbar-account-copy">
              <span className="topbar-account-label">Signed in</span>
              <strong>{account.display_name}</strong>
            </div>
            <button type="button" className="topbar-signout" onClick={() => void logout()}>
              Sign Out
            </button>
          </div>
        ) : (
          <Link to="/login" className="topbar-signin">
            Sign In
          </Link>
        )}
      </div>
    </header>
  );
}
