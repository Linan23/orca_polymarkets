import { NavLink, Link } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";
import logo from "../assets/orca.png";
import { prefetchRoute } from "../lib/routePrefetch";

export default function TopNavbar() {
  const { account, isAuthenticated, logout } = useAuth();

  const getLinkClass = ({ isActive }: { isActive: boolean }) =>
    isActive ? "topbar-link active" : "topbar-link";

  return (
    <header className="topbar">
      <div className="topbar-inner">
        <div className="topbar-left">
          <Link to="/" className="topbar-brand" onMouseEnter={() => prefetchRoute("/")} onFocus={() => prefetchRoute("/")}>
            <img src={logo} alt="Orca logo" className="brand-logo" />
          </Link>

          <nav className="topbar-nav">
            <NavLink to="/" end className={getLinkClass} onMouseEnter={() => prefetchRoute("/")} onFocus={() => prefetchRoute("/")}>
              Homepage
            </NavLink>

            <NavLink to="/following" className={getLinkClass} onMouseEnter={() => prefetchRoute("/following")} onFocus={() => prefetchRoute("/following")}>
              Following
            </NavLink>

            <NavLink to="/leaderboard" className={getLinkClass} onMouseEnter={() => prefetchRoute("/leaderboard")} onFocus={() => prefetchRoute("/leaderboard")}>
              Leaderboard
            </NavLink>

            <NavLink to="/definitions" className={getLinkClass} onMouseEnter={() => prefetchRoute("/definitions")} onFocus={() => prefetchRoute("/definitions")}>
              Definitions
            </NavLink>

            <NavLink to="/about" className={getLinkClass} onMouseEnter={() => prefetchRoute("/about")} onFocus={() => prefetchRoute("/about")}>
              About Us
            </NavLink>
          </nav>
        </div>

        {isAuthenticated && account ? (
          <div className="topbar-account">
            <div className="topbar-account-copy">
              <span className="topbar-account-label">Signed in</span>
              <strong>{account.display_name}</strong>
            </div>

            <button
              type="button"
              className="topbar-signout"
              onClick={() => void logout()}
            >
              Sign Out
            </button>
          </div>
        ) : (
          <Link to="/login" className="topbar-signin" onMouseEnter={() => prefetchRoute("/login")} onFocus={() => prefetchRoute("/login")}>
            Sign In
          </Link>
        )}
      </div>
    </header>
  );
}
