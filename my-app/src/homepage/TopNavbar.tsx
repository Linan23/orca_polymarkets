import { NavLink, Link } from "react-router-dom";

export default function TopNavbar() {
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

            <NavLink
              to="/analytics"
              className={({ isActive }) =>
                isActive ? "topbar-link active" : "topbar-link"
              }
            >
              Analytics
            </NavLink>
          </nav>
        </div>

        <Link to="/login" className="topbar-signin">
          Sign In
        </Link>
      </div>
    </header>
  );
}
