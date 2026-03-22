import { Link } from "react-router-dom";
import TopNavbar from "../homepage/TopNavbar";

export default function LoginPage() {
  return (
    <>
      <TopNavbar />

      <div className="login-page">
        <div className="login-card">
          <h1>Sign In</h1>
          <p className="login-subtext">
            Access your dashboard, watchlist, and leaderboards.
          </p>

          <form className="login-form">
            <input type="email" placeholder="Email" />
            <input type="password" placeholder="Password" />
            <button type="submit">Login</button>
          </form>

          <Link to="/" className="back-home">
            ← Back to homepage
          </Link>
        </div>
      </div>
    </>
  );
}