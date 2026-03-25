import { useEffect, useState } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";
import TopNavbar from "../homepage/TopNavbar";

type LoginMode = "signin" | "signup";

export default function LoginPage() {
  const { loading, isAuthenticated, login, signup } = useAuth();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const [mode, setMode] = useState<LoginMode>("signin");
  const [displayName, setDisplayName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const returnTo = searchParams.get("returnTo") || "/following";

  useEffect(() => {
    if (!loading && isAuthenticated) {
      navigate(returnTo, { replace: true });
    }
  }, [isAuthenticated, loading, navigate, returnTo]);

  async function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);

    if (mode === "signup") {
      if (password !== confirmPassword) {
        setError("Passwords do not match.");
        return;
      }
    }

    setSubmitting(true);
    try {
      if (mode === "signin") {
        await login({ email, password });
      } else {
        await signup({
          display_name: displayName,
          email,
          password,
        });
      }
      navigate(returnTo, { replace: true });
    } catch (submitError) {
      setError(submitError instanceof Error ? submitError.message : "Unable to sign in right now.");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <>
      <TopNavbar />

      <div className="login-page">
        <div className="login-card account-login-card">
          <p className="leaderboard-kicker">Account Access</p>
          <h1>{mode === "signin" ? "Sign In" : "Create Account"}</h1>
          <p className="login-subtext">
            Personal watchlists and saved dashboard preferences live on your account.
          </p>

          <div className="login-mode-toggle" role="tablist" aria-label="Account access mode">
            <button
              type="button"
              className={`login-mode-btn ${mode === "signin" ? "active" : ""}`}
              onClick={() => {
                setMode("signin");
                setError(null);
              }}
            >
              Sign In
            </button>
            <button
              type="button"
              className={`login-mode-btn ${mode === "signup" ? "active" : ""}`}
              onClick={() => {
                setMode("signup");
                setError(null);
              }}
            >
              Create Account
            </button>
          </div>

          <form className="login-form" onSubmit={handleSubmit}>
            {mode === "signup" && (
              <input
                value={displayName}
                onChange={(event) => setDisplayName(event.target.value)}
                type="text"
                placeholder="Display name"
                autoComplete="name"
                required
              />
            )}
            <input
              value={email}
              onChange={(event) => setEmail(event.target.value)}
              type="email"
              placeholder="Email"
              autoComplete="email"
              required
            />
            <input
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              type="password"
              placeholder="Password"
              autoComplete={mode === "signin" ? "current-password" : "new-password"}
              required
            />
            {mode === "signup" && (
              <input
                value={confirmPassword}
                onChange={(event) => setConfirmPassword(event.target.value)}
                type="password"
                placeholder="Confirm password"
                autoComplete="new-password"
                required
              />
            )}

            {error && <div className="login-error">{error}</div>}

            <button type="submit" disabled={submitting || loading}>
              {submitting ? "Saving..." : mode === "signin" ? "Sign In" : "Create Account"}
            </button>
          </form>

          <p className="login-subnote">
            Public research pages stay open. Sign in is only required for following lists and saved preferences.
          </p>

          <Link to="/" className="back-home">
            {"<- Back to homepage"}
          </Link>
        </div>
      </div>
    </>
  );
}
